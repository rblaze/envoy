#include "source/extensions/http/ai_filters/model_router/filter.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "envoy/data/ai/v3/request_info.pb.h"
#include "envoy/data/ai/v3/token_usage.pb.h"

#include "source/common/coroutine/status_macros.h"
#include "source/common/http/utility.h"
#include "source/common/protobuf/utility.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/http/ai_filters/model_router/route_wrapper.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;
using HttpFilters::AiProtocolManager::stringFromFilterState;

namespace {

constexpr absl::string_view RequestInfoMetadataNamespace{"envoy.ai.request_info"};
constexpr absl::string_view TokenUsageMetadataNamespace{"envoy.ai.token_usage"};

} // namespace

absl::StatusOr<std::shared_ptr<ModelRouterFilterConfig>> ModelRouterFilterConfig::create(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
    Stats::Scope& scope) {
  absl::StatusOr<ModelMap> model_map = buildModelMap(proto);

  if (!model_map.ok()) {
    return model_map.status();
  }

  return std::make_shared<ModelRouterFilterConfig>(std::move(model_map).value(), scope);
}

ModelRouterFilterConfig::ModelRouterFilterConfig(ModelMap&& routes, Stats::Scope& scope)
    : routes_(std::move(routes)),
      stats_(ModelRouterFilterStats{ALL_MODEL_ROUTER_FILTER_STATS(
          POOL_COUNTER_PREFIX(scope, "ai_protocol_manager.model_router."))}) {}

absl::StatusOr<ModelRouterFilterConfig::ModelMap> ModelRouterFilterConfig::buildModelMap(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto) {
  namespace Config = envoy::extensions::http::ai_filters::model_router::v3;

  absl::flat_hash_map<std::string, Config::Provider> providers;
  for (const Config::Provider& entry : proto.providers()) {
    providers[entry.name()] = entry;
  }

  absl::flat_hash_map<std::string, Config::Model> models;
  for (const Config::Model& entry : proto.models()) {
    for (const Config::Quota& quota : entry.quotas()) {
      if (quota.quota_type() != Config::REQUESTS && quota.quota_type() != Config::TOKENS) {
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid quota type for model ", entry.name()));
      }
      if (quota.time_unit() != Config::MINUTE && quota.time_unit() != Config::HOUR &&
          quota.time_unit() != Config::DAY) {
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid quota time unit for model ", entry.name()));
      }
    }
    models[entry.name()] = entry;
  }

  ModelRouterFilterConfig::ModelMap model_map;
  for (const Config::RoutingEntry& entry : proto.routes()) {
    envoy::type::ai::v3::LlmRoutes& routes = model_map[entry.requested_model()];

    for (const Config::RoutingTarget& target : entry.targets()) {
      envoy::type::ai::v3::LlmRoute* route = routes.add_routes();

      auto model_it = models.find(target.model());
      if (model_it == models.end()) {
        return absl::InvalidArgumentError("Unknown model " + target.model());
      }
      auto provider_it = providers.find(model_it->second.provider());
      if (provider_it == providers.end()) {
        return absl::InvalidArgumentError("Unknown provider " + model_it->second.provider());
      }

      route->set_llm_protocol(provider_it->second.llm_protocol());
      route->set_model(model_it->second.model());
      route->set_provider(provider_it->first);
      route->set_host_and_port(provider_it->second.host_and_port());
      route->set_api_key(model_it->second.api_key());

      if (provider_it->second.has_binding()) {
        *route->mutable_binding() = provider_it->second.binding();
      }
    }
  }

  return model_map;
}

ModelRouterFilter::ModelRouterFilter(ModelRouterFilterConfigSharedPtr config,
                                     const HttpFilters::AiProtocolManager::AiFilterContext& context)
    : config_(std::move(config)), context_(context) {}

uint64_t ModelRouterFilter::getEstimatedInputTokens() const {
  const auto& typed_metadata = context_.stream_info.dynamicMetadata().typed_filter_metadata();
  const auto it = typed_metadata.find(RequestInfoMetadataNamespace);
  if (it == typed_metadata.end()) {
    return 0;
  }

  envoy::data::ai::v3::RequestInfo request_info;
  if (!MessageUtil::unpackTo(it->second, request_info).ok() ||
      !request_info.has_estimated_input_tokens()) {
    return 0;
  }

  return request_info.estimated_input_tokens().value();
}

Coroutine::Task<absl::StatusOr<envoy::type::ai::v3::LlmRoutes>>
ModelRouterFilter::filterRoutesByQuota(
    const envoy::type::ai::v3::LlmRoutes& configured_routes) const {
  QuotaInterface* quotas = config_->quotas();
  if (!quotas || configured_routes.routes().empty()) {
    co_return configured_routes;
  }

  const uint64_t estimated_input_tokens = getEstimatedInputTokens();
  envoy::type::ai::v3::LlmRoutes filtered_routes;

  for (const envoy::type::ai::v3::LlmRoute& route : configured_routes.routes()) {
    absl::StatusOr<bool> allowed =
        co_await quotas->checkLimits(route.provider(), route.model(), estimated_input_tokens);
    if (!allowed.ok()) {
      ENVOY_LOG(warn, "failed to check model quota: {}", allowed.status().message());
      *filtered_routes.add_routes() = route;
      continue;
    }

    if (*allowed) {
      *filtered_routes.add_routes() = route;
    }
  }

  co_return filtered_routes;
}

Coroutine::Task<absl::Status>
ModelRouterFilter::decode(HttpFilters::AiProtocolManager::AiRequestReceiver receive_request,
                          HttpFilters::AiProtocolManager::AiRequestPropagator propagate_request,
                          HttpFilters::AiProtocolManager::LocalReplier reply_locally) {
  ASSIGN_OR_CO_RETURN(HttpFilters::AiProtocolManager::AiRequestPtr request,
                      co_await std::move(receive_request)());

  const StreamInfo::FilterStateSharedPtr& filter_state = context_.stream_info.filterState();
  auto model_name = stringFromFilterState(*filter_state, FilterStateKeys::ModelRequest);

  if (!model_name) {
    config_->stats().model_name_missing_.inc();
    co_return co_await std::move(propagate_request)(std::move(request));
  }

  auto it = config_->routes().find(*model_name);
  if (it == config_->routes().end()) {
    config_->stats().route_not_found_.inc();
    co_return co_await std::move(propagate_request)(std::move(request));
  }

  envoy::type::ai::v3::LlmRoutes resolved_routes;
  if (!it->second.routes().empty()) {
    ASSIGN_OR_CO_RETURN(resolved_routes, co_await filterRoutesByQuota(it->second));
    if (resolved_routes.routes().empty()) {
      config_->stats().quota_exceeded_.inc();
      std::move(reply_locally)(Http::Code::TooManyRequests,
                               "Quota exceeded for all candidate models");
      co_return absl::OkStatus();
    }
  }

  filter_state->setData(FilterStateKeys::ResolvedRoutes,
                        std::make_shared<LlmRoutesFilterStateObject>(resolved_routes),
                        StreamInfo::FilterState::LifeSpan::Request);

  config_->stats().success_.inc();

  // Temporary fix, until the router and DFP filter support ResolvedRoutes.
  // Assumes that all routes point to the same host.
  if (!resolved_routes.routes().empty()) {
    Envoy::Http::Utility::updateAuthority(context_.request_headers,
                                          resolved_routes.routes(0).host_and_port(), false, false);
    context_.request_headers.setEnvoyMaxRetries(resolved_routes.routes().size() - 1);
  }

  co_return co_await std::move(propagate_request)(std::move(request));
}

bool ModelRouterFilter::shouldConsumeQuota() const {
  if (!config_->quotas()) {
    return false;
  }
  const std::optional<uint32_t> response_code = context_.stream_info.responseCode();
  if (!response_code.has_value() || *response_code < 200 || *response_code >= 300) {
    return false;
  }
  const auto* routes =
      context_.stream_info.filterState()->getDataReadOnly<LlmRoutesFilterStateObject>(
          FilterStateKeys::ResolvedRoutes);
  return routes != nullptr && !routes->proto().routes().empty();
}

Coroutine::Task<absl::Status> ModelRouterFilter::consumeQuota() const {
  if (!shouldConsumeQuota()) {
    co_return absl::OkStatus();
  }

  QuotaInterface* quotas = config_->quotas();
  const auto* routes =
      context_.stream_info.filterState()->getDataReadOnly<LlmRoutesFilterStateObject>(
          FilterStateKeys::ResolvedRoutes);

  const uint32_t attempt = context_.stream_info.attemptCount().value_or(1);
  if (attempt == 0 || attempt > static_cast<uint32_t>(routes->proto().routes_size())) {
    ENVOY_LOG(warn, "invalid attempt count {} for {} resolved routes when consuming quota", attempt,
              routes->proto().routes_size());
    co_return absl::OkStatus();
  }

  const envoy::type::ai::v3::LlmRoute& route = routes->proto().routes(attempt - 1);

  uint64_t input_tokens = getEstimatedInputTokens();
  uint64_t output_tokens = 0;

  const auto& typed_metadata = context_.stream_info.dynamicMetadata().typed_filter_metadata();
  const auto it = typed_metadata.find(TokenUsageMetadataNamespace);
  if (it != typed_metadata.end()) {
    envoy::data::ai::v3::TokenUsage token_usage;
    if (MessageUtil::unpackTo(it->second, token_usage).ok()) {
      if (token_usage.has_input_tokens()) {
        input_tokens = token_usage.input_tokens().value();
      }
      if (token_usage.has_output_tokens()) {
        output_tokens = token_usage.output_tokens().value();
      }
    }
  }

  const absl::Status status =
      co_await quotas->consumeQuota(route.provider(), route.model(), input_tokens, output_tokens);
  if (!status.ok()) {
    ENVOY_LOG(warn, "failed to consume model quota: {}", status.message());
  }

  co_return absl::OkStatus();
}

Coroutine::Task<absl::Status>
ModelRouterFilter::encodeSSE(HttpFilters::AiProtocolManager::SseStreamReceiver receive_sse,
                             HttpFilters::AiProtocolManager::SseStreamPropagator propagate_sse) {
  if (!shouldConsumeQuota()) {
    co_return absl::OkStatus();
  }

  while (true) {
    ASSIGN_OR_CO_RETURN(std::optional<HttpFilters::AiProtocolManager::SseEventPtr> event,
                        co_await receive_sse());
    if (!event.has_value()) {
      break;
    }
    CO_RETURN_IF_ERROR(co_await propagate_sse(std::move(*event)));
  }

  co_return co_await consumeQuota();
}

Coroutine::Task<absl::Status> ModelRouterFilter::encodeUnary(
    HttpFilters::AiProtocolManager::AiResponseStreamReceiver receive_response,
    HttpFilters::AiProtocolManager::AiResponseStreamPropagator propagate_response) {
  if (!shouldConsumeQuota()) {
    co_return absl::OkStatus();
  }

  while (true) {
    ASSIGN_OR_CO_RETURN(std::vector<HttpFilters::AiProtocolManager::FlattenJsonField> batch,
                        co_await receive_response());
    if (batch.empty()) {
      break;
    }
    CO_RETURN_IF_ERROR(co_await propagate_response(std::move(batch)));
  }

  co_return co_await consumeQuota();
}

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
