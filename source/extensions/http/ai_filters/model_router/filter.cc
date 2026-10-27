#include "source/extensions/http/ai_filters/model_router/filter.h"

#include "envoy/data/ai/v3/token_usage.pb.h"

#include "source/common/coroutine/status_macros.h"
#include "source/common/http/utility.h"
#include "source/common/protobuf/utility.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/http/ai_filters/model_router/database_interface.h"
#include "source/extensions/http/ai_filters/model_router/route_wrapper.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;
using HttpFilters::AiProtocolManager::stringFromFilterState;

absl::StatusOr<std::shared_ptr<ModelRouterFilterConfig>> ModelRouterFilterConfig::create(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
    Stats::Scope& scope, DbPtr tl_database) {
  absl::StatusOr<ModelMap> model_map = buildModelMap(proto);

  if (!model_map.ok()) {
    return model_map.status();
  }

  return std::make_shared<ModelRouterFilterConfig>(std::move(model_map).value(), scope,
                                                   std::move(tl_database));
}

ModelRouterFilterConfig::ModelRouterFilterConfig(ModelMap&& routes, Stats::Scope& scope,
                                                 DbPtr tl_database)
    : routes_(std::move(routes)),
      stats_(ModelRouterFilterStats{ALL_MODEL_ROUTER_FILTER_STATS(
          POOL_COUNTER_PREFIX(scope, "ai_protocol_manager.model_router."))}),
      tl_database_(std::move(tl_database)) {}

absl::StatusOr<ModelRouterFilterConfig::ModelMap> ModelRouterFilterConfig::buildModelMap(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto) {
  namespace Config = envoy::extensions::http::ai_filters::model_router::v3;

  absl::flat_hash_map<std::string, Config::Provider> providers;
  for (const Config::Provider& entry : proto.providers()) {
    providers[entry.name()] = entry;
  }

  absl::flat_hash_map<std::string, Config::Model> models;
  for (const Config::Model& entry : proto.models()) {
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

Coroutine::Task<absl::Status>
ModelRouterFilter::decode(HttpFilters::AiProtocolManager::AiRequestReceiver receive_request,
                          HttpFilters::AiProtocolManager::AiRequestPropagator propagate_request,
                          HttpFilters::AiProtocolManager::LocalReplier) {
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

  requested_model_name_ = *model_name;

  filter_state->setData(FilterStateKeys::ResolvedRoutes,
                        std::make_shared<LlmRoutesFilterStateObject>(it->second),
                        StreamInfo::FilterState::LifeSpan::Request);

  config_->stats().success_.inc();

  // Temporary fix, until the router and DFP filter support ResolvedRoutes.
  // Assumes that all routes point to the same host.
  if (!it->second.routes().empty()) {
    Envoy::Http::Utility::updateAuthority(context_.request_headers,
                                          it->second.routes(0).host_and_port(), false, false);
    context_.request_headers.setEnvoyMaxRetries(it->second.routes().size() - 1);
  }

  co_return co_await std::move(propagate_request)(std::move(request));
}

Coroutine::Task<absl::Status>
ModelRouterFilter::encodeSSE(HttpFilters::AiProtocolManager::SseStreamReceiver receive_sse,
                             HttpFilters::AiProtocolManager::SseStreamPropagator propagate_sse) {
  // Propagate all events and wait for the end-of-response event
  while (true) {
    ASSIGN_OR_CO_RETURN(std::optional<HttpFilters::AiProtocolManager::SseEventPtr> event,
                        co_await receive_sse());
    if (!event.has_value()) {
      break;
    }
    CO_RETURN_IF_ERROR(co_await propagate_sse(std::move(*event)));
  }

  absl::Status stat = co_await recordTokenUsage();
  if (!stat.ok()) {
    ENVOY_LOG(warn, "failed to save query stats: {}", stat.message());
  }

  co_return absl::OkStatus();
}

Coroutine::Task<absl::Status> ModelRouterFilter::encodeUnary(
    HttpFilters::AiProtocolManager::AiResponseStreamReceiver receive_response,
    HttpFilters::AiProtocolManager::AiResponseStreamPropagator propagate_response) {
  // Propagate response and wait for the end-of-response event
  while (true) {
    ASSIGN_OR_CO_RETURN(std::vector<HttpFilters::AiProtocolManager::FlattenJsonField> fields,
                        co_await receive_response());
    if (fields.empty()) {
      break;
    }
    CO_RETURN_IF_ERROR(co_await propagate_response(std::move(fields)));
  }

  absl::Status stat = co_await recordTokenUsage();
  if (!stat.ok()) {
    ENVOY_LOG(warn, "failed to save query stats: {}", stat.message());
  }

  co_return absl::OkStatus();
}

Coroutine::Task<absl::Status> ModelRouterFilter::recordTokenUsage() const {
  DatabaseInterface* db = config_->database();
  if (!db) {
    co_return absl::OkStatus();
  }

  if (requested_model_name_.empty()) {
    co_return absl::OkStatus();
  }

  auto routes_it = config_->routes().find(requested_model_name_);
  if (routes_it == config_->routes().end()) {
    IS_ENVOY_BUG("AI route disappeared during request processing");
    co_return absl::InternalError("AI route disappeared during request processing");
  }

  std::optional<uint32_t> attempt = context_.stream_info.attemptCount();
  if (!attempt.has_value()) {
    // Request never went upstream
    co_return absl::OkStatus();
  }

  int attempt_index = attempt.value() - 1;
  if (attempt_index < 0 || attempt_index >= routes_it->second.routes().size()) {
    co_return absl::InternalError("invalid attempt count");
  }

  const envoy::type::ai::v3::LlmRoute& route = routes_it->second.routes(attempt_index);

  const auto& typed_metadata = context_.stream_info.dynamicMetadata().typed_filter_metadata();
  const auto it = typed_metadata.find("envoy.ai.token_usage");
  if (it == typed_metadata.end()) {
    co_return absl::OkStatus();
  }

  envoy::data::ai::v3::TokenUsage token_usage;
  CO_RETURN_IF_ERROR(MessageUtil::unpackTo(it->second, token_usage));
  if (token_usage.extraction_status() == envoy::data::ai::v3::TokenUsage::FAILED) {
    // We have no useful data.
    co_return absl::OkStatus();
  }

  std::string request_id(context_.request_headers.getRequestIdValue());

  // Consider detaching this coroutine via Coroutine::launch().
  // This will allow filter to return end-of-response condition without
  // waiting for database write to complete.
  CO_RETURN_IF_ERROR(co_await db->recordQueryStats(
      context_.stream_info.startTime(), token_usage.input_tokens().value(),
      token_usage.output_tokens().value(), route.model(), route.provider(), std::move(request_id)));

  co_return absl::OkStatus();
}

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
