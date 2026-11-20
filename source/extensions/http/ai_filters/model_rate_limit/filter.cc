#include "source/extensions/http/ai_filters/model_rate_limit/filter.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "envoy/data/ai/v3/request_info.pb.h"
#include "envoy/data/ai/v3/token_usage.pb.h"

#include "source/common/coroutine/status_macros.h"
#include "source/common/protobuf/utility.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;
using HttpFilters::AiProtocolManager::stringFromFilterState;

namespace {

constexpr absl::string_view DefaultRequestInfoMetadataNamespace{"envoy.ai.request_info"};
constexpr absl::string_view DefaultTokenUsageMetadataNamespace{"envoy.ai.token_usage"};

} // namespace

ModelRateLimitFilterConfig::ModelRateLimitFilterConfig(
    QuotaPtr tl_quota, Stats::Scope& scope, absl::string_view request_info_metadata_namespace,
    absl::string_view token_usage_metadata_namespace)
    : tl_quota_(std::move(tl_quota)),
      stats_(ModelRateLimitFilterStats{ALL_MODEL_RATE_LIMIT_FILTER_STATS(
          POOL_COUNTER_PREFIX(scope, "ai_protocol_manager.model_rate_limit."))}),
      request_info_metadata_namespace_(request_info_metadata_namespace.empty()
                                           ? std::string(DefaultRequestInfoMetadataNamespace)
                                           : std::string(request_info_metadata_namespace)),
      token_usage_metadata_namespace_(token_usage_metadata_namespace.empty()
                                          ? std::string(DefaultTokenUsageMetadataNamespace)
                                          : std::string(token_usage_metadata_namespace)) {}

ModelRateLimitFilter::ModelRateLimitFilter(
    ModelRateLimitFilterConfigSharedPtr config,
    const HttpFilters::AiProtocolManager::AiFilterContext& context)
    : config_(std::move(config)), context_(context) {}

uint64_t ModelRateLimitFilter::getEstimatedInputTokens() const {
  const auto& typed_metadata = context_.stream_info.dynamicMetadata().typed_filter_metadata();
  const auto it = typed_metadata.find(config_->requestInfoMetadataNamespace());
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

Coroutine::Task<absl::Status>
ModelRateLimitFilter::decode(HttpFilters::AiProtocolManager::AiRequestReceiver receive_request,
                             HttpFilters::AiProtocolManager::AiRequestPropagator propagate_request,
                             HttpFilters::AiProtocolManager::LocalReplier reply_locally) {
  ASSIGN_OR_CO_RETURN(HttpFilters::AiProtocolManager::AiRequestPtr request,
                      co_await std::move(receive_request)());

  const StreamInfo::FilterStateSharedPtr& filter_state = context_.stream_info.filterState();

  // Model and provider are set by RouteSelector filter (or an earlier filter state setter).
  model_ = stringFromFilterState(*filter_state, FilterStateKeys::ModelResolved).value_or("");
  provider_ = stringFromFilterState(*filter_state, FilterStateKeys::Provider).value_or("");

  if (model_.empty() || provider_.empty()) {
    std::move(reply_locally)(Http::Code::InternalServerError,
                             "Model or provider name not found in filter state");
    config_->stats().model_or_provider_missing_.inc();
    co_return absl::OkStatus();
  }

  estimated_input_tokens_ = getEstimatedInputTokens();
  QuotaInterface* quotas = config_->quotas();
  absl::StatusOr<bool> allowed =
      co_await quotas->checkLimits(provider_, model_, estimated_input_tokens_);

  if (!allowed.ok()) {
    if (absl::IsCancelled(allowed.status())) {
      co_return allowed.status();
    }
    ENVOY_LOG(warn, "failed to check model quota: {}", allowed.status().message());
    config_->stats().backend_error_.inc();
    co_return co_await std::move(propagate_request)(std::move(request));
  }

  if (!*allowed) {
    std::move(reply_locally)(Http::Code::TooManyRequests, "Model quota exceeded");
    config_->stats().quota_exceeded_.inc();
    co_return absl::OkStatus();
  }

  config_->stats().success_.inc();

  co_return co_await std::move(propagate_request)(std::move(request));
}

Coroutine::Task<absl::Status> ModelRateLimitFilter::consumeQuota() const {
  uint64_t input_tokens = estimated_input_tokens_;
  uint64_t output_tokens = 0;

  const auto& typed_metadata = context_.stream_info.dynamicMetadata().typed_filter_metadata();
  const auto it = typed_metadata.find(config_->tokenUsageMetadataNamespace());
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

  QuotaInterface* quotas = config_->quotas();
  const absl::Status status =
      co_await quotas->consumeQuota(provider_, model_, input_tokens, output_tokens);
  if (absl::IsCancelled(status)) {
    co_return status;
  }
  if (!status.ok()) {
    ENVOY_LOG(warn, "failed to consume model quota: {}", status.message());
    config_->stats().backend_error_.inc();
  }

  co_return absl::OkStatus();
}

Coroutine::Task<absl::Status>
ModelRateLimitFilter::encodeSSE(HttpFilters::AiProtocolManager::SseStreamReceiver receive_sse,
                                HttpFilters::AiProtocolManager::SseStreamPropagator propagate_sse) {
  if (model_.empty() || provider_.empty()) {
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

Coroutine::Task<absl::Status> ModelRateLimitFilter::encodeUnary(
    HttpFilters::AiProtocolManager::AiResponseStreamReceiver receive_response,
    HttpFilters::AiProtocolManager::AiResponseStreamPropagator propagate_response) {
  if (model_.empty() || provider_.empty()) {
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

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
