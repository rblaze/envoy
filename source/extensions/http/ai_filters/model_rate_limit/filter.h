#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/thread_local/thread_local.h"

#include "source/common/common/logger.h"
#include "source/common/coroutine/task.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/http/ai_filters/model_rate_limit/quota_interface.h"

#include "absl/status/status.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

// `success`: model passed quota checks.
// `quota_exceeded`: model exceeded its configured quotas.
// `model_or_provider_missing`: the model or provider name isn't present in filter state.
// `backend_error`: quota backend returned an error during check or consumption.
#define ALL_MODEL_RATE_LIMIT_FILTER_STATS(COUNTER)                                                 \
  COUNTER(success)                                                                                 \
  COUNTER(quota_exceeded)                                                                          \
  COUNTER(model_or_provider_missing)                                                               \
  COUNTER(backend_error)

struct ModelRateLimitFilterStats {
  ALL_MODEL_RATE_LIMIT_FILTER_STATS(GENERATE_COUNTER_STRUCT)
};

class ModelRateLimitFilterConfig {
public:
  using QuotaPtr = ThreadLocal::TypedSlotPtr<QuotaInterface>;

  ModelRateLimitFilterConfig(QuotaPtr tl_quota, Stats::Scope& scope);

  const ModelRateLimitFilterStats& stats() const { return stats_; }
  QuotaInterface* quotas() const { return tl_quota_->get().ptr(); }

private:
  QuotaPtr tl_quota_;
  const ModelRateLimitFilterStats stats_;
};
using ModelRateLimitFilterConfigSharedPtr = std::shared_ptr<const ModelRateLimitFilterConfig>;

class ModelRateLimitFilter : public HttpFilters::AiProtocolManager::AiFilter,
                             public Logger::Loggable<Logger::Id::ai_protocol_manager> {
public:
  ModelRateLimitFilter(ModelRateLimitFilterConfigSharedPtr config,
                       const HttpFilters::AiProtocolManager::AiFilterContext& context);

  // HttpFilters::AiProtocolManager::AiFilter
  Coroutine::Task<absl::Status>
  decode(HttpFilters::AiProtocolManager::AiRequestReceiver receive_request,
         HttpFilters::AiProtocolManager::AiRequestPropagator propagate_request,
         HttpFilters::AiProtocolManager::LocalReplier reply_locally) override;

  Coroutine::Task<absl::Status>
  encodeSSE(HttpFilters::AiProtocolManager::SseStreamReceiver receive_sse,
            HttpFilters::AiProtocolManager::SseStreamPropagator propagate_sse) override;

  Coroutine::Task<absl::Status> encodeUnary(
      HttpFilters::AiProtocolManager::AiResponseStreamReceiver receive_response,
      HttpFilters::AiProtocolManager::AiResponseStreamPropagator propagate_response) override;

private:
  uint64_t getEstimatedInputTokens() const;
  Coroutine::Task<absl::Status> consumeQuota() const;

  ModelRateLimitFilterConfigSharedPtr config_;
  HttpFilters::AiProtocolManager::AiFilterContext context_;
  std::string provider_;
  std::string model_;
  uint64_t estimated_input_tokens_;
};

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
