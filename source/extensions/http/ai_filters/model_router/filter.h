#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "envoy/extensions/http/ai_filters/model_router/v3/model_router.pb.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/thread_local/thread_local.h"
#include "envoy/type/ai/v3/routes.pb.h"

#include "source/common/common/logger.h"
#include "source/common/coroutine/task.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/http/ai_filters/model_router/quota_interface.h"
#include "source/extensions/http/ai_filters/model_router/route_wrapper.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

#define ALL_MODEL_ROUTER_FILTER_STATS(COUNTER)                                                     \
  COUNTER(success)                                                                                 \
  COUNTER(model_name_missing)                                                                      \
  COUNTER(route_not_found)                                                                         \
  COUNTER(quota_exceeded)

struct ModelRouterFilterStats {
  ALL_MODEL_ROUTER_FILTER_STATS(GENERATE_COUNTER_STRUCT)
};

class ModelRouterFilterConfig {
public:
  using QuotaPtr = ThreadLocal::TypedSlotPtr<QuotaInterface>;

  static absl::StatusOr<std::shared_ptr<ModelRouterFilterConfig>>
  create(const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
         Stats::Scope& scope);

  using ModelMap = absl::flat_hash_map<std::string, envoy::type::ai::v3::LlmRoutes>;

  ModelRouterFilterConfig(ModelMap&& routes, Stats::Scope& scope);
  const ModelMap& routes() const { return routes_; }
  const ModelRouterFilterStats& stats() const { return stats_; }
  QuotaInterface* quotas() const {
    if (!tl_quota_) {
      return nullptr;
    }

    return tl_quota_->get().ptr();
  }
  void setQuotas(QuotaPtr tl_quota) { tl_quota_ = std::move(tl_quota); }

private:
  static absl::StatusOr<ModelMap>
  buildModelMap(const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto);

  const ModelMap routes_;
  const ModelRouterFilterStats stats_;
  QuotaPtr tl_quota_;
};
using ModelRouterFilterConfigSharedPtr = std::shared_ptr<const ModelRouterFilterConfig>;

class ModelRouterFilter : public HttpFilters::AiProtocolManager::AiFilter,
                          public Logger::Loggable<Logger::Id::ai_protocol_manager> {
public:
  ModelRouterFilter(ModelRouterFilterConfigSharedPtr config,
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
  Coroutine::Task<absl::StatusOr<envoy::type::ai::v3::LlmRoutes>>
  filterRoutesByQuota(const envoy::type::ai::v3::LlmRoutes& configured_routes) const;
  bool shouldConsumeQuota() const;
  Coroutine::Task<absl::Status> consumeQuota() const;

  ModelRouterFilterConfigSharedPtr config_;
  HttpFilters::AiProtocolManager::AiFilterContext context_;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
