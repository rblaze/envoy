#pragma once

#include "envoy/extensions/http/ai_filters/model_router/v3/model_router.pb.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/thread_local/thread_local.h"
#include "envoy/type/ai/v3/routes.pb.h"

#include "source/common/common/logger.h"
#include "source/common/coroutine/task.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/http/ai_filters/model_router/database_interface.h"
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
  COUNTER(route_not_found)

struct ModelRouterFilterStats {
  ALL_MODEL_ROUTER_FILTER_STATS(GENERATE_COUNTER_STRUCT)
};

class ModelRouterFilterConfig {
public:
  using DbPtr = ThreadLocal::TypedSlotPtr<DatabaseInterface>;

  static absl::StatusOr<std::shared_ptr<ModelRouterFilterConfig>>
  create(const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
         Stats::Scope& scope, DbPtr tl_database);

  using ModelMap = absl::flat_hash_map<std::string, envoy::type::ai::v3::LlmRoutes>;

  ModelRouterFilterConfig(ModelMap&& routes, Stats::Scope& scope, DbPtr tl_database);
  const ModelMap& routes() const { return routes_; }
  const ModelRouterFilterStats& stats() const { return stats_; }
  DatabaseInterface* database() const {
    if (!tl_database_) {
      return nullptr;
    }

    return tl_database_->get().ptr();
  }

private:
  static absl::StatusOr<ModelMap>
  buildModelMap(const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto);

  const ModelMap routes_;
  const ModelRouterFilterStats stats_;
  DbPtr tl_database_;
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
  Coroutine::Task<absl::Status> recordTokenUsage() const;

  ModelRouterFilterConfigSharedPtr config_;
  HttpFilters::AiProtocolManager::AiFilterContext context_;
  std::string requested_model_name_;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
