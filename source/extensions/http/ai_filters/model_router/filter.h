#pragma once

#include "envoy/data/ai/v3/routes.pb.h"
#include "envoy/extensions/http/ai_filters/model_router/v3/model_router.pb.h"
#include "envoy/stats/scope.h"
#include "envoy/stats/stats_macros.h"
#include "envoy/stream_info/filter_state.h"

#include "source/common/common/logger.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/http/ai_filters/common/sync_filter.h"

#include "absl/status/status.h"

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
  ModelRouterFilterConfig(
      const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
      Stats::Scope& scope);

  using ModelMap = absl::flat_hash_map<std::string, envoy::data::ai::v3::Routes>;
  const ModelMap& routes() const { return routes_; }
  const ModelRouterFilterStats& stats() const { return stats_; }

private:
  static ModelMap
  buildModelMap(const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto);

  const ModelMap routes_;
  const ModelRouterFilterStats stats_;
};
using ModelRouterFilterConfigSharedPtr = std::shared_ptr<const ModelRouterFilterConfig>;

class ModelRouterFilter : public Common::SyncAiFilter,
                          public Logger::Loggable<Logger::Id::ai_protocol_manager> {
public:
  ModelRouterFilter(ModelRouterFilterConfigSharedPtr config,
                    const HttpFilters::AiProtocolManager::AiFilterContext& context);

  // Common::SyncAiFilter
  absl::Status decodeSync(HttpFilters::AiProtocolManager::AiRequest& request,
                          HttpFilters::AiProtocolManager::LocalReplier reply_locally) override;

private:
  ModelRouterFilterConfigSharedPtr config_;
  HttpFilters::AiProtocolManager::AiFilterContext context_;
};

class RoutesFilterStateObject : public Envoy::StreamInfo::FilterState::Object {
public:
  using Routes = envoy::data::ai::v3::Routes;

  RoutesFilterStateObject(const Routes& routes) : routes_(routes) {}

  const Routes& proto() const { return routes_; }

private:
  const Routes routes_;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
