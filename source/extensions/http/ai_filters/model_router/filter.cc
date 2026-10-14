#include "source/extensions/http/ai_filters/model_router/filter.h"

#include "source/common/http/utility.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;

ModelRouterFilterConfig::ModelRouterFilterConfig(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto,
    Stats::Scope& scope)
    : routes_(buildModelMap(proto)),
      stats_(ModelRouterFilterStats{ALL_MODEL_ROUTER_FILTER_STATS(
          POOL_COUNTER_PREFIX(scope, "ai_protocol_manager.model_router."))}) {}

ModelRouterFilterConfig::ModelMap ModelRouterFilterConfig::buildModelMap(
    const envoy::extensions::http::ai_filters::model_router::v3::ModelRouter& proto) {
  ModelRouterFilterConfig::ModelMap routes;

  for (const envoy::extensions::http::ai_filters::model_router::v3::ModelRoute& route :
       proto.routes()) {
    envoy::data::ai::v3::Routes& targets = routes[route.requested_model()];
    for (const envoy::extensions::http::ai_filters::model_router::v3::Destination& dest :
         route.destinations()) {
      envoy::data::ai::v3::Route* target = targets.add_routes();

      target->set_llm_protocol(dest.llm_protocol());
      target->set_model(dest.model());
      target->set_host_and_port(dest.host_and_port());
      target->set_base_path(dest.base_path());
    }
  }

  return routes;
}

ModelRouterFilter::ModelRouterFilter(ModelRouterFilterConfigSharedPtr config,
                                     const HttpFilters::AiProtocolManager::AiFilterContext& context)
    : config_(std::move(config)), context_(context) {}

absl::Status ModelRouterFilter::decodeSync(HttpFilters::AiProtocolManager::AiRequest&,
                                           HttpFilters::AiProtocolManager::LocalReplier) {
  const StreamInfo::FilterStateSharedPtr& filter_state = context_.stream_info.filterState();
  auto model_name =
      filter_state->getDataReadOnly<Router::StringAccessorImpl>(FilterStateKeys::ModelRequest);

  if (!model_name) {
    config_->stats().model_name_missing_.inc();
    return absl::OkStatus();
  }

  absl::string_view model = model_name->asString();
  auto it = config_->routes().find(model);
  if (it == config_->routes().end()) {
    config_->stats().route_not_found_.inc();
    return absl::OkStatus();
  }

  filter_state->setData(FilterStateKeys::ResolvedRoutes,
                        std::make_shared<RoutesFilterStateObject>(it->second),
                        StreamInfo::FilterState::LifeSpan::FilterChain);

  config_->stats().success_.inc();

  // Temporary fix, until the router and DFP filter support ResolvedRoutes.
  // Assumes that all routes point to the same host.
  if (!it->second.routes().empty()) {
    Envoy::Http::Utility::updateAuthority(context_.request_headers,
                                          it->second.routes(0).host_and_port(), false, false);
  }

  return absl::OkStatus();
}

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
