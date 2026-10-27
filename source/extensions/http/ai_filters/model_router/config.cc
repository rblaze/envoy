#include "source/extensions/http/ai_filters/model_router/config.h"

#include <utility>

#include "envoy/registry/registry.h"
#include "envoy/thread_local/thread_local.h"

#include "source/common/protobuf/utility.h"
#include "source/extensions/http/ai_filters/model_router/filter.h"
#include "source/extensions/http/ai_filters/model_router/postgres_database.h"

#include "absl/status/status_macros.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

using ModelRouterProto = envoy::extensions::http::ai_filters::model_router::v3::ModelRouter;

absl::StatusOr<HttpFilters::AiProtocolManager::AiFilterFactoryCb>
ModelRouterFilterConfigFactory::createAiFilterFactory(
    const Protobuf::Message& config, Server::Configuration::ServerFactoryContext& context,
    Stats::Scope& scope) {
  const auto& proto = MessageUtil::downcastAndValidate<const ModelRouterProto&>(
      config, context.messageValidationVisitor());

  ModelRouterFilterConfig::DbPtr tl_database;
  if (!proto.db_connection_string().empty()) {
    ThreadLocal::SlotAllocator& slot_allocator = context.threadLocal();
    tl_database = ThreadLocal::TypedSlot<DatabaseInterface>::makeUnique(slot_allocator);
    tl_database->set(
        [dbstr = std::string(proto.db_connection_string())](Event::Dispatcher& dispatcher) {
          return std::make_shared<PostgresDatabase>(dispatcher, dbstr);
        });
  }

  ModelRouterFilterConfigSharedPtr filter_config;
  ABSL_ASSIGN_OR_RETURN(filter_config,
                        ModelRouterFilterConfig::create(proto, scope, std::move(tl_database)));

  return [filter_config](const HttpFilters::AiProtocolManager::AiFilterContext& stream_context)
             -> HttpFilters::AiProtocolManager::AiFilterSharedPtr {
    return std::make_shared<ModelRouterFilter>(filter_config, stream_context);
  };
}

REGISTER_FACTORY(ModelRouterFilterConfigFactory,
                 HttpFilters::AiProtocolManager::AiFilterConfigFactory);

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
