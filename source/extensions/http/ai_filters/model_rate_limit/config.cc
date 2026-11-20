#include "source/extensions/http/ai_filters/model_rate_limit/config.h"

#include <memory>
#include <utility>

#include "envoy/grpc/async_client_manager.h"
#include "envoy/registry/registry.h"

#include "source/common/common/assert.h"
#include "source/common/protobuf/utility.h"
#include "source/extensions/http/ai_filters/model_rate_limit/filter.h"
#include "source/extensions/http/ai_filters/model_rate_limit/local_quota.h"
#include "source/extensions/http/ai_filters/model_rate_limit/rls_quota.h"

#include "absl/status/status_macros.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

using ModelRateLimitProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit;
using RlsQuotaConfigProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::RlsQuotaConfig;

absl::StatusOr<HttpFilters::AiProtocolManager::AiFilterFactoryCb>
ModelRateLimitFilterConfigFactory::createAiFilterFactory(
    const Protobuf::Message& config, Server::Configuration::ServerFactoryContext& context,
    Stats::Scope& scope) {
  const auto& proto = MessageUtil::downcastAndValidate<const ModelRateLimitProto&>(
      config, context.messageValidationVisitor());

  ThreadLocal::SlotAllocator& slot_allocator = context.threadLocal();
  ModelRateLimitFilterConfig::QuotaPtr tl_quota =
      ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(slot_allocator);

  switch (proto.backend_case()) {
  case ModelRateLimitProto::kLocal: {
    ABSL_ASSIGN_OR_RETURN(LocalQuota base_quota, LocalQuota::create(proto, context.timeSource()));
    tl_quota->set([base_quota = std::move(base_quota)](
                      Event::Dispatcher&) -> std::shared_ptr<QuotaInterface> {
      return std::make_shared<LocalQuota>(base_quota);
    });
    break;
  }
  case ModelRateLimitProto::kRls: {
    const RlsQuotaConfigProto& rls_config = proto.rls();
    ABSL_ASSIGN_OR_RETURN(std::shared_ptr<const RlsQuota::ModelQuotaMap> quota_map,
                          RlsQuota::buildQuotaMap(proto));
    Grpc::GrpcServiceConfigWithHashKey config_with_hash_key(rls_config.grpc_service());
    ABSL_ASSIGN_OR_RETURN(
        Grpc::RawAsyncClientSharedPtr validation_client,
        context.clusterManager().grpcAsyncClientManager().getOrCreateRawAsyncClientWithHashKey(
            config_with_hash_key, context.scope(), true));
    UNREFERENCED_PARAMETER(validation_client);

    tl_quota->set([quota_map = std::move(quota_map), rls_config, config_with_hash_key,
                   &context](Event::Dispatcher&) -> std::shared_ptr<QuotaInterface> {
      absl::StatusOr<Grpc::RawAsyncClientSharedPtr> raw_client =
          context.clusterManager().grpcAsyncClientManager().getOrCreateRawAsyncClientWithHashKey(
              config_with_hash_key, context.scope(), true);
      RELEASE_ASSERT(raw_client.ok(), raw_client.status().ToString());
      return std::make_shared<RlsQuota>(quota_map, rls_config, std::move(*raw_client));
    });
    break;
  }
  case ModelRateLimitProto::BACKEND_NOT_SET: // LCOV_EXCL_LINE
    PANIC_DUE_TO_PROTO_UNSET;                // LCOV_EXCL_LINE
  }

  auto filter_config = std::make_shared<ModelRateLimitFilterConfig>(
      std::move(tl_quota), scope, proto.request_info_metadata_namespace(),
      proto.token_usage_metadata_namespace());

  return [filter_config](const HttpFilters::AiProtocolManager::AiFilterContext& stream_context)
             -> HttpFilters::AiProtocolManager::AiFilterSharedPtr {
    return std::make_shared<ModelRateLimitFilter>(filter_config, stream_context);
  };
}

REGISTER_FACTORY(ModelRateLimitFilterConfigFactory,
                 HttpFilters::AiProtocolManager::AiFilterConfigFactory);

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
