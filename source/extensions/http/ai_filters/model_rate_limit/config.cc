#include "source/extensions/http/ai_filters/model_rate_limit/config.h"

#include <memory>
#include <string>
#include <utility>

#include "envoy/registry/registry.h"

#include "source/common/config/utility.h"
#include "source/common/protobuf/utility.h"
#include "source/extensions/http/ai_filters/model_rate_limit/filter.h"
#include "source/extensions/http/ai_filters/model_rate_limit/local_quota.h"
#include "source/extensions/http/ai_filters/model_rate_limit/rls_quota.h"

#include "absl/status/status.h"
#include "absl/status/status_macros.h"
#include "absl/strings/str_cat.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

using ModelRateLimitProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit;
using LocalQuotaConfigProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::LocalQuotaConfig;
using RlsQuotaConfigProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::RlsQuotaConfig;

absl::StatusOr<HttpFilters::AiProtocolManager::AiFilterFactoryCb>
ModelRateLimitFilterConfigFactory::createAiFilterFactory(
    const Protobuf::Message& config, Server::Configuration::ServerFactoryContext& context,
    Stats::Scope& scope) {
  const auto& proto = MessageUtil::downcastAndValidate<const ModelRateLimitProto&>(
      config, context.messageValidationVisitor());

  const auto& typed_config = proto.backend().typed_config();
  const std::string type = Config::Utility::getFactoryType(typed_config);
  ThreadLocal::SlotAllocator& slot_allocator = context.threadLocal();
  ModelRateLimitFilterConfig::QuotaPtr tl_quota =
      ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(slot_allocator);

  if (type == LocalQuotaConfigProto::default_instance().GetTypeName()) {
    LocalQuotaConfigProto local_config;
    ABSL_RETURN_IF_ERROR(Config::Utility::translateOpaqueConfig(
        typed_config, context.messageValidationVisitor(), local_config));
    MessageUtil::validate(local_config, context.messageValidationVisitor());

    ABSL_ASSIGN_OR_RETURN(LocalQuota base_quota, LocalQuota::create(proto, context.timeSource()));
    tl_quota->set([base_quota = std::move(base_quota)](
                      Event::Dispatcher&) -> std::shared_ptr<QuotaInterface> {
      return std::make_shared<LocalQuota>(base_quota);
    });
  } else if (type == RlsQuotaConfigProto::default_instance().GetTypeName()) {
    RlsQuotaConfigProto rls_config;
    ABSL_RETURN_IF_ERROR(Config::Utility::translateOpaqueConfig(
        typed_config, context.messageValidationVisitor(), rls_config));
    MessageUtil::validate(rls_config, context.messageValidationVisitor());

    ABSL_ASSIGN_OR_RETURN(std::shared_ptr<const RlsQuota::ModelQuotaMap> quota_map,
                          RlsQuota::buildQuotaMap(proto));
    Grpc::AsyncClientFactoryPtr factory;
    ABSL_ASSIGN_OR_RETURN(factory,
                          context.clusterManager().grpcAsyncClientManager().factoryForGrpcService(
                              rls_config.grpc_service(), scope, true));

    std::shared_ptr<Grpc::AsyncClientFactory> shared_factory = std::move(factory);
    tl_quota->set([quota_map = std::move(quota_map), rls_config,
                   shared_factory](Event::Dispatcher&) -> std::shared_ptr<QuotaInterface> {
      absl::StatusOr<Grpc::RawAsyncClientPtr> raw_client =
          shared_factory->createUncachedRawAsyncClient();
      Grpc::RawAsyncClientSharedPtr client = raw_client.ok() ? std::move(*raw_client) : nullptr;
      return std::make_shared<RlsQuota>(quota_map, rls_config, std::move(client));
    });
  } else {
    return absl::InvalidArgumentError(absl::StrCat("Unknown backend type: ", type));
  }

  auto filter_config = std::make_shared<ModelRateLimitFilterConfig>(std::move(tl_quota), scope);

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
