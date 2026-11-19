#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "envoy/extensions/common/ratelimit/v3/ratelimit.pb.h"
#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.h"
#include "envoy/grpc/async_client.h"

#include "source/common/coroutine/task.h"
#include "source/extensions/http/ai_filters/model_rate_limit/async_rls.h"
#include "source/extensions/http/ai_filters/model_rate_limit/quota_interface.h"

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

class RlsQuota : public QuotaInterface {
public:
  static constexpr absl::string_view DefaultDomain{"ai_model_routing"};

  using RateLimitOverride =
      envoy::extensions::common::ratelimit::v3::RateLimitDescriptor::RateLimitOverride;

  struct ModelQuotas {
    bool disabled{false};
    std::vector<RateLimitOverride> request_overrides;
    std::vector<RateLimitOverride> token_overrides;
  };

  using ModelQuotaMap = absl::flat_hash_map<ModelKey, ModelQuotas>;

  static absl::StatusOr<std::shared_ptr<const ModelQuotaMap>> buildQuotaMap(
      const envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit& proto);

  static absl::StatusOr<std::unique_ptr<RlsQuota>> create(
      const envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit& proto,
      const envoy::extensions::http::ai_filters::model_rate_limit::v3::RlsQuotaConfig& rls_config,
      Grpc::RawAsyncClientSharedPtr async_client);

  RlsQuota(
      std::shared_ptr<const ModelQuotaMap> quotas,
      const envoy::extensions::http::ai_filters::model_rate_limit::v3::RlsQuotaConfig& rls_config,
      Grpc::RawAsyncClientSharedPtr async_client);
  ~RlsQuota() override = default;

  Coroutine::Task<absl::StatusOr<bool>> checkLimits(std::string provider, std::string model,
                                                    uint64_t estimated_input_tokens) override;

  Coroutine::Task<absl::Status> consumeQuota(std::string provider, std::string model,
                                             uint64_t input_tokens,
                                             uint64_t output_tokens) override;

private:
  std::shared_ptr<const ModelQuotaMap> quotas_;
  std::string domain_;
  AsyncRlsClient client_;
};

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
