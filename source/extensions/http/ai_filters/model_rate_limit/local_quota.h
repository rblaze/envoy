#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "envoy/common/time.h"
#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.h"

#include "source/common/common/token_bucket_impl.h"
#include "source/common/coroutine/task.h"
#include "source/extensions/http/ai_filters/model_rate_limit/quota_interface.h"

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

class LocalQuota : public QuotaInterface {
public:
  static absl::StatusOr<LocalQuota>
  create(const envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit& proto,
         TimeSource& time_source);

  LocalQuota(const LocalQuota&) = default;
  LocalQuota(LocalQuota&&) = default;
  ~LocalQuota() override = default;

  Coroutine::Task<absl::StatusOr<bool>> checkLimits(std::string provider, std::string model,
                                                    uint64_t estimated_input_tokens) override;

  Coroutine::Task<absl::Status> consumeQuota(std::string provider, std::string model,
                                             uint64_t input_tokens,
                                             uint64_t output_tokens) override;

private:
  struct ModelQuotas {
    std::vector<std::unique_ptr<AtomicTokenBucketImpl>> request_buckets;
    std::vector<std::unique_ptr<AtomicTokenBucketImpl>> token_buckets;
  };

  using ModelQuotaMap = absl::flat_hash_map<ModelKey, ModelQuotas>;

  explicit LocalQuota(std::shared_ptr<const ModelQuotaMap> quotas);

  static absl::StatusOr<std::shared_ptr<const ModelQuotaMap>> buildQuotaMap(
      const envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit& proto,
      TimeSource& time_source);

  std::shared_ptr<const ModelQuotaMap> quotas_;
};

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
