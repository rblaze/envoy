#include "source/extensions/http/ai_filters/model_rate_limit/local_quota.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "source/common/protobuf/utility.h"

#include "absl/container/flat_hash_set.h"
#include "absl/status/status_macros.h"
#include "absl/strings/str_cat.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

namespace {

namespace Config = envoy::extensions::http::ai_filters::model_rate_limit::v3;

double windowSeconds(Config::TimeUnit time_unit) {
  switch (time_unit) {
  case Config::MINUTE:
    return 60.0;
  case Config::HOUR:
    return 3600.0;
  case Config::DAY:
    return 86400.0;
  default:       // LCOV_EXCL_LINE
    return 60.0; // LCOV_EXCL_LINE
  }
}

} // namespace

absl::StatusOr<LocalQuota> LocalQuota::create(const Config::ModelRateLimit& proto,
                                              TimeSource& time_source) {
  ABSL_ASSIGN_OR_RETURN(std::shared_ptr<const ModelQuotaMap> quotas,
                        buildQuotaMap(proto, time_source));
  return LocalQuota(std::move(quotas));
}

LocalQuota::LocalQuota(std::shared_ptr<const ModelQuotaMap> quotas) : quotas_(std::move(quotas)) {}

absl::StatusOr<std::shared_ptr<const LocalQuota::ModelQuotaMap>>
LocalQuota::buildQuotaMap(const Config::ModelRateLimit& proto, TimeSource& time_source) {
  auto map = std::make_shared<ModelQuotaMap>();
  absl::flat_hash_set<ModelKey> seen_models;

  for (const Config::ModelLimit& model_entry : proto.limits()) {
    ModelKey key{model_entry.provider(), model_entry.model()};
    if (!seen_models.insert(key).second) {
      return absl::InvalidArgumentError(absl::StrCat("Duplicate model limit for provider ",
                                                     key.provider, " and model ", key.model));
    }
    if (model_entry.quotas().empty()) {
      continue;
    }

    ModelQuotas& model_quotas = (*map)[std::move(key)];
    for (const Config::Quota& quota : model_entry.quotas()) {
      const uint64_t limit = quota.limit();
      const uint32_t multiplier = PROTOBUF_GET_WRAPPED_OR_DEFAULT(quota, unit_multiplier, 1);
      const double fill_rate =
          static_cast<double>(limit) / (windowSeconds(quota.time_unit()) * multiplier);
      auto bucket = std::make_unique<AtomicTokenBucketImpl>(limit, time_source, fill_rate);

      switch (quota.quota_type()) {
      case Config::REQUESTS:
        model_quotas.request_buckets.push_back(std::move(bucket));
        break;
      case Config::TOKENS:
        model_quotas.token_buckets.push_back(std::move(bucket));
        break;
      default: // LCOV_EXCL_LINE
        break; // LCOV_EXCL_LINE
      }
    }
  }

  return map;
}

Coroutine::Task<absl::StatusOr<bool>>
LocalQuota::checkLimits(std::string provider, std::string model, uint64_t estimated_input_tokens) {
  const auto it = quotas_->find(ModelKey{std::move(provider), std::move(model)});
  if (it == quotas_->end()) {
    co_return true;
  }

  const ModelQuotas& model_quotas = it->second;
  for (const auto& bucket : model_quotas.request_buckets) {
    if (bucket->remainingTokens() < 1.0) {
      co_return false;
    }
  }

  const double required_tokens = static_cast<double>(std::max<uint64_t>(1, estimated_input_tokens));
  for (const auto& bucket : model_quotas.token_buckets) {
    if (bucket->remainingTokens() < required_tokens) {
      co_return false;
    }
  }

  co_return true;
}

Coroutine::Task<absl::Status> LocalQuota::consumeQuota(std::string provider, std::string model,
                                                       uint64_t input_tokens,
                                                       uint64_t output_tokens) {
  const auto it = quotas_->find(ModelKey{std::move(provider), std::move(model)});
  if (it == quotas_->end()) {
    co_return absl::OkStatus();
  }

  const ModelQuotas& model_quotas = it->second;
  for (const auto& bucket : model_quotas.request_buckets) {
    if (bucket->maxTokens() > 0.0) {
      bucket->consume([](double) { return 1.0; });
    }
  }

  const double total_tokens =
      static_cast<double>(input_tokens) + static_cast<double>(output_tokens);
  if (total_tokens > 0.0) {
    for (const auto& bucket : model_quotas.token_buckets) {
      if (bucket->maxTokens() > 0.0) {
        bucket->consume([total_tokens](double) { return total_tokens; });
      }
    }
  }

  co_return absl::OkStatus();
}

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
