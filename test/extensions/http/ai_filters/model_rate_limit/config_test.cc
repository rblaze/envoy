#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.h"
#include "envoy/registry/registry.h"

#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/http/ai_filters/model_rate_limit/config.h"
#include "source/extensions/http/ai_filters/model_rate_limit/filter.h"

#include "test/mocks/grpc/mocks.h"
#include "test/mocks/server/server_factory_context.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/stream_info/mocks.h"
#include "test/test_common/simulated_time_system.h"
#include "test/test_common/status_utility.h"
#include "test/test_common/utility.h"

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {
namespace {

using HttpFilters::AiProtocolManager::AiFilterConfigFactory;
using HttpFilters::AiProtocolManager::AiFilterContext;
using HttpFilters::AiProtocolManager::AiFilterFactoryCb;
using HttpFilters::AiProtocolManager::LLMProtocol;
using ModelRateLimitProto =
    envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit;
using StatusHelpers::HasStatus;
using testing::_;
using testing::HasSubstr;
using testing::NiceMock;
using testing::Return;
using testing::StrictMock;

class ModelRateLimitConfigTest : public Event::TestUsingSimulatedTime, public testing::Test {
protected:
  ModelRateLimitProto parseYaml(const std::string& yaml) {
    ModelRateLimitProto proto;
    TestUtility::loadFromYaml(yaml, proto);
    return proto;
  }

  absl::StatusOr<AiFilterFactoryCb> createFactory(const ModelRateLimitProto& proto) {
    return factory_.createAiFilterFactory(proto, context_, *stats_store_.rootScope());
  }

  absl::StatusOr<AiFilterFactoryCb> createFactoryFromYaml(const std::string& yaml) {
    return createFactory(parseYaml(yaml));
  }

  ModelRateLimitFilterConfigFactory factory_;
  NiceMock<Server::Configuration::MockServerFactoryContext> context_;
  NiceMock<Stats::MockIsolatedStatsStore> stats_store_;
};

TEST_F(ModelRateLimitConfigTest, IsRegistered) {
  auto* factory = Registry::FactoryRegistry<AiFilterConfigFactory>::getFactory(
      "envoy.http.ai_filters.model_rate_limit");
  ASSERT_NE(factory, nullptr);
  EXPECT_EQ(factory->category(), "envoy.http.ai_filters");
  EXPECT_THAT(factory,
              testing::WhenDynamicCastTo<ModelRateLimitFilterConfigFactory*>(testing::NotNull()));

  const auto empty_proto = factory_.createEmptyConfigProto();
  ASSERT_NE(empty_proto, nullptr);
  EXPECT_NE(dynamic_cast<ModelRateLimitProto*>(empty_proto.get()), nullptr);
}

TEST_F(ModelRateLimitConfigTest, CreatesFilterWithLocalAndRlsBackends) {
  // 1. LocalQuotaConfig with multiple quota types, limit=0, uint64 limit > UINT32_MAX,
  // unit_multiplier, and custom metadata namespaces.
  {
    const auto factory_cb = createFactoryFromYaml(R"EOF(
limits:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 0
      - quota_type: TOKENS
        time_unit: HOUR
        limit: 5000000000
        unit_multiplier: 6
      - quota_type: REQUESTS
        time_unit: DAY
        limit: 500
local: {}
request_info_metadata_namespace: custom.ai.request_info
token_usage_metadata_namespace: custom.ai.token_usage
)EOF");
    ASSERT_TRUE(factory_cb.ok()) << factory_cb.status();

    NiceMock<StreamInfo::MockStreamInfo> stream_info;
    Http::TestRequestHeaderMapImpl headers{{":method", "POST"}, {":path", "/"}};
    const AiFilterContext stream_context{stream_info, headers, LLMProtocol::OpenAiChatCompletions};
    EXPECT_NE((*factory_cb)(stream_context), nullptr);
  }

  // 2. RlsQuotaConfig creates cached per-thread gRPC client via
  // getOrCreateRawAsyncClientWithHashKey.
  {
    auto mock_client = std::make_shared<StrictMock<Grpc::MockAsyncClient>>();
    EXPECT_CALL(context_.cluster_manager_.async_client_manager_,
                getOrCreateRawAsyncClientWithHashKey(_, _, true))
        .Times(2)
        .WillRepeatedly(Return(mock_client));

    const auto factory_cb = createFactoryFromYaml(R"EOF(
limits:
  - provider: anthropic
    model: claude-haiku-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
rls:
  grpc_service:
    envoy_grpc:
      cluster_name: rls_cluster
  domain: custom_domain
  timeout: 2s
)EOF");
    ASSERT_TRUE(factory_cb.ok()) << factory_cb.status();

    NiceMock<StreamInfo::MockStreamInfo> stream_info;
    Http::TestRequestHeaderMapImpl headers{{":method", "POST"}, {":path", "/"}};
    const AiFilterContext stream_context{stream_info, headers, LLMProtocol::OpenAiChatCompletions};
    EXPECT_NE((*factory_cb)(stream_context), nullptr);
  }
}

TEST_F(ModelRateLimitConfigTest, RejectsSemanticConfigErrors) {
  struct Case {
    std::string yaml;
    std::string expected_error;
  };
  const std::vector<Case> cases = {
      // Duplicate (provider, model) with LocalQuotaConfig.
      {R"EOF(
limits:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 5
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
local: {}
)EOF",
       "Duplicate model limit for provider openai and model gpt-4o"},
      // Duplicate (provider, model) with RlsQuotaConfig.
      {R"EOF(
limits:
  - provider: anthropic
    model: claude-sonnet-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 5
  - provider: anthropic
    model: claude-sonnet-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
rls:
  grpc_service:
    envoy_grpc:
      cluster_name: rls_cluster
)EOF",
       "Duplicate model limit for provider anthropic and model claude-sonnet-4-5"},
      // RLS override limit exceeding UINT32_MAX.
      {R"EOF(
limits:
  - provider: anthropic
    model: claude-sonnet-4-5
    quotas:
      - quota_type: TOKENS
        time_unit: DAY
        limit: 5000000000
rls:
  grpc_service:
    envoy_grpc:
      cluster_name: rls_cluster
)EOF",
       "RLS quota override limit 5000000000 exceeds UINT32_MAX"},
  };

  for (const auto& c : cases) {
    const auto factory_cb = createFactoryFromYaml(c.yaml);
    EXPECT_THAT(factory_cb,
                HasStatus(absl::StatusCode::kInvalidArgument, HasSubstr(c.expected_error)))
        << c.yaml;
  }
}

TEST_F(ModelRateLimitConfigTest, PropagatesGrpcClientCreationErrorForRlsConfig) {
  EXPECT_CALL(context_.cluster_manager_.async_client_manager_,
              getOrCreateRawAsyncClientWithHashKey(_, _, true))
      .WillOnce(Return(absl::InvalidArgumentError("invalid grpc service")));

  const auto factory_cb = createFactoryFromYaml(R"EOF(
rls:
  grpc_service:
    envoy_grpc:
      cluster_name: rls_cluster
)EOF");
  EXPECT_THAT(factory_cb,
              HasStatus(absl::StatusCode::kInvalidArgument, HasSubstr("invalid grpc service")));
}

TEST_F(ModelRateLimitConfigTest, RejectsProtoValidationViolations) {
  // Out-of-range QuotaType enum value.
  {
    ModelRateLimitProto proto;
    auto* limit = proto.add_limits();
    limit->set_provider("openai");
    limit->set_model("gpt-4o");
    auto* quota = limit->add_quotas();
    quota->set_quota_type(
        static_cast<envoy::extensions::http::ai_filters::model_rate_limit::v3::QuotaType>(999));
    quota->set_time_unit(envoy::extensions::http::ai_filters::model_rate_limit::v3::MINUTE);
    quota->set_limit(10);
    proto.mutable_local();
    EXPECT_THROW_WITH_REGEX(createFactory(proto).IgnoreError(), EnvoyException,
                            "value must be one of the defined enum values");
  }

  struct YamlCase {
    std::string yaml;
    std::string expected_regex;
  };
  const std::vector<YamlCase> cases = {
      // Missing backend oneof.
      {R"EOF(
limits: []
)EOF",
       R"(field: "backend", reason: is required)"},
      // Missing provider in ModelLimit.
      {R"EOF(
limits:
  - model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
local: {}
)EOF",
       "ModelLimitValidationError\\.Provider"},
      // Missing model in ModelLimit.
      {R"EOF(
limits:
  - provider: openai
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
local: {}
)EOF",
       "ModelLimitValidationError\\.Model"},
      // Empty quotas in ModelLimit.
      {R"EOF(
limits:
  - provider: openai
    model: gpt-4o
    quotas: []
local: {}
)EOF",
       "ModelLimitValidationError\\.Quotas"},
      // QUOTA_UNSPECIFIED.
      {R"EOF(
limits:
  - provider: anthropic
    model: claude-haiku-4-5
    quotas:
      - quota_type: QUOTA_UNSPECIFIED
        time_unit: MINUTE
        limit: 10
local: {}
)EOF",
       "QuotaValidationError\\.QuotaType"},
      // TIME_UNSPECIFIED.
      {R"EOF(
limits:
  - provider: anthropic
    model: claude-haiku-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: TIME_UNSPECIFIED
        limit: 10
local: {}
)EOF",
       "QuotaValidationError\\.TimeUnit"},
      // unit_multiplier == 0.
      {R"EOF(
limits:
  - provider: anthropic
    model: claude-haiku-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: HOUR
        limit: 10
        unit_multiplier: 0
local: {}
)EOF",
       "QuotaValidationError\\.UnitMultiplier"},
      // Missing grpc_service in RlsQuotaConfig.
      {R"EOF(
rls: {}
)EOF",
       "RlsQuotaConfigValidationError\\.GrpcService"},
      // Zero timeout in RlsQuotaConfig.
      {R"EOF(
rls:
  grpc_service:
    envoy_grpc:
      cluster_name: rls_cluster
  timeout: 0s
)EOF",
       "RlsQuotaConfigValidationError\\.Timeout"},
  };

  for (const auto& c : cases) {
    EXPECT_THROW_WITH_REGEX(createFactoryFromYaml(c.yaml).IgnoreError(), EnvoyException,
                            c.expected_regex);
  }
}

} // namespace
} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
