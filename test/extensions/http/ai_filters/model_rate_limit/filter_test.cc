#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "envoy/data/ai/v3/request_info.pb.h"
#include "envoy/data/ai/v3/token_usage.pb.h"

#include "source/common/coroutine/launch.h"
#include "source/common/coroutine/leaf_awaitable.h"
#include "source/common/coroutine/task.h"
#include "source/common/protobuf/protobuf.h"
#include "source/common/protobuf/utility.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/common/stream_info/stream_info_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/filters/http/ai_protocol_manager/json_with_ext_buf.h"
#include "source/extensions/http/ai_filters/model_rate_limit/filter.h"
#include "source/extensions/http/ai_filters/model_rate_limit/quota_interface.h"

#include "test/common/coroutine/manual_executor.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/thread_local/mocks.h"
#include "test/test_common/status_utility.h"
#include "test/test_common/utility.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {
namespace {

using HttpFilters::AiProtocolManager::AiFilterContext;
using HttpFilters::AiProtocolManager::AiRequest;
using HttpFilters::AiProtocolManager::AiRequestPropagator;
using HttpFilters::AiProtocolManager::AiRequestPtr;
using HttpFilters::AiProtocolManager::AiRequestReceiver;
using HttpFilters::AiProtocolManager::FlattenJsonField;
using HttpFilters::AiProtocolManager::JsonWithExtBuf;
using HttpFilters::AiProtocolManager::LLMProtocol;
using HttpFilters::AiProtocolManager::SseEvent;
using HttpFilters::AiProtocolManager::SseEventPtr;
namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;
using StatusHelpers::HasStatusCode;
using StatusHelpers::IsOk;
using testing::NiceMock;
using testing::StrictMock;

// Awaitable that suspends indefinitely until cancelled (or completed manually in tests).
template <typename T> class PendingAwaitable : public Coroutine::LeafAwaitable<T> {
protected:
  std::optional<T> tryImmediate() override { return std::nullopt; }
  void onStart() override {}
  void onCancel() override {}
};

class MockQuota : public QuotaInterface {
public:
  static Coroutine::Task<absl::StatusOr<bool>> makeCheckTask(absl::StatusOr<bool> res) {
    co_return res;
  }
  static Coroutine::Task<absl::Status> makeConsumeTask(absl::Status res) { co_return res; }

  static Coroutine::Task<absl::StatusOr<bool>> makePendingCheckTask() {
    co_return co_await PendingAwaitable<absl::StatusOr<bool>>();
  }
  static Coroutine::Task<absl::Status> makePendingConsumeTask() {
    co_return co_await PendingAwaitable<absl::Status>();
  }

  MOCK_METHOD(Coroutine::Task<absl::StatusOr<bool>>, checkLimits,
              (std::string provider, std::string model, uint64_t estimated_input_tokens),
              (override));
  MOCK_METHOD(Coroutine::Task<absl::Status>, consumeQuota,
              (std::string provider, std::string model, uint64_t input_tokens,
               uint64_t output_tokens),
              (override));
};

class ModelRateLimitFilterTest : public testing::Test {
protected:
  ModelRateLimitFilterTest()
      : api_(Api::createApiForTest()),
        stream_info_(api_->timeSource(), nullptr, StreamInfo::FilterState::LifeSpan::FilterChain),
        mock_quota_(std::make_shared<StrictMock<MockQuota>>()) {
    auto slot = ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(tls_);
    slot->set([q = mock_quota_](Event::Dispatcher&) { return q; });
    config_ =
        std::make_shared<ModelRateLimitFilterConfig>(std::move(slot), *stats_store_.rootScope());
  }

  template <typename T> static T runTask(Coroutine::Task<T> task) {
    auto exec = std::make_shared<Coroutine::ManualExecutor>();
    std::optional<T> result;
    auto handle = Coroutine::launch(
        std::move(task), exec, [&result](T val) { result.emplace(std::move(val)); },
        Coroutine::StartMode::Inline);
    exec->drain();
    RELEASE_ASSERT(result.has_value(), "Task did not complete synchronously");
    return std::move(*result);
  }

  std::unique_ptr<ModelRateLimitFilter> makeFilter() {
    const AiFilterContext context{stream_info_, request_headers_,
                                  LLMProtocol::OpenAiChatCompletions};
    return std::make_unique<ModelRateLimitFilter>(config_, context);
  }

  void setResolvedModelAndProvider(std::optional<absl::string_view> provider,
                                   std::optional<absl::string_view> model) {
    if (provider.has_value()) {
      stream_info_.filterState()->setData(FilterStateKeys::Provider,
                                          std::make_shared<Router::StringAccessorImpl>(*provider),
                                          StreamInfo::FilterState::LifeSpan::FilterChain);
    }
    if (model.has_value()) {
      stream_info_.filterState()->setData(FilterStateKeys::ModelResolved,
                                          std::make_shared<Router::StringAccessorImpl>(*model),
                                          StreamInfo::FilterState::LifeSpan::FilterChain);
    }
  }

  void setEstimatedInputTokensInMetadata(uint64_t estimated_input_tokens) {
    envoy::data::ai::v3::RequestInfo info;
    info.mutable_estimated_input_tokens()->set_value(estimated_input_tokens);
    Protobuf::Any any;
    MessageUtil::packFrom(any, info);
    stream_info_.setDynamicTypedMetadata("envoy.ai.request_info", any);
  }

  void setTokenUsageInMetadata(std::optional<uint64_t> input_tokens,
                               std::optional<uint64_t> output_tokens) {
    envoy::data::ai::v3::TokenUsage usage;
    if (input_tokens.has_value()) {
      usage.mutable_input_tokens()->set_value(*input_tokens);
    }
    if (output_tokens.has_value()) {
      usage.mutable_output_tokens()->set_value(*output_tokens);
    }
    Protobuf::Any any;
    MessageUtil::packFrom(any, usage);
    stream_info_.setDynamicTypedMetadata("envoy.ai.token_usage", any);
  }

  void expectCheckLimits(absl::string_view provider, absl::string_view model,
                         uint64_t estimated_input_tokens, absl::StatusOr<bool> result) {
    EXPECT_CALL(*mock_quota_,
                checkLimits(std::string(provider), std::string(model), estimated_input_tokens))
        .WillOnce([result = std::move(result)](std::string, std::string, uint64_t) {
          return MockQuota::makeCheckTask(result);
        });
  }

  void expectConsumeQuota(absl::string_view provider, absl::string_view model,
                          uint64_t input_tokens, uint64_t output_tokens,
                          absl::Status status = absl::OkStatus()) {
    EXPECT_CALL(*mock_quota_, consumeQuota(std::string(provider), std::string(model), input_tokens,
                                           output_tokens))
        .WillOnce([status = std::move(status)](std::string, std::string, uint64_t, uint64_t) {
          return MockQuota::makeConsumeTask(status);
        });
  }

  absl::Status runDecode(ModelRateLimitFilter& filter,
                         absl::Status propagate_status = absl::OkStatus()) {
    reply_code_.reset();
    reply_details_.clear();
    request_propagated_ = false;
    return runTask(
        filter.decode(AiRequestReceiver([]() -> Coroutine::Task<absl::StatusOr<AiRequestPtr>> {
                        co_return std::make_unique<AiRequest>(JsonWithExtBuf{});
                      }),
                      AiRequestPropagator([this, propagate_status](
                                              AiRequestPtr req) -> Coroutine::Task<absl::Status> {
                        EXPECT_NE(req, nullptr);
                        request_propagated_ = true;
                        co_return propagate_status;
                      }),
                      [this](Http::Code code, std::string details) {
                        reply_code_ = code;
                        reply_details_ = std::move(details);
                      }));
  }

  size_t runEncode(ModelRateLimitFilter& filter, bool sse) {
    size_t sent = 0;
    size_t propagated = 0;
    absl::Status status;
    if (sse) {
      status = runTask(filter.encodeSSE(
          [&sent]() -> Coroutine::Task<absl::StatusOr<std::optional<SseEventPtr>>> {
            if (sent++ == 0) {
              co_return std::make_unique<SseEvent>();
            }
            co_return std::nullopt;
          },
          [&propagated](SseEventPtr event) -> Coroutine::Task<absl::Status> {
            EXPECT_NE(event, nullptr);
            ++propagated;
            co_return absl::OkStatus();
          }));
    } else {
      status = runTask(filter.encodeUnary(
          [&sent]() -> Coroutine::Task<absl::StatusOr<std::vector<FlattenJsonField>>> {
            if (sent++ == 0) {
              co_return std::vector<FlattenJsonField>(1);
            }
            co_return std::vector<FlattenJsonField>{};
          },
          [&propagated](std::vector<FlattenJsonField> batch) -> Coroutine::Task<absl::Status> {
            EXPECT_FALSE(batch.empty());
            ++propagated;
            co_return absl::OkStatus();
          }));
    }
    EXPECT_THAT(status, IsOk());
    return propagated;
  }

  uint64_t counterValue(const std::string& name) {
    const auto counter =
        TestUtility::findCounter(stats_store_, "ai_protocol_manager.model_rate_limit." + name);
    return counter != nullptr ? counter->value() : 0;
  }

  Api::ApiPtr api_;
  StreamInfo::StreamInfoImpl stream_info_;
  NiceMock<Stats::MockIsolatedStatsStore> stats_store_;
  NiceMock<ThreadLocal::MockInstance> tls_;
  std::shared_ptr<StrictMock<MockQuota>> mock_quota_;
  ModelRateLimitFilterConfigSharedPtr config_;
  Http::TestRequestHeaderMapImpl request_headers_{{":method", "POST"},
                                                  {":path", "/v1/chat/completions"}};
  std::optional<Http::Code> reply_code_;
  std::string reply_details_;
  bool request_propagated_{false};
};

TEST_F(ModelRateLimitFilterTest, MissingModelOrProviderReplies500AndSkipsResponseConsumption) {
  // 1. Neither model nor provider set.
  {
    auto filter = makeFilter();
    EXPECT_THAT(runDecode(*filter), IsOk());
    EXPECT_EQ(reply_code_, Http::Code::InternalServerError);
    EXPECT_EQ(reply_details_, "Model or provider name not found in filter state");
    EXPECT_FALSE(request_propagated_);
    EXPECT_EQ(runEncode(*filter, /*sse=*/false), 0);
    EXPECT_EQ(runEncode(*filter, /*sse=*/true), 0);
  }

  // 2. Model set, provider missing.
  {
    setResolvedModelAndProvider(std::nullopt, "claude-haiku-4-5");
    auto filter = makeFilter();
    EXPECT_THAT(runDecode(*filter), IsOk());
    EXPECT_EQ(reply_code_, Http::Code::InternalServerError);
    EXPECT_FALSE(request_propagated_);
    EXPECT_EQ(runEncode(*filter, /*sse=*/false), 0);
    EXPECT_EQ(runEncode(*filter, /*sse=*/true), 0);
  }

  // 3. Provider set, model empty.
  {
    setResolvedModelAndProvider("anthropic", "");
    auto filter = makeFilter();
    EXPECT_THAT(runDecode(*filter), IsOk());
    EXPECT_EQ(reply_code_, Http::Code::InternalServerError);
    EXPECT_FALSE(request_propagated_);
  }

  EXPECT_EQ(counterValue("model_or_provider_missing"), 3);
  EXPECT_EQ(counterValue("success"), 0);
  EXPECT_EQ(counterValue("quota_exceeded"), 0);
  EXPECT_EQ(counterValue("backend_error"), 0);
}

TEST_F(ModelRateLimitFilterTest, AllowsRequestUnderQuotaAndPropagatesReceiveError) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());
  EXPECT_FALSE(reply_code_.has_value());
  EXPECT_TRUE(request_propagated_);
  EXPECT_EQ(counterValue("success"), 1);

  // Receive error short-circuits decode before checking quota.
  auto error_filter = makeFilter();
  const absl::Status err_status = runTask(
      error_filter->decode(AiRequestReceiver([]() -> Coroutine::Task<absl::StatusOr<AiRequestPtr>> {
                             co_return absl::AbortedError("receive failed");
                           }),
                           AiRequestPropagator([](AiRequestPtr) -> Coroutine::Task<absl::Status> {
                             co_return absl::OkStatus();
                           }),
                           [](Http::Code, std::string) { FAIL() << "Unexpected local reply"; }));
  EXPECT_THAT(err_status, HasStatusCode(absl::StatusCode::kAborted));
}

TEST_F(ModelRateLimitFilterTest, RejectsRequestWith429WhenOverQuota) {
  setResolvedModelAndProvider("openai", "gpt-4o");
  setEstimatedInputTokensInMetadata(150);
  expectCheckLimits("openai", "gpt-4o", 150, false);

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());
  EXPECT_EQ(reply_code_, Http::Code::TooManyRequests);
  EXPECT_EQ(reply_details_, "Model quota exceeded");
  EXPECT_FALSE(request_propagated_);

  EXPECT_EQ(counterValue("quota_exceeded"), 1);
  EXPECT_EQ(counterValue("success"), 0);
  EXPECT_EQ(counterValue("backend_error"), 0);
}

TEST_F(ModelRateLimitFilterTest, FailsOpenAndCountsBackendErrorWhenCheckLimitsFails) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  setEstimatedInputTokensInMetadata(37);
  expectCheckLimits("anthropic", "claude-haiku-4-5", 37,
                    absl::UnavailableError("quota backend unavailable"));

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());
  EXPECT_FALSE(reply_code_.has_value());
  EXPECT_TRUE(request_propagated_);

  EXPECT_EQ(counterValue("backend_error"), 1);
  EXPECT_EQ(counterValue("success"), 0);
  EXPECT_EQ(counterValue("quota_exceeded"), 0);
}

TEST_F(ModelRateLimitFilterTest, CancellingDecodeWhileCheckingLimitsDoesNotPropagateOrCountStats) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", 0))
      .WillOnce(
          [](std::string, std::string, uint64_t) { return MockQuota::makePendingCheckTask(); });

  auto filter = makeFilter();
  auto exec = std::make_shared<Coroutine::ManualExecutor>();
  std::optional<absl::Status> result;
  auto handle = Coroutine::launch(
      filter->decode(AiRequestReceiver([]() -> Coroutine::Task<absl::StatusOr<AiRequestPtr>> {
                       co_return std::make_unique<AiRequest>(JsonWithExtBuf{});
                     }),
                     AiRequestPropagator([this](AiRequestPtr) -> Coroutine::Task<absl::Status> {
                       request_propagated_ = true;
                       co_return absl::OkStatus();
                     }),
                     [](Http::Code, std::string) { FAIL() << "Unexpected local reply"; }),
      exec, [&result](absl::Status s) { result.emplace(std::move(s)); },
      Coroutine::StartMode::Inline);

  EXPECT_FALSE(result.has_value());
  handle.cancel();
  exec->drain();

  ASSERT_TRUE(result.has_value());
  EXPECT_THAT(*result, HasStatusCode(absl::StatusCode::kCancelled));
  EXPECT_FALSE(request_propagated_);
  EXPECT_EQ(counterValue("success"), 0);
  EXPECT_EQ(counterValue("backend_error"), 0);
}

TEST_F(ModelRateLimitFilterTest, ConsumesQuotaOnUnaryAndSseResponsesWithTokenUsage) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 120, 45);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 80, 25);

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());

  setTokenUsageInMetadata(120, 45);
  EXPECT_EQ(runEncode(*filter, /*sse=*/false), 1);

  setTokenUsageInMetadata(80, 25);
  EXPECT_EQ(runEncode(*filter, /*sse=*/true), 1);
  EXPECT_EQ(counterValue("backend_error"), 0);
}

TEST_F(ModelRateLimitFilterTest, FallsBackToEstimatedTokensAndHandlesMalformedMetadata) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");

  // 1. Malformed RequestInfo Any unpacks as 0 estimated tokens; missing TokenUsage consumes (0, 0).
  Protobuf::StringValue wrong_type;
  wrong_type.set_value("not_a_proto");
  Protobuf::Any bad_any;
  MessageUtil::packFrom(bad_any, wrong_type);
  stream_info_.setDynamicTypedMetadata("envoy.ai.request_info", bad_any);
  stream_info_.setDynamicTypedMetadata("envoy.ai.token_usage", bad_any);

  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 0, 0);
  auto filter1 = makeFilter();
  EXPECT_THAT(runDecode(*filter1), IsOk());
  EXPECT_EQ(runEncode(*filter1, /*sse=*/false), 1);

  // 2. RequestInfo without estimated_input_tokens field unpacks as 0; TokenUsage without
  // input_tokens falls back to estimated_input_tokens while taking output_tokens.
  Protobuf::Any empty_req_info;
  MessageUtil::packFrom(empty_req_info, envoy::data::ai::v3::RequestInfo{});
  stream_info_.setDynamicTypedMetadata("envoy.ai.request_info", empty_req_info);
  setTokenUsageInMetadata(std::nullopt, 33);

  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 0, 33);
  auto filter2 = makeFilter();
  EXPECT_THAT(runDecode(*filter2), IsOk());
  EXPECT_EQ(runEncode(*filter2, /*sse=*/false), 1);

  // 3. Valid estimated_input_tokens = 42 used when TokenUsage is absent.
  StreamInfo::StreamInfoImpl fresh_stream_info(api_->timeSource(), nullptr,
                                               StreamInfo::FilterState::LifeSpan::FilterChain);
  fresh_stream_info.filterState()->setData(
      FilterStateKeys::Provider, std::make_shared<Router::StringAccessorImpl>("anthropic"),
      StreamInfo::FilterState::LifeSpan::FilterChain);
  fresh_stream_info.filterState()->setData(
      FilterStateKeys::ModelResolved,
      std::make_shared<Router::StringAccessorImpl>("claude-haiku-4-5"),
      StreamInfo::FilterState::LifeSpan::FilterChain);
  envoy::data::ai::v3::RequestInfo info_with_est;
  info_with_est.mutable_estimated_input_tokens()->set_value(42);
  Protobuf::Any est_any;
  MessageUtil::packFrom(est_any, info_with_est);
  fresh_stream_info.setDynamicTypedMetadata("envoy.ai.request_info", est_any);

  expectCheckLimits("anthropic", "claude-haiku-4-5", 42, true);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 42, 0);
  ModelRateLimitFilter filter3(config_, AiFilterContext{fresh_stream_info, request_headers_,
                                                        LLMProtocol::OpenAiChatCompletions});
  EXPECT_THAT(runDecode(filter3), IsOk());
  EXPECT_EQ(runEncode(filter3, /*sse=*/false), 1);
}

TEST_F(ModelRateLimitFilterTest, ConsumeQuotaErrorIncrementsBackendErrorWithoutFailingResponse) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);
  expectConsumeQuota("anthropic", "claude-haiku-4-5", 10, 20,
                     absl::InternalError("backend write failed"));

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());
  setTokenUsageInMetadata(10, 20);

  EXPECT_EQ(runEncode(*filter, /*sse=*/false), 1);
  EXPECT_EQ(counterValue("backend_error"), 1);
}

TEST_F(ModelRateLimitFilterTest, UsesConfiguredCustomMetadataNamespaces) {
  auto slot = ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(tls_);
  slot->set([q = mock_quota_](Event::Dispatcher&) { return q; });
  auto custom_config = std::make_shared<ModelRateLimitFilterConfig>(
      std::move(slot), *stats_store_.rootScope(), "custom.ai.request_info",
      "custom.ai.token_usage");

  setResolvedModelAndProvider("openai", "gpt-4o");
  // Populate conflicting values in default namespaces to ensure custom namespaces are read instead.
  setEstimatedInputTokensInMetadata(999);
  setTokenUsageInMetadata(888, 777);

  envoy::data::ai::v3::RequestInfo custom_info;
  custom_info.mutable_estimated_input_tokens()->set_value(64);
  Protobuf::Any custom_info_any;
  MessageUtil::packFrom(custom_info_any, custom_info);
  stream_info_.setDynamicTypedMetadata("custom.ai.request_info", custom_info_any);

  envoy::data::ai::v3::TokenUsage custom_usage;
  custom_usage.mutable_input_tokens()->set_value(70);
  custom_usage.mutable_output_tokens()->set_value(30);
  Protobuf::Any custom_usage_any;
  MessageUtil::packFrom(custom_usage_any, custom_usage);
  stream_info_.setDynamicTypedMetadata("custom.ai.token_usage", custom_usage_any);

  expectCheckLimits("openai", "gpt-4o", 64, true);
  expectConsumeQuota("openai", "gpt-4o", 70, 30);

  ModelRateLimitFilter filter(custom_config, AiFilterContext{stream_info_, request_headers_,
                                                             LLMProtocol::OpenAiChatCompletions});
  EXPECT_THAT(runDecode(filter), IsOk());
  EXPECT_EQ(runEncode(filter, /*sse=*/false), 1);
}

TEST_F(ModelRateLimitFilterTest, EncodePropagatesStreamErrorsAndCancellationCleanly) {
  setResolvedModelAndProvider("anthropic", "claude-haiku-4-5");
  expectCheckLimits("anthropic", "claude-haiku-4-5", 0, true);

  auto filter = makeFilter();
  EXPECT_THAT(runDecode(*filter), IsOk());

  // 1. Propagate error in encodeSSE aborts before consumeQuota.
  const absl::Status sse_err = runTask(filter->encodeSSE(
      []() -> Coroutine::Task<absl::StatusOr<std::optional<SseEventPtr>>> {
        co_return std::make_unique<SseEvent>();
      },
      [](SseEventPtr) -> Coroutine::Task<absl::Status> {
        co_return absl::InternalError("downstream closed");
      }));
  EXPECT_THAT(sse_err, HasStatusCode(absl::StatusCode::kInternal));

  // 2. Propagate error in encodeUnary aborts before consumeQuota.
  const absl::Status unary_err = runTask(filter->encodeUnary(
      []() -> Coroutine::Task<absl::StatusOr<std::vector<FlattenJsonField>>> {
        co_return std::vector<FlattenJsonField>(1);
      },
      [](std::vector<FlattenJsonField>) -> Coroutine::Task<absl::Status> {
        co_return absl::InternalError("downstream closed");
      }));
  EXPECT_THAT(unary_err, HasStatusCode(absl::StatusCode::kInternal));

  // 3. Cancellation while suspended in consumeQuota propagates CancelledError without
  // backend_error.
  EXPECT_CALL(*mock_quota_, consumeQuota("anthropic", "claude-haiku-4-5", 0, 0))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makePendingConsumeTask();
      });

  auto exec = std::make_shared<Coroutine::ManualExecutor>();
  std::optional<absl::Status> cancel_res;
  auto handle = Coroutine::launch(
      filter->encodeUnary(
          []() -> Coroutine::Task<absl::StatusOr<std::vector<FlattenJsonField>>> {
            co_return std::vector<FlattenJsonField>{};
          },
          [](std::vector<FlattenJsonField>) -> Coroutine::Task<absl::Status> {
            co_return absl::OkStatus();
          }),
      exec, [&cancel_res](absl::Status s) { cancel_res.emplace(std::move(s)); },
      Coroutine::StartMode::Inline);

  EXPECT_FALSE(cancel_res.has_value());
  handle.cancel();
  exec->drain();

  ASSERT_TRUE(cancel_res.has_value());
  EXPECT_THAT(*cancel_res, HasStatusCode(absl::StatusCode::kCancelled));
  EXPECT_EQ(counterValue("backend_error"), 0);
}

} // namespace
} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
