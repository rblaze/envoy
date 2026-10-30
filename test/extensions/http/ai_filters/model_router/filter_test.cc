#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "envoy/data/ai/v3/token_usage.pb.h"
#include "envoy/extensions/http/ai_filters/model_router/v3/model_router.pb.h"
#include "envoy/extensions/http/ai_filters/request_info/v3/request_info.pb.h"
#include "envoy/type/ai/v3/llm_binding.pb.h"
#include "envoy/type/ai/v3/llm_protocol.pb.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/coroutine/task.h"
#include "source/common/protobuf/utility.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/common/stream_info/stream_info_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/filters/http/ai_protocol_manager/buffer_manager.h"
#include "source/extensions/filters/http/ai_protocol_manager/external_buffer_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/filter_manager.h"
#include "source/extensions/filters/http/ai_protocol_manager/json_with_ext_buf.h"
#include "source/extensions/http/ai_filters/model_router/filter.h"
#include "source/extensions/http/ai_filters/model_router/local_quota.h"
#include "source/extensions/http/ai_filters/model_router/quota_interface.h"
#include "source/extensions/http/ai_filters/model_router/route_wrapper.h"
#include "source/extensions/http/ai_filters/request_info/filter.h"

#include "test/extensions/filters/http/ai_protocol_manager/fake_bridge.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/thread_local/mocks.h"
#include "test/test_common/utility.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "nlohmann/json.hpp"

using testing::NiceMock;
using testing::StrictMock;

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {
namespace {

using HttpFilters::AiProtocolManager::AiFilterContext;
using HttpFilters::AiProtocolManager::AiFilterSharedPtr;
using HttpFilters::AiProtocolManager::BufferManager;
using HttpFilters::AiProtocolManager::FakeBridge;
using HttpFilters::AiProtocolManager::FilterManager;
using HttpFilters::AiProtocolManager::InMemoryExternalBufferFactory;
using HttpFilters::AiProtocolManager::JsonWithExtBuf;
using HttpFilters::AiProtocolManager::LLMProtocol;
namespace FilterStateKeys = HttpFilters::AiProtocolManager::FilterStateKeys;
using ModelRouterProto = envoy::extensions::http::ai_filters::model_router::v3::ModelRouter;

class MockQuota : public QuotaInterface {
public:
  static Coroutine::Task<absl::StatusOr<bool>> makeCheckTask(absl::StatusOr<bool> res) {
    co_return res;
  }
  static Coroutine::Task<absl::Status> makeConsumeTask(absl::Status res) { co_return res; }

  MOCK_METHOD(Coroutine::Task<absl::StatusOr<bool>>, checkLimits,
              (std::string provider, std::string model, uint64_t estimated_input_tokens),
              (override));
  MOCK_METHOD(Coroutine::Task<absl::Status>, consumeQuota,
              (std::string provider, std::string model, uint64_t input_tokens,
               uint64_t output_tokens),
              (override));
};

class ModelRouterFilterTest : public testing::Test {
public:
  ModelRouterFilterTest()
      : api_(Api::createApiForTest()), dispatcher_(api_->allocateDispatcher("test")),
        bridge_(*dispatcher_), buffer_manager_(BufferManager::Config{}, factory_, bridge_),
        stream_info_(api_->timeSource(), nullptr, StreamInfo::FilterState::LifeSpan::FilterChain),
        mock_quota_(std::make_shared<StrictMock<MockQuota>>()) {}

  ~ModelRouterFilterTest() override { buffer_manager_.onDestroy(); }

  ModelRouterProto defaultProto() {
    ModelRouterProto proto;

    auto* anthropic_prov = proto.add_providers();
    anthropic_prov->set_name("anthropic");
    anthropic_prov->set_llm_protocol(envoy::type::ai::v3::ANTHROPIC_MESSAGES);
    anthropic_prov->set_host_and_port("api.anthropic.com");
    anthropic_prov->mutable_binding()->set_uri_pattern("/v1/messages");

    auto* primary_openai_prov = proto.add_providers();
    primary_openai_prov->set_name("primary_openai");
    primary_openai_prov->set_llm_protocol(envoy::type::ai::v3::OPENAI_CHAT_COMPLETIONS);
    primary_openai_prov->set_host_and_port("primary.llm.example.com:8443");
    primary_openai_prov->mutable_binding()->set_uri_pattern("/v1/chat/completions");

    auto* primary_anthropic_prov = proto.add_providers();
    primary_anthropic_prov->set_name("primary_anthropic");
    primary_anthropic_prov->set_llm_protocol(envoy::type::ai::v3::ANTHROPIC_MESSAGES);
    primary_anthropic_prov->set_host_and_port("primary.llm.example.com:8443");
    primary_anthropic_prov->mutable_binding()->set_uri_pattern("/v1/messages");

    auto* primary_gemini_prov = proto.add_providers();
    primary_gemini_prov->set_name("primary_gemini");
    primary_gemini_prov->set_llm_protocol(envoy::type::ai::v3::GEMINI_GENERATE_CONTENT);
    primary_gemini_prov->set_host_and_port("primary.llm.example.com:8443");
    primary_gemini_prov->mutable_binding()->set_uri_pattern(
        "/v1beta/models/{model}:{generateContent|streamGenerateContent}");

    auto* haiku_model = proto.add_models();
    haiku_model->set_name("haiku");
    haiku_model->set_provider("anthropic");
    haiku_model->set_model("claude-haiku-4-5");
    haiku_model->set_api_key("Bearer sk-ant");

    auto* gpt4o_model = proto.add_models();
    gpt4o_model->set_name("gpt4o");
    gpt4o_model->set_provider("primary_openai");
    gpt4o_model->set_model("gpt-4o");
    gpt4o_model->set_api_key("Bearer sk-oai");

    auto* sonnet_model = proto.add_models();
    sonnet_model->set_name("sonnet");
    sonnet_model->set_provider("primary_anthropic");
    sonnet_model->set_model("claude-sonnet-4-5");
    sonnet_model->set_api_key("Bearer sk-sonnet");

    auto* gemini_pro_model = proto.add_models();
    gemini_pro_model->set_name("gemini_pro");
    gemini_pro_model->set_provider("primary_gemini");
    gemini_pro_model->set_model("gemini-2.5-pro");
    gemini_pro_model->set_api_key("Bearer g-pro");

    auto* single_entry = proto.add_routes();
    single_entry->set_requested_model("send_me_to_anthropic");
    single_entry->add_targets()->set_model("haiku");

    auto* multi_entry = proto.add_routes();
    multi_entry->set_requested_model("multi_fallback_model");
    multi_entry->add_targets()->set_model("gpt4o");
    multi_entry->add_targets()->set_model("sonnet");
    multi_entry->add_targets()->set_model("gemini_pro");

    auto* empty_entry = proto.add_routes();
    empty_entry->set_requested_model("empty_targets_model");

    return proto;
  }

  ModelRouterFilterConfigSharedPtr
  defaultConfig(ModelRouterFilterConfig::QuotaPtr tl_quota = nullptr) {
    auto config_or = ModelRouterFilterConfig::create(defaultProto(), *stats_store_.rootScope());
    EXPECT_TRUE(config_or.ok()) << config_or.status();
    (*config_or)->setQuotas(std::move(tl_quota));
    return *std::move(config_or);
  }

  ModelRouterFilterConfig::QuotaPtr makeMockQuotaSlot() {
    auto slot = ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(tls_);
    slot->set([q = mock_quota_](Event::Dispatcher&) { return q; });
    return slot;
  }

  void setModelInFilterState(absl::string_view model_name) {
    stream_info_.filterState()->setData(FilterStateKeys::ModelRequest,
                                        std::make_shared<Router::StringAccessorImpl>(model_name),
                                        StreamInfo::FilterState::LifeSpan::FilterChain);
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

  std::string runResponse(const std::string& body, bool sse,
                          ModelRouterFilterConfigSharedPtr config = nullptr,
                          std::optional<uint32_t> response_code = 200) {
    if (response_code.has_value()) {
      stream_info_.setResponseCode(*response_code);
    }
    FakeBridge response_bridge(*dispatcher_);
    BufferManager out_buffer(BufferManager::Config{}, factory_, response_bridge);
    const AiFilterContext context{stream_info_, request_headers_,
                                  LLMProtocol::OpenAiChatCompletions};
    std::vector<AiFilterSharedPtr> filters;
    filters.push_back(std::make_shared<ModelRouterFilter>(
        config != nullptr ? std::move(config) : defaultConfig(), context));
    FilterManager manager(std::move(filters));

    absl::Status status;
    bool done = false;
    auto on_done = [&status, &done](absl::Status s) {
      status = std::move(s);
      done = true;
    };
    if (sse) {
      manager.startSseResponse(factory_, response_bridge, out_buffer, on_done);
    } else {
      manager.startUnaryResponse(factory_, response_bridge, out_buffer, on_done);
    }
    Buffer::OwnedImpl data(body);
    manager.onResponseData(data, /*end_stream=*/true);
    for (int i = 0; i < 20; ++i) {
      dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
    }
    EXPECT_TRUE(done);
    EXPECT_TRUE(status.ok()) << status;
    std::string result = response_bridge.injected_.toString();
    out_buffer.onDestroy();
    return result;
  }

  std::string run(const std::string& payload = R"({"model":"send_me_to_anthropic","messages":[]})",
                  ModelRouterFilterConfigSharedPtr config = nullptr, bool with_request_info = false,
                  bool expect_local_reply = false,
                  std::optional<double> tokens_per_byte = std::nullopt) {
    reply_code_.reset();
    reply_details_.clear();
    bridge_.injected_.drain(bridge_.injected_.length());
    request_headers_ = Http::TestRequestHeaderMapImpl{
        {":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "original.host"}};
    JsonWithExtBuf doc;
    doc.setJson(nlohmann::json::parse(payload));

    const AiFilterContext context{stream_info_, request_headers_,
                                  LLMProtocol::OpenAiChatCompletions, payload.size()};
    std::vector<AiFilterSharedPtr> filters;
    if (with_request_info) {
      envoy::extensions::http::ai_filters::request_info::v3::RequestInfo req_info_proto;
      if (tokens_per_byte.has_value()) {
        req_info_proto.mutable_token_estimation()->set_tokens_per_byte(*tokens_per_byte);
      }
      auto req_info_config = std::make_shared<const RequestInfo::RequestInfoFilterConfig>(
          req_info_proto, *stats_store_.rootScope());
      filters.push_back(
          std::make_shared<RequestInfo::RequestInfoFilter>(std::move(req_info_config), context));
    }
    filters.push_back(std::make_shared<ModelRouterFilter>(
        config != nullptr ? std::move(config) : defaultConfig(), context));
    FilterManager manager(std::move(filters));

    absl::Status status;
    bool completed = false;
    manager.startRequest(
        std::move(doc), &buffer_manager_, *dispatcher_, stream_info_,
        [&status, &completed](absl::Status s) {
          status = std::move(s);
          completed = true;
        },
        &request_headers_,
        [this](Http::Code code, std::string details) {
          reply_code_ = code;
          reply_details_ = std::move(details);
        },
        /*always_serialize=*/true, LLMProtocol::OpenAiChatCompletions);
    for (int i = 0; i < 20; ++i) {
      dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
    }
    EXPECT_TRUE(completed);
    if (expect_local_reply) {
      EXPECT_TRUE(absl::IsCancelled(status)) << status;
    } else {
      EXPECT_TRUE(status.ok()) << status;
    }
    return bridge_.injected_.toString();
  }

  const LlmRoutesFilterStateObject* resolvedRoutes() {
    return stream_info_.filterState()->getDataReadOnly<LlmRoutesFilterStateObject>(
        FilterStateKeys::ResolvedRoutes);
  }

  uint64_t counterValue(const std::string& name) {
    const auto counter =
        TestUtility::findCounter(stats_store_, "ai_protocol_manager.model_router." + name);
    return counter != nullptr ? counter->value() : 0;
  }

  Api::ApiPtr api_;
  Event::DispatcherPtr dispatcher_;
  InMemoryExternalBufferFactory factory_;
  FakeBridge bridge_;
  BufferManager buffer_manager_;
  StreamInfo::StreamInfoImpl stream_info_;
  NiceMock<Stats::MockIsolatedStatsStore> stats_store_;
  NiceMock<ThreadLocal::MockInstance> tls_;
  std::shared_ptr<StrictMock<MockQuota>> mock_quota_;
  Http::TestRequestHeaderMapImpl request_headers_;
  std::optional<Http::Code> reply_code_;
  std::string reply_details_;
};

TEST_F(ModelRouterFilterTest, ModelNameMissingInFilterState) {
  const std::string payload = R"({"model":"send_me_to_anthropic","messages":[]})";
  const std::string replayed = run(payload);
  EXPECT_EQ(nlohmann::json::parse(replayed), nlohmann::json::parse(payload));

  EXPECT_EQ(resolvedRoutes(), nullptr);
  EXPECT_EQ(request_headers_.getHostValue(), "original.host");
  EXPECT_TRUE(request_headers_.getEnvoyMaxRetriesValue().empty());

  EXPECT_EQ(counterValue("model_name_missing"), 1);
  EXPECT_EQ(counterValue("route_not_found"), 0);
  EXPECT_EQ(counterValue("success"), 0);
}

TEST_F(ModelRouterFilterTest, RouteNotFoundForModel) {
  setModelInFilterState("unknown_model");
  run();

  EXPECT_EQ(resolvedRoutes(), nullptr);
  EXPECT_EQ(request_headers_.getHostValue(), "original.host");
  EXPECT_TRUE(request_headers_.getEnvoyMaxRetriesValue().empty());

  EXPECT_EQ(counterValue("model_name_missing"), 0);
  EXPECT_EQ(counterValue("route_not_found"), 1);
  EXPECT_EQ(counterValue("success"), 0);
}

TEST_F(ModelRouterFilterTest, SingleTargetRouteResolved) {
  setModelInFilterState("send_me_to_anthropic");
  run();

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 1);
  EXPECT_EQ(routes_obj->proto().routes(0).llm_protocol(), envoy::type::ai::v3::ANTHROPIC_MESSAGES);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "claude-haiku-4-5");
  EXPECT_EQ(routes_obj->proto().routes(0).provider(), "anthropic");
  EXPECT_EQ(routes_obj->proto().routes(0).host_and_port(), "api.anthropic.com");
  EXPECT_EQ(routes_obj->proto().routes(0).api_key(), "Bearer sk-ant");
  ASSERT_TRUE(routes_obj->proto().routes(0).has_binding());
  EXPECT_EQ(routes_obj->proto().routes(0).binding().uri_pattern(), "/v1/messages");

  EXPECT_EQ(request_headers_.getHostValue(), "api.anthropic.com");
  EXPECT_EQ(request_headers_.getEnvoyMaxRetriesValue(), "0");

  EXPECT_EQ(counterValue("model_name_missing"), 0);
  EXPECT_EQ(counterValue("route_not_found"), 0);
  EXPECT_EQ(counterValue("success"), 1);
}

TEST_F(ModelRouterFilterTest, MultipleTargetsRouteSetsMaxRetriesAndPrimaryAuthority) {
  setModelInFilterState("multi_fallback_model");
  run();

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 3);
  EXPECT_EQ(routes_obj->proto().routes(0).llm_protocol(),
            envoy::type::ai::v3::OPENAI_CHAT_COMPLETIONS);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "gpt-4o");
  EXPECT_EQ(routes_obj->proto().routes(0).provider(), "primary_openai");
  EXPECT_EQ(routes_obj->proto().routes(0).api_key(), "Bearer sk-oai");
  EXPECT_EQ(routes_obj->proto().routes(0).binding().uri_pattern(), "/v1/chat/completions");

  EXPECT_EQ(routes_obj->proto().routes(1).llm_protocol(), envoy::type::ai::v3::ANTHROPIC_MESSAGES);
  EXPECT_EQ(routes_obj->proto().routes(1).model(), "claude-sonnet-4-5");
  EXPECT_EQ(routes_obj->proto().routes(1).provider(), "primary_anthropic");
  EXPECT_EQ(routes_obj->proto().routes(1).api_key(), "Bearer sk-sonnet");
  EXPECT_EQ(routes_obj->proto().routes(1).binding().uri_pattern(), "/v1/messages");

  EXPECT_EQ(routes_obj->proto().routes(2).llm_protocol(),
            envoy::type::ai::v3::GEMINI_GENERATE_CONTENT);
  EXPECT_EQ(routes_obj->proto().routes(2).model(), "gemini-2.5-pro");
  EXPECT_EQ(routes_obj->proto().routes(2).provider(), "primary_gemini");
  EXPECT_EQ(routes_obj->proto().routes(2).api_key(), "Bearer g-pro");
  EXPECT_EQ(routes_obj->proto().routes(2).binding().uri_pattern(),
            "/v1beta/models/{model}:{generateContent|streamGenerateContent}");

  EXPECT_EQ(request_headers_.getHostValue(), "primary.llm.example.com:8443");
  EXPECT_EQ(request_headers_.getEnvoyMaxRetriesValue(), "2");

  EXPECT_EQ(counterValue("success"), 1);
}

TEST_F(ModelRouterFilterTest, EmptyTargetsRouteDoesNotModifyHeaders) {
  setModelInFilterState("empty_targets_model");
  run();

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  EXPECT_EQ(routes_obj->proto().routes_size(), 0);

  EXPECT_EQ(request_headers_.getHostValue(), "original.host");
  EXPECT_TRUE(request_headers_.getEnvoyMaxRetriesValue().empty());

  EXPECT_EQ(counterValue("success"), 1);
  EXPECT_EQ(counterValue("quota_exceeded"), 0);
}

TEST_F(ModelRouterFilterTest, ChainedWithRequestInfoFilter) {
  const std::string payload =
      R"({"model":"send_me_to_anthropic","messages":[{"role":"user","content":"hello"}]})";
  const std::string replayed = run(payload, /*config=*/nullptr, /*with_request_info=*/true);
  EXPECT_EQ(nlohmann::json::parse(replayed), nlohmann::json::parse(payload));

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 1);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "claude-haiku-4-5");
  EXPECT_EQ(request_headers_.getHostValue(), "api.anthropic.com");
  EXPECT_EQ(request_headers_.getEnvoyMaxRetriesValue(), "0");
  EXPECT_EQ(counterValue("success"), 1);
}

TEST_F(ModelRouterFilterTest, FiltersOutModelsOverQuota) {
  EXPECT_CALL(*mock_quota_, checkLimits("primary_openai", "gpt-4o", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(false); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_anthropic", "claude-sonnet-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_gemini", "gemini-2.5-pro", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(false); });

  setModelInFilterState("multi_fallback_model");
  run(R"({"model":"multi_fallback_model","messages":[]})", defaultConfig(makeMockQuotaSlot()));

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 1);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "claude-sonnet-4-5");
  EXPECT_EQ(routes_obj->proto().routes(0).provider(), "primary_anthropic");
  EXPECT_EQ(request_headers_.getHostValue(), "primary.llm.example.com:8443");
  EXPECT_EQ(request_headers_.getEnvoyMaxRetriesValue(), "0");
  EXPECT_EQ(counterValue("success"), 1);
  EXPECT_EQ(counterValue("quota_exceeded"), 0);
}

TEST_F(ModelRouterFilterTest, Returns429WhenAllModelsAreOverQuota) {
  EXPECT_CALL(*mock_quota_, checkLimits("primary_openai", "gpt-4o", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(false); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_anthropic", "claude-sonnet-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(false); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_gemini", "gemini-2.5-pro", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(false); });

  setModelInFilterState("multi_fallback_model");
  run(R"({"model":"multi_fallback_model","messages":[]})", defaultConfig(makeMockQuotaSlot()),
      /*with_request_info=*/false, /*expect_local_reply=*/true);

  EXPECT_EQ(reply_code_, Http::Code::TooManyRequests);
  EXPECT_EQ(reply_details_, "Quota exceeded for all candidate models");
  EXPECT_EQ(resolvedRoutes(), nullptr);
  EXPECT_EQ(request_headers_.getHostValue(), "original.host");
  EXPECT_TRUE(request_headers_.getEnvoyMaxRetriesValue().empty());
  EXPECT_EQ(counterValue("quota_exceeded"), 1);
  EXPECT_EQ(counterValue("success"), 0);
}

TEST_F(ModelRouterFilterTest, PassesEstimatedInputTokensFromRequestInfoAndFailsOpenOnError) {
  const std::string payload =
      R"({"model":"send_me_to_anthropic","messages":[{"role":"user","content":"hello"}]})";
  // ceil(payload.size() * 0.5)
  const uint64_t expected_tokens = (payload.size() + 1) / 2;

  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", expected_tokens))
      .WillOnce([](std::string, std::string, uint64_t) {
        return MockQuota::makeCheckTask(absl::UnavailableError("quota backend unavailable"));
      });

  run(payload, defaultConfig(makeMockQuotaSlot()), /*with_request_info=*/true,
      /*expect_local_reply=*/false, /*tokens_per_byte=*/0.5);

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 1);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "claude-haiku-4-5");
  EXPECT_EQ(counterValue("success"), 1);
  EXPECT_EQ(counterValue("quota_exceeded"), 0);
}

TEST_F(ModelRouterFilterTest, EnforcesLocalQuotaWithZeroLimitDisablingPrimaryModel) {
  namespace Config = envoy::extensions::http::ai_filters::model_router::v3;
  ModelRouterProto proto = defaultProto();
  // Set limit=0 on gpt4o (the first target of multi_fallback_model).
  for (auto& model : *proto.mutable_models()) {
    if (model.name() == "gpt4o") {
      auto* q = model.add_quotas();
      q->set_quota_type(Config::REQUESTS);
      q->set_time_unit(Config::MINUTE);
      q->set_limit(0);
    }
  }

  auto slot = ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(tls_);
  slot->set([base_quota = LocalQuota(proto, api_->timeSource())](Event::Dispatcher&) {
    return std::make_shared<LocalQuota>(base_quota);
  });
  auto config_or = ModelRouterFilterConfig::create(proto, *stats_store_.rootScope());
  ASSERT_TRUE(config_or.ok()) << config_or.status();
  (*config_or)->setQuotas(std::move(slot));

  setModelInFilterState("multi_fallback_model");
  run(R"({"model":"multi_fallback_model","messages":[]})", *std::move(config_or));

  const auto* routes_obj = resolvedRoutes();
  ASSERT_NE(routes_obj, nullptr);
  ASSERT_EQ(routes_obj->proto().routes_size(), 2);
  EXPECT_EQ(routes_obj->proto().routes(0).model(), "claude-sonnet-4-5");
  EXPECT_EQ(routes_obj->proto().routes(1).model(), "gemini-2.5-pro");
  EXPECT_EQ(request_headers_.getEnvoyMaxRetriesValue(), "1");
  EXPECT_EQ(counterValue("success"), 1);
}

TEST_F(ModelRouterFilterTest, ConsumesQuotaOnUnaryResponseWithTokenUsage) {
  auto config = defaultConfig(makeMockQuotaSlot());
  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, consumeQuota("anthropic", "claude-haiku-4-5", 120, 45))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makeConsumeTask(absl::OkStatus());
      });

  setModelInFilterState("send_me_to_anthropic");
  run(R"({"model":"send_me_to_anthropic","messages":[]})", config);

  setTokenUsageInMetadata(120, 45);
  const std::string response_body = R"({"id":"msg_1","content":"hi"})";
  const std::string wire = runResponse(response_body, /*sse=*/false, config);
  EXPECT_EQ(nlohmann::json::parse(wire), nlohmann::json::parse(response_body));
}

TEST_F(ModelRouterFilterTest, ConsumesQuotaOnSseResponseWithTokenUsage) {
  auto config = defaultConfig(makeMockQuotaSlot());
  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, consumeQuota("anthropic", "claude-haiku-4-5", 80, 25))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makeConsumeTask(absl::OkStatus());
      });

  setModelInFilterState("send_me_to_anthropic");
  run(R"({"model":"send_me_to_anthropic","messages":[]})", config);

  setTokenUsageInMetadata(80, 25);
  const std::string sse_body =
      "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"id\":\"1\"}\n\n"
      "data: [DONE]\n\n";
  const std::string wire = runResponse(sse_body, /*sse=*/true, config);
  EXPECT_EQ(wire, sse_body);
}

TEST_F(ModelRouterFilterTest, DoesNotConsumeQuotaOnNon2xxOrMissingResponseCode) {
  auto config = defaultConfig(makeMockQuotaSlot());
  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, consumeQuota(testing::_, testing::_, testing::_, testing::_)).Times(0);

  setModelInFilterState("send_me_to_anthropic");
  run(R"({"model":"send_me_to_anthropic","messages":[]})", config);
  setTokenUsageInMetadata(100, 50);

  // Missing response code (nullopt).
  const std::string response_body = R"({"error":"bad"})";
  EXPECT_EQ(nlohmann::json::parse(
                runResponse(response_body, /*sse=*/false, config, /*response_code=*/std::nullopt)),
            nlohmann::json::parse(response_body));

  // 500 Internal Server Error on unary response.
  EXPECT_EQ(nlohmann::json::parse(
                runResponse(response_body, /*sse=*/false, config, /*response_code=*/500)),
            nlohmann::json::parse(response_body));

  // 429 Too Many Requests on SSE response.
  const std::string sse_body = "data: {\"error\":\"rate_limited\"}\n\n";
  EXPECT_EQ(runResponse(sse_body, /*sse=*/true, config, /*response_code=*/429), sse_body);
}

TEST_F(ModelRouterFilterTest, ConsumesQuotaUsesAttemptCountForRetriedRoute) {
  auto config = defaultConfig(makeMockQuotaSlot());
  EXPECT_CALL(*mock_quota_, checkLimits("primary_openai", "gpt-4o", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_anthropic", "claude-sonnet-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, checkLimits("primary_gemini", "gemini-2.5-pro", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });

  // Attempt 2 corresponds to index 1: primary_anthropic / claude-sonnet-4-5.
  EXPECT_CALL(*mock_quota_, consumeQuota("primary_anthropic", "claude-sonnet-4-5", 200, 100))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makeConsumeTask(absl::OkStatus());
      });

  setModelInFilterState("multi_fallback_model");
  run(R"({"model":"multi_fallback_model","messages":[]})", config);

  stream_info_.setAttemptCount(2);
  setTokenUsageInMetadata(200, 100);
  runResponse(R"({"id":"msg_2"})", /*sse=*/false, config);
}

TEST_F(ModelRouterFilterTest, ConsumesQuotaFallsBackToEstimatedInputTokensWhenTokenUsageMissing) {
  auto config = defaultConfig(makeMockQuotaSlot());
  const std::string payload =
      R"({"model":"send_me_to_anthropic","messages":[{"role":"user","content":"hello"}]})";
  const uint64_t expected_estimated_tokens = (payload.size() + 1) / 2;

  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", expected_estimated_tokens))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_,
              consumeQuota("anthropic", "claude-haiku-4-5", expected_estimated_tokens, 0))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makeConsumeTask(absl::OkStatus());
      });

  run(payload, config, /*with_request_info=*/true, /*expect_local_reply=*/false,
      /*tokens_per_byte=*/0.5);

  // Do not set envoy.ai.token_usage metadata; consumeQuota falls back to estimated_input_tokens.
  runResponse(R"({"id":"msg_3"})", /*sse=*/false, config);
}

TEST_F(ModelRouterFilterTest, ConsumesQuotaLogsWarningAndSucceedsOnQuotaErrorOrOutOfBoundsAttempt) {
  auto config = defaultConfig(makeMockQuotaSlot());
  EXPECT_CALL(*mock_quota_, checkLimits("anthropic", "claude-haiku-4-5", 0))
      .WillOnce([](std::string, std::string, uint64_t) { return MockQuota::makeCheckTask(true); });
  EXPECT_CALL(*mock_quota_, consumeQuota("anthropic", "claude-haiku-4-5", 10, 20))
      .WillOnce([](std::string, std::string, uint64_t, uint64_t) {
        return MockQuota::makeConsumeTask(absl::InternalError("backend write failed"));
      });

  setModelInFilterState("send_me_to_anthropic");
  run(R"({"model":"send_me_to_anthropic","messages":[]})", config);
  setTokenUsageInMetadata(10, 20);

  // Non-OK status from consumeQuota is logged and does not fail the response.
  runResponse(R"({"id":"msg_4"})", /*sse=*/false, config);

  // Out-of-bounds attempt count logs a warning and skips consumeQuota without failing.
  stream_info_.setAttemptCount(5);
  runResponse(R"({"id":"msg_5"})", /*sse=*/false, config);
}

TEST_F(ModelRouterFilterTest, EndToEndLocalQuotaExhaustionAcrossRequests) {
  namespace Config = envoy::extensions::http::ai_filters::model_router::v3;
  ModelRouterProto proto = defaultProto();
  // Configure gpt4o with 1 request/min and sonnet with 100 tokens/min.
  for (auto& model : *proto.mutable_models()) {
    if (model.name() == "gpt4o") {
      auto* q = model.add_quotas();
      q->set_quota_type(Config::REQUESTS);
      q->set_time_unit(Config::MINUTE);
      q->set_limit(1);
    } else if (model.name() == "sonnet") {
      auto* q = model.add_quotas();
      q->set_quota_type(Config::TOKENS);
      q->set_time_unit(Config::MINUTE);
      q->set_limit(100);
    }
  }

  auto slot = ThreadLocal::TypedSlot<QuotaInterface>::makeUnique(tls_);
  slot->set([base_quota = LocalQuota(proto, api_->timeSource())](Event::Dispatcher&) {
    return std::make_shared<LocalQuota>(base_quota);
  });
  auto config_or = ModelRouterFilterConfig::create(proto, *stats_store_.rootScope());
  ASSERT_TRUE(config_or.ok()) << config_or.status();
  (*config_or)->setQuotas(std::move(slot));
  auto config = *std::move(config_or);

  // Request 1: all 3 models are within quota; gpt4o (route 0) is primary.
  setModelInFilterState("multi_fallback_model");
  run(R"({"model":"multi_fallback_model","messages":[]})", config);
  ASSERT_NE(resolvedRoutes(), nullptr);
  ASSERT_EQ(resolvedRoutes()->proto().routes_size(), 3);
  EXPECT_EQ(resolvedRoutes()->proto().routes(0).model(), "gpt-4o");

  // Response 1 completes on attempt 1 (gpt4o), consuming its 1 request/min quota.
  setTokenUsageInMetadata(10, 10);
  runResponse(R"({"id":"resp_1"})", /*sse=*/false, config);

  // Request 2: gpt4o is now over request quota, so sonnet becomes route 0 (2 routes left).
  run(R"({"model":"multi_fallback_model","messages":[]})", config);
  ASSERT_NE(resolvedRoutes(), nullptr);
  ASSERT_EQ(resolvedRoutes()->proto().routes_size(), 2);
  EXPECT_EQ(resolvedRoutes()->proto().routes(0).model(), "claude-sonnet-4-5");
  EXPECT_EQ(resolvedRoutes()->proto().routes(1).model(), "gemini-2.5-pro");

  // Response 2 completes on attempt 1 (now sonnet), consuming 60 + 50 = 110 tokens (> 100 limit).
  setTokenUsageInMetadata(60, 50);
  runResponse("data: {\"id\":\"resp_2\"}\n\n", /*sse=*/true, config);

  // Request 3: both gpt4o and sonnet are now over quota; only gemini-2.5-pro remains.
  run(R"({"model":"multi_fallback_model","messages":[]})", config);
  ASSERT_NE(resolvedRoutes(), nullptr);
  ASSERT_EQ(resolvedRoutes()->proto().routes_size(), 1);
  EXPECT_EQ(resolvedRoutes()->proto().routes(0).model(), "gemini-2.5-pro");
}

} // namespace
} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
