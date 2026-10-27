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

#include "source/common/coroutine/task.h"
#include "source/common/router/string_accessor_impl.h"
#include "source/common/stream_info/stream_info_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"
#include "source/extensions/filters/http/ai_protocol_manager/ai_filter_state.h"
#include "source/extensions/filters/http/ai_protocol_manager/buffer_manager.h"
#include "source/extensions/filters/http/ai_protocol_manager/external_buffer_impl.h"
#include "source/extensions/filters/http/ai_protocol_manager/filter_manager.h"
#include "source/extensions/filters/http/ai_protocol_manager/json_with_ext_buf.h"
#include "source/extensions/http/ai_filters/model_router/database_interface.h"
#include "source/extensions/http/ai_filters/model_router/filter.h"
#include "source/extensions/http/ai_filters/model_router/route_wrapper.h"
#include "source/extensions/http/ai_filters/request_info/filter.h"

#include "test/extensions/filters/http/ai_protocol_manager/fake_bridge.h"
#include "test/mocks/stats/mocks.h"
#include "test/mocks/thread_local/mocks.h"
#include "test/test_common/utility.h"

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

class MockDatabase : public DatabaseInterface {
public:
  static Coroutine::Task<absl::Status> makeStatusTask(absl::Status status) { co_return status; }

  MOCK_METHOD(Coroutine::Task<absl::Status>, recordQueryStats,
              (Envoy::SystemTime request_time, uint64_t input_tokens, uint64_t output_tokens,
               std::string model, std::string provider, std::string request_id),
              (override));
};

class ModelRouterFilterTest : public testing::Test {
public:
  ModelRouterFilterTest()
      : api_(Api::createApiForTest()), dispatcher_(api_->allocateDispatcher("test")),
        bridge_(*dispatcher_), buffer_manager_(BufferManager::Config{}, factory_, bridge_),
        response_buffer_manager_(BufferManager::Config{}, factory_, bridge_),
        stream_info_(api_->timeSource(), nullptr, StreamInfo::FilterState::LifeSpan::FilterChain),
        mock_db_(std::make_shared<StrictMock<MockDatabase>>()) {
    ON_CALL(*mock_db_, recordQueryStats)
        .WillByDefault([](Envoy::SystemTime, uint64_t, uint64_t, std::string, std::string,
                          std::string) { return MockDatabase::makeStatusTask(absl::OkStatus()); });
  }

  ~ModelRouterFilterTest() override {
    buffer_manager_.onDestroy();
    response_buffer_manager_.onDestroy();
  }

  ModelRouterFilterConfigSharedPtr defaultConfig(bool with_db = false) {
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

    ModelRouterFilterConfig::DbPtr tl_database;
    if (with_db) {
      tl_database = ThreadLocal::TypedSlot<DatabaseInterface>::makeUnique(tls_);
      tl_database->set([db = mock_db_](Event::Dispatcher&) { return db; });
    }

    auto config_or =
        ModelRouterFilterConfig::create(proto, *stats_store_.rootScope(), std::move(tl_database));
    EXPECT_TRUE(config_or.ok()) << config_or.status();
    return *std::move(config_or);
  }

  void setModelInFilterState(absl::string_view model_name) {
    stream_info_.filterState()->setData(FilterStateKeys::ModelRequest,
                                        std::make_shared<Router::StringAccessorImpl>(model_name),
                                        StreamInfo::FilterState::LifeSpan::FilterChain);
  }

  void setTokenUsageInMetadata(uint64_t input_tokens, uint64_t output_tokens,
                               envoy::data::ai::v3::TokenUsage::ExtractionStatus status =
                                   envoy::data::ai::v3::TokenUsage::COMPLETE) {
    envoy::data::ai::v3::TokenUsage token_usage;
    token_usage.set_extraction_status(status);
    if (status != envoy::data::ai::v3::TokenUsage::FAILED) {
      token_usage.mutable_input_tokens()->set_value(input_tokens);
      token_usage.mutable_output_tokens()->set_value(output_tokens);
      token_usage.mutable_total_tokens()->set_value(input_tokens + output_tokens);
    }
    Protobuf::Any typed_any;
    MessageUtil::packFrom(typed_any, token_usage);
    stream_info_.setDynamicTypedMetadata("envoy.ai.token_usage", typed_any);
  }

  std::unique_ptr<FilterManager>
  startDecode(const std::string& payload = R"({"model":"send_me_to_anthropic","messages":[]})",
              ModelRouterFilterConfigSharedPtr config = nullptr, bool with_request_info = false) {
    request_headers_ = Http::TestRequestHeaderMapImpl{{":method", "POST"},
                                                      {":path", "/v1/chat/completions"},
                                                      {":authority", "original.host"},
                                                      {"x-request-id", "test-req-id"}};
    JsonWithExtBuf doc;
    doc.setJson(nlohmann::json::parse(payload));

    const AiFilterContext context{*dispatcher_, stream_info_, request_headers_,
                                  LLMProtocol::OpenAiChatCompletions, payload.size()};
    std::vector<AiFilterSharedPtr> filters;
    if (with_request_info) {
      envoy::extensions::http::ai_filters::request_info::v3::RequestInfo req_info_proto;
      auto req_info_config = std::make_shared<const RequestInfo::RequestInfoFilterConfig>(
          req_info_proto, *stats_store_.rootScope());
      filters.push_back(
          std::make_shared<RequestInfo::RequestInfoFilter>(std::move(req_info_config), context));
    }
    filters.push_back(std::make_shared<ModelRouterFilter>(
        config != nullptr ? std::move(config) : defaultConfig(), context));
    auto manager = std::make_unique<FilterManager>(std::move(filters));

    absl::Status status;
    bool completed = false;
    manager->startRequest(
        std::move(doc), &buffer_manager_, *dispatcher_, stream_info_,
        [&status, &completed](absl::Status s) {
          status = std::move(s);
          completed = true;
        },
        &request_headers_, /*local_reply_fn=*/nullptr, /*always_serialize=*/true,
        LLMProtocol::OpenAiChatCompletions);
    for (int i = 0; i < 20; ++i) {
      dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
    }
    EXPECT_TRUE(completed);
    EXPECT_TRUE(status.ok()) << status;
    return manager;
  }

  std::string run(const std::string& payload = R"({"model":"send_me_to_anthropic","messages":[]})",
                  ModelRouterFilterConfigSharedPtr config = nullptr,
                  bool with_request_info = false) {
    auto manager = startDecode(payload, std::move(config), with_request_info);
    return bridge_.injected_.toString();
  }

  std::string runSseResponse(FilterManager& manager, const std::string& sse_payload) {
    bridge_.injected_.drain(bridge_.injected_.length());
    absl::Status status;
    bool completed = false;
    manager.startSseResponse(factory_, bridge_, response_buffer_manager_,
                             [&status, &completed](absl::Status s) {
                               status = std::move(s);
                               completed = true;
                             });
    Buffer::OwnedImpl response_data(sse_payload);
    manager.onResponseData(response_data, /*end_stream=*/true);
    for (int i = 0; i < 20; ++i) {
      dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
    }
    EXPECT_TRUE(completed);
    EXPECT_TRUE(status.ok()) << status;
    return bridge_.injected_.toString();
  }

  std::string runUnaryResponse(FilterManager& manager, const std::string& unary_payload) {
    bridge_.injected_.drain(bridge_.injected_.length());
    absl::Status status;
    bool completed = false;
    manager.startUnaryResponse(factory_, bridge_, response_buffer_manager_,
                               [&status, &completed](absl::Status s) {
                                 status = std::move(s);
                                 completed = true;
                               });
    Buffer::OwnedImpl response_data(unary_payload);
    manager.onResponseData(response_data, /*end_stream=*/true);
    for (int i = 0; i < 20; ++i) {
      dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
    }
    EXPECT_TRUE(completed);
    EXPECT_TRUE(status.ok()) << status;
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
  BufferManager response_buffer_manager_;
  StreamInfo::StreamInfoImpl stream_info_;
  NiceMock<Stats::MockIsolatedStatsStore> stats_store_;
  NiceMock<ThreadLocal::MockInstance> tls_;
  std::shared_ptr<StrictMock<MockDatabase>> mock_db_;
  Http::TestRequestHeaderMapImpl request_headers_;
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
  EXPECT_EQ(routes_obj->proto().routes(0).api_key(), "Bearer sk-oai");
  EXPECT_EQ(routes_obj->proto().routes(0).binding().uri_pattern(), "/v1/chat/completions");

  EXPECT_EQ(routes_obj->proto().routes(1).llm_protocol(), envoy::type::ai::v3::ANTHROPIC_MESSAGES);
  EXPECT_EQ(routes_obj->proto().routes(1).model(), "claude-sonnet-4-5");
  EXPECT_EQ(routes_obj->proto().routes(1).api_key(), "Bearer sk-sonnet");
  EXPECT_EQ(routes_obj->proto().routes(1).binding().uri_pattern(), "/v1/messages");

  EXPECT_EQ(routes_obj->proto().routes(2).llm_protocol(),
            envoy::type::ai::v3::GEMINI_GENERATE_CONTENT);
  EXPECT_EQ(routes_obj->proto().routes(2).model(), "gemini-2.5-pro");
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

TEST_F(ModelRouterFilterTest, EncodeSSEPassThrough) {
  request_headers_ = Http::TestRequestHeaderMapImpl{
      {":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "original.host"}};

  envoy::data::ai::v3::TokenUsage token_usage;
  token_usage.set_model("gpt-4o");
  token_usage.mutable_input_tokens()->set_value(123);
  token_usage.mutable_output_tokens()->set_value(45);
  token_usage.mutable_total_tokens()->set_value(168);
  Protobuf::Any typed_any;
  MessageUtil::packFrom(typed_any, token_usage);
  stream_info_.setDynamicTypedMetadata("envoy.ai.token_usage", typed_any);

  const AiFilterContext context{*dispatcher_, stream_info_, request_headers_,
                                LLMProtocol::OpenAiChatCompletions, 0};
  std::vector<AiFilterSharedPtr> filters;
  filters.push_back(std::make_shared<ModelRouterFilter>(defaultConfig(), context));
  FilterManager manager(std::move(filters));

  absl::Status status;
  bool completed = false;
  manager.startSseResponse(factory_, bridge_, buffer_manager_,
                           [&status, &completed](absl::Status s) {
                             status = std::move(s);
                             completed = true;
                           });

  const std::string sse_payload = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
                                  "data: [DONE]\n\n";
  Buffer::OwnedImpl response_data(sse_payload);
  manager.onResponseData(response_data, /*end_stream=*/true);

  for (int i = 0; i < 20; ++i) {
    dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
  }

  EXPECT_TRUE(completed);
  EXPECT_TRUE(status.ok()) << status;
  EXPECT_EQ(bridge_.injected_.toString(), sse_payload);
}

TEST_F(ModelRouterFilterTest, EncodeUnaryPassThrough) {
  request_headers_ = Http::TestRequestHeaderMapImpl{
      {":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "original.host"}};

  envoy::data::ai::v3::TokenUsage token_usage;
  token_usage.set_model("claude-haiku-4-5");
  token_usage.mutable_input_tokens()->set_value(50);
  token_usage.mutable_output_tokens()->set_value(25);
  token_usage.mutable_total_tokens()->set_value(75);
  Protobuf::Any typed_any;
  MessageUtil::packFrom(typed_any, token_usage);
  stream_info_.setDynamicTypedMetadata("envoy.ai.token_usage", typed_any);

  const AiFilterContext context{*dispatcher_, stream_info_, request_headers_,
                                LLMProtocol::OpenAiChatCompletions, 0};
  std::vector<AiFilterSharedPtr> filters;
  filters.push_back(std::make_shared<ModelRouterFilter>(defaultConfig(), context));
  FilterManager manager(std::move(filters));

  absl::Status status;
  bool completed = false;
  manager.startUnaryResponse(factory_, bridge_, buffer_manager_,
                             [&status, &completed](absl::Status s) {
                               status = std::move(s);
                               completed = true;
                             });

  const std::string unary_payload = R"({"id":"msg_1","model":"claude-haiku-4-5","content":"ok"})";
  Buffer::OwnedImpl response_data(unary_payload);
  manager.onResponseData(response_data, /*end_stream=*/true);

  for (int i = 0; i < 20; ++i) {
    dispatcher_->run(Event::Dispatcher::RunType::NonBlock);
  }

  EXPECT_TRUE(completed);
  EXPECT_TRUE(status.ok()) << status;
  EXPECT_EQ(nlohmann::json::parse(bridge_.injected_.toString()),
            nlohmann::json::parse(unary_payload));
}

TEST_F(ModelRouterFilterTest, EncodeSSERecordsTokenUsageToDatabase) {
  setModelInFilterState("send_me_to_anthropic");
  stream_info_.setAttemptCount(1);
  setTokenUsageInMetadata(123, 45);

  auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                             defaultConfig(/*with_db=*/true));

  EXPECT_CALL(*mock_db_, recordQueryStats(stream_info_.startTime(), 123, 45, "claude-haiku-4-5",
                                          "anthropic", "test-req-id"));

  const std::string sse_payload = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
                                  "data: [DONE]\n\n";
  EXPECT_EQ(runSseResponse(*manager, sse_payload), sse_payload);
}

TEST_F(ModelRouterFilterTest, EncodeUnaryRecordsTokenUsageForRetryAttempt) {
  setModelInFilterState("multi_fallback_model");
  stream_info_.setAttemptCount(2);
  setTokenUsageInMetadata(50, 25);

  auto manager = startDecode(R"({"model":"multi_fallback_model","messages":[]})",
                             defaultConfig(/*with_db=*/true));

  EXPECT_CALL(*mock_db_, recordQueryStats(stream_info_.startTime(), 50, 25, "claude-sonnet-4-5",
                                          "primary_anthropic", "test-req-id"));

  const std::string unary_payload = R"({"id":"msg_1","model":"claude-sonnet-4-5","content":"ok"})";
  EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
            nlohmann::json::parse(unary_payload));
}

TEST_F(ModelRouterFilterTest, SkipsDatabaseRecordWhenExtractionFailed) {
  setModelInFilterState("send_me_to_anthropic");
  stream_info_.setAttemptCount(1);
  setTokenUsageInMetadata(0, 0, envoy::data::ai::v3::TokenUsage::FAILED);

  auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                             defaultConfig(/*with_db=*/true));

  const std::string unary_payload = R"({"id":"msg_1","model":"claude-haiku-4-5","content":"ok"})";
  EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
            nlohmann::json::parse(unary_payload));
}

TEST_F(ModelRouterFilterTest, SkipsDatabaseRecordWhenTokenUsageMetadataMissing) {
  setModelInFilterState("send_me_to_anthropic");
  stream_info_.setAttemptCount(1);

  auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                             defaultConfig(/*with_db=*/true));

  const std::string unary_payload = R"({"id":"msg_1","model":"claude-haiku-4-5","content":"ok"})";
  EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
            nlohmann::json::parse(unary_payload));
}

TEST_F(ModelRouterFilterTest, SkipsDatabaseRecordWhenAttemptCountMissingOrOutOfBounds) {
  setModelInFilterState("send_me_to_anthropic");
  setTokenUsageInMetadata(10, 20);

  // Missing attempt count: request never went upstream.
  {
    auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                               defaultConfig(/*with_db=*/true));
    const std::string unary_payload = R"({"id":"msg_1","content":"ok"})";
    EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
              nlohmann::json::parse(unary_payload));
  }

  // Out-of-bounds attempt count (2 > 1 configured target).
  {
    stream_info_.setAttemptCount(2);
    auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                               defaultConfig(/*with_db=*/true));
    const std::string unary_payload = R"({"id":"msg_1","content":"ok"})";
    EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
              nlohmann::json::parse(unary_payload));
  }
}

TEST_F(ModelRouterFilterTest, SkipsDatabaseRecordWhenRouteNotFound) {
  setModelInFilterState("unknown_model");
  stream_info_.setAttemptCount(1);
  setTokenUsageInMetadata(10, 20);

  auto manager =
      startDecode(R"({"model":"unknown_model","messages":[]})", defaultConfig(/*with_db=*/true));

  const std::string unary_payload = R"({"id":"msg_1","content":"ok"})";
  EXPECT_EQ(nlohmann::json::parse(runUnaryResponse(*manager, unary_payload)),
            nlohmann::json::parse(unary_payload));
}

TEST_F(ModelRouterFilterTest, DatabaseFailureDoesNotFailResponse) {
  setModelInFilterState("send_me_to_anthropic");
  stream_info_.setAttemptCount(1);
  setTokenUsageInMetadata(100, 50);

  auto manager = startDecode(R"({"model":"send_me_to_anthropic","messages":[]})",
                             defaultConfig(/*with_db=*/true));

  EXPECT_CALL(*mock_db_, recordQueryStats(stream_info_.startTime(), 100, 50, "claude-haiku-4-5",
                                          "anthropic", "test-req-id"))
      .WillOnce([](Envoy::SystemTime, uint64_t, uint64_t, std::string, std::string, std::string) {
        return MockDatabase::makeStatusTask(absl::UnavailableError("database unavailable"));
      });

  const std::string sse_payload = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
                                  "data: [DONE]\n\n";
  EXPECT_EQ(runSseResponse(*manager, sse_payload), sse_payload);
}

} // namespace
} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
