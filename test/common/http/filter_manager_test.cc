#include <memory>
#include <optional>

#include "envoy/common/optref.h"
#include "envoy/http/filter.h"
#include "envoy/http/header_map.h"
#include "envoy/matcher/matcher.h"
#include "envoy/stream_info/filter_state.h"

#include "source/common/http/filter_manager.h"
#include "source/common/http/matching/inputs.h"
#include "source/common/matcher/exact_map_matcher.h"
#include "source/common/matcher/matcher.h"
#include "source/common/stream_info/filter_state_impl.h"
#include "source/common/stream_info/stream_info_impl.h"

#include "test/mocks/event/mocks.h"
#include "test/mocks/http/mocks.h"
#include "test/mocks/local_reply/mocks.h"
#include "test/mocks/network/mocks.h"
#include "test/mocks/server/overload_manager.h"
#include "test/test_common/test_runtime.h"
#include "test/test_common/utility.h"

#include "gtest/gtest.h"

using testing::_;
using testing::InSequence;
using testing::Return;
using testing::StrictMock;

namespace Envoy {
namespace Http {
namespace {
using Protobuf::util::MessageDifferencer;

class FilterManagerTest : public testing::Test {
public:
  void initialize() {
    filter_manager_ = std::make_unique<DownstreamFilterManager>(
        filter_manager_callbacks_, dispatcher_, connection_, 0, nullptr, true, 10000,
        filter_factory_, local_reply_, protocol_, time_source_, filter_state_, overload_manager_);
  }

  // Simple helper to wrapper filter to the factory function.
  FilterFactoryCb createDecoderFilterFactoryCb(StreamDecoderFilterSharedPtr filter) {
    return [filter](FilterChainFactoryCallbacks& callbacks) {
      callbacks.addStreamDecoderFilter(filter);
    };
  }
  FilterFactoryCb createEncoderFilterFactoryCb(StreamEncoderFilterSharedPtr filter) {
    return [filter](FilterChainFactoryCallbacks& callbacks) {
      callbacks.addStreamEncoderFilter(filter);
    };
  }
  FilterFactoryCb createStreamFilterFactoryCb(StreamFilterSharedPtr filter) {
    return [filter](FilterChainFactoryCallbacks& callbacks) { callbacks.addStreamFilter(filter); };
  }
  FilterFactoryCb createLogHandlerFactoryCb(AccessLog::InstanceSharedPtr handler) {
    return [handler](FilterChainFactoryCallbacks& callbacks) {
      callbacks.addAccessLogHandler(handler);
    };
  }

  void validateFilterStateData(const std::string& expected_name) {
    ASSERT_TRUE(filter_manager_->streamInfo().filterState()->hasData<LocalReplyOwnerObject>(
        LocalReplyFilterStateKey));
    auto fs_value =
        filter_manager_->streamInfo().filterState()->getDataReadOnly<LocalReplyOwnerObject>(
            LocalReplyFilterStateKey);
    EXPECT_EQ(fs_value->serializeAsString(), expected_name);

    auto expected = std::make_unique<Protobuf::StringValue>();
    expected->set_value(expected_name);
    EXPECT_TRUE(MessageDifferencer::Equals(*(fs_value->serializeAsProto()), *expected));
  }

  void runSendDirectLocalReplySavedResponseMetadataTest(bool flush_saved_response_metadata) {
    initialize();

    std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());
    std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());

    EXPECT_CALL(filter_factory_, createFilterChain(_))
        .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
          auto factory = createStreamFilterFactoryCb(filter_1);
          callbacks.setFilterConfigName("configName1");
          factory(callbacks);
          factory = createStreamFilterFactoryCb(filter_2);
          callbacks.setFilterConfigName("configName2");
          factory(callbacks);
          return true;
        });

    RequestHeaderMapPtr request_headers{
        new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
    ON_CALL(filter_manager_callbacks_, requestHeaders())
        .WillByDefault(Return(makeOptRef(*request_headers)));
    ON_CALL(filter_manager_callbacks_, responseHeaders())
        .WillByDefault([this]() -> ResponseHeaderMapOptRef {
          return makeOptRefFromPtr(filter_manager_callbacks_.response_headers_.get());
        });

    filter_manager_->createDownstreamFilterChain();
    filter_manager_->requestHeadersInitialized();

    EXPECT_CALL(*filter_1, decodeHeaders(_, _)).WillOnce(Return(FilterHeadersStatus::Continue));
    EXPECT_CALL(*filter_2, decodeHeaders(_, _)).WillOnce(Return(FilterHeadersStatus::Continue));
    filter_manager_->decodeHeaders(*request_headers, false);

    MetadataMap metadata_map = {{"local-reply", "metadata"}};
    EXPECT_CALL(filter_manager_callbacks_, setResponseHeaders_(_)).Times(2);
    EXPECT_CALL(local_reply_, rewrite(_, _, _, _, _, _));
    if (!flush_saved_response_metadata) {
      EXPECT_CALL(filter_manager_callbacks_, encodeMetadata(_)).Times(0);
    }

    {
      InSequence s;
      EXPECT_CALL(*filter_2, encodeHeaders(_, false))
          .WillOnce([&](ResponseHeaderMap&, bool) -> FilterHeadersStatus {
            filter_2->encoder_callbacks_->addEncodedMetadata(
                std::make_unique<MetadataMap>(metadata_map));
            return FilterHeadersStatus::Continue;
          });
      EXPECT_CALL(*filter_1, encodeHeaders(_, false))
          .WillOnce([&](ResponseHeaderMap&, bool) -> FilterHeadersStatus {
            filter_1->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "body", nullptr,
                                                         std::nullopt, "direct_local_reply");
            return FilterHeadersStatus::StopIteration;
          });
      EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false));
      EXPECT_CALL(filter_manager_callbacks_, encodeData(_, !flush_saved_response_metadata))
          .WillOnce([&](Buffer::Instance& data, bool end_stream) -> void {
            EXPECT_EQ("body", data.toString());
            EXPECT_EQ(!flush_saved_response_metadata, end_stream);
          });
      if (flush_saved_response_metadata) {
        EXPECT_CALL(filter_manager_callbacks_, encodeMetadata(_))
            .WillOnce([&](MetadataMapPtr&& metadata_map_ptr) -> void {
              EXPECT_EQ(metadata_map, *metadata_map_ptr);
            });
        EXPECT_CALL(filter_manager_callbacks_, encodeData(_, true))
            .WillOnce([](Buffer::Instance& data, bool) -> void { EXPECT_EQ(0, data.length()); });
      }
      EXPECT_CALL(filter_manager_callbacks_, endStream());
    }

    filter_2->decoder_callbacks_->encodeHeaders(
        std::make_unique<TestResponseHeaderMapImpl>(TestResponseHeaderMapImpl{{":status", "200"}}),
        false, "upstream_response");

    validateFilterStateData("configName1");

    filter_manager_->destroyFilters();
  }

  void runAddDecodedDataOnContinueTest(bool forward_data);

  std::unique_ptr<DownstreamFilterManager> filter_manager_;
  NiceMock<MockFilterManagerCallbacks> filter_manager_callbacks_;
  NiceMock<Event::MockDispatcher> dispatcher_;
  NiceMock<Network::MockConnection> connection_;
  Envoy::Http::MockFilterChainFactory filter_factory_;
  NiceMock<LocalReply::MockLocalReply> local_reply_;
  Protocol protocol_{Protocol::Http2};
  NiceMock<MockTimeSystem> time_source_;
  StreamInfo::FilterStateSharedPtr filter_state_ =
      std::make_shared<StreamInfo::FilterStateImpl>(StreamInfo::FilterState::LifeSpan::Connection);
  NiceMock<Server::MockOverloadManager> overload_manager_;
};

TEST_F(FilterManagerTest, LowWatermarkWithoutHighWatermark) {
  initialize();

  EXPECT_ENVOY_BUG(
      filter_manager_->callLowWatermarkCallbacks(),
      "HTTP filter manager low watermark callback without a preceding high watermark callback");
  EXPECT_FALSE(filter_manager_->aboveHighWatermark());
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, RequestHeadersOrResponseHeadersAccess) {
  initialize();

  auto decoder_filter = std::make_shared<NiceMock<MockStreamDecoderFilter>>();
  auto encoder_filter = std::make_shared<NiceMock<MockStreamEncoderFilter>>();

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("");
        decoder_factory(callbacks);
        auto encoder_factory = createEncoderFilterFactoryCb(encoder_filter);
        callbacks.setFilterConfigName("");
        encoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr request_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  RequestTrailerMapPtr request_trailers{new TestRequestTrailerMapImpl{{"foo", "bar"}}};
  ResponseTrailerMapPtr response_trailers{new TestResponseTrailerMapImpl{{"foo", "bar"}}};
  ResponseHeaderMapPtr informational_headers{
      new TestResponseHeaderMapImpl{{":status", "100"}, {"foo", "bar"}}};

  EXPECT_CALL(filter_manager_callbacks_, requestHeaders())
      .Times(2)
      .WillRepeatedly(Return(makeOptRef(*request_headers)));
  EXPECT_CALL(filter_manager_callbacks_, responseHeaders())
      .Times(2)
      .WillRepeatedly(Return(makeOptRef(*response_headers)));
  EXPECT_CALL(filter_manager_callbacks_, requestTrailers())
      .Times(2)
      .WillRepeatedly(Return(makeOptRef(*request_trailers)));
  EXPECT_CALL(filter_manager_callbacks_, responseTrailers())
      .Times(2)
      .WillRepeatedly(Return(makeOptRef(*response_trailers)));
  EXPECT_CALL(filter_manager_callbacks_, informationalHeaders())
      .Times(2)
      .WillRepeatedly(Return(makeOptRef(*informational_headers)));

  EXPECT_EQ(decoder_filter->callbacks_->requestHeaders().ptr(), request_headers.get());
  EXPECT_EQ(decoder_filter->callbacks_->responseHeaders().ptr(), response_headers.get());
  EXPECT_EQ(decoder_filter->callbacks_->requestTrailers().ptr(), request_trailers.get());
  EXPECT_EQ(decoder_filter->callbacks_->responseTrailers().ptr(), response_trailers.get());
  EXPECT_EQ(decoder_filter->callbacks_->informationalHeaders().ptr(), informational_headers.get());

  EXPECT_EQ(encoder_filter->callbacks_->requestHeaders().ptr(), request_headers.get());
  EXPECT_EQ(encoder_filter->callbacks_->responseHeaders().ptr(), response_headers.get());
  EXPECT_EQ(encoder_filter->callbacks_->requestTrailers().ptr(), request_trailers.get());
  EXPECT_EQ(encoder_filter->callbacks_->responseTrailers().ptr(), response_trailers.get());
  EXPECT_EQ(encoder_filter->callbacks_->informationalHeaders().ptr(), informational_headers.get());

  filter_manager_->destroyFilters();
}

// Verifies that the local reply persists the gRPC classification even if the request headers are
// modified.
TEST_F(FilterManagerTest, SendLocalReplyDuringDecodingGrpcClassiciation) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(*filter, decodeHeaders(_, true))
      .WillRepeatedly([&](RequestHeaderMap& headers, bool) -> FilterHeadersStatus {
        headers.setContentType("text/plain");

        filter->callbacks_->sendLocalReply(Code::InternalServerError, "", nullptr, std::nullopt,
                                           "details");

        return FilterHeadersStatus::StopIteration;
      });

  RequestHeaderMapPtr grpc_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"},
                                   {":path", "/"},
                                   {":method", "GET"},
                                   {"content-type", "application/grpc"}}};

  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*grpc_headers)));

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        return true;
      });

  filter_manager_->createDownstreamFilterChain();

  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(local_reply_, rewrite(_, _, _, _, _, _));
  EXPECT_CALL(filter_manager_callbacks_, setResponseHeaders_(_))
      .WillOnce([](auto& response_headers) {
        EXPECT_THAT(response_headers,
                    ContainsHeader(Http::Headers::get().ContentType, "application/grpc"));
      });
  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, _));
  EXPECT_CALL(filter_manager_callbacks_, endStream());

  filter_manager_->decodeHeaders(*grpc_headers, true);

  validateFilterStateData("configName1");

  filter_manager_->destroyFilters();
}

// Verifies that the local reply persists the gRPC classification even if the request headers are
// modified when directly encoding a response.
TEST_F(FilterManagerTest, SendLocalReplyDuringEncodingGrpcClassiciation) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(*decoder_filter, decodeHeaders(_, true))
      .WillRepeatedly([&](RequestHeaderMap& headers, bool) -> FilterHeadersStatus {
        headers.setContentType("text/plain");

        ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
        decoder_filter->callbacks_->encodeHeaders(std::move(response_headers), true, "test");

        return FilterHeadersStatus::StopIteration;
      });

  std::shared_ptr<MockStreamFilter> encoder_filter(new NiceMock<MockStreamFilter>());

  EXPECT_CALL(*encoder_filter, encodeHeaders(_, true))
      .WillRepeatedly([&](auto&, bool) -> FilterHeadersStatus {
        encoder_filter->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "", nullptr,
                                                           std::nullopt, "details");
        return FilterHeadersStatus::StopIteration;
      });

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("configName1");
        decoder_factory(callbacks);

        auto stream_factory = createStreamFilterFactoryCb(encoder_filter);
        callbacks.setFilterConfigName("configName2");
        stream_factory(callbacks);
        return true;
      });

  RequestHeaderMapPtr grpc_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"},
                                   {":path", "/"},
                                   {":method", "GET"},
                                   {"content-type", "application/grpc"}}};

  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*grpc_headers)));
  filter_manager_->createDownstreamFilterChain();

  filter_manager_->requestHeadersInitialized();
  EXPECT_CALL(local_reply_, rewrite(_, _, _, _, _, _));
  EXPECT_CALL(filter_manager_callbacks_, setResponseHeaders_(_))
      .WillOnce([](auto&) {})
      .WillOnce([](auto& response_headers) {
        EXPECT_THAT(response_headers,
                    ContainsHeader(Http::Headers::get().ContentType, "application/grpc"));
      });
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, _));
  EXPECT_CALL(filter_manager_callbacks_, endStream());

  filter_manager_->decodeHeaders(*grpc_headers, true);

  validateFilterStateData("configName2");

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, SendDirectLocalReplyEncodesSavedResponseMetadata) {
  runSendDirectLocalReplySavedResponseMetadataTest(true);
}

TEST_F(FilterManagerTest, SendDirectLocalReplySkipsSavedResponseMetadataWhenRuntimeGuardDisabled) {
  TestScopedRuntime scoped_runtime;
  scoped_runtime.mergeValues(
      {{"envoy.reloadable_features.direct_local_reply_flush_saved_response_metadata", "false"}});
  runSendDirectLocalReplySavedResponseMetadataTest(false);
}

TEST_F(FilterManagerTest, OnLocalReply) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());
  std::shared_ptr<MockStreamEncoderFilter> encoder_filter(new NiceMock<MockStreamEncoderFilter>());
  std::shared_ptr<MockStreamFilter> stream_filter(new NiceMock<MockStreamFilter>());

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};

  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("configName1");
        decoder_factory(callbacks);
        auto stream_factory = createStreamFilterFactoryCb(stream_filter);
        callbacks.setFilterConfigName("configName2");
        stream_factory(callbacks);
        auto encoder_factory = createEncoderFilterFactoryCb(encoder_filter);
        callbacks.setFilterConfigName("configName3");
        encoder_factory(callbacks);
        return true;
      });

  filter_manager_->createDownstreamFilterChain();
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, true);

  // Make sure all 3 filters get onLocalReply, and that the reset is preserved
  // even if not the last return.
  EXPECT_CALL(*decoder_filter, onLocalReply(_))
      .WillOnce(
          [&](const StreamFilterBase::LocalReplyData& local_reply_data) -> Http::LocalErrorStatus {
            EXPECT_THAT(local_reply_data.grpc_status_, testing::Optional(Grpc::Status::Internal));
            EXPECT_EQ(local_reply_data.body_, "body");
            return Http::LocalErrorStatus::Continue;
          });
  EXPECT_CALL(*stream_filter, onLocalReply(_))
      .WillOnce(
          [&](const StreamFilterBase::LocalReplyData& local_reply_data) -> Http::LocalErrorStatus {
            EXPECT_THAT(local_reply_data.grpc_status_, testing::Optional(Grpc::Status::Internal));
            EXPECT_EQ(local_reply_data.body_, "body");
            return LocalErrorStatus::ContinueAndResetStream;
          });
  EXPECT_CALL(*encoder_filter, onLocalReply(_))
      .WillOnce(
          [&](const StreamFilterBase::LocalReplyData& local_reply_data) -> Http::LocalErrorStatus {
            EXPECT_THAT(local_reply_data.grpc_status_, testing::Optional(Grpc::Status::Internal));
            EXPECT_EQ(local_reply_data.body_, "body");
            return Http::LocalErrorStatus::Continue;
          });
  EXPECT_CALL(filter_manager_callbacks_, resetStream(_, _));
  decoder_filter->callbacks_->sendLocalReply(Code::InternalServerError, "body", nullptr,
                                             Grpc::Status::Internal, "details");

  // The reason for the response (in this case the reset) will still be tracked
  // but as no response is sent the response code will remain absent.

  ASSERT_TRUE(filter_manager_->streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ(filter_manager_->streamInfo().responseCodeDetails().value(), "details");
  EXPECT_FALSE(filter_manager_->streamInfo().responseCode().has_value());

  validateFilterStateData("configName1");

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, MultipleOnLocalReply) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());
  std::shared_ptr<MockStreamEncoderFilter> encoder_filter(new NiceMock<MockStreamEncoderFilter>());
  std::shared_ptr<MockStreamFilter> stream_filter(new NiceMock<MockStreamFilter>());

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};

  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("configName1");
        decoder_factory(callbacks);
        auto stream_factory = createStreamFilterFactoryCb(stream_filter);
        callbacks.setFilterConfigName("configName2");
        stream_factory(callbacks);
        auto encoder_factory = createEncoderFilterFactoryCb(encoder_filter);
        callbacks.setFilterConfigName("configName3");
        encoder_factory(callbacks);
        return true;
      });

  filter_manager_->createDownstreamFilterChain();
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, true);

  {
    // Set up expectations to be triggered by sendLocalReply at the bottom of
    // this block.
    InSequence s;

    // Make sure all 3 filters get onLocalReply
    EXPECT_CALL(*decoder_filter, onLocalReply(_));
    EXPECT_CALL(*stream_filter, onLocalReply(_));
    EXPECT_CALL(*encoder_filter, onLocalReply(_));

    // Now response encoding begins. Assume a filter co-opts the original reply
    // with a new local reply.
    EXPECT_CALL(*encoder_filter, encodeHeaders(_, _))
        .WillOnce([&](ResponseHeaderMap&, bool) -> FilterHeadersStatus {
          decoder_filter->callbacks_->sendLocalReply(Code::InternalServerError, "body2", nullptr,
                                                     std::nullopt, "details2");
          return FilterHeadersStatus::StopIteration;
        });

    // All 3 filters should get the second onLocalReply.
    EXPECT_CALL(*decoder_filter, onLocalReply(_));
    EXPECT_CALL(*stream_filter, onLocalReply(_));
    EXPECT_CALL(*encoder_filter, onLocalReply(_));
    // trackedObjectStackIsEmpty() is never called since sendLocalReply will abort encoder filter
    // iteration.
    EXPECT_CALL(dispatcher_, trackedObjectStackIsEmpty()).Times(0);

    decoder_filter->callbacks_->sendLocalReply(Code::InternalServerError, "body", nullptr,
                                               std::nullopt, "details");
  }

  // The final details should be details2.
  ASSERT_TRUE(filter_manager_->streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ(filter_manager_->streamInfo().responseCodeDetails().value(), "details2");
  EXPECT_FALSE(filter_manager_->streamInfo().responseCode().has_value());

  validateFilterStateData("configName1");

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, ResetIdleTimer) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  decoder_filter->callbacks_->resetIdleTimer();

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, SetAndGetUpstreamOverrideHost) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  decoder_filter->callbacks_->setUpstreamOverrideHost(
      Upstream::LoadBalancerContext::OverrideHost{"1.2.3.4", true});

  OptRef<const Upstream::LoadBalancerContext::OverrideHost> override_host =
      decoder_filter->callbacks_->upstreamOverrideHost();
  EXPECT_TRUE(override_host.has_value());
  EXPECT_EQ(override_host->host, "1.2.3.4");
  EXPECT_TRUE(override_host->strict);

  filter_manager_->destroyFilters();
};

TEST_F(FilterManagerTest, GetRouteLevelFilterConfig) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("custom-name");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  std::shared_ptr<Router::MockRoute> route(new NiceMock<Router::MockRoute>());
  auto route_config = std::make_shared<Router::RouteSpecificFilterConfig>();

  NiceMock<MockDownstreamStreamFilterCallbacks> downstream_callbacks;
  ON_CALL(filter_manager_callbacks_, downstreamCallbacks)
      .WillByDefault(Return(OptRef<DownstreamStreamFilterCallbacks>{downstream_callbacks}));
  ON_CALL(downstream_callbacks, route(_))
      .WillByDefault(Return(makeOptRefFromPtr<const Router::Route>(route.get())));

  // Get a valid config by the custom filter name.
  EXPECT_CALL(*route, mostSpecificPerFilterConfig(testing::Eq("custom-name")))
      .WillOnce(Return(route_config.get()));
  EXPECT_EQ(route_config.get(), decoder_filter->callbacks_->mostSpecificPerFilterConfig());

  // Get nothing by the custom filter name.
  EXPECT_CALL(*route, mostSpecificPerFilterConfig(testing::Eq("custom-name")))
      .WillOnce(Return(nullptr));
  EXPECT_EQ(nullptr, decoder_filter->callbacks_->mostSpecificPerFilterConfig());

  // Get a valid config by the custom filter name.
  EXPECT_CALL(*route, perFilterConfigs(testing::Eq("custom-name")))
      .WillOnce([&](absl::string_view) -> Router::RouteSpecificFilterConfigs {
        return {route_config.get()};
      });
  auto route_config_result = decoder_filter->callbacks_->perFilterConfigs();
  EXPECT_EQ(route_config.get(), route_config_result[0]);

  // Get nothing by the custom filter name.
  EXPECT_CALL(*route, perFilterConfigs(testing::Eq("custom-name")))
      .WillOnce([](absl::string_view) -> Router::RouteSpecificFilterConfigs { return {}; });
  route_config_result = decoder_filter->callbacks_->perFilterConfigs();
  EXPECT_TRUE(route_config_result.empty());

  filter_manager_->destroyFilters();
};

TEST_F(FilterManagerTest, GetRouteLevelFilterConfigForNullRoute) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> decoder_filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createDecoderFilterFactoryCb(decoder_filter);
        callbacks.setFilterConfigName("custom-name");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  std::shared_ptr<Router::MockRoute> route(new NiceMock<Router::MockRoute>());
  auto route_config = std::make_shared<Router::RouteSpecificFilterConfig>();

  // Do nothing for no route.
  NiceMock<MockDownstreamStreamFilterCallbacks> downstream_callbacks;
  ON_CALL(filter_manager_callbacks_, downstreamCallbacks)
      .WillByDefault(Return(OptRef<DownstreamStreamFilterCallbacks>{downstream_callbacks}));
  EXPECT_CALL(downstream_callbacks, route(_)).WillOnce(Return(OptRef<const Router::Route>{}));
  decoder_filter->callbacks_->mostSpecificPerFilterConfig();

  EXPECT_CALL(downstream_callbacks, route(_)).WillOnce(Return(OptRef<const Router::Route>{}));
  decoder_filter->callbacks_->perFilterConfigs();

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, MetadataContinueAll) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());

  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        decoder_factory(callbacks);
        decoder_factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  // Decode path:
  EXPECT_CALL(*filter_1, decodeHeaders(_, _)).WillOnce(Return(FilterHeadersStatus::StopIteration));
  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));

  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*basic_headers, false);

  EXPECT_CALL(*filter_1, decodeMetadata(_)).WillOnce(Return(FilterMetadataStatus::ContinueAll));
  MetadataMap map1 = {{"a", "b"}};
  MetadataMap map2 = {{"c", "d"}};
  EXPECT_CALL(*filter_2, decodeHeaders(_, _)).WillOnce([&]() {
    filter_2->decoder_callbacks_->addDecodedMetadata().push_back(
        std::make_unique<MetadataMap>(map2));
    return FilterHeadersStatus::Continue;
  });
  {
    InSequence s;
    // Metadata added by filter_2.decodeHeaders(..) appears second, and goes through all filters.
    EXPECT_CALL(*filter_2, decodeMetadata(testing::Eq(map1)));
    EXPECT_CALL(*filter_1, decodeMetadata(testing::Eq(map2)));
    EXPECT_CALL(*filter_2, decodeMetadata(testing::Eq(map2)));
  }
  filter_manager_->decodeMetadata(map1);

  // Encode Path:
  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  ON_CALL(filter_manager_callbacks_, responseHeaders())
      .WillByDefault(Return(makeOptRef(*response_headers)));

  EXPECT_CALL(*filter_2, encodeHeaders(_, _)).WillOnce(Return(FilterHeadersStatus::StopIteration));
  EXPECT_CALL(*filter_1, encodeHeaders(_, _)).WillOnce([&]() {
    filter_2->encoder_callbacks_->addEncodedMetadata(std::make_unique<MetadataMap>(map2));
    return FilterHeadersStatus::Continue;
  });
  EXPECT_CALL(*filter_2, encodeMetadata(_)).WillOnce(Return(FilterMetadataStatus::ContinueAll));
  {
    InSequence s;
    // Metadata added by filter_1.decodeHeaders(..) appears second.
    EXPECT_CALL(*filter_1, encodeMetadata(testing::Eq(map1)));
    // The encode path for metadata is different (by design or coincidence) than the decode path.
    // On the encode path, calling addEncodedMetadata will only pass through this and later filters,
    // while on the decode path, calling addDecodedMetadata will pass metadata through all filters.
    EXPECT_CALL(*filter_2, encodeMetadata(testing::Eq(map2))).Times(0);
    EXPECT_CALL(*filter_1, encodeMetadata(testing::Eq(map2)));
  }

  filter_2->decoder_callbacks_->encodeHeaders(
      std::make_unique<TestResponseHeaderMapImpl>(*response_headers), false, "none");
  filter_2->decoder_callbacks_->encodeMetadata(std::make_unique<MetadataMap>(map1));

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, DecodeMetadataSendsLocalReply) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());

  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));

  EXPECT_CALL(*filter_1, decodeHeaders(_, _));
  EXPECT_CALL(*filter_2, decodeHeaders(_, _));
  filter_manager_->decodeHeaders(*basic_headers, false);

  EXPECT_CALL(*filter_1, decodeMetadata(_)).WillOnce([&]() {
    filter_1->decoder_callbacks_->sendLocalReply(Code::InternalServerError, "bad_metadata", nullptr,
                                                 std::nullopt, "bad_metadata");
    return FilterMetadataStatus::StopIterationForLocalReply;
  });

  // Metadata does not pass to the next filter due to local reply.
  EXPECT_CALL(*filter_2, decodeMetadata(_)).Times(0);
  // Triggered local reply should pass through encode headers for both filters.
  EXPECT_CALL(*filter_2, encodeHeaders(_, _));
  EXPECT_CALL(*filter_1, encodeHeaders(_, _));
  MetadataMap map = {{"a", "b"}};
  filter_manager_->decodeMetadata(map);

  EXPECT_THAT(*filter_manager_->streamInfo().responseCodeDetails(), "bad_metadata");

  validateFilterStateData("configName1");

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, MetadataContinueAllFollowedByHeadersLocalReply) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());

  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto decoder_factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        decoder_factory(callbacks);
        decoder_factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        decoder_factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  // Decode path:
  EXPECT_CALL(*filter_1, decodeHeaders(_, _)).WillOnce(Return(FilterHeadersStatus::StopIteration));
  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));

  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*basic_headers, false);

  EXPECT_CALL(*filter_1, decodeMetadata(_)).WillOnce(Return(FilterMetadataStatus::ContinueAll));
  MetadataMap map1 = {{"a", "b"}};
  MetadataMap map2 = {{"c", "d"}};
  EXPECT_CALL(*filter_2, decodeHeaders(_, _)).WillOnce([&]() {
    filter_2->decoder_callbacks_->sendLocalReply(Code::InternalServerError, "bad_headers", nullptr,
                                                 std::nullopt, "bad_headers");
    return FilterHeadersStatus::StopIteration;
  });
  // filter_2 should never decode metadata.
  EXPECT_CALL(*filter_2, decodeMetadata(_)).Times(0);
  filter_manager_->decodeMetadata(map1);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, EncodeMetadataSendsLocalReply) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());

  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  // Encode headers first, as metadata can't get ahead of headers.
  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  ON_CALL(filter_manager_callbacks_, responseHeaders())
      .WillByDefault(Return(makeOptRef(*response_headers)));

  EXPECT_CALL(*filter_2, encodeHeaders(_, _));
  EXPECT_CALL(*filter_1, encodeHeaders(_, _));
  filter_2->decoder_callbacks_->encodeHeaders(
      std::make_unique<TestResponseHeaderMapImpl>(*response_headers), false, "");

  EXPECT_CALL(*filter_2, encodeMetadata(_)).WillOnce([&]() {
    filter_2->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "", nullptr,
                                                 std::nullopt, "bad_metadata");
    return FilterMetadataStatus::StopIterationForLocalReply;
  });
  // Headers have already passed through; we will reset the stream.
  EXPECT_CALL(filter_manager_callbacks_, resetStream(StreamResetReason::LocalReset, ""));
  MetadataMap map1 = {{"a", "b"}};
  filter_2->decoder_callbacks_->encodeMetadata(std::make_unique<MetadataMap>(map1));

  validateFilterStateData("configName2");

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, RequestedApplicationProtocols) {
  initialize();

  const auto& protocols =
      filter_manager_->streamInfo().downstreamAddressProvider().requestedApplicationProtocols();
  EXPECT_TRUE(protocols.empty());

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, IdleTimerResets) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));
  filter_manager_->requestHeadersInitialized();

  filter_manager_->decodeHeaders(*basic_headers, false);

  Buffer::OwnedImpl data("absee");
  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_manager_->decodeData(data, false);

  MetadataMap map = {{"a", "b"}};
  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_manager_->decodeMetadata(map);

  RequestTrailerMapPtr basic_trailers{new TestRequestTrailerMapImpl{{"x", "y"}}};
  filter_manager_->decodeTrailers(*basic_trailers);

  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  ON_CALL(filter_manager_callbacks_, responseHeaders())
      .WillByDefault(Return(makeOptRef(*response_headers)));

  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_1->decoder_callbacks_->encodeHeaders(
      std::make_unique<TestResponseHeaderMapImpl>(*response_headers), false, "");

  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_1->decoder_callbacks_->encodeData(data, false);

  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_1->decoder_callbacks_->encodeMetadata(std::make_unique<MetadataMap>(map));

  ResponseTrailerMapPtr basic_resp_trailers{new TestResponseTrailerMapImpl{{"x", "y"}}};
  EXPECT_CALL(filter_manager_callbacks_, resetIdleTimer());
  filter_1->decoder_callbacks_->encodeTrailers(std::move(basic_resp_trailers));
  filter_manager_->destroyFilters();
}

// Verify that decodeData is not called on filters after the stream has been reset.
TEST_F(FilterManagerTest, DecodeDataNotCalledAfterDownstreamReset) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("test_filter");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, false);

  // Simulate a downstream reset.
  filter_manager_->onDownstreamReset();

  // After reset, decodeData should not be called on the filter.
  EXPECT_CALL(*filter, decodeData(_, _)).Times(0);

  Buffer::OwnedImpl data("test_data");
  filter_manager_->decodeData(data, false);

  filter_manager_->destroyFilters();
}

// Verify that decodeHeaders is not called on filters after the stream has been reset.
TEST_F(FilterManagerTest, DecodeHeadersNotCalledAfterDownstreamReset) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("test_filter");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  // Simulate a downstream reset before headers are processed.
  filter_manager_->onDownstreamReset();

  // After reset, decodeHeaders should not be called on the filter.
  EXPECT_CALL(*filter, decodeHeaders(_, _)).Times(0);

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  filter_manager_->decodeHeaders(*headers, true);

  filter_manager_->destroyFilters();
}

// Verify that decodeTrailers is not called on filters after the stream has been reset.
TEST_F(FilterManagerTest, DecodeTrailersNotCalledAfterDownstreamReset) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("test_filter");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, false);

  // Simulate a downstream reset.
  filter_manager_->onDownstreamReset();

  // After reset, decodeTrailers should not be called on the filter.
  EXPECT_CALL(*filter, decodeTrailers(_)).Times(0);

  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"foo", "bar"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));

  filter_manager_->decodeTrailers(*trailers);

  filter_manager_->destroyFilters();
}

// Verify that decodeMetadata is not called on filters after the stream has been reset.
TEST_F(FilterManagerTest, DecodeMetadataNotCalledAfterDownstreamReset) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("test_filter");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, false);

  // Simulate a downstream reset.
  filter_manager_->onDownstreamReset();

  // After reset, decodeMetadata should not be called on the filter.
  EXPECT_CALL(*filter, decodeMetadata(_)).Times(0);

  MetadataMap metadata_map{{"key", "value"}};
  filter_manager_->decodeMetadata(metadata_map);

  filter_manager_->destroyFilters();
}

// Verify that multiple decode operations are all blocked after downstream reset.
TEST_F(FilterManagerTest, AllDecodeOperationsBlockedAfterDownstreamReset) {
  initialize();

  std::shared_ptr<MockStreamDecoderFilter> filter(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter);
        callbacks.setFilterConfigName("test_filter");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_manager_->requestHeadersInitialized();
  filter_manager_->decodeHeaders(*headers, false);

  // Simulate a downstream reset.
  filter_manager_->onDownstreamReset();

  // After reset, none of the decode operations should call the filter.
  EXPECT_CALL(*filter, decodeData(_, _)).Times(0);
  EXPECT_CALL(*filter, decodeTrailers(_)).Times(0);
  EXPECT_CALL(*filter, decodeMetadata(_)).Times(0);

  // Try all decode operations. None of them should reach the filter.
  Buffer::OwnedImpl data("test_data");
  filter_manager_->decodeData(data, false);

  MetadataMap metadata_map{{"key", "value"}};
  filter_manager_->decodeMetadata(metadata_map);

  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"foo", "bar"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));
  filter_manager_->decodeTrailers(*trailers);

  filter_manager_->destroyFilters();
}

// Reproduces the request-body frame loss reported in
// https://github.com/Kuadrant/wasm-shim/issues/388 (and Envoy #46841): a filter
// that returns StopIteration on headers (e.g. a wasm filter with
// allow_on_headers_stop_iteration, mapped to IterationState::StopSingleIteration)
// buffers a body chunk, continues asynchronously, and then on the next chunk
// drains the frame into the filter-manager buffer via addDecodedData() while
// returning Continue. Because the filter's IterationState is already Continue at
// that point, commonHandleAfterDataCallback() forwards the now-empty frame
// instead of the just-buffered data, so a downstream FULL_DUPLEX_STREAMED
// ext_proc filter never receives that chunk. Exactly one mid-stream frame is
// lost while the message count is unchanged.
//
// The `forward_data` parameter toggles the
// `filter_manager_forward_added_data_on_continue` runtime guard: when disabled
// this documents the pre-fix data loss; when enabled it verifies the fix.
void FilterManagerTest::runAddDecodedDataOnContinueTest(bool forward_data) {
  TestScopedRuntime scoped_runtime;
  scoped_runtime.mergeValues(
      {{"envoy.reloadable_features.filter_manager_forward_added_data_on_continue",
        forward_data ? "true" : "false"}});

  initialize();

  // filter_1 mimics the wasm filter (stops on headers, buffers, continues after
  // an async callout); filter_2 mimics a FULL_DUPLEX_STREAMED ext_proc filter
  // that streams (does not re-buffer) every request body byte it receives.
  std::shared_ptr<MockStreamDecoderFilter> filter_1(new NiceMock<MockStreamDecoderFilter>());
  std::shared_ptr<MockStreamDecoderFilter> filter_2(new NiceMock<MockStreamDecoderFilter>());

  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createDecoderFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("filter_1");
        factory(callbacks);
        factory = createDecoderFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("filter_2");
        factory(callbacks);
        return true;
      });

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));

  filter_manager_->createDownstreamFilterChain();
  filter_manager_->requestHeadersInitialized();

  // filter_2 accumulates and drains (streams) every request body byte it sees.
  std::string filter_2_received;
  ON_CALL(*filter_2, decodeData(_, _))
      .WillByDefault([&](Buffer::Instance& data, bool) -> FilterDataStatus {
        filter_2_received += data.toString();
        data.drain(data.length());
        return FilterDataStatus::StopIterationNoBuffer;
      });

  // filter_1 stops iteration on headers to await an async callout; filter_2 must
  // not see the headers yet.
  EXPECT_CALL(*filter_1, decodeHeaders(_, false))
      .WillOnce(Return(FilterHeadersStatus::StopIteration));
  filter_manager_->decodeHeaders(*headers, false);

  // Chunk A arrives while filter_1 is paused: buffer it in the filter manager.
  EXPECT_CALL(*filter_1, decodeData(_, false))
      .WillOnce(Return(FilterDataStatus::StopIterationAndBuffer));
  Buffer::OwnedImpl chunk_a("AAAA");
  filter_manager_->decodeData(chunk_a, false);

  // The async callout completes: filter_1 continues, which delivers the buffered
  // chunk A (and the headers) down to filter_2.
  EXPECT_CALL(*filter_2, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_1->callbacks_->continueDecoding();
  EXPECT_EQ("AAAA", filter_2_received);

  // Chunk B arrives right after the continue. filter_1, still in its buffering
  // mode, moves the frame into the filter-manager buffer via addDecodedData()
  // and then returns Continue. This frame must still reach filter_2.
  EXPECT_CALL(*filter_1, decodeData(_, false))
      .WillOnce([&](Buffer::Instance& data, bool) -> FilterDataStatus {
        filter_1->callbacks_->addDecodedData(data, false);
        return FilterDataStatus::Continue;
      });
  Buffer::OwnedImpl chunk_b("BBBB");
  filter_manager_->decodeData(chunk_b, false);

  // Chunk C (end of stream): filter_1 passes it straight through.
  EXPECT_CALL(*filter_1, decodeData(_, true)).WillOnce(Return(FilterDataStatus::Continue));
  Buffer::OwnedImpl chunk_c("CCCC");
  filter_manager_->decodeData(chunk_c, true);

  // With the fix (guard enabled) all three frames reach filter_2. Without it,
  // chunk B is dropped and filter_2 only sees "AAAACCCC".
  if (forward_data) {
    EXPECT_EQ("AAAABBBBCCCC", filter_2_received);
  } else {
    EXPECT_EQ("AAAACCCC", filter_2_received);
  }

  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, DecodeDataFrameLostAfterContinueWithoutGuard) {
  runAddDecodedDataOnContinueTest(/*forward_data=*/false);
}

TEST_F(FilterManagerTest, DecodeDataFrameNotLostAfterContinueWithAddDecodedData) {
  runAddDecodedDataOnContinueTest(/*forward_data=*/true);
}

TEST_F(FilterManagerTest, TerminalDecoderFilterLocalReplyInDecodeHeadersSkipsAddedMetadata) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());
  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(*filter_1, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2, decodeHeaders(_, true))
      .WillOnce([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        MetadataMap metadata_map = {{"a", "b"}};
        filter_2->decoder_callbacks_->addDecodedMetadata().emplace_back(
            std::make_unique<MetadataMap>(metadata_map));
        filter_2->decoder_callbacks_->sendLocalReply(Code::InternalServerError, "bad_headers",
                                                     nullptr, std::nullopt, "bad_headers");
        return FilterHeadersStatus::StopIteration;
      });

  // Added metadata must not be decoded when the terminal filter aborts with a local reply.
  EXPECT_CALL(*filter_1, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_2, decodeMetadata(_)).Times(0);
  // Request timeout is disarmed during encodeHeaders of the local reply and before returning from
  // decodeHeaders when end_stream is true and the stream has not been destroyed yet.
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(2);
  EXPECT_CALL(*filter_2, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_1, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));

  filter_manager_->decodeHeaders(*basic_headers, true);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, TerminalDecoderFilterLocalReplyInDecodeTrailersSkipsAddedMetadata) {
  initialize();

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());
  std::shared_ptr<MockStreamFilter> filter_2(new NiceMock<MockStreamFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory(callbacks);
        factory = createStreamFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        factory(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(*filter_1, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  filter_manager_->decodeHeaders(*basic_headers, false);

  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"trailer", "value"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));

  EXPECT_CALL(*filter_1, decodeTrailers(_)).WillOnce(Return(FilterTrailersStatus::Continue));
  EXPECT_CALL(*filter_2, decodeTrailers(_))
      .WillOnce([&](RequestTrailerMap&) -> FilterTrailersStatus {
        MetadataMap metadata_map = {{"a", "b"}};
        filter_2->decoder_callbacks_->addDecodedMetadata().emplace_back(
            std::make_unique<MetadataMap>(metadata_map));
        filter_2->decoder_callbacks_->sendLocalReply(Code::InternalServerError, "bad_trailers",
                                                     nullptr, std::nullopt, "bad_trailers");
        return FilterTrailersStatus::StopIteration;
      });

  EXPECT_CALL(*filter_1, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_2, decodeMetadata(_)).Times(0);
  // Request timeout is disarmed during encodeHeaders of the local reply and before returning from
  // decodeTrailers when the stream has not been destroyed yet.
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(2);
  EXPECT_CALL(*filter_2, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_1, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));

  filter_manager_->decodeTrailers(*trailers);
  filter_manager_->destroyFilters();
}

// A terminal decoder filter local reply on a downstream stream with independent half-close enabled
// returns from decodeHeaders() right after the local reply, like a non-terminal filter does. The
// request timeout is disarmed by the local reply's encodeHeaders() (and not called again after
// endStream() destroys the filters), and the stream is closed once encoding finishes because the
// decoder filter chain was aborted.
TEST_F(FilterManagerTest, TerminalDecoderFilterLocalReplyWithHalfCloseEnabledClosesStream) {
  initialize();
  ON_CALL(filter_manager_callbacks_, isHalfCloseEnabled()).WillByDefault(Return(true));

  std::shared_ptr<MockStreamFilter> filter_1(new NiceMock<MockStreamFilter>());
  std::shared_ptr<MockStreamDecoderFilter> filter_2(new NiceMock<MockStreamDecoderFilter>());
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillRepeatedly([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        auto factory_1 = createStreamFilterFactoryCb(filter_1);
        callbacks.setFilterConfigName("configName1");
        factory_1(callbacks);
        auto factory_2 = createDecoderFilterFactoryCb(filter_2);
        callbacks.setFilterConfigName("configName2");
        factory_2(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr basic_headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders())
      .WillByDefault(Return(makeOptRef(*basic_headers)));
  ON_CALL(filter_manager_callbacks_, responseHeaders())
      .WillByDefault([this]() -> ResponseHeaderMapOptRef {
        return makeOptRefFromPtr(filter_manager_callbacks_.response_headers_.get());
      });
  filter_manager_->requestHeadersInitialized();

  // The terminal filter's decodeComplete() must run before the prepared local reply is encoded,
  // since encoding the reply ends the stream and destroys the filters, as the HCM does.
  InSequence s;
  EXPECT_CALL(*filter_1, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2, decodeHeaders(_, true))
      .WillOnce([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2->callbacks_->sendLocalReply(Code::ServiceUnavailable, "no healthy upstream",
                                             nullptr, std::nullopt, "no_healthy_upstream");
        return FilterHeadersStatus::StopIteration;
      });
  EXPECT_CALL(*filter_2, decodeComplete());
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout());
  EXPECT_CALL(*filter_1, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false));
  EXPECT_CALL(*filter_1, encodeData(_, true)).WillOnce(Return(FilterDataStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, true));
  EXPECT_CALL(filter_manager_callbacks_, endStream()).WillOnce([this]() {
    filter_manager_->destroyFilters();
  });
  EXPECT_CALL(*filter_2, onDestroy());

  filter_manager_->decodeHeaders(*basic_headers, true);
}

TEST_F(FilterManagerTest, DecoderFilterAbortWithoutLocalReplyInDecodeHeadersDisarmsOnEndStream) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, true))
      .WillOnce([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter->callbacks_->sendGoAwayAndClose(/*graceful=*/true);
        return FilterHeadersStatus::StopIteration;
      });
  EXPECT_CALL(filter_manager_callbacks_, sendGoAwayAndClose(true));
  EXPECT_CALL(*filter, decodeComplete());
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout());
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, true);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest,
       DecoderFilterAbortWithoutLocalReplyInDecodeHeadersDoesNotDisarmBeforeEndStream) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(0);

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false))
      .WillOnce([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter->callbacks_->sendGoAwayAndClose(/*graceful=*/true);
        return FilterHeadersStatus::StopIteration;
      });
  EXPECT_CALL(filter_manager_callbacks_, sendGoAwayAndClose(true));
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, DecoderFilterAbortWithoutLocalReplyInDecodeDataDisarmsOnEndStream) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter, decodeData(_, true))
      .WillOnce([&](Buffer::Instance&, bool) -> FilterDataStatus {
        filter->callbacks_->sendGoAwayAndClose(/*graceful=*/true);
        return FilterDataStatus::StopIterationNoBuffer;
      });
  EXPECT_CALL(filter_manager_callbacks_, sendGoAwayAndClose(true));
  EXPECT_CALL(*filter, decodeComplete());
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout());
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  Buffer::OwnedImpl body("body");
  filter_manager_->decodeData(body, true);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest,
       DecoderFilterAbortWithoutLocalReplyInDecodeDataDoesNotDisarmBeforeEndStream) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(0);

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter, decodeData(_, false))
      .WillOnce([&](Buffer::Instance&, bool) -> FilterDataStatus {
        filter->callbacks_->sendGoAwayAndClose(/*graceful=*/true);
        return FilterDataStatus::StopIterationNoBuffer;
      });
  EXPECT_CALL(filter_manager_callbacks_, sendGoAwayAndClose(true));
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  Buffer::OwnedImpl body("body");
  filter_manager_->decodeData(body, false);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, DecoderFilterAbortWithoutLocalReplyInDecodeTrailersDisarmsTimeout) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"trailer", "value"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter, decodeTrailers(_)).WillOnce([&](RequestTrailerMap&) -> FilterTrailersStatus {
    filter->callbacks_->sendGoAwayAndClose(/*graceful=*/true);
    return FilterTrailersStatus::StopIteration;
  });
  EXPECT_CALL(filter_manager_callbacks_, sendGoAwayAndClose(true));
  EXPECT_CALL(*filter, decodeComplete());
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout());
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  filter_manager_->decodeTrailers(*trailers);
  filter_manager_->destroyFilters();
}

TEST_F(FilterManagerTest, SynchronousDestroyInDecodeHeadersSkipsDecodeCompleteAndDisarm) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "GET"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(*filter, decodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(0);

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, true))
      .WillOnce([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter->callbacks_->resetStream(StreamResetReason::LocalReset, "");
        return FilterHeadersStatus::StopIteration;
      });
  EXPECT_CALL(filter_manager_callbacks_, resetStream(StreamResetReason::LocalReset, ""))
      .WillOnce(
          [this](StreamResetReason, absl::string_view) { filter_manager_->destroyFilters(); });
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, true);
}

TEST_F(FilterManagerTest, SynchronousDestroyInDecodeDataSkipsDecodeCompleteAndDisarm) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  EXPECT_CALL(*filter, decodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(0);

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter, decodeData(_, true))
      .WillOnce([&](Buffer::Instance&, bool) -> FilterDataStatus {
        filter->callbacks_->resetStream(StreamResetReason::LocalReset, "");
        return FilterDataStatus::StopIterationNoBuffer;
      });
  EXPECT_CALL(filter_manager_callbacks_, resetStream(StreamResetReason::LocalReset, ""))
      .WillOnce(
          [this](StreamResetReason, absl::string_view) { filter_manager_->destroyFilters(); });
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  Buffer::OwnedImpl body("body");
  filter_manager_->decodeData(body, true);
}

TEST_F(FilterManagerTest, SynchronousDestroyInDecodeTrailersSkipsDecodeCompleteAndDisarm) {
  initialize();
  auto filter = std::make_shared<StrictMock<MockStreamDecoderFilter>>();
  EXPECT_CALL(*filter, setDecoderFilterCallbacks(_));
  EXPECT_CALL(filter_factory_, createFilterChain(_))
      .WillOnce([&](FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.setFilterConfigName("configName1");
        createDecoderFilterFactoryCb(filter)(callbacks);
        return true;
      });
  filter_manager_->createDownstreamFilterChain();

  RequestHeaderMapPtr headers{
      new TestRequestHeaderMapImpl{{":authority", "host"}, {":path", "/"}, {":method", "POST"}}};
  ON_CALL(filter_manager_callbacks_, requestHeaders()).WillByDefault(Return(makeOptRef(*headers)));
  filter_manager_->requestHeadersInitialized();

  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"trailer", "value"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));

  EXPECT_CALL(*filter, decodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, disarmRequestTimeout()).Times(0);

  InSequence s;
  EXPECT_CALL(*filter, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter, decodeTrailers(_)).WillOnce([&](RequestTrailerMap&) -> FilterTrailersStatus {
    filter->callbacks_->resetStream(StreamResetReason::LocalReset, "");
    return FilterTrailersStatus::StopIteration;
  });
  EXPECT_CALL(filter_manager_callbacks_, resetStream(StreamResetReason::LocalReset, ""))
      .WillOnce(
          [this](StreamResetReason, absl::string_view) { filter_manager_->destroyFilters(); });
  EXPECT_CALL(*filter, onDestroy());

  filter_manager_->decodeHeaders(*headers, false);
  filter_manager_->decodeTrailers(*trailers);
}

class TestBaseFilterManager : public FilterManager {
public:
  TestBaseFilterManager(FilterManagerCallbacks& filter_manager_callbacks,
                        Event::Dispatcher& dispatcher, OptRef<const Network::Connection> connection,
                        uint64_t stream_id, Buffer::BufferMemoryAccountSharedPtr account,
                        bool proxy_100_continue, uint32_t buffer_limit,
                        StreamInfo::StreamInfo& stream_info)
      : FilterManager(filter_manager_callbacks, dispatcher, connection, stream_id,
                      std::move(account), proxy_100_continue, buffer_limit),
        stream_info_(stream_info) {}

  StreamInfo::StreamInfo& streamInfo() override { return stream_info_; }
  const StreamInfo::StreamInfo& streamInfo() const override { return stream_info_; }

private:
  StreamInfo::StreamInfo& stream_info_;
};

// Tests for the local reply pipeline on the base FilterManager, which the upstream filter manager
// uses. The filter chain is filter_1_ (a stream filter) followed by filter_2_ (a decoder filter).
class BaseFilterManagerTest : public FilterManagerTest {
public:
  void initializeBaseFilterManager(absl::string_view method = "GET", bool grpc = false) {
    base_fm_ = std::make_unique<TestBaseFilterManager>(
        filter_manager_callbacks_, dispatcher_, connection_, 0, nullptr, true, 10000, stream_info_);
    EXPECT_CALL(filter_factory_, createFilterChain(_))
        .WillOnce(Invoke([this](FilterChainFactoryCallbacks& callbacks) -> bool {
          auto factory_1 = createStreamFilterFactoryCb(filter_1_);
          callbacks.setFilterConfigName("filter_1");
          factory_1(callbacks);
          auto factory_2 = createDecoderFilterFactoryCb(filter_2_);
          callbacks.setFilterConfigName("filter_2");
          factory_2(callbacks);
          return true;
        }));
    base_fm_->createFilterChain(filter_factory_);

    request_headers_.reset(new TestRequestHeaderMapImpl{
        {":authority", "host"}, {":path", "/"}, {":method", std::string(method)}});
    if (grpc) {
      request_headers_->setContentType(Headers::get().ContentTypeValues.Grpc);
    }
    ON_CALL(filter_manager_callbacks_, requestHeaders())
        .WillByDefault(Return(makeOptRef(*request_headers_)));
    ON_CALL(filter_manager_callbacks_, responseHeaders())
        .WillByDefault(testing::Invoke([this]() -> ResponseHeaderMapOptRef {
          return makeOptRefFromPtr(filter_manager_callbacks_.response_headers_.get());
        }));
    base_fm_->requestHeadersInitialized();
  }

  // Makes the end of the encoded response destroy the filters synchronously, the way the upstream
  // request is cleaned up when an upstream filter local reply completes or is retried. Records the
  // destruction so tests can check that no filter callback runs afterwards.
  void destroyFiltersOnEndStream() {
    EXPECT_CALL(filter_manager_callbacks_, endStream()).WillOnce(Invoke([this]() {
      base_fm_->destroyFilters();
    }));
    EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
    EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  }

  NiceMock<StreamInfo::MockStreamInfo> stream_info_;
  std::shared_ptr<MockStreamFilter> filter_1_{new NiceMock<MockStreamFilter>()};
  std::shared_ptr<MockStreamDecoderFilter> filter_2_{new NiceMock<MockStreamDecoderFilter>()};
  RequestHeaderMapPtr request_headers_;
  bool filters_destroyed_{false};
  std::unique_ptr<TestBaseFilterManager> base_fm_;
};

TEST_F(BaseFilterManagerTest, SendLocalReplyViaFilterChain) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(
            Code::ServiceUnavailable, "local_reply_body",
            [](ResponseHeaderMap& headers) {
              headers.addCopy(LowerCaseString("x-local-reply"), "true");
            },
            std::nullopt, "base_fm_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false))
      .WillOnce(Invoke([](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        EXPECT_EQ("503", headers.getStatusValue());
        EXPECT_EQ("true",
                  headers.get(LowerCaseString("x-local-reply"))[0]->value().getStringView());
        headers.addCopy(LowerCaseString("x-encoder-modified"), "1");
        return FilterHeadersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, encodeData(_, true))
      .WillOnce(Invoke([](Buffer::Instance& data, bool) -> FilterDataStatus {
        EXPECT_EQ("local_reply_body", data.toString());
        return FilterDataStatus::Continue;
      }));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false))
      .WillOnce(Invoke([](ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("1",
                  headers.get(LowerCaseString("x-encoder-modified"))[0]->value().getStringView());
      }));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, true));
  EXPECT_CALL(filter_manager_callbacks_, endStream());
  // The base setLocalReplyResponseCodeDetails() records the details on streamInfo(), and the
  // callbacks are told about the local reply.
  EXPECT_CALL(stream_info_, setResponseCodeDetails(absl::string_view("base_fm_local_reply")));
  EXPECT_CALL(filter_manager_callbacks_, onLocalReply(Code::ServiceUnavailable));

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// The base rewriteLocalReply() runs before the gRPC handling, so a local reply to a gRPC request
// is still a headers-only response with :status 200 and the mapped grpc-status.
TEST_F(BaseFilterManagerTest, SendLocalReplyToGrpcRequest) {
  initializeBaseFilterManager("POST", /*grpc=*/true);

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(Code::ServiceUnavailable, "local_reply_body", nullptr,
                                              std::nullopt, "base_fm_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));

  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Invoke([](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        EXPECT_EQ("200", headers.getStatusValue());
        // 503 maps to grpc-status UNAVAILABLE (14).
        EXPECT_EQ("14", headers.getGrpcStatusValue());
        EXPECT_EQ("local_reply_body", headers.getGrpcMessageValue());
        return FilterHeadersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, true));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, endStream());

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// A local reply to a HEAD request keeps the status set by the base rewriteLocalReply() and is sent
// headers-only, without the body.
TEST_F(BaseFilterManagerTest, SendLocalReplyToHeadRequest) {
  initializeBaseFilterManager("HEAD");

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(Code::ServiceUnavailable, "local_reply_body", nullptr,
                                              std::nullopt, "base_fm_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));

  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Invoke([](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        EXPECT_EQ("503", headers.getStatusValue());
        return FilterHeadersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, true));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, endStream());

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// A decoder filter local reply in decodeHeaders(end_stream=true) whose encoding destroys the
// filters synchronously. decodeComplete() must run before onDestroy(), and no filter callback may
// run after it.
TEST_F(BaseFilterManagerTest, LocalReplyInDecodeHeadersDestroyingFiltersRunsNoLaterCallbacks) {
  initializeBaseFilterManager();
  destroyFiltersOnEndStream();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_1_, decodeComplete()).WillOnce(Invoke([this]() {
    EXPECT_FALSE(filters_destroyed_);
  }));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        // Metadata added before the local reply must not be decoded.
        filter_2_->callbacks_->addDecodedMetadata().emplace_back(
            std::make_unique<MetadataMap>(MetadataMap{{"a", "b"}}));
        filter_2_->callbacks_->sendLocalReply(Code::ServiceUnavailable, "local_reply_body", nullptr,
                                              std::nullopt, "base_fm_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));
  EXPECT_CALL(*filter_2_, decodeComplete()).WillOnce(Invoke([this]() {
    EXPECT_FALSE(filters_destroyed_);
  }));
  EXPECT_CALL(*filter_1_, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_2_, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_1_, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_1_, encodeData(_, true)).WillOnce(Return(FilterDataStatus::Continue));

  base_fm_->decodeHeaders(*request_headers_, true);
  EXPECT_TRUE(filters_destroyed_);
}

// Same as above for a decoder filter local reply in decodeTrailers().
TEST_F(BaseFilterManagerTest, LocalReplyInDecodeTrailersDestroyingFiltersRunsNoLaterCallbacks) {
  initializeBaseFilterManager("POST");

  EXPECT_CALL(*filter_1_, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, false);

  destroyFiltersOnEndStream();
  RequestTrailerMapPtr trailers{new TestRequestTrailerMapImpl{{"trailer", "value"}}};
  ON_CALL(filter_manager_callbacks_, requestTrailers())
      .WillByDefault(Return(makeOptRef(*trailers)));
  EXPECT_CALL(*filter_1_, decodeTrailers(_)).WillOnce(Return(FilterTrailersStatus::Continue));
  EXPECT_CALL(*filter_1_, decodeComplete()).WillOnce(Invoke([this]() {
    EXPECT_FALSE(filters_destroyed_);
  }));
  EXPECT_CALL(*filter_2_, decodeTrailers(_))
      .WillOnce(Invoke([&](RequestTrailerMap&) -> FilterTrailersStatus {
        filter_2_->callbacks_->addDecodedMetadata().emplace_back(
            std::make_unique<MetadataMap>(MetadataMap{{"a", "b"}}));
        filter_2_->callbacks_->sendLocalReply(Code::ServiceUnavailable, "local_reply_body", nullptr,
                                              std::nullopt, "base_fm_local_reply");
        return FilterTrailersStatus::StopIteration;
      }));
  EXPECT_CALL(*filter_2_, decodeComplete()).WillOnce(Invoke([this]() {
    EXPECT_FALSE(filters_destroyed_);
  }));
  EXPECT_CALL(*filter_1_, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_2_, decodeMetadata(_)).Times(0);
  EXPECT_CALL(*filter_1_, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_1_, encodeData(_, true)).WillOnce(Return(FilterDataStatus::Continue));

  base_fm_->decodeTrailers(*trailers);
  EXPECT_TRUE(filters_destroyed_);
}

TEST_F(BaseFilterManagerTest, SendLocalReplyInEncodeDataResetsWhenHeadersMovedOut) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  // Simulate UpstreamRequestFilterManagerCallbacks::encodeHeaders moving response_headers_ out.
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false))
      .WillOnce(Invoke([this](ResponseHeaderMap&, bool) {
        filter_manager_callbacks_.response_headers_.reset();
      }));

  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), false, "via_upstream");

  EXPECT_CALL(*filter_1_, encodeData(_, true))
      .WillOnce(Invoke([&](Buffer::Instance&, bool) -> FilterDataStatus {
        filter_1_->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "body_error",
                                                      nullptr, std::nullopt, "encode_data_error");
        return FilterDataStatus::StopIterationNoBuffer;
      }));
  EXPECT_CALL(filter_manager_callbacks_, resetStream(_, _));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);

  Buffer::OwnedImpl response_body("ok_body");
  filter_2_->callbacks_->encodeData(response_body, true);
  base_fm_->destroyFilters();
}

// A local reply whose modify_headers callback copies a full response header map (including
// :status) must still produce a single valid :status, so the required header check passes. The
// status comes from the local reply code, not from the copied :status.
TEST_F(BaseFilterManagerTest, LocalReplyModifyHeadersCopyingStatusKeepsValidStatus) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  // Mirrors the upstream codec filter forwarding a failed websocket handshake as a local reply.
  const TestResponseHeaderMapImpl upstream_headers{{":status", "200"}, {"upgrade", "websocket"}};
  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Invoke([](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        EXPECT_EQ("403", headers.getStatusValue());
        EXPECT_EQ("websocket", headers.getUpgradeValue());
        return FilterHeadersStatus::Continue;
      }));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, true))
      .WillOnce(Invoke(
          [](ResponseHeaderMap& headers, bool) { EXPECT_EQ("403", headers.getStatusValue()); }));

  filter_2_->callbacks_->sendLocalReply(
      Code::Forbidden, "",
      [&upstream_headers](ResponseHeaderMap& local_headers) {
        upstream_headers.iterate([&local_headers](const HeaderEntry& header) {
          local_headers.addCopy(LowerCaseString(header.key().getStringView()),
                                header.value().getStringView());
          return HeaderMap::Iterate::Continue;
        });
      },
      std::nullopt, "copied_status_local_reply");
  base_fm_->destroyFilters();
}

// When an encoder filter replaces a non-empty-body local reply with a chained sendLocalReply()
// during encodeHeaders() and endStream() does not synchronously destroy the filters (as on
// UpstreamRequestFilterManagerCallbacks or zombie downstream streams), the outer local reply must
// not encode its superseded body afterward.
TEST_F(BaseFilterManagerTest, ChainedLocalReplyInEncodeHeadersDoesNotEncodeOriginalBody) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(Code::BadRequest, "decoder_body", nullptr,
                                              std::nullopt, "decoder_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false))
      .WillOnce(Invoke([&](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        EXPECT_EQ("400", headers.getStatusValue());
        filter_1_->encoder_callbacks_->sendLocalReply(Code::ServiceUnavailable, "encoder_body",
                                                      nullptr, std::nullopt, "encoder_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));
  EXPECT_CALL(*filter_1_, encodeData(_, _)).Times(0);

  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false))
      .WillOnce(Invoke(
          [](ResponseHeaderMap& headers, bool) { EXPECT_EQ("503", headers.getStatusValue()); }));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, true))
      .WillOnce(
          Invoke([](Buffer::Instance& data, bool) { EXPECT_EQ("encoder_body", data.toString()); }));
  EXPECT_CALL(filter_manager_callbacks_, endStream());

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// When an encoder filter resets the stream while encoding the headers of a non-empty-body local
// reply and resetStream() does not synchronously destroy the filters, the aborted local reply body
// must not be encoded afterward.
TEST_F(BaseFilterManagerTest, ResetStreamInEncodeHeadersDoesNotEncodeLocalReplyBody) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(Code::BadRequest, "decoder_body", nullptr,
                                              std::nullopt, "decoder_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false))
      .WillOnce(Invoke([&](ResponseHeaderMap&, bool) -> FilterHeadersStatus {
        filter_1_->encoder_callbacks_->resetStream();
        return FilterHeadersStatus::StopIteration;
      }));
  EXPECT_CALL(filter_manager_callbacks_, resetStream(_, _));
  EXPECT_CALL(*filter_1_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, endStream()).Times(0);

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// An encoder filter local reply in encodeHeaders(end_stream=true) whose encoding destroys the
// filters synchronously must not invoke encodeComplete() after onDestroy().
TEST_F(BaseFilterManagerTest, LocalReplyInEncodeHeadersDestroyingFiltersSkipsEncodeComplete) {
  initializeBaseFilterManager();
  destroyFiltersOnEndStream();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Invoke([&](ResponseHeaderMap&, bool) -> FilterHeadersStatus {
        filter_1_->encoder_callbacks_->sendLocalReply(Code::ServiceUnavailable, "", nullptr,
                                                      std::nullopt, "encode_headers_local_reply");
        return FilterHeadersStatus::StopIteration;
      }));
  EXPECT_CALL(*filter_1_, encodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, true));

  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), true, "via_upstream");
  EXPECT_TRUE(filters_destroyed_);
}

// An encoder filter local reply in encodeData(end_stream=true) after response headers were sent
// resets the stream. If resetStream() synchronously destroys the filters, encodeComplete() must not
// run after onDestroy().
TEST_F(BaseFilterManagerTest, LocalReplyInEncodeDataDestroyingFiltersSkipsEncodeComplete) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false));
  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), false, "via_upstream");

  EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(filter_manager_callbacks_, resetStream(_, _))
      .WillOnce(
          Invoke([this](StreamResetReason, absl::string_view) { base_fm_->destroyFilters(); }));
  EXPECT_CALL(*filter_1_, encodeData(_, true))
      .WillOnce(Invoke([&](Buffer::Instance&, bool) -> FilterDataStatus {
        filter_1_->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "body_error",
                                                      nullptr, std::nullopt, "encode_data_error");
        return FilterDataStatus::StopIterationNoBuffer;
      }));
  EXPECT_CALL(*filter_1_, encodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);

  Buffer::OwnedImpl response_body("ok_body");
  filter_2_->callbacks_->encodeData(response_body, true);
  EXPECT_TRUE(filters_destroyed_);
}

// An encoder filter local reply in encodeTrailers() resets the stream and aborts trailer encoding.
// If resetStream() synchronously destroys the filters, encodeComplete() and encodeTrailers() must
// not run afterward, even if the filter returns Continue.
TEST_F(BaseFilterManagerTest, LocalReplyInEncodeTrailersDestroyingFiltersSkipsPostDestroyCalls) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, false)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false));
  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), false, "via_upstream");

  EXPECT_CALL(*filter_1_, encodeData(_, false)).WillOnce(Return(FilterDataStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, false));
  Buffer::OwnedImpl response_body("ok_body");
  filter_2_->callbacks_->encodeData(response_body, false);

  EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(filter_manager_callbacks_, resetStream(_, _))
      .WillOnce(
          Invoke([this](StreamResetReason, absl::string_view) { base_fm_->destroyFilters(); }));
  EXPECT_CALL(*filter_1_, encodeTrailers(_))
      .WillOnce(Invoke([&](ResponseTrailerMap&) -> FilterTrailersStatus {
        filter_1_->encoder_callbacks_->sendLocalReply(Code::InternalServerError, "trailer_error",
                                                      nullptr, std::nullopt,
                                                      "encode_trailers_error");
        return FilterTrailersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, encodeComplete()).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, encodeTrailers(_)).Times(0);
  EXPECT_CALL(filter_manager_callbacks_, endStream()).Times(0);

  ResponseTrailerMapPtr response_trailers{new TestResponseTrailerMapImpl{{"x-trailer", "1"}}};
  ON_CALL(filter_manager_callbacks_, responseTrailers())
      .WillByDefault(Return(makeOptRef(*response_trailers)));
  filter_2_->callbacks_->encodeTrailers(std::move(response_trailers));
  EXPECT_TRUE(filters_destroyed_);
}

// If an encoder filter injects body data during encodeHeaders(end_stream=true) and
// filter_manager_callbacks_.encodeHeaders() synchronously destroys the filters (for example when
// Router::Filter retries a 503 response without setting saw_downstream_reset_), FilterManager must
// not continue encoding the buffered body after onDestroy().
TEST_F(BaseFilterManagerTest, DestroyDuringEncodeHeadersCallbackSkipsBodyContinuation) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Invoke([&](ResponseHeaderMap& headers, bool) -> FilterHeadersStatus {
        Buffer::OwnedImpl injected("injected_body");
        filter_1_->encoder_callbacks_->addEncodedData(injected, false);
        headers.setContentLength(injected.length());
        return FilterHeadersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, false))
      .WillOnce(Invoke([this](ResponseHeaderMap&, bool) { base_fm_->destroyFilters(); }));
  EXPECT_CALL(filter_manager_callbacks_, encodeData(_, _)).Times(0);

  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "503"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), true, "via_upstream");
  EXPECT_TRUE(filters_destroyed_);
}

// If a filter prepares a local reply during a decoder callback and then calls sendLocalReply() a
// second time and resets the stream before returning, the duplicate local reply must be ignored and
// resetting must clear the prepared local reply so it is never encoded afterward.
TEST_F(BaseFilterManagerTest, PreparedLocalReplyClearedOnResetAndDuplicateLocalReplyIgnored) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true))
      .WillOnce(Invoke([&](RequestHeaderMap&, bool) -> FilterHeadersStatus {
        filter_2_->callbacks_->sendLocalReply(Code::ServiceUnavailable, "first", nullptr,
                                              std::nullopt, "first_details");
        filter_2_->callbacks_->sendLocalReply(Code::BadGateway, "second", nullptr, std::nullopt,
                                              "second_details");
        filter_2_->callbacks_->resetStream(StreamResetReason::LocalReset, "");
        return FilterHeadersStatus::StopIteration;
      }));
  EXPECT_CALL(stream_info_, setResponseCodeDetails(absl::string_view("first_details")));
  EXPECT_CALL(stream_info_, setResponseCodeDetails(absl::string_view("second_details"))).Times(0);
  EXPECT_CALL(*filter_1_, onLocalReply(_)).WillOnce(Return(LocalErrorStatus::Continue));
  EXPECT_CALL(*filter_2_, onLocalReply(_)).WillOnce(Return(LocalErrorStatus::Continue));
  EXPECT_CALL(filter_manager_callbacks_, resetStream(StreamResetReason::LocalReset, ""));
  EXPECT_CALL(*filter_1_, encodeHeaders(_, _)).Times(0);

  base_fm_->decodeHeaders(*request_headers_, true);
  base_fm_->destroyFilters();
}

// If encode1xxHeaders() synchronously destroys the filters during a filter callback without setting
// encoder_filter_chain_aborted_, FilterManager must not forward the 1xx headers to callbacks after
// onDestroy().
TEST_F(BaseFilterManagerTest, Encode1xxHeadersSkipsCodecCallbackWhenDestroyed) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encode1xxHeaders(_))
      .WillOnce(Invoke([this](ResponseHeaderMap&) -> Filter1xxHeadersStatus {
        base_fm_->destroyFilters();
        return Filter1xxHeadersStatus::Continue;
      }));
  EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(filter_manager_callbacks_, encode1xxHeaders(_)).Times(0);

  ResponseHeaderMapPtr info_headers{new TestResponseHeaderMapImpl{{":status", "100"}}};
  filter_2_->callbacks_->encode1xxHeaders(std::move(info_headers));
  EXPECT_TRUE(filters_destroyed_);
}

// Calling continueEncoding() on a paused encoder filter after destroyFilters() has run must be a
// no-op even when saw_downstream_reset_ was not set.
TEST_F(BaseFilterManagerTest, ContinueEncodingNoOpAfterDestroy) {
  initializeBaseFilterManager();

  EXPECT_CALL(*filter_1_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  EXPECT_CALL(*filter_2_, decodeHeaders(_, true)).WillOnce(Return(FilterHeadersStatus::Continue));
  base_fm_->decodeHeaders(*request_headers_, true);

  EXPECT_CALL(*filter_1_, encodeHeaders(_, true))
      .WillOnce(Return(FilterHeadersStatus::StopIteration));
  EXPECT_CALL(filter_manager_callbacks_, encodeHeaders(_, _)).Times(0);

  ResponseHeaderMapPtr response_headers{new TestResponseHeaderMapImpl{{":status", "200"}}};
  filter_2_->callbacks_->encodeHeaders(std::move(response_headers), true, "via_upstream");

  EXPECT_CALL(*filter_1_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  EXPECT_CALL(*filter_2_, onDestroy()).WillOnce(Invoke([this]() { filters_destroyed_ = true; }));
  base_fm_->destroyFilters();
  EXPECT_TRUE(filters_destroyed_);

  filter_1_->encoder_callbacks_->continueEncoding();
}

} // namespace
} // namespace Http
} // namespace Envoy
