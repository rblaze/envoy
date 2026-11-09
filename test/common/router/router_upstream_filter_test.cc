#include "envoy/config/route/v3/route_components.pb.h"
#include "envoy/extensions/filters/http/router/v3/router.pb.h"
#include "envoy/extensions/filters/http/upstream_codec/v3/upstream_codec.pb.h"
#include "envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.pb.h"

#include "source/common/router/config_impl.h"
#include "source/common/router/router.h"
#include "source/common/router/upstream_codec_filter.h"
#include "source/extensions/filters/http/common/pass_through_filter.h"

#include "test/common/http/common.h"
#include "test/integration/filters/test_filters.pb.h"
#include "test/mocks/event/mocks.h"
#include "test/mocks/http/mocks.h"
#include "test/mocks/network/mocks.h"
#include "test/mocks/router/mocks.h"
#include "test/mocks/server/factory_context.h"
#include "test/test_common/registry.h"
#include "test/test_common/test_runtime.h"
#include "test/test_common/utility.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using testing::_;
using testing::Eq;
using testing::NiceMock;
using testing::Return;
using testing::ReturnRef;
namespace Envoy {
namespace Router {
namespace {

using envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter;

class TestFilter : public Filter {
public:
  using Filter::Filter;

  // Creates the retry state up front, so that tests can set expectations on it before the router
  // creates it in decodeHeaders().
  void preallocateRetryState() {
    if (preallocated_retry_state_ == nullptr) {
      preallocated_retry_state_ = std::make_unique<NiceMock<MockRetryState>>();
      retry_state_ = preallocated_retry_state_.get();
    }
  }

  // Filter
  RetryStatePtr createRetryState(const RetryPolicy&, Http::RequestHeaderMap&,
                                 const Upstream::ClusterInfo&,
                                 Server::Configuration::CommonFactoryContext&, Event::Dispatcher&,
                                 Upstream::ResourcePriority) override {
    if (preallocated_retry_state_ != nullptr) {
      return std::move(preallocated_retry_state_);
    }
    EXPECT_EQ(nullptr, retry_state_);
    retry_state_ = new NiceMock<MockRetryState>();
    return RetryStatePtr{retry_state_};
  }

  const Network::Connection* downstreamConnection() const override {
    return &downstream_connection_;
  }

  NiceMock<Network::MockConnection> downstream_connection_;
  std::unique_ptr<NiceMock<MockRetryState>> preallocated_retry_state_;
  MockRetryState* retry_state_{};
};

// An upstream HTTP encoder filter that marks the response it sees, or replaces it with a local
// reply from encodeHeaders().
class ResponseModifyingEncoderFilter : public Http::PassThroughEncoderFilter {
public:
  // Http::StreamEncoderFilter
  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers, bool) override {
    ++encode_headers_called_;
    if (send_local_reply_in_encode_headers_) {
      encoder_callbacks_->sendLocalReply(local_reply_code_, local_reply_body_, nullptr,
                                         std::nullopt, local_reply_details_);
      return Http::FilterHeadersStatus::StopIteration;
    }
    headers.addCopy(Http::LowerCaseString("x-upstream-encoder"), "modified");
    if (headers.ContentLength() != nullptr && !body_suffix_.empty()) {
      uint64_t content_length = 0;
      if (absl::SimpleAtoi(headers.getContentLengthValue(), &content_length)) {
        headers.setContentLength(content_length + body_suffix_.size());
      }
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool) override {
    ++encode_data_called_;
    if (send_local_reply_in_encode_data_) {
      encoder_callbacks_->sendLocalReply(local_reply_code_, local_reply_body_, nullptr,
                                         std::nullopt, local_reply_details_);
      return Http::FilterDataStatus::StopIterationNoBuffer;
    }
    if (!body_suffix_.empty()) {
      data.add(body_suffix_);
    }
    return Http::FilterDataStatus::Continue;
  }

  int encode_headers_called_{0};
  int encode_data_called_{0};
  std::string body_suffix_{" [encoder-modified]"};
  bool send_local_reply_in_encode_headers_{false};
  bool send_local_reply_in_encode_data_{false};
  Http::Code local_reply_code_{Http::Code::BadGateway};
  std::string local_reply_body_{"encoder local reply"};
  std::string local_reply_details_{"encoder_local_reply_details"};
};

// An upstream HTTP decoder filter that sends a local reply in the configured phase.
class UpstreamLocalReplyDecoderFilter : public Http::PassThroughDecoderFilter,
                                        public Http::UpstreamCallbacks {
public:
  enum class TriggerPhase { None, OnHostSelected, DecodeHeaders, DecodeData, DecodeTrailers };

  explicit UpstreamLocalReplyDecoderFilter(TriggerPhase phase,
                                           Http::Code code = Http::Code::ServiceUnavailable,
                                           std::string body = "upstream filter local reply",
                                           std::string details = "upstream_filter_details")
      : phase_(phase), code_(code), body_(std::move(body)), details_(std::move(details)) {}

  // Http::StreamDecoderFilter
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap&, bool) override {
    if (phase_ == TriggerPhase::DecodeHeaders) {
      sendLocalReply();
      return Http::FilterHeadersStatus::StopIteration;
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override {
    if (phase_ == TriggerPhase::DecodeData) {
      sendLocalReply();
      return Http::FilterDataStatus::StopIterationNoBuffer;
    }
    return Http::FilterDataStatus::Continue;
  }
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override {
    if (phase_ == TriggerPhase::DecodeTrailers) {
      sendLocalReply();
      return Http::FilterTrailersStatus::StopIteration;
    }
    return Http::FilterTrailersStatus::Continue;
  }
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override {
    Http::PassThroughDecoderFilter::setDecoderFilterCallbacks(callbacks);
    if (auto cb = decoder_callbacks_->upstreamCallbacks(); cb) {
      cb->addUpstreamCallbacks(*this);
    }
  }

  void onDestroy() override { destroyed_ = true; }
  Http::LocalErrorStatus onLocalReply(const LocalReplyData&) override {
    ++on_local_reply_called_;
    return reset_on_local_reply_ ? Http::LocalErrorStatus::ContinueAndResetStream
                                 : Http::LocalErrorStatus::Continue;
  }

  // Http::UpstreamCallbacks
  void onHostSelected(const Upstream::HostDescriptionConstSharedPtr&) override {
    if (phase_ == TriggerPhase::OnHostSelected) {
      sendLocalReply();
      destroyed_during_on_host_selected_ = destroyed_;
    }
  }
  void onUpstreamConnectionEstablished() override {}

  // Applied to the local reply headers, if set.
  std::function<void(Http::ResponseHeaderMap&)> modify_headers_;
  // If true, onLocalReply() asks for the stream to be reset instead of sending the local reply.
  bool reset_on_local_reply_{false};
  int on_local_reply_called_{0};
  bool destroyed_{false};
  // Whether the filter was destroyed by the time sendLocalReply() returned in onHostSelected().
  bool destroyed_during_on_host_selected_{false};

private:
  void sendLocalReply() {
    decoder_callbacks_->sendLocalReply(code_, body_, modify_headers_, std::nullopt, details_);
  }

  TriggerPhase phase_;
  Http::Code code_;
  std::string body_;
  std::string details_;
};

// An upstream HTTP filter that tracks its own lifecycle. It is used to detect the router
// driving decode callbacks on a filter that has already been destroyed.
class LifecycleTrackingFilter : public Http::StreamDecoderFilter {
public:
  // Http::StreamDecoderFilter
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap&, bool) override {
    ++decode_headers_;
    EXPECT_FALSE(destroyed_);
    if (send_local_reply_in_decode_headers_) {
      callbacks_->sendLocalReply(Http::Code::ServiceUnavailable, "lifecycle local reply", nullptr,
                                 std::nullopt, "lifecycle_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override {
    ++decode_data_;
    EXPECT_FALSE(destroyed_);
    return Http::FilterDataStatus::Continue;
  }
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override {
    ++decode_trailers_;
    EXPECT_FALSE(destroyed_);
    return Http::FilterTrailersStatus::Continue;
  }
  void decodeComplete() override {
    ++decode_complete_;
    EXPECT_FALSE(destroyed_);
  }
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override {
    callbacks_ = &callbacks;
  }
  void onDestroy() override { destroyed_ = true; }

  Http::StreamDecoderFilterCallbacks* callbacks_{};
  bool send_local_reply_in_decode_headers_{false};
  bool destroyed_{false};
  int decode_headers_{0};
  int decode_data_{0};
  int decode_trailers_{0};
  int decode_complete_{0};
};

// Records what the router hands to the factory of an upstream HTTP filter.
class StatsRecordingFilterFactory : public Server::Configuration::UpstreamHttpFilterConfigFactory {
public:
  absl::StatusOr<Http::FilterFactoryCb> createHttpFilterFactoryFromProto(
      const Protobuf::Message&, Server::Configuration::ServerFactoryContext& context,
      Server::Configuration::ExtraFactoryContext& extra_context) override {
    stats_prefix_ = extra_context.stats_prefix;
    scope_ = &extra_context.scopeOr(context);
    stats_prefix_scope_ = &extra_context.statsPrefixScopeOr(context);
    stats_prefix_or_ = extra_context.statsPrefixOr();
    return [](Http::FilterChainFactoryCallbacks&) {};
  }
  bool isUnifiedFilter() override { return true; }
  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<test::integration::filters::PassthroughFilterConfig>();
  }
  std::string name() const override { return "stats-recording-filter"; }

  std::string stats_prefix_;
  Stats::Scope* scope_{};
  Stats::Scope* stats_prefix_scope_{};
  std::string stats_prefix_or_;
};

class RouterUpstreamFilterTest : public testing::Test {
public:
  void init(std::vector<HttpFilter> upstream_filters, const std::string& stats_prefix = "prefix.",
            OptRef<Stats::Scope> stats_prefix_scope = {}) {
    envoy::extensions::filters::http::router::v3::Router router_proto;
    static const std::string cluster_name = "cluster_0";

    cluster_info_ = std::make_shared<NiceMock<Upstream::MockClusterInfo>>();
    ON_CALL(*cluster_info_, name()).WillByDefault(ReturnRef(cluster_name));
    ON_CALL(*cluster_info_, observabilityName()).WillByDefault(ReturnRef(cluster_name));
    callbacks_.stream_info_.upstream_cluster_info_ = cluster_info_;
    EXPECT_CALL(callbacks_.dispatcher_, deferredDelete_).Times(testing::AnyNumber());
    for (const auto& filter : upstream_filters) {
      *router_proto.add_upstream_http_filters() = filter;
    }

    auto extra_context = Server::Configuration::ExtraFactoryContext::create(context_, stats_prefix);
    extra_context.stats_prefix_scope = stats_prefix_scope;
    shadow_writer_ = new MockShadowWriter();
    config_ = *FilterConfig::create(context_.server_factory_context_, extra_context,
                                    ShadowWriterPtr(shadow_writer_), router_proto);
    router_ = std::make_shared<TestFilter>(config_, config_->default_stats_);
    router_->setDecoderFilterCallbacks(callbacks_);
    EXPECT_CALL(callbacks_.dispatcher_, pushTrackedObject(_)).Times(testing::AnyNumber());
    EXPECT_CALL(callbacks_.dispatcher_, popTrackedObject(_)).Times(testing::AnyNumber());

    upstream_locality_.set_zone("to_az");
    context_.server_factory_context_.cluster_manager_.initializeThreadLocalClusters(
        {"fake_cluster"});
    ON_CALL(
        *context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_.host_,
        address())
        .WillByDefault(Return(host_address_));
    ON_CALL(
        *context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_.host_,
        locality())
        .WillByDefault(ReturnRef(upstream_locality_));
    ON_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_, chooseHost(_))
        .WillByDefault([this] {
          return Upstream::HostSelectionResponse{
              context_.server_factory_context_.cluster_manager_.thread_local_cluster_.lb_.host_};
        });
    router_->downstream_connection_.stream_info_.downstream_connection_info_provider_
        ->setLocalAddress(host_address_);
    router_->downstream_connection_.stream_info_.downstream_connection_info_provider_
        ->setRemoteAddress(Network::Utility::parseInternetAddressAndPortNoThrow("1.2.3.4:80"));
  }

  // Initializes the router with only the upstream codec filter in its upstream filter chain. Tests
  // add their own upstream filters ahead of it through the cluster's filter chain factory (see
  // addFilters()).
  void initWithCodecFilter() {
    HttpFilter codec_filter;
    codec_filter.set_name("envoy.filters.http.upstream_codec");
    envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec upstream_codec_config;
    std::ignore = codec_filter.mutable_typed_config()->PackFrom(upstream_codec_config);

    init({codec_filter});
    ON_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_, info())
        .WillByDefault(Return(cluster_info_));
  }

  // Adds a route shadow policy for "shadow_cluster" that is always enabled.
  void addShadowPolicy() {
    envoy::config::route::v3::RouteAction::RequestMirrorPolicy policy;
    policy.set_cluster("shadow_cluster");
    NiceMock<Server::Configuration::MockServerFactoryContext> factory_context;
    callbacks_.route_->route_entry_.shadow_policies_.push_back(THROW_OR_RETURN_VALUE(
        ShadowPolicyImpl::create(policy, factory_context), std::shared_ptr<ShadowPolicyImpl>));
    ON_CALL(context_.server_factory_context_.runtime_loader_.snapshot_,
            featureEnabled(_, testing::Matcher<const envoy::type::v3::FractionalPercent&>(_),
                           testing::Matcher<uint64_t>(_)))
        .WillByDefault(Return(true));
  }

  auto& connPool() {
    return context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_;
  }

  // Returns a cluster filter chain factory action that adds the given upstream filters (either can
  // be null) and then falls through to the router's upstream filter chain, which provides the
  // codec filter.
  static auto addFilters(Http::StreamEncoderFilterSharedPtr encoder_filter,
                         Http::StreamDecoderFilterSharedPtr decoder_filter) {
    return Invoke(
        [encoder_filter, decoder_filter](Http::FilterChainFactoryCallbacks& callbacks) -> bool {
          if (encoder_filter != nullptr) {
            callbacks.addStreamEncoderFilter(encoder_filter);
          }
          if (decoder_filter != nullptr) {
            callbacks.addStreamDecoderFilter(decoder_filter);
          }
          return false;
        });
  }

  // Returns a connection pool newStream() action that makes the pool ready right away with
  // `encoder`, and stores the response decoder in `response_decoder` if it is not null.
  auto poolReadyWith(Http::MockRequestEncoder& encoder,
                     Http::ResponseDecoder** response_decoder = nullptr) {
    return Invoke([this, &encoder, response_decoder](
                      Http::ResponseDecoder& decoder, Http::ConnectionPool::Callbacks& callbacks,
                      const Http::ConnectionPool::Instance::StreamOptions&)
                      -> Http::ConnectionPool::Cancellable* {
      if (response_decoder != nullptr) {
        *response_decoder = &decoder;
      }
      EXPECT_CALL(encoder.stream_, connectionInfoProvider())
          .WillRepeatedly(ReturnRef(connection_info1_));
      callbacks.onPoolReady(encoder, connPool().host_, stream_info_, Http::Protocol::Http11);
      return nullptr;
    });
  }

  Http::TestRequestHeaderMapImpl run() {
    NiceMock<Http::MockRequestEncoder> encoder;
    Http::ResponseDecoder* response_decoder = nullptr;

    EXPECT_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_,
                newStream(_, _, _))
        .WillOnce([&](Http::ResponseDecoder& decoder, Http::ConnectionPool::Callbacks& callbacks,
                      const Http::ConnectionPool::Instance::StreamOptions&)
                      -> Http::ConnectionPool::Cancellable* {
          response_decoder = &decoder;
          EXPECT_CALL(encoder.stream_, connectionInfoProvider())
              .WillRepeatedly(ReturnRef(connection_info1_));
          callbacks.onPoolReady(encoder,
                                context_.server_factory_context_.cluster_manager_
                                    .thread_local_cluster_.conn_pool_.host_,
                                stream_info_, Http::Protocol::Http10);
          return nullptr;
        });

    Http::TestRequestHeaderMapImpl headers;
    HttpTestUtility::addDefaultHeaders(headers);
    router_->decodeHeaders(headers, true);

    EXPECT_CALL(*router_->retry_state_, shouldRetryHeaders(_, _, _))
        .WillOnce(Return(RetryStatus::No));

    Http::ResponseHeaderMapPtr response_headers(new Http::TestResponseHeaderMapImpl());
    response_headers->setStatus(200);

    EXPECT_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_
                    .host_->outlier_detector_,
                putResult(_, std::optional<uint64_t>(200)));
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    response_decoder->decodeHeaders(std::move(response_headers), true);
    return headers;
  }

  NiceMock<Server::Configuration::MockFactoryContext> context_;

  envoy::config::core::v3::Locality upstream_locality_;
  Network::Address::InstanceConstSharedPtr host_address_{
      *Network::Utility::resolveUrl("tcp://10.0.0.5:9211")};
  Network::Address::InstanceConstSharedPtr upstream_local_address1_{
      *Network::Utility::resolveUrl("tcp://10.0.0.5:10211")};
  Network::ConnectionInfoSetterImpl connection_info1_{upstream_local_address1_,
                                                      upstream_local_address1_};

  NiceMock<Http::MockStreamDecoderFilterCallbacks> callbacks_;
  MockShadowWriter* shadow_writer_{}; // Owned by config_.
  std::shared_ptr<FilterConfig> config_;
  std::shared_ptr<TestFilter> router_;
  std::shared_ptr<NiceMock<Upstream::MockClusterInfo>> cluster_info_;
  NiceMock<StreamInfo::MockStreamInfo> stream_info_;
};

TEST_F(RouterUpstreamFilterTest, UpstreamFilter) {
  HttpFilter add_header_filter;
  add_header_filter.set_name("add-header-filter");
  test::integration::filters::AddHeaderEmptyFilterConfig add_header_config;
  std::ignore = add_header_filter.mutable_typed_config()->PackFrom(add_header_config);

  HttpFilter codec_filter;
  codec_filter.set_name("envoy.filters.http.upstream_codec");
  envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec upstream_codec_config;
  std::ignore = codec_filter.mutable_typed_config()->PackFrom(upstream_codec_config);

  init({add_header_filter, codec_filter});
  auto headers = run();
  EXPECT_FALSE(headers.get(Http::LowerCaseString("x-header-to-add")).empty());
}

// Regression test for decode-after-destroy of the upstream HTTP filter chain.
//
// A circuit breaker overflow surfacing synchronously inside
// ConnPoolImplBase::newStream() (pending requests overflow, connection limit
// load shedding, or max requests overflow when a ready client exists) resets
// the upstream request and deferred-deletes it before newStream() returns.
// Deferred deletion runs UpstreamRequest::deleteIsPending() -> cleanUp() ->
// destroyFilters() synchronously, so every upstream HTTP filter has already
// seen onDestroy() when UpstreamRequest::acceptHeadersFromRouter() resumes and
// drives decodeHeaders() on the destroyed filter chain. Native C++ filters
// tolerate decode-after-destroy; the dynamic modules HTTP filter does not: it
// releases its in-module filter in onDestroy() and dereferences stale state
// when decode headers run afterwards. acceptHeadersFromRouter() must not
// continue filter chain processing when the request was reset inside
// newStream().
TEST_F(RouterUpstreamFilterTest, SyncPoolFailureDoesNotDecodeAfterDestroy) {
  auto filter = std::make_shared<LifecycleTrackingFilter>();

  HttpFilter codec_filter;
  codec_filter.set_name("envoy.filters.http.upstream_codec");
  envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec upstream_codec_config;
  std::ignore = codec_filter.mutable_typed_config()->PackFrom(upstream_codec_config);

  init({codec_filter});

  // The upstream request resolves the cluster through the thread-local cluster, so point its
  // cluster info at our instrumented mock.
  ON_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_, info())
      .WillByDefault(Return(cluster_info_));

  // Install the tracking filter ahead of the codec filter via the cluster's filter chain
  // factory, which takes precedence over the router-specified chain.
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce([&](Http::FilterChainFactoryCallbacks& callbacks) -> bool {
        callbacks.addStreamDecoderFilter(filter);
        // Fall through to the router-specified chain, which provides the codec filter.
        return false;
      });

  // Fail the pool synchronously inside newStream(), the way pending requests circuit
  // breaker overflow does in ConnPoolImplBase::newStreamImpl().
  EXPECT_CALL(context_.server_factory_context_.cluster_manager_.thread_local_cluster_.conn_pool_,
              newStream(_, _, _))
      .WillOnce([&](Http::ResponseDecoder&, Http::ConnectionPool::Callbacks& callbacks,
                    const Http::ConnectionPool::Instance::StreamOptions&)
                    -> Http::ConnectionPool::Cancellable* {
        callbacks.onPoolFailure(Http::ConnectionPool::PoolFailureReason::Overflow,
                                "max pending streams overflow",
                                context_.server_factory_context_.cluster_manager_
                                    .thread_local_cluster_.conn_pool_.host_);
        return nullptr;
      });

  // The request should shed cleanly with a 503.
  EXPECT_CALL(callbacks_, sendLocalReply(Http::Code::ServiceUnavailable, _, _, _, _));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  // The synchronous overflow reset the upstream request, which ran cleanUp() and
  // onDestroy() on the upstream HTTP filters before newStream() returned.
  EXPECT_TRUE(filter->destroyed_);
  // The destroyed filter chain must not be driven any further.
  EXPECT_EQ(0, filter->decode_headers_);
  EXPECT_EQ(0, filter->decode_data_);
  EXPECT_EQ(0, filter->decode_trailers_);
}

// Regression test for a use-after-free at teardown. A dynamic (ECDS) upstream HTTP filter causes
// FilterConfig to create a Filter::FilterConfigSubscription, which holds a raw reference to the
// filter config provider manager and dereferences it from its destructor. That manager is an
// *unpinned* singleton, so it survives only as long as something holds a strong reference to it:
// FilterConfig must be that holder for the providers it owns.
TEST_F(RouterUpstreamFilterTest, DynamicFilterKeepsConfigProviderManagerAlive) {
  HttpFilter dynamic_filter;
  dynamic_filter.set_name("dynamic-filter");
  auto* config_discovery = dynamic_filter.mutable_config_discovery();
  config_discovery->mutable_config_source()->mutable_ads();
  config_discovery->add_type_urls(
      "type.googleapis.com/test.integration.filters.AddHeaderEmptyFilterConfig");

  HttpFilter codec_filter;
  codec_filter.set_name("envoy.filters.http.upstream_codec");
  envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec upstream_codec_config;
  std::ignore = codec_filter.mutable_typed_config()->PackFrom(upstream_codec_config);

  init({dynamic_filter, codec_filter});
  ASSERT_NE(nullptr, config_);

  // Looking the singleton up again returns the live instance if one exists and creates a fresh one
  // otherwise. Binding it to a weak_ptr and letting the returned strong reference expire at the end
  // of the statement therefore reports whether `config_` is keeping the manager alive: without that
  // reference, the manager created during init() is already gone and every subscription it owns is
  // left holding a dangling reference.
  std::weak_ptr<Http::UpstreamFilterConfigProviderManager> manager =
      Http::FilterChainUtility::createSingletonUpstreamFilterConfigProviderManager(
          context_.server_factory_context_);
  EXPECT_FALSE(manager.expired());

  // Destroying the router config destroys the providers and their subscription, which writes
  // through that reference. The manager must still be alive at that point (this is where ASAN
  // reports the use-after-free without the fix) and may be released only afterwards.
  router_.reset();
  config_.reset();
  EXPECT_TRUE(manager.expired());
}

class RouterUpstreamFilterStatsTest : public RouterUpstreamFilterTest {
public:
  void initWithRecordingFilter(const std::string& stats_prefix,
                               OptRef<Stats::Scope> stats_prefix_scope = {}) {
    HttpFilter recording_filter;
    recording_filter.set_name("stats-recording-filter");
    std::ignore = recording_filter.mutable_typed_config()->PackFrom(
        test::integration::filters::PassthroughFilterConfig());

    HttpFilter codec_filter;
    codec_filter.set_name("envoy.filters.http.upstream_codec");
    envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec upstream_codec_config;
    std::ignore = codec_filter.mutable_typed_config()->PackFrom(upstream_codec_config);

    init({recording_filter, codec_filter}, stats_prefix, stats_prefix_scope);
  }

  StatsRecordingFilterFactory factory_;
  Registry::InjectFactory<Server::Configuration::UpstreamHttpFilterConfigFactory> registration_{
      factory_};
};

TEST_F(RouterUpstreamFilterStatsTest, NoStatsPrefixScope) {
  initWithRecordingFilter("http.ingress.");

  EXPECT_EQ("http.ingress.rq_total", config_->default_stats_.rq_total_.name());
  // The prefix is passed as is rather than re-decoded from a stat name, which drops the trailing
  // dot.
  EXPECT_EQ("http.ingress.", factory_.stats_prefix_);
  EXPECT_EQ(&context_.scope(), factory_.scope_);
  EXPECT_EQ(&context_.scope(), factory_.stats_prefix_scope_);
  EXPECT_EQ("http.ingress.", factory_.stats_prefix_or_);
}

TEST_F(RouterUpstreamFilterStatsTest, StatsPrefixScope) {
  // Named differently from the prefix to tell which one the stats got their name from.
  Stats::ScopeSharedPtr prefixed_scope = context_.scope().createScope("prefixed.");
  initWithRecordingFilter("http.ingress.", *prefixed_scope);

  EXPECT_EQ("prefixed.rq_total", config_->default_stats_.rq_total_.name());
  // The vhost, vcluster and route level stats stay in the root scope.
  EXPECT_EQ(&context_.scope(), &config_->scope_);

  EXPECT_EQ("http.ingress.", factory_.stats_prefix_);
  EXPECT_EQ(&context_.scope(), factory_.scope_);
  EXPECT_EQ(prefixed_scope.get(), factory_.stats_prefix_scope_);
  EXPECT_EQ("", factory_.stats_prefix_or_);
}

// TODO(wbpcode): remove this test once the
// envoy.reloadable_features.ecds_filter_use_parent_stats_prefix runtime flag is removed.
TEST_F(RouterUpstreamFilterStatsTest, StatsPrefixScopeNotGivenToOtherPrefix) {
  Stats::ScopeSharedPtr prefixed_scope = context_.scope().createScope("prefixed.");
  initWithRecordingFilter("http.ingress.", *prefixed_scope);

  // E.g. an ECDS filter that is created with the prefix of its subscription.
  const std::string other_prefix = "extension_config_discovery.upstream_http_filter.foo.";
  auto extra_context =
      Server::Configuration::ExtraFactoryContext::create(*config_->upstream_ctx_, other_prefix);
  EXPECT_FALSE(extra_context.stats_prefix_scope.has_value());
  EXPECT_EQ(other_prefix, extra_context.statsPrefixOr());
}

TEST_F(RouterUpstreamFilterStatsTest, StatsPrefixScopeWithLegacyEmptyPrefix) {
  TestScopedRuntime scoped_runtime;
  scoped_runtime.mergeValues(
      {{"envoy.reloadable_features.upstream_http_filters_correct_stats_prefix", "false"}});
  Stats::ScopeSharedPtr prefixed_scope = context_.scope().createScope("prefixed.");
  initWithRecordingFilter("http.ingress.", *prefixed_scope);

  EXPECT_EQ("", factory_.stats_prefix_);
  EXPECT_EQ(&context_.scope(), factory_.stats_prefix_scope_);
  EXPECT_EQ("", factory_.stats_prefix_or_);
}

TEST_F(RouterUpstreamFilterTest, LocalReplyInOnHostSelectedFlowsThroughUpstreamEncoderChain) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::OnHostSelected, Http::Code::Forbidden,
      "rejected host", "host_rejected_details");

  initWithCodecFilter();
  router_->preallocateRetryState();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter, decoder_filter));
  EXPECT_CALL(connPool(), newStream(_, _, _)).Times(0);

  // The rejected host is reported to the retry state, so retry host predicates can skip it.
  EXPECT_CALL(*router_->retry_state_, onHostAttempted(_))
      .WillOnce(Invoke([this](Upstream::HostDescriptionConstSharedPtr host) {
        EXPECT_EQ(connPool().host_.get(), host.get());
      }));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("403", headers.getStatusValue());
        const auto encoder_header = headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("rejected host [encoder-modified]", data.toString());
  }));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_EQ(1, encoder_filter->encode_headers_called_);
  EXPECT_EQ(1, encoder_filter->encode_data_called_);
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ("host_rejected_details", *callbacks_.streamInfo().responseCodeDetails());
  EXPECT_EQ("host_rejected_details", callbacks_.details());
}

// A local reply sent from onHostSelected() is only encoded once onHostSelected() returns. Encoding
// it completes the upstream request and destroys the upstream filters, which must not happen while
// the filter's onHostSelected() is still running.
TEST_F(RouterUpstreamFilterTest, LocalReplyInOnHostSelectedDoesNotDestroyFilterReentrantly) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::OnHostSelected, Http::Code::Forbidden,
      "rejected host", "host_rejected_details");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  EXPECT_CALL(connPool(), newStream(_, _, _)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_CALL(callbacks_, encodeData(_, true));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_FALSE(decoder_filter->destroyed_during_on_host_selected_);
  EXPECT_TRUE(decoder_filter->destroyed_);
  EXPECT_EQ("host_rejected_details", callbacks_.details());
}

// Same as above when the local reply from onHostSelected() is retried, which also cleans up the
// upstream request.
TEST_F(RouterUpstreamFilterTest, RetriedLocalReplyInOnHostSelectedDoesNotDestroyFilterReentrantly) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::OnHostSelected);

  initWithCodecFilter();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  EXPECT_CALL(connPool(), newStream(_, _, _)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_FALSE(decoder_filter->destroyed_during_on_host_selected_);
  EXPECT_TRUE(decoder_filter->destroyed_);
  EXPECT_TRUE(router_->upstreamRequests().empty());
  router_->onDestroy();
}

// An upstream filter can ask for the stream to be reset instead of sending a local reply by
// returning ContinueAndResetStream from onLocalReply(). The downstream stream is reset, and the
// local reply's details are still recorded on the downstream stream.
TEST_F(RouterUpstreamFilterTest, UpstreamFilterOnLocalReplyContinueAndResetStream) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::ServiceUnavailable,
      "unavailable", "upstream_filter_reset_details");
  decoder_filter->reset_on_local_reply_ = true;

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(cancellable, cancel(_));

  EXPECT_CALL(connPool().host_->outlier_detector_, putResult(_, _)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);
  EXPECT_CALL(callbacks_, encodeData(_, _)).Times(0);
  EXPECT_CALL(callbacks_, sendLocalReply(_, _, _, _, _)).Times(0);
  EXPECT_CALL(callbacks_, resetStream(_, _));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_EQ(1, decoder_filter->on_local_reply_called_);
  EXPECT_EQ("upstream_filter_reset_details",
            callbacks_.streamInfo().responseCodeDetails().value_or(""));
  router_->onDestroy();
}

TEST_F(RouterUpstreamFilterTest, LocalReplyInDecodeHeadersFlowsThroughUpstreamEncoderChain) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::BadRequest,
      "rejected headers", "decode_headers_local_reply");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter, decoder_filter));

  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(cancellable, cancel(_));

  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("400", headers.getStatusValue());
        const auto encoder_header = headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("rejected headers [encoder-modified]", data.toString());
  }));
  EXPECT_CALL(callbacks_, sendLocalReply(_, _, _, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  // Send with end_stream=false followed by decodeData to verify onUpstreamComplete cleans up
  // the aborted upstream request and decodeData does not emit a second EarlyUpstreamReset reply.
  router_->decodeHeaders(headers, false);
  Buffer::OwnedImpl body("request body");
  router_->decodeData(body, true);

  EXPECT_EQ(1, encoder_filter->encode_headers_called_);
  EXPECT_EQ(1, encoder_filter->encode_data_called_);
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ("decode_headers_local_reply", *callbacks_.streamInfo().responseCodeDetails());
  EXPECT_EQ("decode_headers_local_reply", callbacks_.details());
}

// With the runtime guard disabled, an upstream filter local reply skips the upstream encoder
// filters and the router, and goes straight to the downstream filter manager as before.
TEST_F(RouterUpstreamFilterTest, LocalReplyBypassesUpstreamFilterChainWhenRuntimeGuardDisabled) {
  TestScopedRuntime scoped_runtime;
  scoped_runtime.mergeValues(
      {{"envoy.reloadable_features.upstream_filter_local_reply_via_upstream_filter_chain",
        "false"}});
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::BadRequest,
      "rejected headers", "decode_headers_local_reply");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));

  EXPECT_CALL(callbacks_, sendLocalReply(Http::Code::BadRequest, Eq("rejected headers"), _, _,
                                         "decode_headers_local_reply"));
  // The downstream local reply is not modified by the upstream encoder filter.
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("400", headers.getStatusValue());
        EXPECT_TRUE(headers.get(Http::LowerCaseString("x-upstream-encoder")).empty());
      }));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_EQ(0, encoder_filter->encode_headers_called_);
  router_->onDestroy();
}

// A filter that sends a local reply in decodeHeaders(end_stream=true) must see decodeComplete()
// before onDestroy(). The local reply completes the upstream request synchronously, which destroys
// the upstream filters before decodeHeaders() returns.
TEST_F(RouterUpstreamFilterTest, LocalReplyInDecodeHeadersCallsDecodeCompleteBeforeDestroy) {
  auto filter = std::make_shared<LifecycleTrackingFilter>();
  filter->send_local_reply_in_decode_headers_ = true;

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_CALL(callbacks_, encodeData(_, true));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_TRUE(filter->destroyed_);
  EXPECT_EQ(1, filter->decode_headers_);
  EXPECT_EQ(1, filter->decode_complete_);
}

// Same as above when the local reply is retried, which also cleans up the upstream request before
// decodeHeaders() returns.
TEST_F(RouterUpstreamFilterTest, RetriedLocalReplyInDecodeHeadersCallsDecodeCompleteBeforeDestroy) {
  auto filter = std::make_shared<LifecycleTrackingFilter>();
  filter->send_local_reply_in_decode_headers_ = true;

  initWithCodecFilter();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_TRUE(filter->destroyed_);
  EXPECT_EQ(1, filter->decode_headers_);
  EXPECT_EQ(1, filter->decode_complete_);
  router_->onDestroy();
}

// A 5xx local reply from an upstream HTTP filter did not come from the upstream host, so it must
// not be reported to outlier detection or counted in host and upstream response code stats.
TEST_F(RouterUpstreamFilterTest, UpstreamFilterLocalReplyNotReportedToOutlierDetection) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::ServiceUnavailable,
      "unavailable", "upstream_filter_unavailable");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));

  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(cancellable, cancel(_));

  EXPECT_CALL(connPool().host_->outlier_detector_, putResult(_, _)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("503", headers.getStatusValue());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_EQ("upstream_filter_unavailable", callbacks_.details());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_error_.value());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_success_.value());
  EXPECT_EQ(0U, cluster_info_->stats_store_.counter("upstream_rq_503").value());
  EXPECT_EQ(0U, cluster_info_->stats_store_.counter("upstream_rq_5xx").value());
}

// An upstream encoder filter that replaces a real upstream response with a local reply after the
// request is complete. The router must not charge the reply as an upstream response: no outlier
// detection result or response time, no host or upstream response code stats, and no
// x-envoy-upstream-service-time header.
TEST_F(RouterUpstreamFilterTest, UpstreamEncoderFilterLocalReplyNotChargedAsUpstreamResponse) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  encoder_filter->send_local_reply_in_encode_headers_ = true;
  encoder_filter->local_reply_details_ = "encoder_filter_rejected_response";

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(encoder_filter, nullptr));
  Http::ResponseDecoder* response_decoder = nullptr;
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(poolReadyWith(encoder, &response_decoder));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  ASSERT_NE(nullptr, response_decoder);

  EXPECT_CALL(connPool().host_->outlier_detector_, putResult(_, _)).Times(0);
  EXPECT_CALL(connPool().host_->outlier_detector_, putResponseTime(_)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("502", headers.getStatusValue());
        EXPECT_EQ(nullptr, headers.EnvoyUpstreamServiceTime());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true));

  Http::ResponseHeaderMapPtr response_headers(
      new Http::TestResponseHeaderMapImpl{{":status", "200"}});
  response_decoder->decodeHeaders(std::move(response_headers), true);

  EXPECT_EQ("encoder_filter_rejected_response", callbacks_.details());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_error_.value());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_success_.value());
  EXPECT_EQ(0U, cluster_info_->stats_store_.counter("upstream_rq_200").value());
  EXPECT_EQ(0U, cluster_info_->stats_store_.counter("upstream_rq_502").value());
}

// An upstream encoder filter local reply sent after the response headers were forwarded cannot be
// forwarded itself, so the downstream stream is reset. The local reply's details must still be
// recorded on the downstream stream, since the router never sees the reply.
TEST_F(RouterUpstreamFilterTest, UpstreamEncoderFilterLocalReplyAfterHeadersResetRecordsDetails) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  encoder_filter->send_local_reply_in_encode_data_ = true;
  encoder_filter->local_reply_details_ = "encoder_filter_rejected_body";

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(encoder_filter, nullptr));
  Http::ResponseDecoder* response_decoder = nullptr;
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(poolReadyWith(encoder, &response_decoder));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  ASSERT_NE(nullptr, response_decoder);

  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  Http::ResponseHeaderMapPtr response_headers(
      new Http::TestResponseHeaderMapImpl{{":status", "200"}});
  response_decoder->decodeHeaders(std::move(response_headers), false);
  EXPECT_EQ(StreamInfo::ResponseCodeDetails::get().ViaUpstream,
            callbacks_.streamInfo().responseCodeDetails().value_or(""));

  EXPECT_CALL(callbacks_, encodeData(_, _)).Times(0);
  EXPECT_CALL(callbacks_, resetStream(_, _));
  Buffer::OwnedImpl body("response body");
  response_decoder->decodeData(body, true);

  EXPECT_EQ("encoder_filter_rejected_body",
            callbacks_.streamInfo().responseCodeDetails().value_or(""));
  router_->onDestroy();
}

// A local reply's Location header was not sent by the upstream host, so the router does not follow
// it as an internal redirect and forwards the local reply instead.
TEST_F(RouterUpstreamFilterTest, UpstreamFilterLocalReplyIsNotInternallyRedirected) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::Found, "",
      "local_redirect");
  decoder_filter->modify_headers_ = [](Http::ResponseHeaderMap& headers) {
    headers.setLocation("http://www.foo.com/redirected");
  };

  initWithCodecFilter();
  auto& redirect_policy = callbacks_.route_->route_entry_.internal_redirect_policy_;
  ON_CALL(redirect_policy, enabled()).WillByDefault(Return(true));
  ON_CALL(redirect_policy, shouldRedirectForResponseCode(_)).WillByDefault(Return(true));
  ON_CALL(redirect_policy, maxInternalRedirects()).WillByDefault(Return(1));
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));

  EXPECT_CALL(callbacks_, recreateStream(_)).Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, true))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("302", headers.getStatusValue());
        EXPECT_EQ("http://www.foo.com/redirected", headers.getLocationValue());
      }));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  EXPECT_EQ("local_redirect", callbacks_.details());
}

// An upstream filter local reply that completes the response while the request headers are being
// decoded means the request is not proxied, so no shadow request is started.
TEST_F(RouterUpstreamFilterTest, NoShadowWhenUpstreamFilterLocalReplyCompletesResponse) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders);

  initWithCodecFilter();
  addShadowPolicy();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_CALL(callbacks_, encodeData(_, true));
  EXPECT_CALL(*shadow_writer_, shadow_(_, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
}

// A retried upstream filter local reply does not complete the response, so the shadow request is
// still started.
TEST_F(RouterUpstreamFilterTest, ShadowStartedWhenUpstreamFilterLocalReplyIsRetried) {
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders);

  initWithCodecFilter();
  addShadowPolicy();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(Return(&cancellable));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);
  EXPECT_CALL(*shadow_writer_, shadow_("shadow_cluster", _, _));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  router_->onDestroy();
}

// With upstream half-close enabled for a TCP tunnel, an upstream response that ends before the
// request normally keeps the upstream request open. An upstream filter local reply has already
// reset the upstream, so the router must clean up the upstream request instead.
TEST_F(RouterUpstreamFilterTest, UpstreamFilterLocalReplyCleansUpWithUpstreamHalfClose) {
  TestScopedRuntime scoped_runtime;
  scoped_runtime.mergeValues(
      {{"envoy.reloadable_features.tcp_tunnel_allow_upstream_half_close", "true"}});
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders);

  initWithCodecFilter();
  callbacks_.route_->route_entry_.connect_config_ = std::make_optional<RouteEntry::ConnectConfig>();
  callbacks_.stream_info_.protocol_ = Http::Protocol::Http2;
  EXPECT_CALL(*cluster_info_, createFilterChain(_)).WillOnce(addFilters(nullptr, decoder_filter));
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable;
  EXPECT_CALL(
      context_.server_factory_context_.cluster_manager_.thread_local_cluster_.tcp_conn_pool_,
      newConnection(_))
      .WillOnce(Return(&cancellable));
  EXPECT_CALL(callbacks_.downstream_callbacks_, enableHalfClose());
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false));
  EXPECT_CALL(callbacks_, encodeData(_, true));
  EXPECT_CALL(callbacks_, sendLocalReply(_, _, _, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  headers.setMethod("CONNECT");
  headers.removePath();
  router_->decodeHeaders(headers, false);
  EXPECT_TRUE(router_->upstreamRequests().empty());

  // The rest of the request body is dropped.
  Buffer::OwnedImpl body("request body");
  EXPECT_EQ(Http::FilterDataStatus::StopIterationNoBuffer, router_->decodeData(body, true));
}

// The upstream codec filter's own local replies, such as the 503 for a request that is missing a
// required header, go through the router like other upstream filter local replies. A retry policy
// that covers them retries them.
//
// The 503 is retried synchronously while decoding the last request frame, before the router sees
// the end of the request. The global route timeout must still be armed for the retry.
TEST_F(RouterUpstreamFilterTest, CodecFilterLocalReplyIsRetried) {
  initWithCodecFilter();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  Event::MockTimer* response_timeout = new NiceMock<Event::MockTimer>(&callbacks_.dispatcher_);
  EXPECT_CALL(*response_timeout, enableTimer(std::chrono::milliseconds(10), _));
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(encoder, encodeHeaders(_, true))
      .WillOnce(Invoke([](const Http::RequestHeaderMap& headers, bool) -> Http::Status {
        return Http::HeaderUtility::checkRequiredRequestHeaders(headers);
      }));
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(poolReadyWith(encoder));
  // The pool connection result is still reported, but not the codec filter's 503.
  EXPECT_CALL(connPool().host_->outlier_detector_, putResult(_, _)).Times(testing::AnyNumber());
  EXPECT_CALL(connPool().host_->outlier_detector_,
              putResult(Upstream::Outlier::Result::ExtOriginRequestFailed, _))
      .Times(0);
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);
  EXPECT_CALL(callbacks_, sendLocalReply(_, _, _, _, _)).Times(0);

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  headers.removeMethod();
  router_->decodeHeaders(headers, true);

  // The 503 was retried, so the upstream request is gone and no response has started.
  EXPECT_TRUE(router_->upstreamRequests().empty());
  EXPECT_FALSE(router_->downstreamResponseStarted());

  // The route timeout fires before the retry starts and ends the request with a 504.
  testing::Mock::VerifyAndClearExpectations(&callbacks_);
  EXPECT_CALL(callbacks_, sendLocalReply(Http::Code::GatewayTimeout, _, _, _,
                                         StreamInfo::ResponseCodeDetails::get().ResponseTimeout))
      .WillOnce(Return());
  response_timeout->invokeCallback();
  router_->onDestroy();
}

TEST_F(RouterUpstreamFilterTest, LocalReplyInDecodeDataFlowsThroughUpstreamEncoderChain) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeData, Http::Code::PayloadTooLarge,
      "rejected body", "decode_data_local_reply");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter, decoder_filter));
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(poolReadyWith(encoder));

  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("413", headers.getStatusValue());
        const auto encoder_header = headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("rejected body [encoder-modified]", data.toString());
  }));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, false);

  Buffer::OwnedImpl body("request body");
  router_->decodeData(body, true);

  EXPECT_EQ(1, encoder_filter->encode_headers_called_);
  EXPECT_EQ(1, encoder_filter->encode_data_called_);
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ("decode_data_local_reply", *callbacks_.streamInfo().responseCodeDetails());
  EXPECT_EQ("decode_data_local_reply", callbacks_.details());
}

TEST_F(RouterUpstreamFilterTest, LocalReplyInDecodeTrailersFlowsThroughUpstreamEncoderChain) {
  auto encoder_filter = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeTrailers,
      Http::Code::UnprocessableEntity, "rejected trailers", "decode_trailers_local_reply");

  initWithCodecFilter();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter, decoder_filter));
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(connPool(), newStream(_, _, _)).WillOnce(poolReadyWith(encoder));

  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& headers, bool) {
        EXPECT_EQ("422", headers.getStatusValue());
        const auto encoder_header = headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("rejected trailers [encoder-modified]", data.toString());
  }));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, false);

  Buffer::OwnedImpl body("request body");
  router_->decodeData(body, false);

  Http::TestRequestTrailerMapImpl trailers{{"x-trailer", "value"}};
  router_->decodeTrailers(trailers);

  EXPECT_EQ(1, encoder_filter->encode_headers_called_);
  EXPECT_EQ(1, encoder_filter->encode_data_called_);
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ("decode_trailers_local_reply", *callbacks_.streamInfo().responseCodeDetails());
  EXPECT_EQ("decode_trailers_local_reply", callbacks_.details());
}

TEST_F(RouterUpstreamFilterTest, UpstreamFilterLocalReplyRetriedAndSucceeds) {
  auto encoder_filter_1 = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter_1 = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::ServiceUnavailable,
      "attempt 1 local reply", "attempt_1_details");
  auto encoder_filter_2 = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter_2 = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::None);

  initWithCodecFilter();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter_1, decoder_filter_1))
      .WillOnce(addFilters(encoder_filter_2, decoder_filter_2));

  // First attempt initiates newStream before decodeHeaders aborts with a local reply (cancelling
  // the pending stream); second attempt reaches the connection pool and succeeds with 200 OK.
  NiceMock<Envoy::ConnectionPool::MockCancellable> cancellable_1;
  EXPECT_CALL(cancellable_1, cancel(_));
  Http::ResponseDecoder* response_decoder = nullptr;
  NiceMock<Http::MockRequestEncoder> encoder;
  EXPECT_CALL(connPool(), newStream(_, _, _))
      .WillOnce(Return(&cancellable_1))
      .WillOnce(poolReadyWith(encoder, &response_decoder));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  // Attempt 1's local reply headers ran through encoder_filter_1 and triggered retry in
  // Router::Filter::onUpstreamHeaders (which cleans up Attempt 1 before body encoding).
  EXPECT_EQ(1, encoder_filter_1->encode_headers_called_);
  EXPECT_EQ(0, encoder_filter_1->encode_data_called_);
  // The retried local reply leaves no details on the downstream stream and no host error stats.
  EXPECT_FALSE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_error_.value());

  // Trigger retry (attempt 2).
  EXPECT_CALL(*router_->retry_state_, shouldRetryHeaders(_, _, _))
      .WillOnce(Return(RetryStatus::No));
  router_->retry_state_->callback_();
  ASSERT_NE(nullptr, response_decoder);

  EXPECT_CALL(callbacks_, encodeHeaders_(_, true))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& resp_headers, bool) {
        EXPECT_EQ("200", resp_headers.getStatusValue());
        const auto encoder_header = resp_headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));

  Http::ResponseHeaderMapPtr response_headers(
      new Http::TestResponseHeaderMapImpl{{":status", "200"}});
  response_decoder->decodeHeaders(std::move(response_headers), true);

  EXPECT_EQ(1, encoder_filter_2->encode_headers_called_);
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ(StreamInfo::ResponseCodeDetails::get().ViaUpstream,
            *callbacks_.streamInfo().responseCodeDetails());
}

TEST_F(RouterUpstreamFilterTest, UpstreamFilterLocalReplyRetryExhaustedPreservesDetailsAndHeaders) {
  auto encoder_filter_1 = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter_1 = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::ServiceUnavailable,
      "attempt 1 local reply", "attempt_1_details");
  auto encoder_filter_2 = std::make_shared<ResponseModifyingEncoderFilter>();
  auto decoder_filter_2 = std::make_shared<UpstreamLocalReplyDecoderFilter>(
      UpstreamLocalReplyDecoderFilter::TriggerPhase::DecodeHeaders, Http::Code::ServiceUnavailable,
      "attempt 2 local reply", "exhausted_local_reply_details");

  initWithCodecFilter();
  router_->preallocateRetryState();
  router_->retry_state_->expectHeadersRetry();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter_1, decoder_filter_1))
      .WillOnce(addFilters(encoder_filter_2, decoder_filter_2));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);

  EXPECT_CALL(*router_->retry_state_, shouldRetryHeaders(_, _, _))
      .WillOnce(Return(RetryStatus::NoRetryLimitExceeded));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, false))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& resp_headers, bool) {
        EXPECT_EQ("503", resp_headers.getStatusValue());
        const auto encoder_header = resp_headers.get(Http::LowerCaseString("x-upstream-encoder"));
        ASSERT_FALSE(encoder_header.empty());
        EXPECT_EQ("modified", encoder_header[0]->value().getStringView());
      }));
  EXPECT_CALL(callbacks_, encodeData(_, true)).WillOnce(Invoke([](Buffer::Instance& data, bool) {
    EXPECT_EQ("attempt 2 local reply [encoder-modified]", data.toString());
  }));

  router_->retry_state_->callback_();

  EXPECT_TRUE(callbacks_.streamInfo().hasResponseFlag(
      StreamInfo::CoreResponseFlag::UpstreamRetryLimitExceeded));
  ASSERT_TRUE(callbacks_.streamInfo().responseCodeDetails().has_value());
  EXPECT_EQ("exhausted_local_reply_details", *callbacks_.streamInfo().responseCodeDetails());
  EXPECT_EQ("exhausted_local_reply_details", callbacks_.details());
}

TEST_F(RouterUpstreamFilterTest, UpstreamEncoderFilterSendLocalReplyInEncodeHeadersRetried) {
  auto encoder_filter_1 = std::make_shared<ResponseModifyingEncoderFilter>();
  encoder_filter_1->send_local_reply_in_encode_headers_ = true;
  encoder_filter_1->local_reply_code_ = Http::Code::BadGateway;
  encoder_filter_1->local_reply_body_ = "bad upstream response";
  encoder_filter_1->local_reply_details_ = "encoder_filter_rejected_response";

  auto encoder_filter_2 = std::make_shared<ResponseModifyingEncoderFilter>();

  initWithCodecFilter();
  router_->preallocateRetryState();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(encoder_filter_1, nullptr))
      .WillOnce(addFilters(encoder_filter_2, nullptr));

  Http::ResponseDecoder* response_decoder_1 = nullptr;
  Http::ResponseDecoder* response_decoder_2 = nullptr;
  NiceMock<Http::MockRequestEncoder> encoder_1;
  NiceMock<Http::MockRequestEncoder> encoder_2;
  EXPECT_CALL(connPool(), newStream(_, _, _))
      .WillOnce(poolReadyWith(encoder_1, &response_decoder_1))
      .WillOnce(poolReadyWith(encoder_2, &response_decoder_2));

  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  ASSERT_NE(nullptr, response_decoder_1);

  // When upstream responds with 200 OK on attempt 1, encoder_filter_1 calls sendLocalReply(502),
  // which sends a direct local reply to Router::Filter and triggers a retry.
  router_->retry_state_->expectHeadersRetry();
  Http::ResponseHeaderMapPtr resp_headers_1(
      new Http::TestResponseHeaderMapImpl{{":status", "200"}});
  response_decoder_1->decodeHeaders(std::move(resp_headers_1), true);

  // Trigger retry (attempt 2).
  router_->retry_state_->callback_();
  ASSERT_NE(nullptr, response_decoder_2);

  EXPECT_CALL(*router_->retry_state_, shouldRetryHeaders(_, _, _))
      .WillOnce(Return(RetryStatus::No));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, true))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& resp_headers, bool) {
        EXPECT_EQ("200", resp_headers.getStatusValue());
      }));

  Http::ResponseHeaderMapPtr resp_headers_2(
      new Http::TestResponseHeaderMapImpl{{":status", "200"}});
  response_decoder_2->decodeHeaders(std::move(resp_headers_2), true);
}

// A local reply on a hedged attempt that cannot be retried is dropped while another attempt is
// still awaiting headers, like a bad upstream response would be. It is not counted as a host error,
// and the response of the other attempt is forwarded.
TEST_F(RouterUpstreamFilterTest, NonRetriableLocalReplyWhileHedgedRequestInFlight) {
  auto encoder_filter_2 = std::make_shared<ResponseModifyingEncoderFilter>();
  encoder_filter_2->send_local_reply_in_encode_headers_ = true;
  encoder_filter_2->local_reply_details_ = "hedged_attempt_local_reply";

  initWithCodecFilter();
  auto& hedge_policy = callbacks_.route_->route_entry_.hedge_policy_;
  hedge_policy.hedge_on_per_try_timeout_ = true;
  callbacks_.route_->route_entry_.retry_policy_->per_try_timeout_ = std::chrono::milliseconds(5);
  hedge_policy.additional_request_chance_ = envoy::type::v3::FractionalPercent{};
  hedge_policy.additional_request_chance_.set_numerator(0);
  hedge_policy.additional_request_chance_.set_denominator(
      envoy::type::v3::FractionalPercent::HUNDRED);
  router_->preallocateRetryState();
  EXPECT_CALL(*cluster_info_, createFilterChain(_))
      .WillOnce(addFilters(nullptr, nullptr))
      .WillOnce(addFilters(encoder_filter_2, nullptr));

  Http::ResponseDecoder* response_decoder_1 = nullptr;
  Http::ResponseDecoder* response_decoder_2 = nullptr;
  NiceMock<Http::MockRequestEncoder> encoder_1;
  NiceMock<Http::MockRequestEncoder> encoder_2;
  EXPECT_CALL(connPool(), newStream(_, _, _))
      .WillOnce(poolReadyWith(encoder_1, &response_decoder_1))
      .WillOnce(poolReadyWith(encoder_2, &response_decoder_2));

  // The response timer is created first, then the per-try timer of the first attempt.
  Event::MockTimer* per_try_timeout_1 = new NiceMock<Event::MockTimer>(&callbacks_.dispatcher_);
  new NiceMock<Event::MockTimer>(&callbacks_.dispatcher_);
  Http::TestRequestHeaderMapImpl headers;
  HttpTestUtility::addDefaultHeaders(headers);
  router_->decodeHeaders(headers, true);
  ASSERT_NE(nullptr, response_decoder_1);

  // The per-try timeout of the first attempt starts a hedged second attempt, and the first attempt
  // keeps waiting for its response.
  router_->retry_state_->expectHedgedPerTryTimeoutRetry();
  per_try_timeout_1->invokeCallback();
  new NiceMock<Event::MockTimer>(&callbacks_.dispatcher_);
  router_->retry_state_->callback_();
  ASSERT_NE(nullptr, response_decoder_2);
  EXPECT_EQ(2U, router_->upstreamRequests().size());

  // The second attempt's response is replaced by an upstream filter local reply that cannot be
  // retried. The first attempt is still awaiting headers, so the local reply is dropped.
  EXPECT_CALL(*router_->retry_state_, shouldRetryHeaders(_, _, _))
      .WillOnce(Return(RetryStatus::NoOverflow));
  EXPECT_CALL(callbacks_, encodeHeaders_(_, _)).Times(0);
  EXPECT_CALL(encoder_1.stream_, resetStream(_)).Times(0);
  response_decoder_2->decodeHeaders(
      Http::ResponseHeaderMapPtr{new Http::TestResponseHeaderMapImpl{{":status", "200"}}}, true);
  EXPECT_EQ(1, encoder_filter_2->encode_headers_called_);
  EXPECT_EQ(1U, router_->upstreamRequests().size());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_error_.value());
  testing::Mock::VerifyAndClearExpectations(&callbacks_);

  // The first attempt's response is forwarded.
  EXPECT_CALL(callbacks_, encodeHeaders_(_, true))
      .WillOnce(Invoke([](Http::ResponseHeaderMap& response_headers, bool) {
        EXPECT_EQ("200", response_headers.getStatusValue());
      }));
  response_decoder_1->decodeHeaders(
      Http::ResponseHeaderMapPtr{new Http::TestResponseHeaderMapImpl{{":status", "200"}}}, true);
  EXPECT_EQ(StreamInfo::ResponseCodeDetails::get().ViaUpstream, callbacks_.details());
  EXPECT_EQ(0U, connPool().host_->stats_.rq_error_.value());
}

} // namespace
} // namespace Router
} // namespace Envoy
