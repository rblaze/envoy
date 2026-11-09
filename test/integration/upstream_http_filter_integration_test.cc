#include <atomic>
#include <ostream>
#include <string>
#include <vector>

#include "envoy/config/bootstrap/v3/bootstrap.pb.h"
#include "envoy/extensions/access_loggers/file/v3/file.pb.h"
#include "envoy/extensions/filters/http/router/v3/router.pb.h"
#include "envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.pb.h"
#include "envoy/grpc/status.h"

#include "source/extensions/filters/http/common/pass_through_filter.h"

#include "test/config/utility.h"
#include "test/integration/filters/add_header_filter.pb.h"
#include "test/integration/filters/repick_cluster_filter.h"
#include "test/integration/filters/test_filters.pb.h"
#include "test/integration/http_integration.h"
#include "test/mocks/http/mocks.h"

#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"

namespace Envoy {
namespace {

constexpr absl::string_view EcdsClusterName = "ecds_cluster";
constexpr absl::string_view Ecds2ClusterName = "ecds2_cluster";
constexpr absl::string_view expected_types[] = {
    "type.googleapis.com/envoy.admin.v3.BootstrapConfigDump",
    "type.googleapis.com/envoy.admin.v3.ClustersConfigDump",
    "type.googleapis.com/envoy.admin.v3.EcdsConfigDump",
    "type.googleapis.com/envoy.admin.v3.ListenersConfigDump",
    "type.googleapis.com/envoy.admin.v3.ScopedRoutesConfigDump",
    "type.googleapis.com/envoy.admin.v3.RoutesConfigDump",
    "type.googleapis.com/envoy.admin.v3.SecretsConfigDump"};

using HttpFilterProto =
    envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter;
using testing::ElementsAre;
using testing::Eq;
using testing::Ge;
using testing::HasSubstr;
using testing::Not;

class FilterCallTracker {
public:
  static void reset() {
    absl::MutexLock lock(mutex_);
    trace_.clear();
  }

  static void record(absl::string_view event) {
    absl::MutexLock lock(mutex_);
    trace_.emplace_back(event);
  }

  static std::vector<std::string> trace() {
    absl::MutexLock lock(mutex_);
    return trace_;
  }

private:
  static inline absl::Mutex mutex_;
  static inline std::vector<std::string> trace_ ABSL_GUARDED_BY(mutex_);
};

class DownstreamResponseModifyingEncoderFilter : public Http::PassThroughEncoderFilter {
public:
  static constexpr absl::string_view kBodySuffix = " [downstream-encoded]";

  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers,
                                          bool end_stream) override {
    FilterCallTracker::record("downstream:encodeHeaders");
    headers.addCopy(Http::LowerCaseString("x-downstream-encoder-modified"), "yes");
    if (!end_stream && headers.ContentLength() != nullptr) {
      uint64_t len = 0;
      if (absl::SimpleAtoi(headers.getContentLengthValue(), &len)) {
        headers.setContentLength(len + kBodySuffix.size());
      }
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override {
    FilterCallTracker::record("downstream:encodeData");
    if (end_stream) {
      data.add(kBodySuffix);
    }
    return Http::FilterDataStatus::Continue;
  }
};

class DownstreamResponseModifyingEncoderFilterFactory
    : public Server::Configuration::NamedHttpFilterConfigFactory {
public:
  std::string name() const override { return "downstream-response-modifying-encoder-filter"; }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<test::integration::filters::DownstreamResponseModifyingFilterConfig>();
  }

  absl::StatusOr<Http::FilterFactoryCb>
  createFilterFactoryFromProto(const Protobuf::Message&, const std::string&,
                               Server::Configuration::FactoryContext&) override {
    return [](Http::FilterChainFactoryCallbacks& callbacks) -> void {
      callbacks.addStreamEncoderFilter(
          std::make_shared<DownstreamResponseModifyingEncoderFilter>());
    };
  }
};

Registry::RegisterFactory<DownstreamResponseModifyingEncoderFilterFactory,
                          Server::Configuration::NamedHttpFilterConfigFactory>
    register_downstream_response_modifying_encoder_filter_;

class UpstreamResponseModifyingEncoderFilter
    : public Http::PassThroughEncoderFilter,
      public std::enable_shared_from_this<UpstreamResponseModifyingEncoderFilter> {
public:
  static constexpr absl::string_view kBodySuffix = " [upstream-encoded]";

  // Per-test knobs. `max_action_attempts_` limits the status rewrite and chained local reply
  // actions to the first N attempts (-1 means every attempt). `async_pause_attempts_` pauses the
  // response on the first N attempts and resumes it from a posted dispatcher callback.
  static inline std::atomic<int> total_attempts_{0};
  static inline std::atomic<int> max_action_attempts_{-1};
  static inline std::atomic<int> rewrite_status_code_{0};
  static inline std::atomic<int> chained_local_reply_code_{0};
  static inline std::atomic<int> async_pause_attempts_{0};

  static void resetState() {
    total_attempts_ = 0;
    max_action_attempts_ = -1;
    rewrite_status_code_ = 0;
    chained_local_reply_code_ = 0;
    async_pause_attempts_ = 0;
  }

  UpstreamResponseModifyingEncoderFilter() : attempt_(++total_attempts_) {}

  bool shouldPerformAction() const {
    const int max_attempts = max_action_attempts_.load();
    return max_attempts < 0 || attempt_ <= max_attempts;
  }

  void onDestroy() override { destroyed_ = true; }

  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers,
                                          bool end_stream) override {
    if (chained_local_reply_code_.load() > 0 && shouldPerformAction()) {
      encoder_callbacks_->sendLocalReply(static_cast<Http::Code>(chained_local_reply_code_.load()),
                                         "encoder override reply", nullptr, std::nullopt,
                                         "upstream_chained_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }

    if (rewrite_status_code_.load() > 0 && shouldPerformAction()) {
      headers.setStatus(rewrite_status_code_.load());
    }

    if (async_pause_attempts_.load() > 0 && attempt_ <= async_pause_attempts_.load()) {
      // Pause the response and resume it from a posted dispatcher callback. For a header-only
      // response, resume here; otherwise wait for the full body to be buffered in encodeData().
      latched_headers_ = &headers;
      if (end_stream) {
        encoder_callbacks_->dispatcher().post([weak_self = weak_from_this()]() {
          auto self = weak_self.lock();
          if (self == nullptr || self->destroyed_) {
            return;
          }
          self->latched_headers_->addCopy(Http::LowerCaseString("x-async-encoder-resumed"), "yes");
          self->latched_headers_->addCopy(Http::LowerCaseString("x-upstream-encoder-modified"),
                                          "yes");
          self->encoder_callbacks_->continueEncoding();
        });
      }
      return Http::FilterHeadersStatus::StopIteration;
    }

    headers.addCopy(Http::LowerCaseString("x-upstream-encoder-modified"), "yes");
    if (!end_stream && headers.ContentLength() != nullptr) {
      uint64_t len = 0;
      if (absl::SimpleAtoi(headers.getContentLengthValue(), &len)) {
        headers.setContentLength(len + kBodySuffix.size());
      }
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus encodeData(Buffer::Instance& data, bool end_stream) override {
    if (async_pause_attempts_.load() > 0 && attempt_ <= async_pause_attempts_.load()) {
      if (end_stream && !async_resume_posted_) {
        async_resume_posted_ = true;
        encoder_callbacks_->dispatcher().post([weak_self = weak_from_this()]() {
          auto self = weak_self.lock();
          if (self == nullptr || self->destroyed_) {
            return;
          }
          self->latched_headers_->addCopy(Http::LowerCaseString("x-async-encoder-resumed"), "yes");
          self->latched_headers_->addCopy(Http::LowerCaseString("x-upstream-encoder-modified"),
                                          "yes");
          self->encoder_callbacks_->modifyEncodingBuffer(
              [](Buffer::Instance& buffer) { buffer.add(kBodySuffix); });
          if (self->latched_headers_->ContentLength() != nullptr) {
            self->latched_headers_->setContentLength(
                self->encoder_callbacks_->encodingBuffer()->length());
          }
          self->encoder_callbacks_->continueEncoding();
        });
      }
      return Http::FilterDataStatus::StopIterationAndBuffer;
    }

    if (end_stream) {
      data.add(kBodySuffix);
    }
    return Http::FilterDataStatus::Continue;
  }

  const int attempt_;
  bool destroyed_{false};
  bool async_resume_posted_{false};
  Http::ResponseHeaderMap* latched_headers_{};
};

class UpstreamResponseModifyingEncoderFilterFactory
    : public Server::Configuration::UpstreamHttpFilterConfigFactory {
public:
  std::string name() const override { return "upstream-response-modifying-encoder-filter"; }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<test::integration::filters::UpstreamResponseModifyingFilterConfig>();
  }

  absl::StatusOr<Http::FilterFactoryCb>
  createFilterFactoryFromProto(const Protobuf::Message&, const std::string&,
                               Server::Configuration::UpstreamFactoryContext&) override {
    return [](Http::FilterChainFactoryCallbacks& callbacks) -> void {
      callbacks.addStreamEncoderFilter(std::make_shared<UpstreamResponseModifyingEncoderFilter>());
    };
  }
};

Registry::RegisterFactory<UpstreamResponseModifyingEncoderFilterFactory,
                          Server::Configuration::UpstreamHttpFilterConfigFactory>
    register_upstream_response_modifying_encoder_filter_;

class UpstreamConfigurableLocalReplyFilter
    : public Http::PassThroughFilter,
      public Http::UpstreamCallbacks,
      public std::enable_shared_from_this<UpstreamConfigurableLocalReplyFilter> {
public:
  static inline std::atomic<int> total_attempts_{0};
  static inline std::atomic<int> fail_attempts_{-1};
  static inline std::atomic<int> on_host_selected_code_{0};

  static void resetState(int fail_attempts = -1, int on_host_selected_code = 0) {
    total_attempts_ = 0;
    fail_attempts_ = fail_attempts;
    on_host_selected_code_ = on_host_selected_code;
    UpstreamResponseModifyingEncoderFilter::resetState();
    FilterCallTracker::reset();
  }

  explicit UpstreamConfigurableLocalReplyFilter(std::string filter_name = "")
      : filter_name_(std::move(filter_name)), attempt_(++total_attempts_) {}

  bool shouldFail() const {
    if (!target_filter_.empty() && target_filter_ != filter_name_) {
      return false;
    }
    const int max_fails = fail_attempts_.load();
    return max_fails < 0 || attempt_ <= max_fails;
  }

  void onDestroy() override { destroyed_ = true; }

  // Http::StreamDecoderFilter
  Http::FilterHeadersStatus decodeHeaders(Http::RequestHeaderMap& headers, bool) override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":decodeHeaders"));
      headers.appendCopy(Http::LowerCaseString("x-upstream-decode-order"), filter_name_);
    }
    const auto target_header = headers.get(Http::LowerCaseString("x-local-reply-target-filter"));
    if (!target_header.empty()) {
      target_filter_ = std::string(target_header[0]->value().getStringView());
    }
    const auto phase_header = headers.get(Http::LowerCaseString("x-local-reply-phase"));
    if (!phase_header.empty()) {
      phase_ = std::string(phase_header[0]->value().getStringView());
    }
    const auto location_header = headers.get(Http::LowerCaseString("x-local-reply-location"));
    if (!location_header.empty()) {
      location_ = std::string(location_header[0]->value().getStringView());
    }
    const auto status_header = headers.get(Http::LowerCaseString("x-local-reply-status"));
    if (!status_header.empty()) {
      uint32_t code = 0;
      if (absl::SimpleAtoi(status_header[0]->value().getStringView(), &code)) {
        status_code_ = static_cast<Http::Code>(code);
      }
    }
    if (phase_ == "decode-headers" && shouldFail()) {
      std::function<void(Http::ResponseHeaderMap&)> modify_headers;
      if (!location_.empty()) {
        modify_headers = [location = location_](Http::ResponseHeaderMap& response_headers) {
          response_headers.setLocation(location);
        };
      }
      decoder_callbacks_->sendLocalReply(status_code_, "upstream local reply", modify_headers,
                                         std::nullopt, "upstream_decode_headers_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    if (phase_ == "async-decode-headers" && shouldFail()) {
      postAsyncLocalReply("upstream_async_decode_headers_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    if (phase_ == "decode-headers-grpc" && shouldFail()) {
      decoder_callbacks_->sendLocalReply(Http::Code::ServiceUnavailable, "upstream grpc reject",
                                         nullptr, Grpc::Status::WellKnownGrpcStatus::Unavailable,
                                         "upstream_grpc_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    if (phase_ == "decode-headers-head" && shouldFail()) {
      decoder_callbacks_->sendLocalReply(status_code_, "head error body", nullptr, std::nullopt,
                                         "upstream_head_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus decodeData(Buffer::Instance&, bool) override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":decodeData"));
    }
    if (phase_ == "decode-data" && shouldFail()) {
      decoder_callbacks_->sendLocalReply(status_code_, "upstream local reply", nullptr,
                                         std::nullopt, "upstream_decode_data_local_reply");
      return Http::FilterDataStatus::StopIterationNoBuffer;
    }
    if (phase_ == "async-decode-data" && shouldFail()) {
      if (!async_posted_) {
        async_posted_ = true;
        postAsyncLocalReply("upstream_async_decode_data_local_reply");
      }
      return Http::FilterDataStatus::StopIterationAndBuffer;
    }
    return Http::FilterDataStatus::Continue;
  }
  Http::FilterTrailersStatus decodeTrailers(Http::RequestTrailerMap&) override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":decodeTrailers"));
    }
    if (phase_ == "decode-trailers" && shouldFail()) {
      decoder_callbacks_->sendLocalReply(status_code_, "upstream local reply", nullptr,
                                         std::nullopt, "upstream_decode_trailers_local_reply");
      return Http::FilterTrailersStatus::StopIteration;
    }
    return Http::FilterTrailersStatus::Continue;
  }
  void decodeComplete() override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":decodeComplete"));
    }
  }
  void setDecoderFilterCallbacks(Http::StreamDecoderFilterCallbacks& callbacks) override {
    decoder_callbacks_ = &callbacks;
    if (auto cb = decoder_callbacks_->upstreamCallbacks(); cb) {
      cb->addUpstreamCallbacks(*this);
    }
  }

  // Http::StreamEncoderFilter
  Http::FilterHeadersStatus encodeHeaders(Http::ResponseHeaderMap& headers, bool) override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":encodeHeaders"));
      headers.appendCopy(Http::LowerCaseString("x-upstream-encode-order"), filter_name_);
    }
    if (phase_ == "encode-headers" && shouldFail()) {
      encoder_callbacks_->sendLocalReply(status_code_, "upstream local reply", nullptr,
                                         std::nullopt, "upstream_encode_headers_local_reply");
      return Http::FilterHeadersStatus::StopIteration;
    }
    return Http::FilterHeadersStatus::Continue;
  }
  Http::FilterDataStatus encodeData(Buffer::Instance&, bool) override {
    if (!filter_name_.empty()) {
      FilterCallTracker::record(absl::StrCat(filter_name_, ":encodeData"));
    }
    if (phase_ == "encode-data" && shouldFail()) {
      encoder_callbacks_->sendLocalReply(status_code_, "upstream local reply", nullptr,
                                         std::nullopt, "upstream_encode_data_local_reply");
      return Http::FilterDataStatus::StopIterationNoBuffer;
    }
    return Http::FilterDataStatus::Continue;
  }

  // Http::UpstreamCallbacks
  void onHostSelected(const Upstream::HostDescriptionConstSharedPtr&) override {
    if (on_host_selected_code_.load() > 0 && shouldFail()) {
      decoder_callbacks_->sendLocalReply(static_cast<Http::Code>(on_host_selected_code_.load()),
                                         "upstream local reply", nullptr, std::nullopt,
                                         "upstream_on_host_selected_local_reply");
    }
  }
  void onUpstreamConnectionEstablished() override {}

  // Sends a local reply from a posted dispatcher callback, i.e. outside of any decoder filter
  // callback, which exercises FilterManager::sendLocalReplyViaFilterChain().
  void postAsyncLocalReply(absl::string_view details) {
    decoder_callbacks_->dispatcher().post(
        [weak_self = weak_from_this(), details = std::string(details)]() {
          auto self = weak_self.lock();
          if (self == nullptr || self->destroyed_) {
            return;
          }
          self->decoder_callbacks_->sendLocalReply(self->status_code_, "upstream local reply",
                                                   nullptr, std::nullopt, details);
        });
  }

  const std::string filter_name_;
  const int attempt_;
  bool destroyed_{false};
  bool async_posted_{false};
  std::string target_filter_;
  std::string phase_;
  // Location header added to a decode-headers local reply, if set.
  std::string location_;
  Http::Code status_code_{Http::Code::ServiceUnavailable};
};

class UpstreamConfigurableLocalReplyFilterFactory
    : public Server::Configuration::UpstreamHttpFilterConfigFactory {
public:
  std::string name() const override { return "upstream-configurable-local-reply-filter"; }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<
        test::integration::filters::UpstreamConfigurableLocalReplyFilterConfig>();
  }

  absl::StatusOr<Http::FilterFactoryCb>
  createFilterFactoryFromProto(const Protobuf::Message&, const std::string&,
                               Server::Configuration::UpstreamFactoryContext&) override {
    return [this](Http::FilterChainFactoryCallbacks& callbacks) -> void {
      // Filters configured under a custom name (e.g. "chain-filter-b") record their callbacks in
      // FilterCallTracker so tests can assert on cross-filter ordering.
      const absl::string_view config_name = callbacks.filterConfigName();
      callbacks.addStreamFilter(std::make_shared<UpstreamConfigurableLocalReplyFilter>(
          config_name == name() ? std::string() : std::string(config_name)));
    };
  }
};

Registry::RegisterFactory<UpstreamConfigurableLocalReplyFilterFactory,
                          Server::Configuration::UpstreamHttpFilterConfigFactory>
    register_upstream_configurable_local_reply_filter_;

class UpstreamHttpFilterIntegrationTestBase : public HttpIntegrationTest {
public:
  UpstreamHttpFilterIntegrationTestBase(Network::Address::IpVersion version,
                                        bool use_router_filters)
      : HttpIntegrationTest(Http::CodecType::HTTP2, version),
        use_router_filters_(use_router_filters) {
    setUpstreamProtocol(Http::CodecType::HTTP2);
    skip_tag_extraction_rule_check_ = true;
    autonomous_upstream_ = true;
  }

  void addStaticClusterFilter(const HttpFilterProto& config) {
    config_helper_.addConfigModifier([config](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      auto* cluster = bootstrap.mutable_static_resources()->mutable_clusters(0);
      ConfigHelper::HttpProtocolOptions protocol_options =
          MessageUtil::anyConvert<ConfigHelper::HttpProtocolOptions>(
              (*cluster->mutable_typed_extension_protocol_options())
                  ["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]);
      *protocol_options.add_http_filters() = config;
      std::ignore = (*cluster->mutable_typed_extension_protocol_options())
                        ["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]
                            .PackFrom(protocol_options);
    });
  }

  void addStaticRouterFilter(const HttpFilterProto& config) {
    config_helper_.addConfigModifier(
        [config](
            envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
                hcm) -> void {
          HttpFilterProto& router_filter = *hcm.mutable_http_filters()->rbegin();
          ASSERT_EQ(router_filter.name(), "envoy.filters.http.router");
          envoy::extensions::filters::http::router::v3::Router router;
          std::ignore = router_filter.typed_config().UnpackTo(&router);
          *router.add_upstream_http_filters() = config;
          std::ignore = router_filter.mutable_typed_config()->PackFrom(router);
        });
  }

  HttpFilterProto getAddHeaderFilterConfig(const std::string& name, const std::string& key,
                                           const std::string& value, bool disabled = false) {
    HttpFilterProto filter_config;
    filter_config.set_name(name);
    auto configuration = test::integration::filters::AddHeaderFilterConfig();
    configuration.set_header_key(key);
    configuration.set_header_value(value);
    filter_config.set_disabled(disabled);
    std::ignore = filter_config.mutable_typed_config()->PackFrom(configuration);
    return filter_config;
  }

  HttpFilterProto getCodecFilterConfig() {
    HttpFilterProto filter_config;
    filter_config.set_name("envoy.filters.http.upstream_codec");
    auto configuration = envoy::extensions::filters::http::upstream_codec::v3::UpstreamCodec();
    std::ignore = filter_config.mutable_typed_config()->PackFrom(configuration);
    return filter_config;
  }

  void addStaticFilter(const std::string& name, const std::string& key, const std::string& value) {
    if (useRouterFilters()) {
      addStaticRouterFilter(getAddHeaderFilterConfig(name, key, value));
    } else {
      addStaticClusterFilter(getAddHeaderFilterConfig(name, key, value));
    }
  }

  void addCodecRouterFilter() { addStaticRouterFilter(getCodecFilterConfig()); }

  void addCodecClusterFilter() { addStaticClusterFilter(getCodecFilterConfig()); }

  void addCodecFilter() {
    if (useRouterFilters()) {
      addCodecRouterFilter();
    } else {
      addCodecClusterFilter();
    }
  }

  bool useRouterFilters() const { return use_router_filters_; }

  void expectHeaderKeyAndValue(std::unique_ptr<Http::RequestHeaderMap>& headers,
                               const std::string value) {
    expectHeaderKeyAndValue(headers, default_header_key_, value);
  }

  void expectHeaderKeyAndValue(std::unique_ptr<Http::RequestHeaderMap>& headers,
                               const std::string key, const std::string value) {
    auto header = headers->get(Http::LowerCaseString(key));
    ASSERT_FALSE(header.empty());
    EXPECT_EQ(value, header[0]->value().getStringView());
  }

  std::unique_ptr<Http::RequestHeaderMap> sendRequestAndGetHeaders() {
    codec_client_ = makeHttpConnection(lookupPort("http"));
    IntegrationStreamDecoderPtr response =
        codec_client_->makeHeaderOnlyRequest(default_request_headers_);
    EXPECT_TRUE(response->waitForEndStream());
    EXPECT_TRUE(response->complete());
    auto upstream_headers =
        reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
    EXPECT_TRUE(upstream_headers != nullptr);
    cleanupUpstreamAndDownstream();
    return upstream_headers;
  }

  void addStaticNoConfigFilter(const std::string& name) {
    HttpFilterProto filter_config;
    filter_config.set_name(name);
    std::ignore = filter_config.mutable_typed_config()->PackFrom(Envoy::Protobuf::Struct());
    if (useRouterFilters()) {
      addStaticRouterFilter(filter_config);
    } else {
      addStaticClusterFilter(filter_config);
    }
  }

  template <typename ProtoConfig>
  void addStaticTypedConfigFilter(const std::string& name, const ProtoConfig& config) {
    HttpFilterProto filter_config;
    filter_config.set_name(name);
    std::ignore = filter_config.mutable_typed_config()->PackFrom(config);
    if (useRouterFilters()) {
      addStaticRouterFilter(filter_config);
    } else {
      addStaticClusterFilter(filter_config);
    }
  }

  bool use_router_filters_{false};
  const std::string default_header_key_ = "header-key";
  const std::string default_header_value_ = "default-value";
};

// Configures a retry policy on the default route. `retriable_status_codes` is only consulted
// with `retry_on: retriable-status-codes`.
void addRouteRetryPolicy(ConfigHelper& config_helper, const std::string& retry_on,
                         uint32_t num_retries,
                         const std::vector<uint32_t>& retriable_status_codes = {}) {
  config_helper.addConfigModifier(
      [retry_on, num_retries, retriable_status_codes](
          envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
              hcm) {
        auto* retry_policy = hcm.mutable_route_config()
                                 ->mutable_virtual_hosts(0)
                                 ->mutable_routes(0)
                                 ->mutable_route()
                                 ->mutable_retry_policy();
        retry_policy->set_retry_on(retry_on);
        retry_policy->mutable_num_retries()->set_value(num_retries);
        for (const uint32_t code : retriable_status_codes) {
          retry_policy->add_retriable_status_codes(code);
        }
      });
}

// Returns request headers that instruct UpstreamConfigurableLocalReplyFilter to send a local reply
// with `status` in `phase`.
Http::TestRequestHeaderMapImpl localReplyHeaders(absl::string_view method, absl::string_view phase,
                                                 absl::string_view status) {
  return Http::TestRequestHeaderMapImpl{{":method", std::string(method)},
                                        {":path", "/"},
                                        {":scheme", "https"},
                                        {":authority", "host"},
                                        {"x-local-reply-phase", std::string(phase)},
                                        {"x-local-reply-status", std::string(status)}};
}

class StaticRouterOrClusterFiltersIntegrationTest
    : public testing::TestWithParam<std::tuple<Network::Address::IpVersion, bool>>,
      public UpstreamHttpFilterIntegrationTestBase {
public:
  StaticRouterOrClusterFiltersIntegrationTest()
      : UpstreamHttpFilterIntegrationTestBase(std::get<0>(GetParam()), std::get<1>(GetParam())) {}

  // Returns the value of the counter `name`, or 0 if it was never created. Response code stats
  // are only created once they are charged.
  uint64_t counterValueOrZero(const std::string& name) {
    Stats::CounterSharedPtr counter = test_server_->counter(name);
    return counter == nullptr ? 0 : counter->value();
  }

  // Adds UpstreamResponseModifyingEncoderFilter to the upstream filter chain.
  void addUpstreamResponseModifyingFilter() {
    addStaticTypedConfigFilter("upstream-response-modifying-encoder-filter",
                               test::integration::filters::UpstreamResponseModifyingFilterConfig());
  }

  // Adds UpstreamConfigurableLocalReplyFilter to the upstream filter chain under `name`.
  void addUpstreamConfigurableLocalReplyFilter(
      const std::string& name = "upstream-configurable-local-reply-filter") {
    addStaticTypedConfigFilter(
        name, test::integration::filters::UpstreamConfigurableLocalReplyFilterConfig());
  }

  // Sends a POST with body and trailers. On attempt 1 the upstream filter sends a 503 local reply
  // in `phase` ("decode-data" or "decode-trailers") after the request headers already reached the
  // backend. Verifies that the in-flight backend stream is reset and that the retry replays the
  // headers, body and trailers to a new backend stream.
  void testLocalReplyResetsUpstreamAndRetryReplaysRequest(const std::string& phase) {
    autonomous_upstream_ = false;
    addRouteRetryPolicy(config_helper_, "5xx", 2);
    UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
    addUpstreamConfigurableLocalReplyFilter();
    addCodecFilter();
    initialize();

    codec_client_ = makeHttpConnection(lookupPort("http"));
    auto encoder_decoder = codec_client_->startRequest(localReplyHeaders("POST", phase, "503"));
    IntegrationStreamDecoderPtr response = std::move(encoder_decoder.second);

    // Attempt 1: the request headers pass the upstream filter chain and reach the backend.
    ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection_));
    FakeStreamPtr first_attempt;
    ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, first_attempt));
    ASSERT_TRUE(first_attempt->waitForHeadersComplete());

    codec_client_->sendData(encoder_decoder.first, "request body", false);
    if (phase == "decode-trailers") {
      // The body passes the upstream filter and reaches the backend before the local reply.
      ASSERT_TRUE(first_attempt->waitForData(*dispatcher_, "request body"));
    }
    Http::TestRequestTrailerMapImpl request_trailers{{"x-request-trailer", "val"}};
    codec_client_->sendTrailers(encoder_decoder.first, request_trailers);

    // The upstream filter local reply resets the in-flight backend stream.
    ASSERT_TRUE(first_attempt->waitForReset());

    // Attempt 2: the router replays headers, body and trailers on a new backend stream.
    ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
    ASSERT_TRUE(upstream_request_->waitForEndStream(*dispatcher_));
    EXPECT_EQ("POST", upstream_request_->headers().getMethodValue());
    EXPECT_EQ("request body", upstream_request_->body().toString());
    ASSERT_NE(nullptr, upstream_request_->trailers());
    EXPECT_THAT(*upstream_request_->trailers(), ContainsHeader("x-request-trailer", "val"));
    upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
    upstream_request_->encodeData(10, true);

    ASSERT_TRUE(response->waitForEndStream());
    ASSERT_TRUE(response->complete());
    EXPECT_EQ("200", response->headers().getStatusValue());
    EXPECT_EQ(10U, response->body().size());
    EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
    test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

    cleanupUpstreamAndDownstream();
  }
};

TEST_P(StaticRouterOrClusterFiltersIntegrationTest, BasicSuccess) {
  addStaticFilter("envoy.test.add_header_upstream", default_header_key_, default_header_value_);
  addCodecFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_key_, default_header_value_);
}

TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       ClusterUpstreamFiltersOverrideRouterUpstreamFilters) {
  // If both cluster and router upstream filters are set, only the cluster ones are applied.
  addStaticRouterFilter(
      getAddHeaderFilterConfig("envoy.test.add_header_upstream", "x-test-router", "aa"));
  addCodecRouterFilter();
  addStaticClusterFilter(
      getAddHeaderFilterConfig("envoy.test.add_header_upstream", "x-test-cluster", "bb"));
  addCodecClusterFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  EXPECT_THAT(*headers, Not(ContainsHeader("x-test-router", "aa")));
  EXPECT_THAT(*headers, ContainsHeader("x-test-cluster", "bb"));
}

TEST_P(StaticRouterOrClusterFiltersIntegrationTest, ClusterUpstreamFiltersDisabled) {
  addStaticRouterFilter(
      getAddHeaderFilterConfig("envoy.test.add_header_upstream", "x-test-router", "aa", true));
  addCodecRouterFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  EXPECT_THAT(*headers, Not(ContainsHeader("x-test-router", "aa")));
}

TEST_P(StaticRouterOrClusterFiltersIntegrationTest, RouterUpstreamFiltersDisabled) {
  addStaticClusterFilter(
      getAddHeaderFilterConfig("envoy.test.add_header_upstream", "x-test-cluster", "bb", true));
  addCodecClusterFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  EXPECT_THAT(*headers, Not(ContainsHeader("x-test-cluster", "bb")));
}

TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       AsyncClientUsesClusterFiltersButNotRouterFilters) {
  // This filter intercepts the downstream request and reissues it via AsyncClient,
  // which should bypass the main filter chain and should use cluster upstream filters.
  config_helper_.prependFilter(R"(
  name: "envoy.test.async_upstream"
  typed_config:
      "@type": "type.googleapis.com/test.integration.filters.AsyncUpstreamFilterConfig"
  )");
  addStaticFilter("envoy.test.add_header_upstream", default_header_key_, default_header_value_);
  addCodecFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  if (useRouterFilters()) {
    EXPECT_THAT(*headers, Not(ContainsHeader(default_header_key_, default_header_value_)));
  } else {
    EXPECT_THAT(*headers, ContainsHeader(default_header_key_, default_header_value_));
  }
}

TEST_P(StaticRouterOrClusterFiltersIntegrationTest, TwoFilters) {
  addStaticFilter("foo", default_header_key_, "value1");
  addStaticFilter("bar", default_header_key_, "value2");
  addCodecFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_key_, "value1,value2");
}

// Verifies that an upstream filter calling sendLocalReply() from onHostSelected()
// (before decodeHeaders) correctly returns an error to the client without
// corrupting the connection state. Two requests on the same HTTP/2 connection
// (header-only + POST with body) prove the state machine handles early abort cleanly.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, OnHostSelectedLocalReply) {
  addStaticTypedConfigFilter("local-reply-during-host-selection",
                             test::integration::filters::LocalReplyDuringHostSelectionConfig());
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // Request 1: header-only GET — rejected during onHostSelected.
  auto response1 = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  EXPECT_TRUE(response1->waitForEndStream());
  EXPECT_TRUE(response1->complete());
  EXPECT_EQ("403", response1->headers().getStatusValue());

  // Request 2: POST with body — rejected during onHostSelected, body must be drained cleanly.
  Http::TestRequestHeaderMapImpl post_headers{
      {":method", "POST"}, {":path", "/"}, {":scheme", "https"}, {":authority", "host"}};
  auto response2 = codec_client_->makeRequestWithBody(post_headers, "hello");
  EXPECT_TRUE(response2->waitForEndStream());
  EXPECT_TRUE(response2->complete());
  EXPECT_EQ("403", response2->headers().getStatusValue());

  // No upstream request was made — newStream() was never called.
  auto upstream_headers =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  EXPECT_TRUE(upstream_headers == nullptr);

  cleanupUpstreamAndDownstream();
}

// Verify that a local reply sent from onHostSelected is not retried when the route retry policy
// does not match the local reply status code.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, OnHostSelectedLocalReplyNoRetry) {
  addRouteRetryPolicy(config_helper_, "5xx", 3);
  addStaticTypedConfigFilter("local-reply-during-host-selection",
                             test::integration::filters::LocalReplyDuringHostSelectionConfig());
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  EXPECT_TRUE(response->waitForEndStream());
  EXPECT_TRUE(response->complete());
  EXPECT_EQ("403", response->headers().getStatusValue());
  EXPECT_EQ(0UL, test_server_->counter("cluster.cluster_0.upstream_rq_retry")->value());

  // No upstream connection was ever initiated — newStream() was never called on any attempt.
  auto upstream_headers =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  EXPECT_TRUE(upstream_headers == nullptr);

  cleanupUpstreamAndDownstream();
}

// Verify that a local reply sent from onHostSelected flows through Router::Filter and IS retried
// when the route retry policy matches the local reply status code.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, OnHostSelectedLocalReplyWithRetry) {
  addRouteRetryPolicy(config_helper_, "retriable-status-codes", 3, {403});
  addStaticTypedConfigFilter("local-reply-during-host-selection",
                             test::integration::filters::LocalReplyDuringHostSelectionConfig());
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("403", response->headers().getStatusValue());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(3));
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry_limit_exceeded",
                               testing::Eq(1));

  // No upstream connection was ever initiated — newStream() was never called on any attempt.
  auto upstream_headers =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  EXPECT_TRUE(upstream_headers == nullptr);

  cleanupUpstreamAndDownstream();
}

// Verify that upstream decoder filter local replies in onHostSelected, decodeHeaders, decodeData,
// and decodeTrailers traverse earlier upstream encoder filters before reaching Router::Filter.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyModifiedByUpstreamEncoderFilter) {
  autonomous_allow_incomplete_streams_ = true;
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // 1. Local reply in onHostSelected.
  UpstreamConfigurableLocalReplyFilter::resetState(-1, 403);
  auto response1 = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("403", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response1->body());

  // 2. Local reply in decodeHeaders.
  UpstreamConfigurableLocalReplyFilter::resetState();
  auto response2 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "400"));
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("400", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response2->body());

  // 3. Local reply in decodeData.
  UpstreamConfigurableLocalReplyFilter::resetState();
  auto response3 = codec_client_->makeRequestWithBody(
      localReplyHeaders("POST", "decode-data", "413"), "request body");
  ASSERT_TRUE(response3->waitForEndStream());
  ASSERT_TRUE(response3->complete());
  EXPECT_EQ("413", response3->headers().getStatusValue());
  EXPECT_THAT(response3->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response3->body());

  // 4. Local reply in decodeTrailers.
  UpstreamConfigurableLocalReplyFilter::resetState();
  auto encoder_decoder =
      codec_client_->startRequest(localReplyHeaders("POST", "decode-trailers", "422"));
  codec_client_->sendData(encoder_decoder.first, "request body", false);
  Http::TestRequestTrailerMapImpl request_trailers{{"x-request-trailer", "val"}};
  codec_client_->sendTrailers(encoder_decoder.first, request_trailers);
  auto& response4 = encoder_decoder.second;
  ASSERT_TRUE(response4->waitForEndStream());
  ASSERT_TRUE(response4->complete());
  EXPECT_EQ("422", response4->headers().getStatusValue());
  EXPECT_THAT(response4->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response4->body());

  cleanupUpstreamAndDownstream();
}

// Verify that an upstream filter local reply (in decodeHeaders or onHostSelected) is retried by
// Router::Filter and succeeds when a subsequent retry attempt passes.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, UpstreamFilterLocalReplyRetriedAndSucceeds) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // Case 1: Attempt 1 fails with 503 local reply in decodeHeaders; Attempt 2 succeeds with 200 OK.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response1 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("200", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  // Case 2: Attempt 1 fails with 503 local reply in onHostSelected; Attempt 2 succeeds with 200 OK.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1,
                                                   /*on_host_selected_code=*/503);
  auto response2 = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("200", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(2));

  cleanupUpstreamAndDownstream();
}

// Verify that when retries are exhausted on an upstream filter local reply, the client receives
// the upstream-encoder-modified local reply.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, UpstreamFilterLocalReplyRetryExhausted) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_THAT(response->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response->body());
  EXPECT_EQ(3, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(2));
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry_limit_exceeded",
                               testing::Eq(1));
  // The local replies did not come from the upstream host: they are not charged as upstream
  // responses and get no service time header, but the retries are still charged.
  EXPECT_EQ(0UL, counterValueOrZero("cluster.cluster_0.upstream_rq_5xx"));
  EXPECT_EQ(0UL, counterValueOrZero("cluster.cluster_0.upstream_rq_503"));
  EXPECT_EQ(0UL, counterValueOrZero("cluster.cluster_0.upstream_rq_completed"));
  EXPECT_EQ(2UL, test_server_->counter("cluster.cluster_0.retry.upstream_rq_503")->value());
  EXPECT_TRUE(
      response->headers().get(Http::LowerCaseString("x-envoy-upstream-service-time")).empty());

  cleanupUpstreamAndDownstream();
}

// Upstream filter local replies did not come from the upstream host, so a 503 local reply is not
// reported to outlier detection and does not eject the host, even with consecutive_5xx: 1.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyNotReportedToOutlierDetection) {
  config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
    auto* outlier_detection =
        bootstrap.mutable_static_resources()->mutable_clusters(0)->mutable_outlier_detection();
    outlier_detection->mutable_consecutive_5xx()->set_value(1);
    outlier_detection->mutable_max_ejection_percent()->set_value(100);
  });
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  for (int i = 0; i < 2; ++i) {
    auto response =
        codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
    ASSERT_TRUE(response->waitForEndStream());
    ASSERT_TRUE(response->complete());
    EXPECT_EQ("503", response->headers().getStatusValue());
  }
  const std::string outlier_prefix = "cluster.cluster_0.outlier_detection.";
  EXPECT_EQ(0UL, counterValueOrZero(outlier_prefix + "ejections_detected_consecutive_5xx"));
  EXPECT_EQ(0UL, counterValueOrZero(outlier_prefix + "ejections_enforced_total"));
  EXPECT_EQ(0UL, counterValueOrZero("cluster.cluster_0.upstream_rq_503"));

  cleanupUpstreamAndDownstream();
}

// A 302 upstream filter local reply with a Location header was not sent by the upstream host, so
// the router does not follow it as an internal redirect and forwards it to the client instead.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyIsNotInternallyRedirected) {
  config_helper_.addConfigModifier(
      [](envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
             hcm) {
        auto* route = hcm.mutable_route_config()->mutable_virtual_hosts(0)->mutable_routes(0);
        route->mutable_route()->mutable_internal_redirect_policy()->add_redirect_response_codes(
            302);
      });
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  Http::TestRequestHeaderMapImpl headers = localReplyHeaders("GET", "decode-headers", "302");
  headers.addCopy("x-local-reply-location", "https://host/redirected");
  auto response = codec_client_->makeHeaderOnlyRequest(headers);
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("302", response->headers().getStatusValue());
  EXPECT_EQ("https://host/redirected", response->headers().getLocationValue());
  EXPECT_EQ(1, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  EXPECT_EQ(0UL,
            counterValueOrZero("cluster.cluster_0.upstream_internal_redirect_succeeded_total"));

  cleanupUpstreamAndDownstream();
}

// Verify the legacy behavior when
// envoy.reloadable_features.upstream_filter_local_reply_via_upstream_filter_chain is disabled: an
// upstream filter local reply sent from decodeHeaders goes straight to the downstream filter
// chain. It bypasses the earlier upstream encoder filters and Router::Filter, so it is not retried
// even though the route retries on 5xx.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyBypassesUpstreamFilterChainWhenRuntimeFlagDisabled) {
  config_helper_.addRuntimeOverride(
      "envoy.reloadable_features.upstream_filter_local_reply_via_upstream_filter_chain", "false");
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_THAT(response->headers(), Not(ContainsHeader("x-upstream-encoder-modified", "yes")));
  EXPECT_EQ("upstream local reply", response->body());
  EXPECT_EQ(1, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  EXPECT_EQ(1, UpstreamResponseModifyingEncoderFilter::total_attempts_.load());
  EXPECT_EQ(0UL, test_server_->counter("cluster.cluster_0.upstream_rq_retry")->value());

  cleanupUpstreamAndDownstream();
}

// Verify that an upstream encoder filter calling sendLocalReply during encodeHeaders sends a
// direct local reply to Router::Filter and is retriable.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterLocalReplyInEncodeHeadersRetried) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "encode-headers", "502"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  cleanupUpstreamAndDownstream();
}

// Upstream filter chain: chain-filter-a -> chain-filter-b -> chain-filter-c -> codec, plus a
// downstream encoder filter. When chain-filter-b sends a local reply from decodeHeaders,
// chain-filter-c never sees decodeHeaders, the local reply traverses the upstream encoder chain in
// reverse order (c, b, a), and the downstream encoder filter processes the resulting response.
// chain-filter-b's decodeComplete() runs before the local reply is encoded.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, MultiFilterChainLocalReplyOrdering) {
  UpstreamConfigurableLocalReplyFilter::resetState();
  config_helper_.prependFilter(R"EOF(
name: downstream-response-modifying-encoder-filter
typed_config:
  "@type": type.googleapis.com/test.integration.filters.DownstreamResponseModifyingFilterConfig
)EOF");
  addUpstreamConfigurableLocalReplyFilter("chain-filter-a");
  addUpstreamConfigurableLocalReplyFilter("chain-filter-b");
  addUpstreamConfigurableLocalReplyFilter("chain-filter-c");
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // Request 1: no local reply. The request is decoded in configuration order and the backend
  // response is encoded in reverse order.
  auto response1 = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("200", response1->headers().getStatusValue());
  auto upstream_headers =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  ASSERT_NE(nullptr, upstream_headers);
  EXPECT_THAT(*upstream_headers, ContainsHeader("x-upstream-decode-order",
                                                "chain-filter-a,chain-filter-b,chain-filter-c"));
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encode-order",
                                                   "chain-filter-c,chain-filter-b,chain-filter-a"));
  EXPECT_THAT(response1->headers(), ContainsHeader("x-downstream-encoder-modified", "yes"));

  // Request 2: chain-filter-b sends a local reply from decodeHeaders.
  FilterCallTracker::reset();
  Http::TestRequestHeaderMapImpl headers2 = localReplyHeaders("GET", "decode-headers", "503");
  headers2.addCopy("x-local-reply-target-filter", "chain-filter-b");
  auto response2 = codec_client_->makeHeaderOnlyRequest(headers2);
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("503", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encode-order",
                                                   "chain-filter-c,chain-filter-b,chain-filter-a"));
  EXPECT_THAT(response2->headers(), ContainsHeader("x-downstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [downstream-encoded]", response2->body());
  EXPECT_THAT(FilterCallTracker::trace(),
              ElementsAre("chain-filter-a:decodeHeaders", "chain-filter-a:decodeComplete",
                          "chain-filter-b:decodeHeaders", "chain-filter-b:decodeComplete",
                          "chain-filter-c:encodeHeaders", "chain-filter-b:encodeHeaders",
                          "chain-filter-a:encodeHeaders", "downstream:encodeHeaders",
                          "chain-filter-c:encodeData", "chain-filter-b:encodeData",
                          "chain-filter-a:encodeData", "downstream:encodeData"));

  cleanupUpstreamAndDownstream();
}

// Verify that local replies sent asynchronously from a posted dispatcher callback (after the
// upstream decoder filter returned StopIteration / StopIterationAndBuffer) traverse earlier
// upstream encoder filters.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       AsyncUpstreamDecoderFilterLocalReplyModifiedByUpstreamEncoderFilter) {
  autonomous_allow_incomplete_streams_ = true;
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // 1. decodeHeaders returns StopIteration and the local reply is sent from a posted callback.
  UpstreamConfigurableLocalReplyFilter::resetState();
  auto response1 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "async-decode-headers", "503"));
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("503", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response1->body());

  // 2. decodeData returns StopIterationAndBuffer and the local reply is sent from a posted
  // callback.
  UpstreamConfigurableLocalReplyFilter::resetState();
  auto response2 = codec_client_->makeRequestWithBody(
      localReplyHeaders("POST", "async-decode-data", "413"), "request body");
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("413", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response2->body());

  cleanupUpstreamAndDownstream();
}

// Verify that asynchronous upstream filter local replies are retried by Router::Filter and
// succeed on the second attempt.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       AsyncUpstreamDecoderFilterLocalReplyRetriedAndSucceeds) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  autonomous_allow_incomplete_streams_ = true;
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // Case 1: async local reply after decodeHeaders returned StopIteration.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response1 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "async-decode-headers", "503"));
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("200", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  // Case 2: async local reply after decodeData returned StopIterationAndBuffer. The router replays
  // the buffered request body on the retry.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response2 = codec_client_->makeRequestWithBody(
      localReplyHeaders("POST", "async-decode-data", "503"), "request body");
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("200", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(2));

  cleanupUpstreamAndDownstream();
}

// Verify that an earlier upstream encoder filter can pause a local reply (StopIteration /
// StopIterationAndBuffer), modify its headers and buffered body from a posted dispatcher callback,
// and resume it with continueEncoding().
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterPausesAndResumesLocalReply) {
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // 1. Local reply with a body: resumed after the whole body is buffered.
  UpstreamConfigurableLocalReplyFilter::resetState();
  UpstreamResponseModifyingEncoderFilter::async_pause_attempts_ = 1;
  auto response1 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("503", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-async-encoder-resumed", "yes"));
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  const std::string expected_body = "upstream local reply [upstream-encoded]";
  EXPECT_EQ(expected_body, response1->body());
  EXPECT_EQ(std::to_string(expected_body.size()), response1->headers().getContentLengthValue());

  // 2. Header-only local reply (HEAD request): resumed directly from encodeHeaders.
  UpstreamConfigurableLocalReplyFilter::resetState();
  UpstreamResponseModifyingEncoderFilter::async_pause_attempts_ = 1;
  auto response2 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("HEAD", "decode-headers-head", "503"));
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("503", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-async-encoder-resumed", "yes"));
  EXPECT_TRUE(response2->body().empty());

  cleanupUpstreamAndDownstream();
}

// Verify that a local reply paused and resumed by an upstream encoder filter is still retriable
// by Router::Filter.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterPausedLocalReplyRetriedAndSucceeds) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  UpstreamResponseModifyingEncoderFilter::async_pause_attempts_ = 1;
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("200", response->headers().getStatusValue());
  // Attempt 2 is served by the backend without pausing.
  EXPECT_THAT(response->headers(), Not(ContainsHeader("x-async-encoder-resumed", "yes")));
  EXPECT_THAT(response->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  EXPECT_EQ(2, UpstreamResponseModifyingEncoderFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  cleanupUpstreamAndDownstream();
}

// Verify that an upstream filter local reply sent while decoding the request body resets the
// in-flight upstream stream, and that the retry replays headers, body and trailers.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyInDecodeDataResetsUpstreamAndRetryReplaysRequest) {
  testLocalReplyResetsUpstreamAndRetryReplaysRequest("decode-data");
}

// Same as above, with the local reply sent while decoding the request trailers.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyInDecodeTrailersResetsUpstreamAndRetryReplaysRequest) {
  testLocalReplyResetsUpstreamAndRetryReplaysRequest("decode-trailers");
}

// Verify that an upstream encoder filter rewriting the :status of a local reply changes whether
// Router::Filter retries it.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterStatusRewriteChangesLocalReplyRetriability) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));

  // Case A: a 400 local reply rewritten to 503 is retried, and attempt 2 succeeds.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  UpstreamResponseModifyingEncoderFilter::rewrite_status_code_ = 503;
  UpstreamResponseModifyingEncoderFilter::max_action_attempts_ = 1;
  auto response1 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "400"));
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("200", response1->headers().getStatusValue());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  // Case B: a 503 local reply rewritten to 400 is not retried.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  UpstreamResponseModifyingEncoderFilter::rewrite_status_code_ = 400;
  auto response2 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("400", response2->headers().getStatusValue());
  EXPECT_THAT(response2->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ("upstream local reply [upstream-encoded]", response2->body());
  EXPECT_EQ(1, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  EXPECT_EQ(1UL, test_server_->counter("cluster.cluster_0.upstream_rq_retry")->value());

  cleanupUpstreamAndDownstream();
}

// Verify that an upstream encoder filter local reply on a backend response resets the backend
// stream and is retried by Router::Filter.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterLocalReplyOnBackendResponseResetsAndRetries) {
  autonomous_upstream_ = false;
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "encode-headers", "503"));

  // Attempt 1: the backend starts a 200 response, which the upstream filter replaces with a 503.
  waitForNextUpstreamRequest();
  FakeStreamPtr first_attempt = std::move(upstream_request_);
  first_attempt->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  first_attempt->encodeData(5, false);
  ASSERT_TRUE(first_attempt->waitForReset());

  // Attempt 2 succeeds.
  ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
  ASSERT_TRUE(upstream_request_->waitForEndStream(*dispatcher_));
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  upstream_request_->encodeData(10, true);

  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_EQ(10U, response->body().size());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  cleanupUpstreamAndDownstream();
}

// Verify that when an upstream encoder filter local reply on a backend response is not retried,
// the client receives the direct local reply, which bypasses the remaining encoder filters.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterLocalReplyOnBackendResponseNotRetried) {
  autonomous_upstream_ = false;
  UpstreamConfigurableLocalReplyFilter::resetState();
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "encode-headers", "503"));

  waitForNextUpstreamRequest();
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  upstream_request_->encodeData(5, false);
  ASSERT_TRUE(upstream_request_->waitForReset());

  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_THAT(response->headers(), Not(ContainsHeader("x-upstream-encoder-modified", "yes")));
  EXPECT_EQ("upstream local reply", response->body());

  cleanupUpstreamAndDownstream();
}

// Verify that when an earlier upstream encoder filter sends its own local reply while encoding a
// decoder filter's local reply, Router::Filter receives the encoder filter's local reply.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       ChainedUpstreamEncoderLocalReplyOverridesDecoderLocalReply) {
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  UpstreamConfigurableLocalReplyFilter::resetState();
  UpstreamResponseModifyingEncoderFilter::chained_local_reply_code_ = 503;
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "400"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("503", response->headers().getStatusValue());
  EXPECT_EQ("encoder override reply", response->body());

  cleanupUpstreamAndDownstream();
}

// Verify that the chained encoder filter local reply is retried under retry_on: 5xx even though the
// original decoder filter local reply (400) is not retriable.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       ChainedUpstreamEncoderLocalReplyRetriedAndSucceeds) {
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  UpstreamResponseModifyingEncoderFilter::chained_local_reply_code_ = 503;
  UpstreamResponseModifyingEncoderFilter::max_action_attempts_ = 1;
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "400"));
  ASSERT_TRUE(response->waitForEndStream());
  ASSERT_TRUE(response->complete());
  EXPECT_EQ("200", response->headers().getStatusValue());
  EXPECT_THAT(response->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(1));

  cleanupUpstreamAndDownstream();
}

// Verify that an upstream encoder filter local reply sent from encodeData, after the backend's
// response headers were already forwarded downstream, resets both streams cleanly.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamEncoderFilterLocalReplyInEncodeDataAfterHeadersResetsStream) {
  autonomous_upstream_ = false;
  useAccessLog("%RESPONSE_CODE_DETAILS%|%RESPONSE_FLAGS%");
  UpstreamConfigurableLocalReplyFilter::resetState();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  auto response =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "encode-data", "500"));

  waitForNextUpstreamRequest();
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
  response->waitForHeaders();
  EXPECT_EQ("200", response->headers().getStatusValue());

  upstream_request_->encodeData(10, false);
  ASSERT_TRUE(upstream_request_->waitForReset());
  ASSERT_TRUE(response->waitForReset());
  EXPECT_FALSE(response->complete());
  EXPECT_TRUE(response->body().empty());
  // The local reply never reached the router, but its details are still recorded downstream.
  EXPECT_THAT(waitForAccessLog(access_log_name_), HasSubstr("upstream_encode_data_local_reply"));

  cleanupUpstreamAndDownstream();
}

// Verify gRPC upstream filter local replies: a header-only gRPC response that traverses the
// upstream encoder filters and is retried under retry_on: unavailable.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, UpstreamFilterGrpcLocalReplyAndRetry) {
  addRouteRetryPolicy(config_helper_, "unavailable", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  Http::TestRequestHeaderMapImpl grpc_headers =
      localReplyHeaders("POST", "decode-headers-grpc", "503");
  grpc_headers.addCopy("content-type", "application/grpc");
  grpc_headers.addCopy("te", "trailers");

  // Case 1: every attempt fails; the client receives the header-only gRPC local reply.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  auto response1 = codec_client_->makeHeaderOnlyRequest(grpc_headers);
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("200", response1->headers().getStatusValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("content-type", "application/grpc"));
  EXPECT_THAT(response1->headers(), ContainsHeader("grpc-status", "14"));
  EXPECT_THAT(response1->headers(), ContainsHeader("grpc-message", "upstream grpc reject"));
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_TRUE(response1->body().empty());
  EXPECT_EQ(nullptr, response1->trailers());
  EXPECT_EQ(3, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(2));
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry_limit_exceeded",
                               testing::Eq(1));

  // Case 2: attempt 1 fails with the gRPC local reply and attempt 2 succeeds.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response2 = codec_client_->makeHeaderOnlyRequest(grpc_headers);
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("200", response2->headers().getStatusValue());
  EXPECT_EQ(nullptr, response2->headers().GrpcStatus());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(3));

  cleanupUpstreamAndDownstream();
}

// Verify HEAD request upstream filter local replies: header-only responses that preserve the
// local reply content-length and are retried under retry_on: 5xx.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, UpstreamFilterHeadLocalReplyAndRetry) {
  // The backend is driven explicitly so that it can answer the HEAD retry with a header-only
  // response.
  autonomous_upstream_ = false;
  addRouteRetryPolicy(config_helper_, "5xx", 2);
  addUpstreamResponseModifyingFilter();
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  const Http::TestRequestHeaderMapImpl head_headers =
      localReplyHeaders("HEAD", "decode-headers-head", "503");

  // Case 1: every attempt fails; the client receives the header-only local reply.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  auto response1 = codec_client_->makeHeaderOnlyRequest(head_headers);
  ASSERT_TRUE(response1->waitForEndStream());
  ASSERT_TRUE(response1->complete());
  EXPECT_EQ("503", response1->headers().getStatusValue());
  EXPECT_EQ(std::to_string(absl::string_view("head error body").size()),
            response1->headers().getContentLengthValue());
  EXPECT_THAT(response1->headers(), ContainsHeader("x-upstream-encoder-modified", "yes"));
  EXPECT_TRUE(response1->body().empty());
  EXPECT_EQ(3, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(2));

  // Case 2: attempt 1 fails and attempt 2 is served by the backend.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response2 = codec_client_->makeHeaderOnlyRequest(head_headers);
  waitForNextUpstreamRequest();
  EXPECT_EQ("HEAD", upstream_request_->headers().getMethodValue());
  upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, true);
  ASSERT_TRUE(response2->waitForEndStream());
  ASSERT_TRUE(response2->complete());
  EXPECT_EQ("200", response2->headers().getStatusValue());
  EXPECT_TRUE(response2->body().empty());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  test_server_->waitForCounter("cluster.cluster_0.upstream_rq_retry", testing::Eq(3));

  cleanupUpstreamAndDownstream();
}

// Verify %RESPONSE_CODE_DETAILS%, %UPSTREAM_HOST%, %UPSTREAM_REQUEST_ATTEMPT_COUNT% and
// %RESPONSE_FLAGS% in the downstream access log for upstream filter local replies, and that a
// successful retry does not keep stale details from attempt 1.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest,
       UpstreamFilterLocalReplyAccessLogDetailsAndUpstreamHost) {
  useAccessLog("%RESPONSE_CODE_DETAILS%|%UPSTREAM_HOST%|%UPSTREAM_REQUEST_ATTEMPT_COUNT%|"
               "%RESPONSE_FLAGS%");
  addRouteRetryPolicy(config_helper_, "5xx", 1);
  addUpstreamConfigurableLocalReplyFilter();
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  const std::string upstream_host = fake_upstreams_[0]->localAddress()->asString();

  // Entry 0: local reply from onHostSelected (403 is not retriable under 5xx).
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1,
                                                   /*on_host_selected_code=*/403);
  auto response1 = codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  ASSERT_TRUE(response1->waitForEndStream());
  EXPECT_EQ("403", response1->headers().getStatusValue());
  const std::string log1 = waitForAccessLog(access_log_name_, 0);
  EXPECT_THAT(log1, HasSubstr(absl::StrCat("upstream_on_host_selected_local_reply|", upstream_host,
                                           "|1|-")));
  EXPECT_THAT(log1, Not(HasSubstr("via_upstream")));

  // Entry 1: local reply from decodeHeaders returned after exhausting retries.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/-1);
  auto response2 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response2->waitForEndStream());
  EXPECT_EQ("503", response2->headers().getStatusValue());
  EXPECT_EQ(2, UpstreamConfigurableLocalReplyFilter::total_attempts_.load());
  const std::string log2 = waitForAccessLog(access_log_name_, 1);
  EXPECT_THAT(log2, HasSubstr(absl::StrCat("upstream_decode_headers_local_reply|", upstream_host,
                                           "|2|URX")));
  EXPECT_THAT(log2, Not(HasSubstr("via_upstream")));

  // Entry 2: attempt 1 fails with a local reply and attempt 2 is served by the backend.
  UpstreamConfigurableLocalReplyFilter::resetState(/*fail_attempts=*/1);
  auto response3 =
      codec_client_->makeHeaderOnlyRequest(localReplyHeaders("GET", "decode-headers", "503"));
  ASSERT_TRUE(response3->waitForEndStream());
  EXPECT_EQ("200", response3->headers().getStatusValue());
  const std::string log3 = waitForAccessLog(access_log_name_, 2);
  EXPECT_THAT(log3, HasSubstr(absl::StrCat("via_upstream|", upstream_host, "|2|-")));
  EXPECT_THAT(log3, Not(HasSubstr("local_reply")));

  cleanupUpstreamAndDownstream();
}

INSTANTIATE_TEST_SUITE_P(
    IpVersions, StaticRouterOrClusterFiltersIntegrationTest,
    testing::Combine(testing::ValuesIn(TestEnvironment::getIpVersionsForTest()), testing::Bool()));

class StaticRouterAndClusterFiltersIntegrationTest
    : public testing::TestWithParam<Network::Address::IpVersion>,
      public UpstreamHttpFilterIntegrationTestBase {
public:
  StaticRouterAndClusterFiltersIntegrationTest()
      : UpstreamHttpFilterIntegrationTestBase(GetParam(), false) {}

  void routeRetryAndFilterReturnStopIteration(bool send_body) {
    autonomous_upstream_ = false;
    config_helper_.prependFilter(R"EOF(
name: encode-headers-return-stop-iteration-filter
typed_config:
  "@type": type.googleapis.com/test.integration.filters.EncodeHeadersReturnStopIterationFilterConfig
)EOF",
                                 false);

    config_helper_.addConfigModifier(
        [](envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
               hcm) {
          auto* route = hcm.mutable_route_config()
                            ->mutable_virtual_hosts(0)
                            ->mutable_routes(0)
                            ->mutable_route();
          route->mutable_timeout()->set_seconds(60);
          auto* retry_policy = route->mutable_retry_policy();
          retry_policy->set_retry_on("5xx,connect-failure,refused-stream");
          retry_policy->mutable_num_retries()->set_value(5);
          retry_policy->mutable_per_try_timeout()->set_seconds(30);
          retry_policy->mutable_retry_back_off()->mutable_base_interval()->set_seconds(1);
        });
    initialize();

    codec_client_ = makeHttpConnection(lookupPort("http"));
    auto response = codec_client_->makeRequestWithBody(default_request_headers_, 10);
    waitForNextUpstreamRequest();
    upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "503"}}, false);
    if (send_body) {
      upstream_request_->encodeData(100, true);
    } else {
      upstream_request_->encodeTrailers(
          Http::TestResponseTrailerMapImpl{{"x-test-trailers", "Yes"}});
    }
    EXPECT_TRUE(upstream_request_->complete());

    // Router filter send retries.
    ASSERT_TRUE(fake_upstream_connection_->waitForNewStream(*dispatcher_, upstream_request_));
    ASSERT_TRUE(upstream_request_->waitForEndStream(*dispatcher_));
    upstream_request_->encodeHeaders(Http::TestResponseHeaderMapImpl{{":status", "200"}}, false);
    upstream_request_->encodeData(100, true);
    EXPECT_TRUE(upstream_request_->complete());
    ASSERT_TRUE(response->waitForEndStream());
    ASSERT_TRUE(response->complete());
  }
};

// Only cluster-specified filters should be applied.
TEST_P(StaticRouterAndClusterFiltersIntegrationTest, StaticRouterAndClusterFilters) {
  addStaticClusterFilter(
      getAddHeaderFilterConfig("foo", default_header_key_, "value-from-cluster"));
  addStaticRouterFilter(getAddHeaderFilterConfig("bar", default_header_key_, "value-from-router"));
  addCodecClusterFilter();
  addCodecRouterFilter();
  initialize();

  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_key_, "value-from-cluster");
}

// Test route retries on 5xx and an upstream filter returns StopIteration.
TEST_P(StaticRouterAndClusterFiltersIntegrationTest, RouterRetrySendBody) {
  routeRetryAndFilterReturnStopIteration(/*send_body*/ true);
}

TEST_P(StaticRouterAndClusterFiltersIntegrationTest, RouterRetrySendTrailers) {
  routeRetryAndFilterReturnStopIteration(/*send_body*/ false);
}

INSTANTIATE_TEST_SUITE_P(IpVersions, StaticRouterAndClusterFiltersIntegrationTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()));

class GrpcClientIntegrationParamTestWithVariantUpstreamConfig
    : public testing::TestWithParam<
          std::tuple<std::tuple<Network::Address::IpVersion, Grpc::ClientType>, bool>> {
public:
  static std::string protocolTestParamsToString(
      const ::testing::TestParamInfo<
          std::tuple<std::tuple<Network::Address::IpVersion, Grpc::ClientType>, bool>>& p) {
    return fmt::format(
        "{}_{}_{}", TestUtility::ipVersionToString(std::get<0>(std::get<0>(p.param))),
        std::get<1>(std::get<0>(p.param)) == Grpc::ClientType::GoogleGrpc ? "GoogleGrpc"
                                                                          : "EnvoyGrpc",
        std::get<1>(p.param) ? "WithRouterFilters" : "WithClusterFilters");
  }
};

class UpstreamHttpExtensionDiscoveryIntegrationTestBase
    : public Grpc::BaseGrpcClientIntegrationParamTest,
      public UpstreamHttpFilterIntegrationTestBase {
public:
  UpstreamHttpExtensionDiscoveryIntegrationTestBase(Network::Address::IpVersion version,
                                                    bool use_router_filters)
      : UpstreamHttpFilterIntegrationTestBase(version, use_router_filters) {}

  void setDynamicFilterConfig(HttpFilterProto* filter, const std::string& name,
                              bool apply_without_warming, bool set_default_config = true,
                              bool rate_limit = false, bool second_connection = false) {
    filter->set_name(name);

    auto* discovery = filter->mutable_config_discovery();
    discovery->add_type_urls("type.googleapis.com/test.integration.filters.AddHeaderFilterConfig");
    if (set_default_config) {
      auto default_configuration = test::integration::filters::AddHeaderFilterConfig();
      default_configuration.set_header_key(default_header_key_);
      default_configuration.set_header_value(default_header_value_);
      std::ignore = discovery->mutable_default_config()->PackFrom(default_configuration);
    }

    discovery->set_apply_default_config_without_warming(apply_without_warming);
    discovery->mutable_config_source()->set_resource_api_version(
        envoy::config::core::v3::ApiVersion::V3);
    auto* api_config_source = discovery->mutable_config_source()->mutable_api_config_source();
    api_config_source->set_api_type(envoy::config::core::v3::ApiConfigSource::GRPC);
    api_config_source->set_transport_api_version(envoy::config::core::v3::ApiVersion::V3);
    if (rate_limit) {
      api_config_source->mutable_rate_limit_settings()->mutable_max_tokens()->set_value(10);
    }
    auto* grpc_service = api_config_source->add_grpc_services();
    if (!second_connection) {
      setGrpcService(*grpc_service, std::string(EcdsClusterName),
                     getEcdsFakeUpstream().localAddress());
    } else {
      setGrpcService(*grpc_service, std::string(Ecds2ClusterName),
                     getEcds2FakeUpstream().localAddress());
    }
  }

  void addDynamicClusterFilter(const std::string& name, bool apply_without_warming,
                               bool set_default_config = true, bool rate_limit = false,
                               bool second_connection = false) {
    config_helper_.addConfigModifier([name, apply_without_warming, set_default_config, rate_limit,
                                      second_connection,
                                      this](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      auto* cluster = bootstrap.mutable_static_resources()->mutable_clusters(0);
      ConfigHelper::HttpProtocolOptions protocol_options =
          MessageUtil::anyConvert<ConfigHelper::HttpProtocolOptions>(
              (*cluster->mutable_typed_extension_protocol_options())
                  ["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]);
      auto* filter = protocol_options.add_http_filters();
      setDynamicFilterConfig(filter, name, apply_without_warming, set_default_config, rate_limit,
                             second_connection);
      std::ignore = (*cluster->mutable_typed_extension_protocol_options())
                        ["envoy.extensions.upstreams.http.v3.HttpProtocolOptions"]
                            .PackFrom(protocol_options);
    });
  }

  void addDynamicRouterFilter(const std::string& name, bool apply_without_warming,
                              bool set_default_config = true, bool rate_limit = false,
                              bool second_connection = false) {
    config_helper_.addConfigModifier(
        [name, apply_without_warming, set_default_config, rate_limit, second_connection, this](
            envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
                hcm) -> void {
          HttpFilterProto& router_filter = *hcm.mutable_http_filters(0);
          ASSERT_EQ(router_filter.name(), "envoy.filters.http.router");
          envoy::extensions::filters::http::router::v3::Router router;
          std::ignore = router_filter.typed_config().UnpackTo(&router);
          auto* filter = router.add_upstream_http_filters();
          setDynamicFilterConfig(filter, name, apply_without_warming, set_default_config,
                                 rate_limit, second_connection);
          std::ignore = router_filter.mutable_typed_config()->PackFrom(router);
        });
  }

  void addDynamicFilter(const std::string& name, bool apply_without_warming,
                        bool set_default_config = true, bool rate_limit = false,
                        bool second_connection = false) {
    if (useRouterFilters()) {
      addDynamicRouterFilter(name, apply_without_warming, set_default_config, rate_limit,
                             second_connection);
    } else {
      addDynamicClusterFilter(name, apply_without_warming, set_default_config, rate_limit,
                              second_connection);
    }
  }

  void addEcdsCluster(const std::string& cluster_name) {
    // Add an xDS cluster for extension config discovery.
    config_helper_.addConfigModifier(
        [cluster_name](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
          auto* ecds_cluster = bootstrap.mutable_static_resources()->add_clusters();
          ecds_cluster->MergeFrom(bootstrap.static_resources().clusters()[0]);
          ecds_cluster->set_name(cluster_name);
          ecds_cluster->mutable_typed_extension_protocol_options()->clear();
          ConfigHelper::setHttp2(*ecds_cluster);
        });
  }

  void initialize() override {
    defer_listener_finalization_ = true;
    setUpstreamCount(1);
    addEcdsCluster(std::string(EcdsClusterName));
    // Use gRPC LDS instead of default file LDS.
    use_lds_ = false;
    config_helper_.addConfigModifier([](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      auto* lds_cluster = bootstrap.mutable_static_resources()->add_clusters();
      lds_cluster->MergeFrom(bootstrap.static_resources().clusters()[0]);
      lds_cluster->set_name("lds_cluster");
      lds_cluster->mutable_typed_extension_protocol_options()->clear();
      ConfigHelper::setHttp2(*lds_cluster);
    });

    // In case to configure both HTTP and Listener ECDS filters, adding the 2nd ECDS cluster.
    if (two_ecds_filters_) {
      addEcdsCluster(std::string(Ecds2ClusterName));
    }

    // Must be the last since it nukes static listeners.
    config_helper_.addConfigModifier([this](envoy::config::bootstrap::v3::Bootstrap& bootstrap) {
      listener_config_.Swap(bootstrap.mutable_static_resources()->mutable_listeners(0));
      listener_config_.set_name(listener_name_);
      ENVOY_LOG_MISC(debug, "listener config: {}", listener_config_.DebugString());
      bootstrap.mutable_static_resources()->mutable_listeners()->Clear();
      auto* lds_config_source = bootstrap.mutable_dynamic_resources()->mutable_lds_config();
      lds_config_source->set_resource_api_version(envoy::config::core::v3::ApiVersion::V3);
      auto* lds_api_config_source = lds_config_source->mutable_api_config_source();
      lds_api_config_source->set_api_type(envoy::config::core::v3::ApiConfigSource::GRPC);
      lds_api_config_source->set_transport_api_version(envoy::config::core::v3::V3);
      envoy::config::core::v3::GrpcService* grpc_service =
          lds_api_config_source->add_grpc_services();
      setGrpcService(*grpc_service, "lds_cluster", getLdsFakeUpstream().localAddress());
    });

    HttpIntegrationTest::initialize();
    test_server_->waitForCounter("listener_manager.lds.update_success", Ge(1));
    EXPECT_EQ(test_server_->server().initManager().state(), Init::Manager::State::Initialized);
    registerTestServerPorts({"http"});
  }

  void resetConnection(FakeHttpConnectionPtr& connection) {
    if (connection != nullptr) {
      AssertionResult result = connection->close();
      RELEASE_ASSERT(result, result.message());
      result = connection->waitForDisconnect();
      RELEASE_ASSERT(result, result.message());
      connection.reset();
    }
  }

  ~UpstreamHttpExtensionDiscoveryIntegrationTestBase() override {
    resetConnection(ecds_connection_);
    resetConnection(lds_connection_);
    resetConnection(ecds2_connection_);
  }

  void createUpstreams() override {
    HttpIntegrationTest::createUpstreams();
    // Create the extension config discovery upstream (fake_upstreams_[1]).
    addFakeUpstream(Http::CodecType::HTTP2);
    // Create the listener config discovery upstream (fake_upstreams_[2]).
    addFakeUpstream(Http::CodecType::HTTP2);
    if (two_ecds_filters_) {
      addFakeUpstream(Http::CodecType::HTTP2);
    }
  }

  // Wait for ECDS stream.
  void waitForEcdsStream(FakeUpstream& upstream, FakeHttpConnectionPtr& connection,
                         FakeStreamPtr& stream) {
    AssertionResult result = upstream.waitForHttpConnection(*dispatcher_, connection);
    ASSERT_TRUE(result);
    result = connection->waitForNewStream(*dispatcher_, stream);
    ASSERT_TRUE(result);
    stream->startGrpcStream();
  }

  void waitXdsStream() {
    // Wait for LDS stream.
    auto& lds_upstream = getLdsFakeUpstream();
    AssertionResult result = lds_upstream.waitForHttpConnection(*dispatcher_, lds_connection_);
    RELEASE_ASSERT(result, result.message());
    result = lds_connection_->waitForNewStream(*dispatcher_, lds_stream_);
    RELEASE_ASSERT(result, result.message());
    lds_stream_->startGrpcStream();

    // Response with initial LDS.
    sendLdsResponse("initial");

    waitForEcdsStream(getEcdsFakeUpstream(), ecds_connection_, ecds_stream_);
    if (two_ecds_filters_) {
      // Wait for 2nd ECDS stream.
      waitForEcdsStream(getEcds2FakeUpstream(), ecds2_connection_, ecds2_stream_);
    }
  }

  void sendLdsResponse(const std::string& version) {
    envoy::service::discovery::v3::DiscoveryResponse response;
    response.set_version_info(version);
    response.set_type_url(Config::TestTypeUrl::get().Listener);
    std::ignore = response.add_resources()->PackFrom(listener_config_);
    lds_stream_->sendGrpcMessage(response);
  }

  void sendXdsResponse(const std::string& name, const std::string& version, const std::string& key,
                       const std::string& val, bool ttl = false, bool second_connection = false) {
    envoy::config::core::v3::TypedExtensionConfig typed_config;
    typed_config.set_name(name);

    auto configuration = test::integration::filters::AddHeaderFilterConfig();
    configuration.set_header_key(key);
    configuration.set_header_value(val);
    std::ignore = typed_config.mutable_typed_config()->PackFrom(configuration);

    envoy::service::discovery::v3::Resource resource;
    resource.set_name(name);
    std::ignore = resource.mutable_resource()->PackFrom(typed_config);
    if (ttl) {
      resource.mutable_ttl()->set_seconds(1);
    }

    envoy::service::discovery::v3::DiscoveryResponse response;
    response.set_version_info(version);
    response.set_type_url("type.googleapis.com/envoy.config.core.v3.TypedExtensionConfig");
    std::ignore = response.add_resources()->PackFrom(resource);

    if (!second_connection) {
      ecds_stream_->sendGrpcMessage(response);
    } else {
      ecds2_stream_->sendGrpcMessage(response);
    }
  }

  // Utilities used for config dump.
  absl::string_view request(const std::string port_key, const std::string method,
                            const std::string endpoint, BufferingStreamDecoderPtr& response) {
    response = IntegrationUtil::makeSingleRequest(lookupPort(port_key), method, endpoint, "",
                                                  Http::CodecType::HTTP1, version_);
    EXPECT_TRUE(response->complete());
    return response->headers().getStatusValue();
  }

  absl::string_view contentType(const BufferingStreamDecoderPtr& response) {
    const Http::HeaderEntry* entry = response->headers().ContentType();
    if (entry == nullptr) {
      return "(null)";
    }
    return entry->value().getStringView();
  }

  const std::string filter_name_ = "foo";
  bool two_ecds_filters_{false};
  FakeUpstream& getEcdsFakeUpstream() const { return *fake_upstreams_[1]; }
  FakeUpstream& getLdsFakeUpstream() const { return *fake_upstreams_[2]; }
  FakeUpstream& getEcds2FakeUpstream() const { return *fake_upstreams_[3]; }

  // gRPC LDS set-up
  envoy::config::listener::v3::Listener listener_config_;
  std::string listener_name_{"testing-listener-0"};
  FakeHttpConnectionPtr lds_connection_{nullptr};
  FakeStreamPtr lds_stream_{nullptr};

  // gRPC ECDS set-up
  FakeHttpConnectionPtr ecds_connection_{nullptr};
  FakeStreamPtr ecds_stream_{nullptr};
  FakeHttpConnectionPtr ecds2_connection_{nullptr};
  FakeStreamPtr ecds2_stream_{nullptr};
};

class DynamicRouterOrClusterFiltersIntegrationTest
    : public GrpcClientIntegrationParamTestWithVariantUpstreamConfig,
      public UpstreamHttpExtensionDiscoveryIntegrationTestBase {
public:
  DynamicRouterOrClusterFiltersIntegrationTest()
      : UpstreamHttpExtensionDiscoveryIntegrationTestBase(
            DynamicRouterOrClusterFiltersIntegrationTest::ipVersion(), std::get<1>(GetParam())) {}

  Network::Address::IpVersion ipVersion() const override {
    return std::get<0>(std::get<0>(GetParam()));
  }
  Grpc::ClientType clientType() const override { return std::get<1>(std::get<0>(GetParam())); }
};

// All upstream HTTP filters that are applied to cluster config must apply default config
// without warming. Otherwise, clusters initialization enters deadlock.
TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, BasicSuccess) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  // Expect default headers before config is updated
  auto headers1 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers1, default_header_value_);

  // Send 1st config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "test-val1");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers2 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers2, "test-val1");

  // Send 2nd config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "test-val2");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(2));
  auto headers3 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers3, "test-val2");
};

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, BasicSuccessWithTtl) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  // Send 1st config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "test-val1", true);
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers1 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers1, "test-val1");

  // Wait for configuration expiry, the default configuration should be applied.
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(2));
  auto headers2 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers2, default_header_value_);
};

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, BasicWithConfigFail) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  // Send config update with invalid config (header value length has to >=2).
  sendXdsResponse(filter_name_, "1", default_header_key_, "x");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_fail", Eq(1));
  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_value_);
};

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, TwoSubscriptionsSameName) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  sendXdsResponse(filter_name_, "1", default_header_key_, "test-val");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, "test-val,test-val");
}

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, TwoSubscriptionsDifferentName) {
  two_ecds_filters_ = true;
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter("foo", true);
  addDynamicFilter("bar", true, true, false, true);
  addCodecFilter();
  initialize();

  auto headers1 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers1, default_header_value_ + "," + default_header_value_);

  // Send 1st config update.
  sendXdsResponse("foo", "1", "header-key1", "test-val1");
  sendXdsResponse("bar", "1", "header-key2", "test-val1", false, true);
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.foo.config_reload",
                               Eq(1));
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.bar.config_reload",
                               Eq(1));
  auto headers2 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers2, "header-key1", "test-val1");
  expectHeaderKeyAndValue(headers2, "header-key2", "test-val1");

  // Send 2nd config update.
  sendXdsResponse("foo", "2", "header-key1", "test-val2");
  sendXdsResponse("bar", "2", "header-key2", "test-val2", false, true);
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.foo.config_reload",
                               Eq(2));
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.bar.config_reload",
                               Eq(2));
  auto headers3 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers3, "header-key1", "test-val2");
  expectHeaderKeyAndValue(headers3, "header-key2", "test-val2");
}

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, TwoDynamicTwoStaticFilterMixed) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addStaticFilter("bar", default_header_key_, "static-val1");
  addDynamicFilter(filter_name_, true);
  addStaticFilter("foobar", "header2", "static-val2");
  addCodecFilter();
  initialize();

  sendXdsResponse(filter_name_, "1", default_header_key_, "xds-val");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_key_, "xds-val,static-val1,xds-val");
  expectHeaderKeyAndValue(headers, "header2", "static-val2");
}

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, DynamicStaticFilterMixedDifferentOrder) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addStaticFilter("bar", default_header_key_, "static-val1");
  addStaticFilter("foobar", "header2", "static-val2");
  addDynamicFilter(filter_name_, true);
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  sendXdsResponse(filter_name_, "1", default_header_key_, "xds-val");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers, default_header_key_, "static-val1,xds-val,xds-val");
  expectHeaderKeyAndValue(headers, "header2", "static-val2");
}

TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, UpdateDuringConnection) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  IntegrationStreamDecoderPtr response1 =
      codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  EXPECT_TRUE(response1->waitForEndStream());
  EXPECT_TRUE(response1->complete());
  std::unique_ptr<Http::RequestHeaderMap> headers1 =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  EXPECT_TRUE(headers1 != nullptr);

  // Expect default headers before config is updated
  expectHeaderKeyAndValue(headers1, default_header_value_);

  // Send config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "xds-val");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));

  IntegrationStreamDecoderPtr response2 =
      codec_client_->makeHeaderOnlyRequest(default_request_headers_);
  EXPECT_TRUE(response2->waitForEndStream());
  EXPECT_TRUE(response2->complete());
  std::unique_ptr<Http::RequestHeaderMap> headers2 =
      reinterpret_cast<AutonomousUpstream*>(fake_upstreams_[0].get())->lastRequestHeaders();
  EXPECT_TRUE(headers2 != nullptr);

  // Expect default headers before config is updated
  expectHeaderKeyAndValue(headers2, "xds-val");

  cleanupUpstreamAndDownstream();
};

// Basic ECDS config dump test with one filter.
TEST_P(DynamicRouterOrClusterFiltersIntegrationTest, BasicSuccessWithConfigDump) {
  DISABLE_IF_ADMIN_DISABLED; // Uses admin interface.
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicFilter(filter_name_, true);
  addCodecFilter();
  initialize();

  // Send 1st config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "xds-val");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));

  // Verify ECDS config dump are working correctly.
  BufferingStreamDecoderPtr response;
  EXPECT_EQ("200", request("admin", "GET", "/config_dump", response));
  EXPECT_EQ("application/json", contentType(response));
  Json::ObjectSharedPtr json = Json::Factory::loadFromString(response->body()).value();
  size_t index = 0;
  auto array = json->getObjectArray("configs").value();
  for (const Json::ObjectSharedPtr& obj_ptr : array) {
    EXPECT_TRUE(expected_types[index] == obj_ptr->getString("@type").value());
    index++;
  }

  // Validate we can parse as proto.
  envoy::admin::v3::ConfigDump config_dump;
  TestUtility::loadFromJson(response->body(), config_dump);
  EXPECT_EQ(7, config_dump.configs_size());

  // With /config_dump, the response has the format: EcdsConfigDump.
  envoy::admin::v3::EcdsConfigDump ecds_config_dump;
  std::ignore = config_dump.configs(2).UnpackTo(&ecds_config_dump);
  EXPECT_EQ("1", ecds_config_dump.ecds_filters(0).version_info());
  envoy::config::core::v3::TypedExtensionConfig filter_config;
  EXPECT_TRUE(ecds_config_dump.ecds_filters(0).ecds_filter().UnpackTo(&filter_config));
  EXPECT_EQ("foo", filter_config.name());
  test::integration::filters::AddHeaderFilterConfig http_filter_config;
  std::ignore = filter_config.typed_config().UnpackTo(&http_filter_config);
  EXPECT_EQ(default_header_key_, http_filter_config.header_key());
  EXPECT_EQ("xds-val", http_filter_config.header_value());
}

INSTANTIATE_TEST_SUITE_P(
    IpVersionsClientType, DynamicRouterOrClusterFiltersIntegrationTest,
    testing::Combine(GRPC_CLIENT_INTEGRATION_PARAMS, testing::Bool()),
    GrpcClientIntegrationParamTestWithVariantUpstreamConfig::protocolTestParamsToString);

class GrpcClientIntegrationParamTestParams
    : public testing::TestWithParam<std::tuple<Network::Address::IpVersion, Grpc::ClientType>> {
public:
  static std::string protocolTestParamsToString(
      const ::testing::TestParamInfo<std::tuple<Network::Address::IpVersion, Grpc::ClientType>>&
          p) {
    return fmt::format("{}_{}", TestUtility::ipVersionToString(std::get<0>(p.param)),
                       std::get<1>(p.param) == Grpc::ClientType::GoogleGrpc ? "GoogleGrpc"
                                                                            : "EnvoyGrpc");
  }
};

class DynamicRouterAndClusterFiltersIntegrationTest
    : public GrpcClientIntegrationParamTestParams,
      public UpstreamHttpExtensionDiscoveryIntegrationTestBase {
public:
  DynamicRouterAndClusterFiltersIntegrationTest()
      : UpstreamHttpExtensionDiscoveryIntegrationTestBase(
            DynamicRouterAndClusterFiltersIntegrationTest::ipVersion(), false) {}

  Network::Address::IpVersion ipVersion() const override { return std::get<0>(GetParam()); }
  Grpc::ClientType clientType() const override { return std::get<1>(GetParam()); }
};

TEST_P(DynamicRouterAndClusterFiltersIntegrationTest, DynamicRouterAndClusterSameName) {
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicClusterFilter(filter_name_, true);
  addDynamicRouterFilter(filter_name_, true);
  addCodecClusterFilter();
  addCodecRouterFilter();
  initialize();

  // Expect default headers before config is updated.
  auto headers1 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers1, default_header_value_);

  // Send 1st config update.
  sendXdsResponse(filter_name_, "1", default_header_key_, "test-val1");
  test_server_->waitForCounter(
      "extension_config_discovery.upstream_http_filter." + filter_name_ + ".config_reload", Eq(1));
  auto headers2 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers2, "test-val1");
};

TEST_P(DynamicRouterAndClusterFiltersIntegrationTest, DynamicRouterAndClusterDifferentName) {
  two_ecds_filters_ = true;
  on_server_init_function_ = [&]() { waitXdsStream(); };
  addDynamicClusterFilter("foo", true);
  addDynamicRouterFilter("bar", true, true, false, true);
  addCodecClusterFilter();
  addCodecRouterFilter();
  initialize();

  // Expect default headers before config is updated.
  auto headers1 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers1, default_header_value_);

  // Send 1st config update.
  sendXdsResponse("foo", "1", default_header_key_, "value-from-cluster");
  sendXdsResponse("bar", "1", default_header_key_, "value-from-router", false, true);
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.foo.config_reload",
                               Eq(1));
  test_server_->waitForCounter("extension_config_discovery.upstream_http_filter.bar.config_reload",
                               Eq(1));
  auto headers2 = sendRequestAndGetHeaders();
  expectHeaderKeyAndValue(headers2, "value-from-cluster");
};

INSTANTIATE_TEST_SUITE_P(IpVersionsClientType, DynamicRouterAndClusterFiltersIntegrationTest,
                         GRPC_CLIENT_INTEGRATION_PARAMS,
                         GrpcClientIntegrationParamTestParams::protocolTestParamsToString);

// Retrying a POST request with both buffered body and trailers must not crash in continueDoRetry()
// when an upstream HTTP filter sends a local reply in decodeData() on the retry attempt.
TEST_P(StaticRouterOrClusterFiltersIntegrationTest, DecodeDataLocalReplyRetryWithTrailers) {
  autonomous_allow_incomplete_streams_ = true;
  config_helper_.addConfigModifier(
      [](envoy::extensions::filters::network::http_connection_manager::v3::HttpConnectionManager&
             hcm) {
        auto* retry_policy = hcm.mutable_route_config()
                                 ->mutable_virtual_hosts(0)
                                 ->mutable_routes(0)
                                 ->mutable_route()
                                 ->mutable_retry_policy();
        retry_policy->set_retry_on("5xx");
        retry_policy->mutable_num_retries()->set_value(1);
        retry_policy->mutable_retry_back_off()->mutable_base_interval()->set_nanos(100 * 1000 *
                                                                                   1000);
      });

  addStaticTypedConfigFilter("local-reply-during-decode",
                             test::integration::filters::LocalReplyDuringDecodeConfig());
  addCodecFilter();
  initialize();

  codec_client_ = makeHttpConnection(lookupPort("http"));
  Http::TestRequestHeaderMapImpl post_headers{{":method", "POST"},
                                              {":path", "/"},
                                              {":scheme", "https"},
                                              {":authority", "host"},
                                              {"local-reply-during-data", "true"}};
  auto encoder_decoder = codec_client_->startRequest(post_headers);
  request_encoder_ = &encoder_decoder.first;
  auto response = std::move(encoder_decoder.second);
  codec_client_->sendData(*request_encoder_, "request body", false);
  Http::TestRequestTrailerMapImpl request_trailers{{"x-req-trailer", "val"}};
  codec_client_->sendTrailers(*request_encoder_, request_trailers);

  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_TRUE(response->complete());
  EXPECT_EQ("500", response->headers().getStatusValue());
  cleanupUpstreamAndDownstream();
}

} // namespace
} // namespace Envoy
