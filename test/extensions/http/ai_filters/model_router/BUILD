load(
    "//bazel:envoy_build_system.bzl",
    "envoy_package",
)
load(
    "//test/extensions:extensions_build_system.bzl",
    "envoy_extension_cc_test",
)

licenses(["notice"])  # Apache 2

envoy_package()

envoy_extension_cc_test(
    name = "local_quota_test",
    srcs = ["local_quota_test.cc"],
    extension_names = ["envoy.http.ai_filters.model_router"],
    rbe_pool = "linux_x64_small",
    deps = [
        "//source/common/coroutine:launch_lib",
        "//source/common/coroutine:task_lib",
        "//source/extensions/http/ai_filters/model_router:local_quota_lib",
        "//test/common/coroutine:manual_executor_lib",
        "//test/test_common:simulated_time_system_lib",
        "//test/test_common:status_utility_lib",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@envoy_api//envoy/extensions/http/ai_filters/model_router/v3:pkg_cc_proto",
    ],
)

envoy_extension_cc_test(
    name = "config_test",
    srcs = ["config_test.cc"],
    extension_names = ["envoy.http.ai_filters.model_router"],
    rbe_pool = "linux_x64_small",
    deps = [
        "//envoy/registry",
        "//source/extensions/filters/http/ai_protocol_manager:ai_filter_lib",
        "//source/extensions/http/ai_filters/model_router:config",
        "//source/extensions/http/ai_filters/model_router:filter_lib",
        "//test/mocks/server:server_factory_context_mocks",
        "//test/mocks/stats:stats_mocks",
        "//test/mocks/stream_info:stream_info_mocks",
        "//test/test_common:utility_lib",
        "@envoy_api//envoy/extensions/http/ai_filters/model_router/v3:pkg_cc_proto",
        "@envoy_api//envoy/type/ai/v3:pkg_cc_proto",
    ],
)

envoy_extension_cc_test(
    name = "filter_test",
    srcs = ["filter_test.cc"],
    extension_names = [
        "envoy.http.ai_filters.model_router",
        "envoy.http.ai_filters.request_info",
    ],
    rbe_pool = "linux_x64_small",
    deps = [
        "//source/common/buffer:buffer_lib",
        "//source/common/coroutine:task_lib",
        "//source/common/protobuf:utility_lib",
        "//source/common/router:string_accessor_lib",
        "//source/common/stream_info:stream_info_lib",
        "//source/extensions/filters/http/ai_protocol_manager:ai_filter_lib",
        "//source/extensions/filters/http/ai_protocol_manager:ai_filter_state_lib",
        "//source/extensions/filters/http/ai_protocol_manager:buffer_manager_lib",
        "//source/extensions/filters/http/ai_protocol_manager:external_buffer_impl_lib",
        "//source/extensions/filters/http/ai_protocol_manager:filter_manager_lib",
        "//source/extensions/filters/http/ai_protocol_manager:json_with_ext_buf_lib",
        "//source/extensions/http/ai_filters/model_router:filter_lib",
        "//source/extensions/http/ai_filters/model_router:local_quota_lib",
        "//source/extensions/http/ai_filters/model_router:quota_interface_lib",
        "//source/extensions/http/ai_filters/model_router:route_wrapper_lib",
        "//source/extensions/http/ai_filters/request_info:filter_lib",
        "//test/extensions/filters/http/ai_protocol_manager:fake_bridge_lib",
        "//test/mocks/stats:stats_mocks",
        "//test/mocks/thread_local:thread_local_mocks",
        "//test/test_common:utility_lib",
        "@abseil-cpp//absl/status",
        "@abseil-cpp//absl/status:statusor",
        "@envoy_api//envoy/data/ai/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/http/ai_filters/model_router/v3:pkg_cc_proto",
        "@envoy_api//envoy/extensions/http/ai_filters/request_info/v3:pkg_cc_proto",
        "@envoy_api//envoy/type/ai/v3:pkg_cc_proto",
        "@nlohmann_json//:json",
    ],
)
