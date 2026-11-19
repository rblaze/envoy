#pragma once

#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.h"
#include "envoy/extensions/http/ai_filters/model_rate_limit/v3/model_rate_limit.pb.validate.h"

#include "source/extensions/filters/http/ai_protocol_manager/ai_filter.h"

#include "absl/status/statusor.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRateLimit {

class ModelRateLimitFilterConfigFactory
    : public HttpFilters::AiProtocolManager::AiFilterConfigFactory {
public:
  absl::StatusOr<HttpFilters::AiProtocolManager::AiFilterFactoryCb>
  createAiFilterFactory(const Protobuf::Message& config,
                        Server::Configuration::ServerFactoryContext& context,
                        Stats::Scope& scope) override;

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return std::make_unique<
        envoy::extensions::http::ai_filters::model_rate_limit::v3::ModelRateLimit>();
  }

  std::string name() const override { return "envoy.http.ai_filters.model_rate_limit"; }
};

} // namespace ModelRateLimit
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
