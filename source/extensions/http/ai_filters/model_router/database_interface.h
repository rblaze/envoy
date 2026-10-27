#pragma once

#include <cstdint>
#include <string>

#include "envoy/common/pure.h"
#include "envoy/common/time.h"
#include "envoy/thread_local/thread_local_object.h"

#include "source/common/coroutine/task.h"

#include "absl/status/status.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

class DatabaseInterface : public ThreadLocal::ThreadLocalObject {
public:
  ~DatabaseInterface() override = default;

  virtual Coroutine::Task<absl::Status>
  recordQueryStats(Envoy::SystemTime request_time, uint64_t input_tokens, uint64_t output_tokens,
                   std::string model, std::string provider, std::string request_id) PURE;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
