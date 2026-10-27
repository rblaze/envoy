#pragma once

#include <memory>
#include <string>
#include <vector>

#include "envoy/common/time.h"
#include "envoy/event/dispatcher.h"

#include "source/common/coroutine/task.h"
#include "source/extensions/http/ai_filters/model_router/async_pq.h"
#include "source/extensions/http/ai_filters/model_router/database_interface.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

class PostgresDatabase : public DatabaseInterface {
public:
  PostgresDatabase(Event::Dispatcher& dispatcher, const std::string& conn_info,
                   const size_t max_free_connections = 10);

  Coroutine::Task<absl::Status> recordQueryStats(Envoy::SystemTime request_time,
                                                 uint64_t input_tokens, uint64_t output_tokens,
                                                 std::string model, std::string provider,
                                                 std::string request_id) override;

private:
  using PqPtr = std::unique_ptr<AsyncPq>;

  Coroutine::Task<absl::StatusOr<PqPtr>> getPq();
  void returnPq(PqPtr pq);

  Coroutine::Task<absl::Status> finishCommand(PqPtr& pq);

  const size_t max_free_connections_;
  Event::Dispatcher& dispatcher_;
  std::string conn_info_;
  std::vector<PqPtr> pq_free_list_;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
