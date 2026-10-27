#pragma once

#include <memory>
#include <string>

#include "envoy/event/dispatcher.h"

#include "source/common/coroutine/task.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "libpq/libpq-fe.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

struct PqResultDeleter {
  void operator()(PGresult* res) const { PQclear(res); }
};
using PgResultPtr = std::unique_ptr<PGresult, PqResultDeleter>;

class AsyncPq {
public:
  static absl::StatusOr<std::unique_ptr<AsyncPq>> open(Event::Dispatcher& dispatcher,
                                                       const std::string& conn_info);
  ~AsyncPq();

  absl::Status checkConnection();
  Coroutine::Task<absl::Status> connect();

  // Returns next PGresult or nullptr if there is no active command.
  Coroutine::Task<absl::StatusOr<PgResultPtr>> getResult();

  Coroutine::Task<absl::Status> exec(const std::string& query);

  Coroutine::Task<absl::Status>
  execParams(const std::string& command, absl::Span<const char* const> param_values,
             absl::Span<const Oid> param_types = {}, absl::Span<const int> param_lengths = {},
             absl::Span<const int> param_formats = {}, int result_format = 0);

private:
  AsyncPq(Event::Dispatcher& dispatcher, PGconn* connection)
      : dispatcher_(dispatcher), conn_(*connection) {}

  Coroutine::Task<absl::Status> finishQuery();

  Event::Dispatcher& dispatcher_;
  PGconn& conn_;
};

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
