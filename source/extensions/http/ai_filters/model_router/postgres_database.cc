#include "source/extensions/http/ai_filters/model_router/postgres_database.h"

#include <endian.h>

#include "source/common/coroutine/status_macros.h"
#include "source/extensions/http/ai_filters/model_router/async_pq.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "libpq/libpq-fe.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

PostgresDatabase::PostgresDatabase(Event::Dispatcher& dispatcher, const std::string& conn_info,
                                   const size_t max_free_connections)
    : max_free_connections_(max_free_connections), dispatcher_(dispatcher), conn_info_(conn_info) {
  pq_free_list_.reserve(max_free_connections);
}

Coroutine::Task<absl::Status>
PostgresDatabase::recordQueryStats(Envoy::SystemTime request_time, uint64_t input_tokens,
                                   uint64_t output_tokens, std::string model, std::string provider,
                                   std::string request_id) {
  ASSIGN_OR_CO_RETURN(PqPtr pq, co_await getPq());

  uint64_t timestamp_bin = htobe64(
      std::chrono::duration_cast<std::chrono::seconds>(request_time.time_since_epoch()).count());
  uint64_t input_tokens_bin = htobe64(input_tokens);
  uint64_t output_tokens_bin = htobe64(output_tokens);

  CO_RETURN_IF_ERROR(co_await pq->execParams(
      "INSERT INTO queries (time, input_tokens, output_tokens, model, provider, request_id) "
      "VALUES (to_timestamp($1::bigint), $2::bigint, $3::bigint, $4::text, $5::text, $6::text);",
      {reinterpret_cast<const char*>(&timestamp_bin),
       reinterpret_cast<const char*>(&input_tokens_bin),
       reinterpret_cast<const char*>(&output_tokens_bin), model.c_str(), provider.c_str(),
       request_id.c_str()},
      {}, {sizeof(timestamp_bin), sizeof(input_tokens_bin), sizeof(output_tokens_bin), 0, 0, 0},
      {1, 1, 1, 0, 0, 0}, 0));

  CO_RETURN_IF_ERROR(co_await finishCommand(pq));

  // Only reuse connections if the command was successful.
  returnPq(std::move(pq));

  co_return absl::OkStatus();
}

Coroutine::Task<absl::Status> PostgresDatabase::finishCommand(PqPtr& pq) {
  PgResultPtr result;

  do {
    ASSIGN_OR_CO_RETURN(result, co_await pq->getResult());

    if (result) {
      ExecStatusType exec_status = PQresultStatus(result.get());
      if (exec_status != PGRES_COMMAND_OK) {
        co_return absl::InternalError(PQresultErrorMessage(result.get()));
      }
    }
  } while (result != nullptr);

  co_return absl::OkStatus();
}

Coroutine::Task<absl::StatusOr<PostgresDatabase::PqPtr>> PostgresDatabase::getPq() {
  while (!pq_free_list_.empty()) {
    PqPtr pq = std::move(pq_free_list_.back());
    pq_free_list_.pop_back();

    if (pq->checkConnection().ok()) {
      co_return pq;
    }

    // Connection is invalid, try another one.
  }

  ASSIGN_OR_CO_RETURN(PqPtr pq, AsyncPq::open(dispatcher_, conn_info_));
  CO_RETURN_IF_ERROR(co_await pq->connect());

  co_return pq;
}

void PostgresDatabase::returnPq(PqPtr pq) {
  if (pq_free_list_.size() < max_free_connections_) {
    pq_free_list_.push_back(std::move(pq));
  }
}

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
