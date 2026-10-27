#include "source/extensions/http/ai_filters/model_router/async_pq.h"

#include "envoy/event/dispatcher.h"
#include "envoy/event/file_event.h"

#include "source/common/coroutine/leaf_awaitable.h"
#include "source/common/coroutine/status_macros.h"

#include "libpq/libpq-fe.h"

namespace Envoy {
namespace Extensions {
namespace AiFilters {
namespace ModelRouter {

namespace {

class PqSocketAwaiter : public Coroutine::LeafAwaitable<absl::Status> {
public:
  PqSocketAwaiter(Event::Dispatcher& dispatcher, PGconn& connection, uint32_t events)
      : dispatcher_(dispatcher), sock_(PQsocket(&connection)), events_(events) {}
  ~PqSocketAwaiter() override = default;

protected:
  void onStart() override {
    auto cb = [this](uint32_t) {
      complete(absl::OkStatus());

      return absl::OkStatus();
    };
    file_event_ = dispatcher_.createFileEvent(sock_, cb, Event::FileTriggerType::Level, events_);
  }
  void onCancel() override { file_event_ = nullptr; }

  std::optional<absl::Status> tryImmediate() override {
    if (sock_ < 0) {
      return absl::UnavailableError("Invalid PostgreSQL socket");
    }
    return std::nullopt;
  }

private:
  Event::Dispatcher& dispatcher_;
  int sock_;
  uint32_t events_;
  Event::FileEventPtr file_event_;
};

} // namespace

AsyncPq::~AsyncPq() { PQfinish(&conn_); }

absl::StatusOr<std::unique_ptr<AsyncPq>> AsyncPq::open(Event::Dispatcher& dispatcher,
                                                       const std::string& conn_info) {
  PGconn* conn = PQconnectStart(conn_info.c_str());
  if (conn == nullptr) {
    return absl::InternalError("PQconnectStart failed");
  }

  return absl::WrapUnique(new AsyncPq(dispatcher, conn));
}

Coroutine::Task<absl::Status> AsyncPq::connect() {
  for (;;) {
    PostgresPollingStatusType poll_status = PQconnectPoll(&conn_);

    if (poll_status == PGRES_POLLING_OK) {
      break;
    }

    if (poll_status == PGRES_POLLING_FAILED) {
      co_return absl::UnavailableError(PQerrorMessage(&conn_));
    }

    uint32_t events = poll_status == PGRES_POLLING_READING ? Event::FileReadyType::Read
                                                           : Event::FileReadyType::Write;
    CO_RETURN_IF_ERROR(co_await PqSocketAwaiter(dispatcher_, conn_, events));
  }

  if (PQsetnonblocking(&conn_, 1) != 0) {
    co_return absl::InternalError(PQerrorMessage(&conn_));
  }

  co_return absl::OkStatus();
}

absl::Status AsyncPq::checkConnection() {
  if (PQconsumeInput(&conn_) == 0 || PQstatus(&conn_) != CONNECTION_OK) {
    return absl::UnavailableError(PQerrorMessage(&conn_));
  }

  return absl::OkStatus();
}

Coroutine::Task<absl::Status> AsyncPq::exec(const std::string& query) {
  if (PQsendQuery(&conn_, query.c_str()) == 0) {
    co_return absl::InternalError(PQerrorMessage(&conn_));
  }

  co_return co_await finishQuery();
}

Coroutine::Task<absl::Status>
AsyncPq::execParams(const std::string& command, absl::Span<const char* const> param_values,
                    absl::Span<const Oid> param_types, absl::Span<const int> param_lengths,
                    absl::Span<const int> param_formats, int result_format) {
  if (!param_types.empty() && param_types.size() != param_values.size()) {
    co_return absl::InvalidArgumentError(
        "param_types size must be 0 or equal to param_values size");
  }
  if (!param_lengths.empty() && param_lengths.size() != param_values.size()) {
    co_return absl::InvalidArgumentError(
        "param_lengths size must be 0 or equal to param_values size");
  }
  if (!param_formats.empty() && param_formats.size() != param_values.size()) {
    co_return absl::InvalidArgumentError(
        "param_formats size must be 0 or equal to param_values size");
  }
  if (PQsendQueryParams(&conn_, command.c_str(), static_cast<int>(param_values.size()),
                        param_types.empty() ? nullptr : param_types.data(),
                        param_values.empty() ? nullptr : param_values.data(),
                        param_lengths.empty() ? nullptr : param_lengths.data(),
                        param_formats.empty() ? nullptr : param_formats.data(),
                        result_format) == 0) {
    co_return absl::InternalError(PQerrorMessage(&conn_));
  }

  co_return co_await finishQuery();
}

Coroutine::Task<absl::Status> AsyncPq::finishQuery() {
  for (;;) {
    int flush_status = PQflush(&conn_);
    if (flush_status == 0) {
      break;
    }
    if (flush_status == -1) {
      co_return absl::UnavailableError(PQerrorMessage(&conn_));
    }
    CO_RETURN_IF_ERROR(co_await PqSocketAwaiter(
        dispatcher_, conn_, Event::FileReadyType::Read | Event::FileReadyType::Write));
    if (PQconsumeInput(&conn_) == 0) {
      co_return absl::UnavailableError(PQerrorMessage(&conn_));
    }
  }

  co_return absl::OkStatus();
}

Coroutine::Task<absl::StatusOr<PgResultPtr>> AsyncPq::getResult() {
  while (PQisBusy(&conn_) == 1) {
    CO_RETURN_IF_ERROR(co_await PqSocketAwaiter(dispatcher_, conn_, Event::FileReadyType::Read));
    if (PQconsumeInput(&conn_) == 0) {
      co_return absl::UnavailableError(PQerrorMessage(&conn_));
    }
  }

  PgResultPtr result(PQgetResult(&conn_));

  co_return result;
}

} // namespace ModelRouter
} // namespace AiFilters
} // namespace Extensions
} // namespace Envoy
