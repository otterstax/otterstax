// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "driver.hpp"

#include "errors.hpp"

#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <poll.h>

#include <cerrno>
#include <charconv>
#include <utility>
#include <variant>

namespace pg {

    namespace {

        using clock = std::chrono::steady_clock;

        // Waits for `events` on the connection's socket until `deadline`. false: the deadline
        // passed. A socket error counts as ready: the next libpq call reports it.
        bool wait_socket(PGconn* conn, short events, clock::time_point deadline) {
            for (;;) {
                const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - clock::now()).count();
                if (left <= 0) {
                    return false;
                }
                pollfd fd{PQsocket(conn), events, 0};
                const int ready = ::poll(&fd, 1, static_cast<int>(left));
                if (ready > 0) {
                    return true;
                }
                if (ready == 0) {
                    return false;
                }
                if (errno != EINTR) {
                    return true;
                }
            }
        }

        uint64_t command_tag_count(PGresult* result) {
            const char* tuples = PQcmdTuples(result);
            uint64_t value = 0;
            if (tuples != nullptr && *tuples != '\0') {
                const std::string_view text{tuples};
                std::from_chars(text.data(), text.data() + text.size(), value);
            }
            return value;
        }

    } // namespace

    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, PGresult* result, const conn::statement_t& statement) {
        OTX_ZONE_N("pg::decode");
        static const tsl::pg_enum_oid_map no_enums;
        const auto* hint = std::get_if<conn::pg_enums_ptr>(&statement.hints);
        const tsl::pg_enum_oid_map& enums = (hint != nullptr && *hint) ? **hint : no_enums;

        conn::statement_result_t out;
        switch (statement.kind) {
            case conn::result_kind::affected_rows:
                out.affected_rows = command_tag_count(result);
                break;
            case conn::result_kind::schema:
                if (PQnfields(result) > 0) { // a command without columns leaves `schema` empty
                    out.schema.emplace(tsl::pg_to_struct(resource, result, enums));
                }
                break;
            case conn::result_kind::rows: {
                // A DML's command tag becomes the affected-row carrier right here.
                auto chunk = tsl::pg_to_chunk(resource, result, enums);
                if (chunk.has_error()) {
                    return chunk.convert_error<conn::statement_result_t>();
                }
                out.rows.emplace(std::move(chunk.value()));
                break;
            }
        }
        return std::move(out);
    }

    driver_t::driver_t(std::pmr::memory_resource* resource, connect_params params, std::string_view alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        , params_(std::move(params))
        , alias_(alias, resource) {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
    }

    driver_t::~driver_t() { close(); }

    core::error_t driver_t::connection_error(std::pmr::string what) const {
        return core::error_t(core::error_code_t::io_error, std::move(what));
    }

    core::error_t driver_t::query_error(const PGresult* result, std::string_view sql) const {
        const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        const char* message = PQresultErrorMessage(result);
        const std::string_view state = sqlstate ? sqlstate : "";
        auto what = otterstax::concat(resource_, {"[Run query] Alias: ", alias_, " query [", sql, "] failed"});
        if (!state.empty()) {
            what += " (SQLSTATE ";
            what += state;
            what += ")";
        }
        what += ": ";
        what += message ? message : "no server message";
        log_->error(std::string_view(what));
        return core::error_t(classify_sqlstate(state), std::move(what));
    }

    driver_t::failure_t driver_t::transport_failure(std::string_view sql) const {
        auto what = otterstax::concat(resource_,
                                      {"[Run query] Alias: ",
                                       alias_,
                                       " query [",
                                       sql,
                                       "] failed: ",
                                       conn_ ? PQerrorMessage(conn_.get()) : "not connected"});
        log_->error(std::string_view(what));
        return {connection_error(std::move(what)), failure_kind::stale};
    }

    driver_t::failure_t driver_t::timeout_failure(std::string_view sql) const {
        auto what = otterstax::concat(
            resource_,
            {"[Run query] Alias: ", alias_, " query [", sql, "] failed: the statement did not finish within its deadline"});
        log_->error(std::string_view(what));
        return {connection_error(std::move(what)), failure_kind::broken};
    }

    core::error_t driver_t::connect() {
        OTX_ZONE_N("pg::driver::connect");
        log_->debug("Alias: {} connecting to {}:{} dbname={}", alias_, params_.host, params_.port, params_.database);
        conn_.reset(PQconnectdb(params_.connection_string(resource_).c_str()));
        if (PQstatus(conn_.get()) != CONNECTION_OK) {
            auto what =
                otterstax::concat(resource_, {"[Connector] Alias: ", alias_, " connect failed ", PQerrorMessage(conn_.get())});
            log_->warn(std::string_view(what));
            conn_.reset();
            return connection_error(std::move(what));
        }
        // Non-blocking, so a send cannot block either: send() flushes against the deadline.
        if (PQsetnonblocking(conn_.get(), 1) != 0) {
            auto what = otterstax::concat(
                resource_,
                {"[Connector] Alias: ", alias_, " cannot switch to non-blocking mode: ", PQerrorMessage(conn_.get())});
            log_->warn(std::string_view(what));
            conn_.reset();
            return connection_error(std::move(what));
        }
        return core::error_t::no_error();
    }

    void driver_t::drop_connection(failure_kind kind) {
        if (kind == failure_kind::broken && conn_) {
            cancel_on_server();
        }
        conn_.reset();
    }

    // A statement past its deadline keeps running on the server after the socket closes, so it is
    // cancelled first. PQcancel sends a CancelRequest on a socket of its own, which a pooler such as
    // PgBouncer routes correctly (pg_cancel_backend would name the pooler's fake pid). libpq 15 connects
    // it without a timeout; acceptable, as the server just answered. Best-effort: a failure is logged.
    void driver_t::cancel_on_server() {
        OTX_ZONE_N("pg::driver::cancel_on_server");
        PGcancel* cancel = PQgetCancel(conn_.get());
        if (cancel == nullptr) {
            log_->warn("Alias: {} cannot cancel the timed-out statement: no cancel key", alias_);
            return;
        }
        char reason[256] = {};
        if (PQcancel(cancel, reason, sizeof(reason)) == 0) {
            log_->warn("Alias: {} cannot cancel the timed-out statement: {}", alias_, reason);
        } else {
            log_->debug("Alias: {} cancelled the statement past its deadline", alias_);
        }
        PQfreeCancel(cancel);
    }

    // The exchange PQexec does, through libpq's asynchronous API so it can be bounded: send,
    // flush, then read until the statement's result is complete, waiting on the socket with
    // poll() against one deadline.
    driver_t::sent_t driver_t::send(std::string_view sql, std::chrono::milliseconds limit) {
        OTX_ZONE_N("pg::driver::send");
        const auto deadline = clock::now() + limit;
        PGconn* conn = conn_.get();
        sent_t sent;
        assert(sql.data()[sql.size()] == '\0');
        if (PQsendQuery(conn, sql.data()) == 0) {
            sent.failure = transport_failure(sql);
            return sent;
        }
        for (int pending = PQflush(conn); pending != 0; pending = PQflush(conn)) {
            if (pending < 0) {
                sent.failure = transport_failure(sql);
                return sent;
            }
            if (!wait_socket(conn, POLLOUT | POLLIN, deadline)) {
                sent.failure = timeout_failure(sql);
                return sent;
            }
            if (PQconsumeInput(conn) == 0) {
                sent.failure = transport_failure(sql);
                return sent;
            }
        }
        for (;;) {
            while (PQisBusy(conn) != 0) {
                if (!wait_socket(conn, POLLIN, deadline)) {
                    sent.failure = timeout_failure(sql);
                    return sent;
                }
                if (PQconsumeInput(conn) == 0) {
                    sent.failure = transport_failure(sql);
                    return sent;
                }
            }
            PGresult* next = PQgetResult(conn);
            if (next == nullptr) {
                break; // the statement is complete
            }
            // One statement answers one result; should a second follow an error, the error
            // is the one kept.
            if (sent.result && PQresultStatus(sent.result.get()) == PGRES_FATAL_ERROR) {
                PQclear(next);
            } else {
                sent.result.reset(next);
            }
        }
        if (!sent.result) {
            sent.failure = transport_failure(sql);
            return sent;
        }
        switch (PQresultStatus(sent.result.get())) {
            case PGRES_TUPLES_OK:
            case PGRES_COMMAND_OK:
            case PGRES_EMPTY_QUERY: // ping()
                return sent;
            default:
                // The server's verdict, unless libpq marked the connection bad meanwhile.
                if (PQstatus(conn) == CONNECTION_BAD) {
                    sent.failure = transport_failure(sql);
                } else {
                    sent.failure = failure_t{query_error(sent.result.get(), sql), failure_kind::server};
                }
                return sent;
        }
    }

    core::error_t driver_t::ping() {
        OTX_ZONE_N("pg::driver::ping");
        if (!conn_) {
            return connection_error("[Connector] Alias: " + alias_ + " is not connected");
        }
        auto sent = send("", params_.connect_timeout);
        if (sent.failure) {
            if (sent.failure->kind != failure_kind::server) {
                conn_.reset(); // the next execute() reconnects first
            }
            return std::move(sent.failure->error);
        }
        return core::error_t::no_error();
    }

    void driver_t::close() noexcept { conn_.reset(); }

    std::unique_ptr<conn::IDriver> driver_t::clone_disconnected() const {
        return std::make_unique<driver_t>(resource_, params_, alias_);
    }

    conn::statement_outcome_t driver_t::execute(const conn::statement_t& statement) {
        OTX_ZONE_N("pg::driver::execute");
        // An earlier failure closed the connection: reconnect first. This replaces the
        // SELECT 1 the old connector sent in front of every query.
        if (!conn_) {
            if (auto err = connect(); err.contains_error()) {
                return std::move(err);
            }
        }
        log_->debug("Alias: {} query: {}", alias_, statement.sql);
        const auto limit = statement.timeout.count() > 0 ? statement.timeout : params_.statement_timeout;
        auto sent = send(statement.sql, limit);
        if (sent.failure && sent.failure->kind != failure_kind::server) {
            drop_connection(sent.failure->kind);
        }
        // A stale connection (server restarted, an idle connection closed): reconnect and
        // repeat a read once. Not after the deadline, and never a DML or DDL
        // (statement.repeatable).
        if (sent.failure && sent.failure->kind == failure_kind::stale && statement.repeatable &&
            !connect().contains_error()) {
            sent = send(statement.sql, limit);
            if (sent.failure && sent.failure->kind != failure_kind::server) {
                drop_connection(sent.failure->kind);
            }
        }
        if (sent.failure) {
            return std::move(sent.failure->error);
        }
        return decode(resource_, sent.result.get(), statement);
    }

    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string_view alias) {
        return std::make_unique<driver_t>(resource, std::move(params), alias);
    }

} // namespace pg
