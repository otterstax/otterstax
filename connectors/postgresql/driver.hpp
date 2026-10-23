// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "types.hpp"

#include "connectors/common/driver.hpp"

#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>

#include <libpq-fe.h>

#include <chrono>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

namespace pg {

    struct PGResultDeleter {
        void operator()(PGresult* res) const noexcept {
            if (res) {
                PQclear(res);
            }
        }
    };
    using PGResultPtr = std::unique_ptr<PGresult, PGResultDeleter>;

    struct PGConnDeleter {
        void operator()(PGconn* conn) const noexcept {
            if (conn) {
                PQfinish(conn);
            }
        }
    };
    using PGConnPtr = std::unique_ptr<PGconn, PGConnDeleter>;

    // A statement's raw answer decoded as the statement asks, under the connection's ENUM
    // map: rows through tsl::pg_to_chunk (a DML's command tag becomes the affected-row
    // carrier there), the schema through tsl::pg_to_struct — none for a result without
    // columns — and the count from the command tag. A free function so a test driver
    // answering canned PGresults decodes them exactly as the driver does.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, PGresult* result, const conn::statement_t& statement);

    // One PostgreSQL connection over libpq (15), sync from the outside.
    //
    // Blocking PQexec has no deadline: a backend that stops answering mid-statement held
    // the thread for good. The driver runs the same exchange through libpq's asynchronous
    // API on a non-blocking connection — PQsendQuery, PQflush, PQconsumeInput / PQisBusy,
    // PQgetResult — and waits on the socket with poll() against one deadline per statement.
    // On expiry the connection is closed (PQfinish), the statement is not repeated, and the
    // server is told to cancel it (PQcancel, the protocol's CancelRequest): closing
    // the socket alone leaves the statement running — a DML that may still commit, and a
    // server connection outside the pool's max_size — until the server next writes to it.
    class driver_t final : public conn::IDriver {
    public:
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
        // An empty query: one round trip, nothing parsed.
        core::error_t ping() override;
        void close() noexcept override;
        std::unique_ptr<conn::IDriver> clone_disconnected() const override;

        const connect_params& params() const noexcept { return params_; }
        const std::string& alias() const noexcept { return alias_; }

    private:
        using clock = std::chrono::steady_clock;

        // What went wrong decides what happens to the connection (conn::IDriver).
        enum class failure_kind
        {
            server, // the server's verdict (SQLSTATE): connection kept, never repeated
            stale,  // libpq marked the connection bad: reset; a read is repeated once
            broken, // our deadline fired: reset, not repeated
        };
        struct failure_t {
            core::error_t error;
            failure_kind kind;
        };
        struct sent_t {
            PGResultPtr result;
            std::optional<failure_t> failure;
        };

        // Closes the connection after a failure; past the deadline (broken) it first cancels
        // the statement on the server.
        void drop_connection(failure_kind kind);
        void cancel_on_server();
        sent_t send(std::string_view sql, std::chrono::milliseconds limit);
        failure_t transport_failure(std::string_view sql) const;
        failure_t timeout_failure(std::string_view sql) const;
        core::error_t query_error(const PGresult* result, std::string_view sql) const;
        core::error_t connection_error(std::string_view what) const;

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::string alias_;
        PGConnPtr conn_; // null: not connected
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string alias);

} // namespace pg
