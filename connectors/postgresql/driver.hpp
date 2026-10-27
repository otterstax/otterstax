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

    // A statement's raw answer decoded as the statement asks, under the connection's ENUM map: rows
    // (pg_to_chunk), schema (pg_to_struct) and the count from the command tag. A free function so a
    // test driver decodes canned PGresults exactly as the driver does.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, PGresult* result, const conn::statement_t& statement);

    // One PostgreSQL connection over libpq (15), sync from the outside. It runs the statement through
    // libpq's asynchronous API and poll() against one deadline, since blocking PQexec has none. On expiry
    // it closes the connection (PQfinish), does not repeat the statement and sends PQcancel: closing the
    // socket alone would leave the statement running outside the pool's max_size.
    class driver_t final : public conn::IDriver {
    public:
        // `params` is the startup config descriptor (std::string); the alias is copied onto
        // `resource`.
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
        // An empty query: one round trip, nothing parsed.
        core::error_t ping() override;
        void close() noexcept override;
        std::unique_ptr<conn::IDriver> clone_disconnected() const override;

        const connect_params& params() const noexcept { return params_; }
        std::string_view alias() const noexcept { return alias_; }

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
        // `sql` is NUL-terminated (a std::pmr::string or a literal): libpq takes a C string.
        sent_t send(std::string_view sql, std::chrono::milliseconds limit);
        failure_t transport_failure(std::string_view sql) const;
        failure_t timeout_failure(std::string_view sql) const;
        core::error_t query_error(const PGresult* result, std::string_view sql) const;
        core::error_t connection_error(std::pmr::string what) const;

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::pmr::string alias_;
        PGConnPtr conn_; // null: not connected
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);

} // namespace pg
