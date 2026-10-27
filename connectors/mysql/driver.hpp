// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/driver.hpp"

#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/connect_params.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/results.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>

namespace mysql {

    namespace bm = boost::mysql;
    namespace asio = boost::asio;

    struct connect_params {
        bm::connect_params server;                            // host, port, user, password, database
        std::chrono::milliseconds connect_timeout{10'000};    // also bounds ping and close
        std::chrono::milliseconds statement_timeout{90'000};  // total time one statement may take
    };

    // Decodes a statement's raw answer: rows via tsl::mysql_to_chunk, the schema via mysql_to_struct
    // (none for a bare OK packet), the count from affected_rows(). A free function so a test driver
    // with canned results decodes exactly as the driver does.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const bm::results& rows, const conn::statement_t& statement);

    // One MySQL / MariaDB connection over Boost.MySQL, sync from the outside.
    // 
    // Boost.MySQL's sync calls take no deadline and Asio's blocking reads ignore socket timeouts, so a
    // silent backend would hold the thread forever. The driver runs the async operations under
    // asio::cancel_after with the call's remaining deadline, in a coroutine on a private io_context
    // that only the calling pool thread runs, so the timer and the operation never race.
    // 
    // Statements use the binary protocol, never COM_QUERY: the text protocol rounds FLOAT to six digits.
    // Prepare, execute and close go in one pipeline; close runs even if execute fails.
    // 
    // A statement past its deadline is killed with KILL QUERY on its own connection (cancel_on_server),
    // since closing the socket leaves it running. The session id is read once per connect. Behind a
    // proxy that multiplexes sessions, that id is a backend connection's, not this session's.
    class driver_t final : public conn::IDriver {
    public:
        // `params` is the startup config descriptor (std::string: what bm::connect_params
        // takes); the alias is copied onto `resource`.
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
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
            server, // the server's verdict: connection kept, never repeated
            stale,  // a client / transport error: connection reset; a read is repeated once
            broken, // our deadline fired, or the call failed outright: reset, not repeated
        };
        struct failure_t {
            core::error_t error;
            failure_kind kind;
        };
        struct sent_t {
            bm::results rows;
            std::optional<failure_t> failure;
        };

        sent_t send(const conn::statement_t& statement);

        // The coroutines run on io_ and report into the caller's frame, which outlives them:
        // the caller returns only after io_.run() has.
        asio::awaitable<void> connect_(bm::diagnostics& diag, std::optional<core::error_t>& out);
        asio::awaitable<void> send_(std::string_view sql, clock::time_point deadline, sent_t& sent);
        asio::awaitable<void> ping_(boost::system::error_code& ec, bm::diagnostics& diag);
        asio::awaitable<void> close_(boost::system::error_code& ec, bm::diagnostics& diag);
        asio::awaitable<void> kill_(uint64_t session_id, boost::system::error_code& ec, bm::diagnostics& diag);

        void cancel_on_server(uint64_t session_id);

        failure_t classify(const boost::system::error_code& ec, const bm::diagnostics& diag, std::string_view sql) const;
        core::error_t query_error(const boost::system::error_code& ec,
                                  const bm::diagnostics& diag,
                                  std::string_view sql) const;
        core::error_t connection_error(std::pmr::string what) const;
        // A fresh any_connection on io_: one whose operation was cancelled is unusable, and
        // the old socket closes without COM_QUIT. Safe only while nothing is pending on io_,
        // which holds between calls.
        void reset_connection();

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::pmr::string alias_;
        asio::io_context io_;     // private; run only by the thread that calls this driver
        bm::any_connection conn_; // on io_
        bool connected_{false};
        std::optional<uint64_t> session_id_; // CONNECTION_ID() of conn_; empty: unknown
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string_view alias);

} // namespace mysql
