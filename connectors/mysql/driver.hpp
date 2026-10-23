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

    // A statement's raw answer decoded as the statement asks: rows through
    // tsl::mysql_to_chunk (a DML's OK packet becomes the affected-row carrier there), the
    // schema through tsl::mysql_to_struct of the column definitions — none for a bare OK
    // packet — and the count from affected_rows(). A free function so a test driver
    // answering canned results decodes them exactly as the driver does.
    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const bm::results& rows, const conn::statement_t& statement);

    // One MySQL / MariaDB connection over Boost.MySQL, sync from the outside.
    //
    // Boost.MySQL's own sync functions take no deadline, and Asio's blocking reads ignore a
    // socket timeout: on EAGAIN sync_recv1 polls again without a limit. So a backend that
    // stops answering would hold the connection's thread for good. The driver therefore runs
    // Boost.MySQL's async operations, each under asio::cancel_after with the time left of
    // the call's one deadline, inside a coroutine on a private io_context that only the
    // calling (pool) thread runs. With one thread, the deadline timer's handler and the
    // operation's run one after the other, so the timed_cancel_op race the old connector's
    // strand guarded against (two pool threads) cannot happen.
    //
    // Every statement goes over the binary protocol, never COM_QUERY: the text protocol
    // renders a FLOAT column with FLT_DIG (six) significant digits, so rows read over it are
    // not the values the backend holds. The statement is prepared, then executed and closed
    // in one pipeline round trip; the close stage runs even when execute fails, so no
    // server-side handle outlives the statement.
    //
    // A statement past its deadline is killed on the server (KILL QUERY over a connection of
    // its own, cancel_on_server): closing the socket alone leaves it running — a DML that may
    // still commit, and a server connection outside the pool's max_size. The session id it
    // names is read once per connect (SELECT CONNECTION_ID()). Behind a proxy that
    // multiplexes client sessions onto backend connections and forwards both statements
    // as-is, that id is a backend connection's, not this session's.
    class driver_t final : public conn::IDriver {
    public:
        driver_t(std::pmr::memory_resource* resource, connect_params params, std::string alias);
        ~driver_t() override;

        core::error_t connect() override;
        conn::statement_outcome_t execute(const conn::statement_t& statement) override;
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
        core::error_t connection_error(std::string_view what) const;
        // A fresh any_connection on io_: one whose operation was cancelled is unusable, and
        // the old socket closes without COM_QUIT. Safe only while nothing is pending on io_,
        // which holds between calls.
        void reset_connection();

        log_t log_;
        std::pmr::memory_resource* resource_;
        connect_params params_;
        std::string alias_;
        asio::io_context io_;     // private; run only by the thread that calls this driver
        bm::any_connection conn_; // on io_
        bool connected_{false};
        std::optional<uint64_t> session_id_; // CONNECTION_ID() of conn_; empty: unknown
    };

    // A disconnected driver: the prototype connection_pool_t::add clones every connection from.
    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string alias);

} // namespace mysql
