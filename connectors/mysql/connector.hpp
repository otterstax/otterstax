// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include <boost/asio.hpp>
#include <boost/mysql.hpp>
#include <boost/mysql/any_address.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/results.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/asio_error.hpp"
#include "utility/tracy_profiler.hpp"
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <string>

namespace mysql {

    namespace bm = boost::mysql;
    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    enum class Status
    {
        Created,
        Connected,
        Disconnected,
        Working,
        Closed
    };

    class IConnector {
    public:
        virtual ~IConnector() = default;
        virtual Status status() const noexcept = 0;
        virtual bm::connect_params params() const noexcept = 0;
        virtual void close() = 0;
        virtual void connect() = 0;
        virtual bool isConnected() = 0;
        virtual void tryReconnect() = 0;
        virtual bool isClosed() const noexcept = 0;
        virtual std::string alias() const noexcept = 0;

        virtual asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler) = 0;
        virtual asio::awaitable<int64_t> runQuery(std::string_view query,
                                                  std::function<int64_t(const boost::mysql::results&)> handler) = 0;
        virtual asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(const boost::mysql::results&)> handler) = 0;
    };

    class Connector : public IConnector {
    public:
        Connector(asio::io_context& io_ctx, bm::connect_params params, std::string alias = "");
        Status status() const noexcept override;
        bm::connect_params params() const noexcept override;
        void close() override;
        ~Connector() override;
        void connect() override;
        bool isConnected() override;
        void tryReconnect() override;
        bool isClosed() const noexcept override;
        std::string alias() const noexcept override;

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<int64_t> runQuery(std::string_view query,
                                          std::function<int64_t(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        // Connect with a hard per-attempt deadline. The plain sync
        // connect(params, ec, diag) performs blocking socket I/O on the calling
        // thread with NO timeout: pointed at a non-MySQL server (e.g.
        // PostgreSQL on 5432) both sides wait for the other's first packet and
        // the call blocks until the remote auth timeout closes the socket —
        // freezing the single HTTP API thread for minutes. The async op runs on
        // the connector's io_context (pool started before any addConnection)
        // and asio::cancel_after bounds it; use_future always becomes ready
        // (success, error_code, or operation_aborted on timeout).
        boost::system::error_code connectWithTimeout(boost::mysql::diagnostics& diag);

        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            asio::awaitable<std::invoke_result_t<Callable, const boost::mysql::results&>>
            runQuery_(std::string_view query, Callable handler) {
            // OTX_ZONE_N omitted: this coroutine crosses io_context threads via
            // co_await — opening a Tracy zone here causes "Zone is ended twice".
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                throw std::runtime_error(err);
            }
            boost::system::error_code ec;
            co_await conn_.async_ping(asio::redirect_error(asio::use_awaitable, ec));

            if (ec) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " ping failed: " + ec.message();
                log_->error(err);
                throw std::runtime_error(err);
            }

            // TODO add timeout or table check asio::cancel_after(std::chrono::seconds(5)) use boost 1.87
            // TODO add atomic working status to block removing while get results from
            // DB
            // Issue the SQL query to the server
            log_->debug("Alias: {} query: {}", alias_, query);
            bm::results result;
            co_await conn_.async_execute(query, result, asio::redirect_error(asio::use_awaitable, ec));

            if (ec) {
                log_->error("Alias: {} query [{}] failed: {}", alias_, std::string(query), ec.message());
                throw std::runtime_error("[Run query] Alias: " + alias_ + " query [" + std::string(query) +
                                         "]\nfailed: " + ec.message());
            }

            co_return handler(result);
        }

    private:
        bm::any_connection conn_;
        bm::connect_params params_;
        Status status_;
        std::mutex mutex_;
        std::string alias_;
    };

    using connector_factory =
        std::function<std::unique_ptr<IConnector>(asio::io_context&, bm::connect_params, std::string)>;

} // namespace mysql
