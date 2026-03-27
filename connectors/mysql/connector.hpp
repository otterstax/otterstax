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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include <components/catalog/catalog_error.hpp>
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <string>

namespace mysqlc {

    namespace mysql = boost::mysql;
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
        virtual mysql::connect_params params() const noexcept = 0;
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
        virtual asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const boost::mysql::results&)> handler) = 0;
    };

    class Connector : public IConnector {
    public:
        Connector(asio::io_context& io_ctx, mysql::connect_params params, std::string alias = "");
        Status status() const noexcept override;
        mysql::connect_params params() const noexcept override;
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
        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            asio::awaitable<std::invoke_result_t<Callable, const boost::mysql::results&>>
            runQuery_(std::string_view query, Callable handler) {
            // log_->trace("Thread id: {}", std::this_thread::get_id()); // Removed: fmt doesn't format thread::id
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
            mysql::results result;
            co_await conn_.async_execute(query, result, asio::redirect_error(asio::use_awaitable, ec));

            if (ec) {
                log_->error("Alias: {} query [{}] failed: {}", alias_, std::string(query), ec.message());
                throw std::runtime_error("[Run query] Alias: " + alias_ + " query [" + std::string(query) +
                                         "]\nfailed: " + ec.message());
            }

            co_return handler(result);
        }

    private:
        mysql::any_connection conn_;
        mysql::connect_params params_;
        Status status_;
        std::mutex mutex_;
        std::string alias_;
    };

    using connector_factory =
        std::function<std::unique_ptr<IConnector>(asio::io_context&, mysql::connect_params, std::string)>;

} // namespace mysqlc
