// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include <libpq-fe.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "types.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/asio_error.hpp"
#include "utility/tracy_profiler.hpp"
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <string>

namespace pg {

    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;

    // RAII wrapper for PGresult
    struct PGResultDeleter {
        void operator()(PGresult* res) const noexcept {
            if (res) PQclear(res);
        }
    };
    using PGResultPtr = std::unique_ptr<PGresult, PGResultDeleter>;

    // RAII wrapper for PGconn
    struct PGConnDeleter {
        void operator()(PGconn* conn) const noexcept {
            if (conn) PQfinish(conn);
        }
    };
    using PGConnPtr = std::unique_ptr<PGconn, PGConnDeleter>;

    class IConnector {
    public:
        virtual ~IConnector() = default;
        virtual Status status() const noexcept = 0;
        virtual connect_params params() const noexcept = 0;
        virtual void close() = 0;
        virtual void connect() = 0;
        virtual bool isConnected() = 0;
        virtual void tryReconnect() = 0;
        virtual bool isClosed() const noexcept = 0;
        virtual std::string alias() const noexcept = 0;

        virtual asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(PGresult*)> handler) = 0;
        virtual asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(PGresult*)> handler) = 0;
        virtual asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(PGresult*)> handler) = 0;
    };

    class Connector : public IConnector {
    public:
        Connector(connect_params params, std::string alias = "");
        Status status() const noexcept override;
        connect_params params() const noexcept override;
        void close() override;
        ~Connector() override;
        void connect() override;
        bool isConnected() override;
        void tryReconnect() override;
        bool isClosed() const noexcept override;
        std::string alias() const noexcept override;

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        template<typename Callable>
        requires std::invocable<Callable, PGresult*>
            asio::awaitable<std::invoke_result_t<Callable, PGresult*>>
            runQuery_(std::string_view query, Callable handler) {
            OTX_ZONE_N("pg::Connector::runQuery");
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                throw std::runtime_error(err);
            }

            // Check connection status
            if (PQstatus(conn_.get()) != CONNECTION_OK) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " connection check failed";
                log_->error(err);
                throw std::runtime_error(err);
            }

            log_->debug("Alias: {} query: {}", alias_, query);

            // Execute query synchronously (libpq doesn't have native coroutine support)
            PGResultPtr result(PQexec(conn_.get(), std::string(query).c_str()));

            if (!result) {
                std::string err = "[Run query] Alias: " + alias_ + " query execution failed: null result";
                log_->error(err);
                throw std::runtime_error(err);
            }

            ExecStatusType status = PQresultStatus(result.get());
            if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
                std::string err = "[Run query] Alias: " + alias_ + " query [" + std::string(query) +
                                  "] failed: " + std::string(PQerrorMessage(conn_.get()));
                log_->error(err);
                throw std::runtime_error(err);
            }

            co_return handler(result.get());
        }

    private:
        PGConnPtr conn_;
        connect_params params_;
        Status status_;
        std::string alias_;
    };

    using connector_factory =
        std::function<std::unique_ptr<IConnector>(connect_params, std::string)>;

} // namespace pg
