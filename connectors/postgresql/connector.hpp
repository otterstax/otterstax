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
#include "utility/function_ref.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <memory_resource>
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

    // Every failure a connector can report travels as a core::error_t value:
    // connect()/tryReconnect() return it, and each runQuery overload resolves to
    // the marshaled outcome of its handler (see otterstax::query_result_t).
    // runQuery takes the handler as a non-owning otterstax::function_ref_t: the
    // callable must outlive the returned awaitable. ConnectorManager::executeQuery
    // binds the copy otterstax::run_with_owned_handler keeps in its frame.
    class IConnector {
    public:
        virtual ~IConnector() = default;
        virtual Status status() const noexcept = 0;
        virtual connect_params params() const noexcept = 0;
        virtual void close() = 0;
        virtual core::error_t connect() = 0;
        virtual bool isConnected() = 0;
        virtual core::error_t tryReconnect() = 0;
        virtual bool isClosed() const noexcept = 0;
        virtual std::string alias() const noexcept = 0;

        virtual asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)> handler) = 0;
        virtual asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<int64_t(PGresult*)> handler) = 0;
        virtual asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) = 0;
    };

    class Connector : public IConnector {
    public:
        Connector(std::pmr::memory_resource* resource, connect_params params, std::string alias = "");
        Status status() const noexcept override;
        connect_params params() const noexcept override;
        void close() override;
        ~Connector() override;
        core::error_t connect() override;
        bool isConnected() override;
        core::error_t tryReconnect() override;
        bool isClosed() const noexcept override;
        std::string alias() const noexcept override;

        asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<int64_t(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        // The backend's verdict on a statement, classified by SQLSTATE (errors.hpp)
        // and carrying the server message so the frontend can show what the server
        // said.
        core::error_t query_error(const PGresult* result, std::string_view query) const;
        core::error_t connection_error(std::string_view what) const;

        template<typename Callable>
        requires std::invocable<Callable, PGresult*>
            asio::awaitable<otterstax::query_result_t<std::invoke_result_t<Callable, PGresult*>>>
            runQuery_(std::string_view query, Callable handler) {
            OTX_ZONE_N("pg::Connector::runQuery");
            using result_t = std::invoke_result_t<Callable, PGresult*>;
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                co_return connection_error(err);
            }

            // Check connection status
            if (PQstatus(conn_.get()) != CONNECTION_OK) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " connection check failed";
                log_->error(err);
                co_return connection_error(err);
            }

            log_->debug("Alias: {} query: {}", alias_, query);

            // Execute query synchronously (libpq doesn't have native coroutine support)
            PGResultPtr result(PQexec(conn_.get(), std::string(query).c_str()));

            if (!result) {
                std::string err = "[Run query] Alias: " + alias_ + " query execution failed: null result";
                log_->error(err);
                co_return connection_error(err);
            }

            ExecStatusType status = PQresultStatus(result.get());
            if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
                co_return query_error(result.get(), query);
            }

            co_return otterstax::as_query_result<result_t>(handler(result.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        PGConnPtr conn_;
        connect_params params_;
        Status status_;
        std::string alias_;
    };

    // Stateless connector factory: a plain function pointer (NOT std::function),
    // so production passes &make_pg_connector and tests pass their own free
    // function. The leading memory_resource owns every core::error_t message the
    // connector produces on the io thread.
    using connector_factory = std::unique_ptr<IConnector> (*)(std::pmr::memory_resource*, connect_params, std::string);

} // namespace pg
