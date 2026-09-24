// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include <boost/asio.hpp>
#include <boost/mysql.hpp>
#include <boost/mysql/any_address.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/pipeline.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/statement.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
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
#include <vector>

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
        virtual bm::connect_params params() const noexcept = 0;
        virtual void close() = 0;
        virtual core::error_t connect() = 0;
        virtual bool isConnected() = 0;
        virtual core::error_t tryReconnect() = 0;
        virtual bool isClosed() const noexcept = 0;
        virtual std::string alias() const noexcept = 0;

        virtual asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler) = 0;
        virtual asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const boost::mysql::results&)> handler) = 0;
        virtual asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) = 0;
    };

    class Connector : public IConnector {
    public:
        Connector(std::pmr::memory_resource* resource,
                  asio::io_context& io_ctx,
                  bm::connect_params params,
                  std::string alias = "");
        Status status() const noexcept override;
        bm::connect_params params() const noexcept override;
        void close() override;
        ~Connector() override;
        core::error_t connect() override;
        bool isConnected() override;
        core::error_t tryReconnect() override;
        bool isClosed() const noexcept override;
        std::string alias() const noexcept override;

        asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler)
            override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<int64_t(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        // Connect with a hard per-attempt deadline. The plain sync
        // connect(params, ec, diag) performs blocking socket I/O on the calling
        // thread with NO timeout: pointed at a non-MySQL server (e.g.
        // PostgreSQL on 5432) both sides wait for the other's first packet and
        // the call blocks until the remote auth timeout closes the socket —
        // freezing the startup thread for minutes. asyncConnect_ runs the async
        // op under asio::cancel_after on the connection's strand (the pool is
        // started before any addConnection) and this wrapper blocks on its
        // marshaled outcome (spawn_marshaled), so the verdict — success, the
        // driver's error, operation_aborted on timeout, io_error if the pool
        // went away — always arrives as a value, never as an exception.
        //
        // The strand is load-bearing. conn_ lives on an io_context run by N
        // pool threads, and asio::cancel_after (boost 1.88,
        // asio/detail/timed_cancel_op.hpp) completes through TWO handlers that
        // share a ref_count of 2: the operation's handle_op does
        // `timer_.cancel(); release();`, the cancelled timer's handle_timer
        // does `complete();`, and only the decrement that reaches zero invokes
        // the final handler — release() destroys it WITHOUT invoking. Both run
        // on the final handler's associated executor. With use_future on the
        // bare io_context that was any pool thread, so handle_timer could run
        // between cancel() and release(): complete() took 2->1 (no call),
        // release() took 1->0 (destroy) — the promise died unfulfilled and
        // get() threw broken_promise on the startup thread, i.e.
        // std::terminate at server start, on every connect that beat the
        // deadline. A use_awaitable coroutine spawned on the strand makes the
        // strand the associated executor of both handlers, so they run one
        // after the other. tests/unit/utility/test_mysql_connector.cpp pins it.
        core::error_t connectWithTimeout(boost::mysql::diagnostics& diag);

        // The connect itself: one async_connect under the deadline, awaited on
        // conn_'s strand. On failure the error is io_error carrying
        // ec.message(); `diag` (the caller's) carries the server message. No
        // Tracy zone: the body runs on a pool thread while connectWithTimeout
        // blocks on the startup thread.
        asio::awaitable<core::error_t> asyncConnect_(boost::mysql::diagnostics& diag);

        // The backend's verdict on a statement, classified (errors.hpp) and carrying
        // the server message so the frontend can show what the server said.
        core::error_t query_error(const boost::system::error_code& ec,
                                  const boost::mysql::diagnostics& diag,
                                  std::string_view query) const;
        core::error_t connection_error(std::string_view what) const;

        template<typename Callable>
        requires std::invocable<Callable, const boost::mysql::results&>
            asio::awaitable<otterstax::query_result_t<std::invoke_result_t<Callable, const boost::mysql::results&>>>
            runQuery_(std::string_view query, Callable handler) {
            using result_t = std::invoke_result_t<Callable, const boost::mysql::results&>;
            // No Tracy zone: this coroutine crosses io_context threads at every
            // co_await, so a zone opened here would be closed on another thread.
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                co_return connection_error(err);
            }
            boost::system::error_code ec;
            bm::diagnostics diag;
            co_await conn_.async_ping(diag, asio::redirect_error(asio::use_awaitable, ec));

            if (ec) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " ping failed: " + ec.message();
                log_->error(err);
                co_return connection_error(err);
            }

            // TODO add timeout or table check asio::cancel_after(std::chrono::seconds(5)) use boost 1.87
            // TODO add atomic working status to block removing while get results from
            // DB
            // Issue the SQL query to the server
            log_->debug("Alias: {} query: {}", alias_, query);
            // Binary protocol, never COM_QUERY: the text protocol renders a FLOAT
            // column with FLT_DIG significant digits (the stored 64647.5390625
            // arrives as "64647.5"), so the rows would not be the values the
            // backend holds. The statement is prepared, then executed and closed
            // in one pipeline round trip; the close stage runs even when the
            // execute stage fails, so no server-side handle outlives the query.
            bm::statement statement =
                co_await conn_.async_prepare_statement(query, diag, asio::redirect_error(asio::use_awaitable, ec));
            if (ec) {
                co_return query_error(ec, diag, query);
            }

            bm::pipeline_request pipeline;
            pipeline.add_execute(statement).add_close_statement(statement);
            std::vector<bm::stage_response> stages;
            co_await conn_.async_run_pipeline(pipeline, stages, diag, asio::redirect_error(asio::use_awaitable, ec));
            const bm::stage_response& executed = stages.front();
            if (ec) {
                co_return query_error(ec, executed.error() ? executed.diag() : diag, query);
            }

            co_return otterstax::as_query_result<result_t>(handler(executed.as_results()));
        }

    private:
        std::pmr::memory_resource* resource_;
        bm::any_connection conn_;
        bm::connect_params params_;
        Status status_;
        std::mutex mutex_;
        std::string alias_;
    };

    // Stateless connector factory: a plain function pointer (NOT std::function), so
    // production passes &make_mysql_connector and tests pass their own free
    // function. The leading memory_resource owns every core::error_t message the
    // connector produces on the io thread.
    using connector_factory = std::unique_ptr<IConnector> (*)(std::pmr::memory_resource*,
                                                              asio::io_context&,
                                                              bm::connect_params,
                                                              std::string);

} // namespace mysql
