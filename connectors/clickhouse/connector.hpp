// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include <clickhouse/client.h>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "types.hpp"
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
#include <optional>
#include <string>
#include <vector>

namespace ch {

    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;
    using components::vector::data_chunk_t;

    // What one statement streamed back: every data block (the 0-row header
    // block schema probes rely on included) and the rows the server reported
    // writing. No block ever carries an affected-row count — an INSERT returns
    // no data at all — so the count is read from the Progress packets, each one
    // a delta, summed over the statement. What the total means depends on the
    // statement: the rows an INSERT wrote (materialized views add theirs;
    // async_insert reports none), and always 0 for a lightweight DELETE or an
    // ALTER mutation, which run outside the statement's pipeline.
    struct select_result_t {
        std::vector<clickhouse::Block> blocks;
        uint64_t written_rows{0};
    };

    // The driver statement for `sql`, wired to collect into `out`; `out` must
    // outlive the statement's execution.
    clickhouse::Query make_statement(std::string_view sql, select_result_t& out);

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
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const select_result_t&)> handler) = 0;
        virtual asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const select_result_t&)> handler) = 0;
        virtual asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const select_result_t&)> handler) = 0;
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
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const select_result_t&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const select_result_t&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const select_result_t&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        // Runs the statement against the driver, collecting its blocks and
        // written-row progress into `out`. The driver reports failure by
        // throwing; this is the only place that throw is observed, and it comes
        // back as the classified core::error_t (errors.hpp) carrying the server
        // message.
        std::optional<core::error_t> execute_statement(std::string_view query, select_result_t& out);
        core::error_t connection_error(std::string_view what) const;

        template<typename Callable>
            requires std::invocable<Callable, const select_result_t&>
        asio::awaitable<otterstax::query_result_t<std::invoke_result_t<Callable, const select_result_t&>>>
        runQuery_(std::string_view query, Callable handler) {
            OTX_ZONE_N("ch::Connector::runQuery");
            using result_t = std::invoke_result_t<Callable, const select_result_t&>;
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                co_return connection_error(err);
            }

            log_->debug("Alias: {} query: {}", alias_, query);

            select_result_t result;
            if (auto failure = execute_statement(query, result); failure.has_value()) {
                co_return std::move(*failure);
            }

            co_return otterstax::as_query_result<result_t>(handler(result));
        }

    private:
        std::pmr::memory_resource* resource_;
        std::unique_ptr<clickhouse::Client> client_;
        connect_params params_;
        Status status_;
        std::string alias_;
    };

    // Stateless connector factory: a plain function pointer (NOT std::function),
    // so production passes &make_ch_connector and tests pass their own free
    // function. The leading memory_resource owns every core::error_t message the
    // connector produces on the io thread.
    using connector_factory = std::unique_ptr<IConnector> (*)(std::pmr::memory_resource*, connect_params, std::string);

} // namespace ch
