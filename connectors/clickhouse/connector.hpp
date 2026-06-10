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
#include "utility/tracy_profiler.hpp"
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <string>

namespace ch {

    namespace asio = boost::asio;
    using asio::awaitable;
    using asio::co_spawn;
    using asio::use_awaitable;
    using components::vector::data_chunk_t;

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
                 std::function<std::unique_ptr<data_chunk_t>(const std::vector<clickhouse::Block>&)> handler) = 0;
        virtual asio::awaitable<int64_t>
        runQuery(std::string_view query, std::function<int64_t(const std::vector<clickhouse::Block>&)> handler) = 0;
        virtual asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(const std::vector<clickhouse::Block>&)> handler) = 0;
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
                 std::function<std::unique_ptr<data_chunk_t>(const std::vector<clickhouse::Block>&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const std::vector<clickhouse::Block>&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<otterstax::asio_error_t> runQuery(
            std::string_view query,
            std::function<otterstax::asio_error_t(const std::vector<clickhouse::Block>&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        template<typename Callable>
            requires std::invocable<Callable, const std::vector<clickhouse::Block>&>
        asio::awaitable<std::invoke_result_t<Callable, const std::vector<clickhouse::Block>&>>
        runQuery_(std::string_view query, Callable handler) {
            OTX_ZONE_N("ch::Connector::runQuery");
            if (status_ != Status::Connected) {
                std::string err = "[Run query] Connector with alias: " + alias_ + " is not connected";
                log_->error(err);
                throw std::runtime_error(err);
            }

            log_->debug("Alias: {} query: {}", alias_, query);

            // Collect all blocks including 0-row header blocks (needed for schema queries)
            std::vector<clickhouse::Block> blocks;
            client_->Select(std::string(query), [&blocks](const clickhouse::Block& block) { blocks.push_back(block); });

            co_return handler(blocks);
        }

    private:
        std::unique_ptr<clickhouse::Client> client_;
        connect_params params_;
        Status status_;
        std::string alias_;
    };

    using connector_factory = std::function<std::unique_ptr<IConnector>(connect_params, std::string)>;

} // namespace ch
