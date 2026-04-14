// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "types.hpp"
#include <components/catalog/catalog_error.hpp>
#include <otterbrix/otterbrix.hpp>

#include <concepts>
#include <coroutine>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>

namespace filec {

    namespace asio = boost::asio;
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
                 std::function<std::unique_ptr<data_chunk_t>(const FileData&)> handler) = 0;
        virtual asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const FileData&)> handler) = 0;
        virtual asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const FileData&)> handler) = 0;
    };

    class FileConnector : public IConnector {
    public:
        FileConnector(connect_params params, std::string alias = "");
        Status status() const noexcept override;
        connect_params params() const noexcept override;
        void close() override;
        void connect() override;
        bool isConnected() override;
        void tryReconnect() override;
        bool isClosed() const noexcept override;
        std::string alias() const noexcept override;

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(const FileData&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const FileData&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const FileData&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        FileData read_file();
        FileFormat detect_format(const std::string& path);

        template<typename Callable>
            requires std::invocable<Callable, const FileData&>
        asio::awaitable<std::invoke_result_t<Callable, const FileData&>>
        runQuery_(std::string_view /*query*/, Callable handler) {
            if (status_ != Status::Connected) {
                throw std::runtime_error("[FileConnector] Not connected, alias: " + alias_);
            }
            auto fd = read_file();
            co_return handler(fd);
        }

        connect_params params_;
        Status status_{Status::Created};
        std::string alias_;
    };

    using connector_factory = std::function<std::unique_ptr<IConnector>(connect_params, std::string)>;

} // namespace filec
