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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <concepts>
#include <coroutine>
#include <functional>
#include <memory>
#include <string>

namespace s3c {

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
                 std::function<std::unique_ptr<data_chunk_t>(const S3Data&)> handler) = 0;
        virtual asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const S3Data&)> handler) = 0;
        virtual asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const S3Data&)> handler) = 0;
    };

    class S3Connector : public IConnector {
    public:
        S3Connector(connect_params params, std::string alias = "");
        ~S3Connector() override;
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
                 std::function<std::unique_ptr<data_chunk_t>(const S3Data&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const S3Data&)> handler) override {
            return runQuery_(query, handler);
        }
        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const S3Data&)> handler) override {
            return runQuery_(query, handler);
        }

    private:
        log_t log_;

        S3Data fetch_and_merge();
        S3Data fetch_object(const std::string& key);
        std::vector<std::string> list_keys();
        filec::FileFormat detect_format(const std::string& key);

        template<typename Callable>
            requires std::invocable<Callable, const S3Data&>
        asio::awaitable<std::invoke_result_t<Callable, const S3Data&>>
        runQuery_(std::string_view /*query*/, Callable handler) {
            if (status_ != Status::Connected) {
                throw std::runtime_error("[S3Connector] Not connected, alias: " + alias_);
            }
            auto sd = fetch_and_merge();
            co_return handler(sd);
        }

        connect_params params_;
        Status status_{Status::Created};
        std::string alias_;
        std::unique_ptr<Aws::S3::S3Client> s3_client_;

        static std::atomic<int> sdk_init_count_;
    };

    using connector_factory = std::function<std::unique_ptr<IConnector>(connect_params, std::string)>;

} // namespace s3c
