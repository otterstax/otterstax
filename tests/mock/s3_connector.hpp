#pragma once

#include "connectors/s3/connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>

#include <iostream>
#include <memory>

namespace s3c {

    class MockS3Connector : public s3c::IConnector {
    public:
        explicit MockS3Connector(mock_config config = {}, std::string alias = "mock_s3")
            : config_(config)
            , alias_(alias) {}

        Status status() const noexcept override { return Status::Connected; }
        connect_params params() const noexcept override { return params_; }
        void close() override {}
        void connect() override {}
        bool isConnected() override { return true; }
        void tryReconnect() override {}
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(const S3Data&)> handler) override {
            if (config_.can_throw) {
                throw std::runtime_error(
                    config_.error_message.empty() ? "MockS3Connector: exception in runQuery"
                                                  : config_.error_message);
            }
            std::this_thread::sleep_for(config_.wait_time);

            S3Data sd;
            sd.format = filec::FileFormat::Parquet;
            sd.s3_key = "mock/test.parquet";

            if (config_.return_empty) {
                sd.bytes = {};
            }
            co_return handler(sd);
        }

        asio::awaitable<int64_t>
        runQuery(std::string_view query,
                 std::function<int64_t(const S3Data&)> handler) override {
            S3Data sd;
            co_return handler(sd);
        }

        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const S3Data&)> handler) override {
            S3Data sd;
            co_return handler(sd);
        }

    private:
        mock_config config_;
        connect_params params_;
        std::string alias_;
    };

} // namespace s3c

inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{}, alias);
}

inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector_throw(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{.can_throw = true}, alias);
}

inline std::unique_ptr<s3c::IConnector>
make_s3_mock_connector_empty(s3c::connect_params params, std::string alias) {
    return std::make_unique<s3c::MockS3Connector>(mock_config{.return_empty = true}, alias);
}
