// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/mysql_connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/mysql.hpp>
#include <boost/mysql/any_address.hpp>
#include <boost/mysql/any_connection.hpp>

#include <iostream>
#include <memory>
#include <utility>

namespace mysqlc {

    class MockConnector : public mysqlc::IConnector {
    public:
        explicit MockConnector(mock_config config = {})
            : config_(std::move(config)) {
            std::cout << "MockConnector created with config: " << std::endl;
            std::cout << "can_throw: " << config_.can_throw << std::endl;
            std::cout << "return_empty: " << config_.return_empty << std::endl;
            std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
            std::cout << "error_message: " << config_.error_message << std::endl;
        }

        mysqlc::Status status() const noexcept override { return mysqlc::Status::Connected; }

        mysql::connect_params params() const noexcept override { return mysql::connect_params{}; }

        void close() override { std::cout << "MockConnector closed." << std::endl; }

        void connect() override { std::cout << "MockConnector connected." << std::endl; }

        bool isConnected() override { return true; }

        void tryReconnect() override {
            std::cout << "MockConnector trying to reconnect." << std::endl;
            connect();
        }

        bool isClosed() const noexcept override { return false; }

        std::string alias() const noexcept override { return "mock_connector"; }

        data_chunk_t get_chunk() {
            std::pmr::vector<types::complex_logical_type> fields(config_.resource);
            if (config_.return_empty) {
                std::cout << "MockConnector returning empty result." << std::endl;
                data_chunk_t chunk(config_.resource, fields);
                return chunk;
            }

            fields.reserve(2);
            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            data_chunk_t result(config_.resource, fields, 2);
            result.set_cardinality(2);
            return result;
        }

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler) override {
            std::cout << "MockConnector running query: " << query << std::endl;

            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time)); // Simulate some processing delay
            co_return std::make_unique<data_chunk_t>(get_chunk());
        }

        asio::awaitable<int64_t> runQuery(std::string_view query,
                                          std::function<int64_t(const boost::mysql::results&)> handler) override {
            // Mock implementation
            std::cout << "MockConnector running update query: " << query << std::endl;
            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            co_return 42;
        }

        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(const boost::mysql::results&)> handler) override {
            throw std::runtime_error("Unimplemented");
        }

    private:
        mock_config config_;
    };

} // namespace mysqlc

inline auto mysql_mock_connector_factory(std::pmr::memory_resource* resource) {
    return [resource](boost::asio::io_context& io_ctx, boost::mysql::connect_params, std::string) {
        std::cout << "Creating MockConnector." << std::endl;
        return std::make_unique<mysqlc::MockConnector>(mock_config{.resource = resource});
    };
}

inline auto mysql_mock_connector_factory_throw(std::pmr::memory_resource* resource) {
    return [resource](boost::asio::io_context& io_ctx, boost::mysql::connect_params, std::string) {
        std::cout << "Creating MockConnector." << std::endl;
        return std::make_unique<mysqlc::MockConnector>(mock_config{.resource = resource, .can_throw = true});
    };
}

inline auto mysql_mock_connector_factory_return_empty(std::pmr::memory_resource* resource) {
    return [resource](boost::asio::io_context& io_ctx, boost::mysql::connect_params, std::string) {
        std::cout << "Creating MockConnector." << std::endl;
        return std::make_unique<mysqlc::MockConnector>(mock_config{.resource = resource, .return_empty = true});
    };
}
