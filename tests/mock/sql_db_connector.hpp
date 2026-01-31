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

namespace mysqlc {

    class MockConnector : public mysqlc::IConnector {
    public:
        explicit MockConnector(mock_config config = {})
            : config_(config) {
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

            auto* resource = std::pmr::get_default_resource();
            std::pmr::vector<components::types::complex_logical_type> fields(resource);
            if (config_.return_empty) {
                components::vector::data_chunk_t result(resource, fields);
                std::cout << "MockConnector returning empty result." << std::endl;
                co_return std::make_unique<data_chunk_t>(std::move(result));
            }

            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            components::vector::data_chunk_t result(resource, fields);
            result.set_cardinality(2);

            co_return std::make_unique<data_chunk_t>(std::move(result));
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

inline std::unique_ptr<mysqlc::IConnector>
make_mysql_mock_connector(boost::asio::io_context& io_ctx, boost::mysql::connect_params params, std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysqlc::MockConnector>();
}

inline std::unique_ptr<mysqlc::IConnector> make_mysql_mock_connector_throw(boost::asio::io_context& io_ctx,
                                                                           boost::mysql::connect_params params,
                                                                           std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysqlc::MockConnector>(mock_config{.can_throw = true});
}

inline std::unique_ptr<mysqlc::IConnector> make_mysql_mock_connector_return_empty(boost::asio::io_context& io_ctx,
                                                                                  boost::mysql::connect_params params,
                                                                                  std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysqlc::MockConnector>(mock_config{.return_empty = true});
}