// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/postgresql/connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>

#include <iostream>
#include <memory>

namespace pgc {

    class MockConnector : public pgc::IConnector {
    public:
        explicit MockConnector(mock_config config = {}, std::string alias = "pg_mock_connector")
            : config_(config)
            , alias_(alias) {
            std::cout << "PG MockConnector created with config: " << std::endl;
            std::cout << "can_throw: " << config_.can_throw << std::endl;
            std::cout << "return_empty: " << config_.return_empty << std::endl;
            std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
            std::cout << "error_message: " << config_.error_message << std::endl;
            std::cout << "alias: " << alias_ << std::endl;
        }

        Status status() const noexcept override { return Status::Connected; }

        connect_params params() const noexcept override { return connect_params{}; }

        void close() override { std::cout << "PG MockConnector closed." << std::endl; }

        void connect() override { std::cout << "PG MockConnector connected." << std::endl; }

        bool isConnected() override { return true; }

        void tryReconnect() override {
            std::cout << "PG MockConnector trying to reconnect." << std::endl;
            connect();
        }

        bool isClosed() const noexcept override { return false; }

        std::string alias() const noexcept override { return alias_; }

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query,
                 std::function<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            std::cout << "PG MockConnector running query: " << query << std::endl;

            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time));

            auto* resource = std::pmr::get_default_resource();
            std::pmr::vector<components::types::complex_logical_type> fields(resource);
            if (config_.return_empty) {
                components::vector::data_chunk_t result(resource, fields);
                std::cout << "PG MockConnector returning empty result." << std::endl;
                co_return std::make_unique<data_chunk_t>(std::move(result));
            }

            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            components::vector::data_chunk_t result(resource, fields);
            result.set_cardinality(2);

            co_return std::make_unique<data_chunk_t>(std::move(result));
        }

        asio::awaitable<int64_t> runQuery(std::string_view query,
                                          std::function<int64_t(PGresult*)> handler) override {
            std::cout << "PG MockConnector running update query: " << query << std::endl;
            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            co_return 42;
        }

        asio::awaitable<components::catalog::catalog_error>
        runQuery(std::string_view query,
                 std::function<components::catalog::catalog_error(PGresult*)> handler) override {
            throw std::runtime_error("Unimplemented");
        }

    private:
        mock_config config_;
        std::string alias_;
    };

} // namespace pgc

inline std::unique_ptr<pgc::IConnector>
make_pg_mock_connector(pgc::connect_params params, std::string alias) {
    std::cout << "Creating PG MockConnector." << std::endl;
    return std::make_unique<pgc::MockConnector>(mock_config{}, alias);
}

inline std::unique_ptr<pgc::IConnector> make_pg_mock_connector_throw(pgc::connect_params params, std::string alias) {
    std::cout << "Creating PG MockConnector (throw)." << std::endl;
    return std::make_unique<pgc::MockConnector>(mock_config{.can_throw = true}, alias);
}

inline std::unique_ptr<pgc::IConnector> make_pg_mock_connector_return_empty(pgc::connect_params params, std::string alias) {
    std::cout << "Creating PG MockConnector (return empty)." << std::endl;
    return std::make_unique<pgc::MockConnector>(mock_config{.return_empty = true}, alias);
}