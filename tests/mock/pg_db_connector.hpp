// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/postgresql/connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>

#include <iostream>
#include <memory>
#include <utility>

namespace pg {

    class MockConnector : public pg::IConnector {
    public:
        explicit MockConnector(mock_config config = {}, std::string alias = "pg_mock_connector")
            : config_(std::move(config))
            , alias_(std::move(alias)) {
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

        data_chunk_t get_chunk() {
            std::pmr::vector<components::types::complex_logical_type> fields(config_.resource);
            if (config_.return_empty) {
                std::cout << "PG MockConnector returning empty result." << std::endl;
                components::vector::data_chunk_t result(config_.resource, fields);
                return result;
            }

            fields.reserve(2);
            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            components::vector::data_chunk_t result(config_.resource, fields);
            result.set_cardinality(2);
            return result;
        }

        asio::awaitable<std::unique_ptr<data_chunk_t>>
        runQuery(std::string_view query, std::function<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            std::cout << "PG MockConnector running query: " << query << std::endl;

            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time));

            co_return std::make_unique<data_chunk_t>(get_chunk());
        }

        asio::awaitable<int64_t> runQuery(std::string_view query, std::function<int64_t(PGresult*)> handler) override {
            std::cout << "PG MockConnector running update query: " << query << std::endl;
            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            co_return 42;
        }

        asio::awaitable<otterstax::asio_error_t>
        runQuery(std::string_view query,
                 std::function<otterstax::asio_error_t(PGresult*)> handler) override {
            throw std::runtime_error("Unimplemented");
        }

    private:
        mock_config config_;
        std::string alias_;
    };

} // namespace pg

inline auto pg_mock_connector_factory(std::pmr::memory_resource* resource) {
    return [resource](pg::connect_params, std::string alias) {
        std::cout << "Creating PG MockConnector." << std::endl;
        return std::make_unique<pg::MockConnector>(mock_config{.resource = resource}, std::move(alias));
    };
}

inline auto pg_mock_connector_factory_throw(std::pmr::memory_resource* resource) {
    return [resource](pg::connect_params, std::string alias) {
        std::cout << "Creating PG MockConnector (throw)." << std::endl;
        return std::make_unique<pg::MockConnector>(mock_config{.resource = resource, .can_throw = true},
                                                    std::move(alias));
    };
}

inline auto pg_mock_connector_factory_return_empty(std::pmr::memory_resource* resource) {
    return [resource](pg::connect_params, std::string alias) {
        std::cout << "Creating PG MockConnector (return empty)." << std::endl;
        return std::make_unique<pg::MockConnector>(mock_config{.resource = resource, .return_empty = true},
                                                    std::move(alias));
    };
}
