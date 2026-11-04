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
        explicit MockConnector(mock_config config,
                               std::string alias = "pg_mock_connector",
                               connect_params params = {})
            : config_(std::move(config))
            , alias_(std::move(alias))
            , params_(std::move(params)) {
            std::cout << "PG MockConnector created with config: " << std::endl;
            std::cout << "can_throw: " << config_.can_throw << std::endl;
            std::cout << "return_empty: " << config_.return_empty << std::endl;
            std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
            std::cout << "error_message: " << config_.error_message << std::endl;
            std::cout << "alias: " << alias_ << std::endl;
        }

        Status status() const noexcept override { return Status::Connected; }

        connect_params params() const noexcept override { return params_; }

        void close() override { std::cout << "PG MockConnector closed." << std::endl; }

        core::error_t connect() override {
            std::cout << "PG MockConnector connected." << std::endl;
            return core::error_t::no_error();
        }

        bool isConnected() override { return true; }

        core::error_t tryReconnect() override {
            std::cout << "PG MockConnector trying to reconnect." << std::endl;
            return connect();
        }

        bool isClosed() const noexcept override { return false; }

        std::string alias() const noexcept override { return alias_; }

        data_chunk_t get_chunk(std::string_view query = {}) {
            // The columns the SELECT list names, in its order (mock_projection).
            const auto projection = mock_projection(query, '"');
            std::pmr::vector<types::complex_logical_type> fields(config_.resource);
            for (auto c : projection) {
                if (c == 0) {
                    fields.emplace_back(types::logical_type::INTEGER, "id");
                } else {
                    fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
                }
            }
            if (config_.return_empty) {
                return data_chunk_t(config_.resource, std::pmr::vector<types::complex_logical_type>(config_.resource));
            }
            data_chunk_t result(config_.resource, fields, 2);
            result.set_cardinality(2);
            // Defined values: the slice reaches a real engine, which copies and
            // reads every cell.
            for (uint64_t row = 0; row < 2; ++row) {
                for (std::size_t at = 0; at < projection.size(); ++at) {
                    if (projection[at] == 0) {
                        result.set_value(at, row, static_cast<int32_t>(row + 1));
                    } else {
                        const std::string name = "name_" + std::to_string(row);
                        result.set_value(at, row, std::string_view{name});
                    }
                }
            }
            return result;
        }

        // The data overloads throw when configured to: that models a driver or
        // translator exception raised inside the coroutine on the io thread, which
        // the backend agent (backend_agent_t::run) must hand back as an error value.
        asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            std::cout << "PG MockConnector running query: " << query << std::endl;

            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.wait_time));

            co_return std::make_unique<data_chunk_t>(get_chunk(query));
        }

        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(PGresult*)> handler) override {
            std::cout << "PG MockConnector running update query: " << query << std::endl;
            if (config_.can_throw) {
                std::string error_message =
                    config_.error_message.empty() ? "PG MockConnector: exception in runQuery" : config_.error_message;
                std::cout << error_message << std::endl;
                throw std::runtime_error(error_message);
            }
            co_return 42;
        }

        // Schema-discovery overload (the PostgreSQL discovery of backend_agent_t: the pg_enum
        // metadata query, the table listing and the schema probes) and DDL.
        // Always succeeds so startup opens the connection regardless of the
        // data-path throw configuration: pg_enum answers no labels, the
        // information_schema listing one table `public`.`table`, and a probe (or DDL)
        // the header of the columns the data path answers (id int4, name
        // varchar).
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                 &PQclear);
            if (query.find("pg_enum") != std::string_view::npos) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            if (query.find("information_schema") != std::string_view::npos) {
                PGresAttDesc names[2] = {{const_cast<char*>("table_schema"), 0, 0, 0, 1043, -1, -1},
                                         {const_cast<char*>("table_name"), 0, 0, 0, 1043, -1, -1}};
                PQsetResultAttrs(result.get(), 2, names);
                PQsetvalue(result.get(), 0, 0, const_cast<char*>("public"), 6);
                PQsetvalue(result.get(), 0, 1, const_cast<char*>("table"), 5);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            PGresAttDesc columns[2] = {{const_cast<char*>("id"), 0, 0, 0, 23, -1, -1},
                                       {const_cast<char*>("name"), 0, 0, 0, 1043, -1, -1}};
            PQsetResultAttrs(result.get(), 2, columns);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        mock_config config_;
        std::string alias_;
        connect_params params_;
    };

} // namespace pg

inline std::unique_ptr<pg::IConnector>
pg_mock_connector_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
    std::cout << "Creating PG MockConnector." << std::endl;
    return std::make_unique<pg::MockConnector>(mock_config{.resource = resource},
                                               std::move(alias),
                                               std::move(params));
}

inline std::unique_ptr<pg::IConnector>
pg_mock_connector_factory_throw(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
    std::cout << "Creating PG MockConnector (throw)." << std::endl;
    return std::make_unique<pg::MockConnector>(mock_config{.resource = resource, .can_throw = true},
                                               std::move(alias),
                                               std::move(params));
}

inline std::unique_ptr<pg::IConnector> pg_mock_connector_factory_return_empty(std::pmr::memory_resource* resource,
                                                                              pg::connect_params params,
                                                                              std::string alias) {
    std::cout << "Creating PG MockConnector (return empty)." << std::endl;
    return std::make_unique<pg::MockConnector>(mock_config{.resource = resource, .return_empty = true},
                                               std::move(alias),
                                               std::move(params));
}
