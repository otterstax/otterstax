// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/mysql/connector.hpp"
#include "mock_config.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/mysql.hpp>
#include <boost/mysql/any_address.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/execution_processor/execution_processor.hpp>
#include <boost/mysql/detail/flags.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mysql {

    // A valid, completed boost::mysql::results carrying a single OK packet
    // (zero columns, zero rows). Lets the catalog's schema-discovery handler
    // call results.meta() safely against the mock connector.
    inline const boost::mysql::results& mock_ok_results() {
        static const boost::mysql::results results = [] {
            boost::mysql::results r;
            auto& impl = boost::mysql::detail::access::get_impl(r);
            impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::minimal);
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto ec = impl.on_head_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}}, diag);
            assert(!ec && "mock result set: on_head_ok_packet must not fail");
            return r;
        }();
        return results;
    }

    // One column of a manufactured result set.
    struct mock_column_t {
        std::string name;
        boost::mysql::column_type type;
        bool is_unsigned = false;
        // PRI_KEY_FLAG: the column is part of the table's primary key, as the
        // server marks it in a probe's column definitions.
        bool primary_key = false;
    };

    using mock_row_t = std::vector<std::optional<std::string>>;

    // A completed text result set with `columns` and `rows` (each value in the
    // text form MySQL sends, NULL as nullopt), manufactured through
    // boost::mysql::detail::access the way the connection fills one: the column
    // definitions in metadata_mode::full, then every row as its wire message (a
    // length-encoded string per field, 0xFB for NULL).
    inline boost::mysql::results mock_results(const std::vector<mock_column_t>& columns,
                                              const std::vector<mock_row_t>& rows = {}) {
        boost::mysql::results result;
        auto& impl = boost::mysql::detail::access::get_impl(result);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
        impl.on_num_meta(columns.size());
        for (const auto& column : columns) {
            boost::mysql::detail::coldef_view coldef{};
            coldef.name = column.name;
            coldef.type = column.type;
            coldef.flags = static_cast<std::uint16_t>(
                (column.is_unsigned ? boost::mysql::detail::column_flags::unsigned_ : 0) |
                (column.primary_key ? boost::mysql::detail::column_flags::pri_key : 0));
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto meta_ec = impl.on_meta(coldef, diag);
            assert(!meta_ec && "mock result set: on_meta must not fail");
        }
        if (!rows.empty()) {
            impl.on_row_batch_start();
            std::vector<boost::mysql::field_view> storage;
            // The fields point into the messages until the batch ends (its
            // strings are copied into the result set then).
            std::vector<std::vector<std::uint8_t>> messages;
            messages.reserve(rows.size());
            for (const auto& row : rows) {
                auto& message = messages.emplace_back();
                for (const auto& value : row) {
                    if (!value) {
                        message.push_back(0xFB);
                        continue;
                    }
                    assert(value->size() < 251 && "mock result set: a value of at most 250 bytes");
                    message.push_back(static_cast<std::uint8_t>(value->size()));
                    message.insert(message.end(), value->begin(), value->end());
                }
                [[maybe_unused]] auto row_ec =
                    impl.on_row(boost::span<const std::uint8_t>(message.data(), message.size()),
                                boost::mysql::detail::output_ref{},
                                storage);
                assert(!row_ec && "mock result set: on_row must not fail");
            }
            impl.on_row_batch_finish();
        }
        [[maybe_unused]] auto ok_ec = impl.on_row_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}});
        assert(!ok_ec && "mock result set: on_row_ok_packet must not fail");
        return result;
    }

    // What the default mock discovers: one table `table` in the connection's
    // database, (id INT, name VARCHAR) — the columns its data path answers.
    inline const boost::mysql::results& mock_table_listing() {
        static const boost::mysql::results results =
            mock_results({{"TABLE_NAME", boost::mysql::column_type::varchar}}, {{std::string{"table"}}});
        return results;
    }
    inline const boost::mysql::results& mock_table_probe() {
        static const boost::mysql::results results =
            mock_results({{"id", boost::mysql::column_type::int_}, {"name", boost::mysql::column_type::varchar}});
        return results;
    }

    class MockConnector : public mysql::IConnector {
    public:
        explicit MockConnector(mock_config config, std::string alias = "mock_connector")
            : config_(std::move(config))
            , alias_(std::move(alias)) {
            std::cout << "MockConnector created with config: " << std::endl;
            std::cout << "can_throw: " << config_.can_throw << std::endl;
            std::cout << "return_empty: " << config_.return_empty << std::endl;
            std::cout << "wait_time: " << config_.wait_time.count() << " milliseconds" << std::endl;
            std::cout << "error_message: " << config_.error_message << std::endl;
            std::cout << "alias: " << alias_ << std::endl;
        }

        mysql::Status status() const noexcept override { return mysql::Status::Connected; }

        bm::connect_params params() const noexcept override { return bm::connect_params{}; }

        void close() override { std::cout << "MockConnector closed." << std::endl; }

        core::error_t connect() override {
            std::cout << "MockConnector connected." << std::endl;
            return core::error_t::no_error();
        }

        bool isConnected() override { return true; }

        core::error_t tryReconnect() override {
            std::cout << "MockConnector trying to reconnect." << std::endl;
            return connect();
        }

        bool isClosed() const noexcept override { return false; }

        std::string alias() const noexcept override { return alias_; }

        data_chunk_t get_chunk() {
            std::pmr::vector<types::complex_logical_type> fields(config_.resource);
            fields.reserve(2);
            fields.emplace_back(types::logical_type::INTEGER, "id");
            fields.emplace_back(types::logical_type::STRING_LITERAL, "name");
            if (config_.return_empty) {
                // The table's columns, no row: what an empty table answers.
                std::cout << "MockConnector returning empty result." << std::endl;
                data_chunk_t chunk(config_.resource, fields, 1);
                chunk.set_cardinality(0);
                return chunk;
            }

            data_chunk_t result(config_.resource, fields, 2);
            result.set_cardinality(2);
            // Defined values: the slice reaches a real engine, which copies and
            // reads every cell.
            for (uint64_t row = 0; row < 2; ++row) {
                result.set_value(0, row, types::logical_value_t(config_.resource, static_cast<int32_t>(row + 1)));
                result.set_value(1,
                                 row,
                                 types::logical_value_t(config_.resource, std::string{"name_" + std::to_string(row)}));
            }
            return result;
        }

        // The data overloads throw when configured to: that models a driver or
        // translator exception raised inside the coroutine on the io thread, which
        // executeQuery must hand back as an io_error value.
        asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler)
            override {
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

        asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<int64_t(const boost::mysql::results&)> handler) override {
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

        // Schema-discovery overload (MySQLManager::discover) and DDL. Always
        // succeeds — startup must open even a connector whose data path is
        // configured to throw, mirroring a connection that came up healthy and
        // only fails later at query time. The information_schema listing
        // answers one table, `table`; its probe (`... WHERE 1 = 0`) the columns
        // the data path answers; anything else (DDL) an OK packet.
        asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            std::cout << "MockConnector running schema query: " << query << std::endl;
            if (query.find("information_schema") != std::string_view::npos) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mock_table_listing()));
            }
            if (query.find("1 = 0") != std::string_view::npos) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mock_table_probe()));
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mock_ok_results()));
        }

    private:
        mock_config config_;
        std::string alias_;
    };

} // namespace mysql

inline std::unique_ptr<mysql::IConnector> mysql_mock_connector_factory(std::pmr::memory_resource* resource,
                                                                       boost::asio::io_context&,
                                                                       boost::mysql::connect_params,
                                                                       std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource}, std::move(alias));
}

inline std::unique_ptr<mysql::IConnector> mysql_mock_connector_factory_throw(std::pmr::memory_resource* resource,
                                                                             boost::asio::io_context&,
                                                                             boost::mysql::connect_params,
                                                                             std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource, .can_throw = true},
                                                  std::move(alias));
}

inline std::unique_ptr<mysql::IConnector> mysql_mock_connector_factory_return_empty(std::pmr::memory_resource* resource,
                                                                                    boost::asio::io_context&,
                                                                                    boost::mysql::connect_params,
                                                                                    std::string alias) {
    std::cout << "Creating MockConnector." << std::endl;
    return std::make_unique<mysql::MockConnector>(mock_config{.resource = resource, .return_empty = true},
                                                  std::move(alias));
}
