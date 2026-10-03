// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "integration/clickhouse/connection_manager.hpp"
#include "typed_backend.hpp"

#include "../mock/aliases.hpp"

#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace otterstax::test {
    enum class ch_wire
    {
        Int32,
        Int64,
        UInt64,
        String,
        NamedTuple
    };

    struct ch_backend {
        using wire = ch_wire;
        using column = wire_column<ch_wire>;
        using status = ch::Status;
        using connect_params = ch::connect_params;
        using connector_iface = ch::IConnector;
        using connector_manager = ch::ConnectorManager;
        using manager = db::ClickhouseManager;
        using api_params = conn::api_server::ChConnectionParams;
        using handler_arg = const ch::select_result_t&;
        using answer = ch::select_result_t;

        enum class probe_mode
        {
            header,     // the header block of the case's columns, then the column-less end marker
            no_columns, // the column-less end marker alone
            no_blocks,  // no block at all
            refused     // the connector's io_error
        };

        static constexpr test_connection_t connection{"chx", backend_type_t::ClickHouse, "chdb", "", "events"};
        static constexpr std::string_view events = "chx.chdb.events";

        static const answer& view(const answer& result) { return result; }

        static answer rows(const std::vector<column>& columns, std::size_t count) {
            constexpr std::size_t first_block_rows = 2;
            const auto split = std::min(count, first_block_rows);
            answer result;
            result.blocks.push_back(block(columns, 0, split));
            result.blocks.push_back(block(columns, split, count));
            return result;
        }

        static answer discovery(std::string_view query) {
            answer result;
            if (query.find("system.columns") != std::string_view::npos) {
                result.blocks.push_back(named_types());
            } else {
                result.blocks.push_back(block(base_columns(), 0, 0));
            }
            return result;
        }

        static core::result_wrapper_t<answer>
        describe(const std::vector<column>& columns, probe_mode mode, std::pmr::memory_resource* resource) {
            answer result;
            switch (mode) {
                case probe_mode::header:
                    result.blocks.push_back(block(columns, 0, 0));
                    result.blocks.emplace_back();
                    return result;
                case probe_mode::no_columns:
                    result.blocks.emplace_back();
                    return result;
                case probe_mode::no_blocks:
                    return result;
                case probe_mode::refused:
                    break;
            }
            return probe_refusal(resource);
        }

        static std::unique_ptr<connector_iface>
        factory(std::pmr::memory_resource* resource, connect_params params, std::string alias) {
            return std::make_unique<typed_connector<ch_backend>>(resource, std::move(params), std::move(alias));
        }

    private:
        struct base_column {
            const char* name;
            const char* named_type;
            ch_wire wire;
        };

        static constexpr std::array<base_column, 4> kBase{{
            {"id", "Int32", ch_wire::Int32},
            {"score", "Int32", ch_wire::Int32},
            {"name", "String", ch_wire::String},
            {"rec", "Tuple(a Int32, b Nullable(String))", ch_wire::NamedTuple},
        }};

        static std::vector<column> base_columns() {
            std::vector<column> columns;
            for (const auto& base : kBase) {
                columns.push_back({base.name, base.wire});
            }
            return columns;
        }

        static clickhouse::Block named_types() {
            auto names = std::make_shared<clickhouse::ColumnString>();
            auto types = std::make_shared<clickhouse::ColumnString>();
            for (const auto& base : kBase) {
                names->Append(std::string{base.name});
                types->Append(std::string{base.named_type});
            }
            clickhouse::Block block;
            block.AppendColumn("name", names);
            block.AppendColumn("type", types);
            return block;
        }

        static clickhouse::Block block(const std::vector<column>& columns, std::size_t first, std::size_t last) {
            clickhouse::Block block;
            for (const auto& column : columns) {
                block.AppendColumn(column.name, values(column.wire, first, last));
            }
            return block;
        }

        static clickhouse::ColumnRef values(ch_wire wire, std::size_t first, std::size_t last) {
            switch (wire) {
                case ch_wire::Int32: {
                    auto column = std::make_shared<clickhouse::ColumnInt32>();
                    for (auto row = first; row < last; ++row) {
                        column->Append(cell::int32(row));
                    }
                    return column;
                }
                case ch_wire::Int64: {
                    auto column = std::make_shared<clickhouse::ColumnInt64>();
                    for (auto row = first; row < last; ++row) {
                        column->Append(cell::int64(row));
                    }
                    return column;
                }
                case ch_wire::UInt64: {
                    auto column = std::make_shared<clickhouse::ColumnUInt64>();
                    for (auto row = first; row < last; ++row) {
                        column->Append(cell::uint64(row));
                    }
                    return column;
                }
                case ch_wire::String: {
                    auto column = std::make_shared<clickhouse::ColumnString>();
                    for (auto row = first; row < last; ++row) {
                        column->Append(cell::text(row));
                    }
                    return column;
                }
                case ch_wire::NamedTuple: {
                    auto a = std::make_shared<clickhouse::ColumnInt32>();
                    auto b_values = std::make_shared<clickhouse::ColumnString>();
                    auto b_nulls = std::make_shared<clickhouse::ColumnUInt8>();
                    for (auto row = first; row < last; ++row) {
                        const bool null_b = row % 2 == 1;
                        a->Append(cell::int32(row));
                        b_values->Append(null_b ? std::string{} : "b" + std::to_string(row));
                        b_nulls->Append(static_cast<uint8_t>(null_b));
                    }
                    return std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
                        a,
                        std::make_shared<clickhouse::ColumnNullable>(b_values, b_nulls)});
                }
            }
            return {};
        }
    };
} // namespace otterstax::test
