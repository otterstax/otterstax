// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "name_resolution.hpp"

#include "otterbrix/federation/remote_storage.hpp"
#include "otterbrix/federation/rules/pushdown.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <algorithm>
#include <cassert>
#include <span>
#include <string>
#include <utility>

using namespace components;

namespace otterstax::federation {

    namespace {

        using tsl::make_error;

        std::string text_at(const vector::data_chunk_t& chunk, std::size_t column, std::uint64_t row) {
            const auto cell = chunk.value(column, row);
            if (cell.is_null()) {
                return {};
            }
            return std::string{cell.value<std::string_view>()};
        }

        struct column_row_t {
            std::int64_t ordinal;
            std::string column_name;
            std::string remote_column;
            std::string column_type;
            std::int64_t key_position;
        };

        // The storage of one name from its rows in federation.remote_columns, or no storage ("not
        // mine") when there are none, or they were discovered on another kind of backend than the
        // connection is now (a config.yaml change: the rows are stale until rediscovered).
        core::result_wrapper_t<planner::table_storage_answer_t>
        decide_one(std::pmr::memory_resource* resource,
                   const resolved_name_t& name,
                   const std::pmr::vector<vector::data_chunk_t>& rows) {
            using layout = remote_columns_layout;
            planner::table_storage_answer_t answer{std::pmr::vector<types::complex_logical_type>{resource}};

            std::vector<column_row_t> columns;
            remote_table_name_t remote;
            for (const auto& chunk : rows) {
                if (chunk.size() != 0 && chunk.column_count() < layout::count) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "federation.remote_columns answered " + std::to_string(chunk.column_count()) +
                                          " columns where " + std::to_string(layout::count) + " are expected");
                }
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    const auto backend = parse_backend_kind(text_at(chunk, layout::backend, row));
                    if (!backend || *backend != name.server->kind) {
                        return answer;
                    }
                    const auto ordinal = chunk.value(layout::ordinal, row);
                    const auto key_position = chunk.value(layout::key_position, row);
                    columns.push_back(column_row_t{ordinal.is_null() ? 0 : ordinal.value<std::int64_t>(),
                                                   text_at(chunk, layout::column_name, row),
                                                   text_at(chunk, layout::remote_column, row),
                                                   text_at(chunk, layout::column_type, row),
                                                   key_position.is_null() ? 0 : key_position.value<std::int64_t>()});
                    remote.database = text_at(chunk, layout::remote_database, row);
                    remote.schema = text_at(chunk, layout::remote_schema, row);
                    remote.table = text_at(chunk, layout::remote_table, row);
                }
            }
            if (columns.empty()) {
                return answer;
            }
            std::stable_sort(columns.begin(), columns.end(), [](const column_row_t& a, const column_row_t& b) {
                return a.ordinal < b.ordinal;
            });
            // Two rediscoveries of one table racing each other may both have inserted its rows: an
            // ordinal or a name seen twice is the same column, read once.
            std::vector<column_row_t> unique;
            unique.reserve(columns.size());
            for (auto& column : columns) {
                const bool seen = std::any_of(unique.begin(), unique.end(), [&](const column_row_t& kept) {
                    return kept.ordinal == column.ordinal || kept.column_name == column.column_name;
                });
                if (!seen) {
                    unique.push_back(std::move(column));
                }
            }
            columns = std::move(unique);

            remote_table_t table;
            table.server = *name.server;
            table.key = name.key;
            table.remote = std::move(remote);
            // The primary key: the declared positions of its columns, in key order.
            std::vector<std::pair<std::int64_t, std::size_t>> key;
            for (std::size_t i = 0; i < columns.size(); ++i) {
                if (columns[i].key_position > 0) {
                    key.emplace_back(columns[i].key_position, i);
                }
            }
            std::sort(key.begin(), key.end());
            for (const auto& [position, column] : key) {
                table.key_columns.push_back(column);
            }
            for (auto& column : columns) {
                auto type = components::catalog::decode_type_spec(resource, column.column_type);
                if (type.has_error()) {
                    return make_error(resource,
                                      type.error().type,
                                      "column \"" + column.column_name + "\" of remote table \"" +
                                          display_name(name.key) + "\": " + type.error().what.c_str());
                }
                auto declared = std::move(type.value());
                declared.set_alias(column.column_name);
                answer.columns.push_back(declared);
                table.columns.push_back(std::move(declared));
                table.remote_columns.push_back(std::move(column.remote_column));
            }
            answer.storage = core::pmr::make_polymorphic_unique<remote_storage_t>(resource, std::move(table));
            return answer;
        }

        // The read `need` issues for one key: the rows of federation.remote_columns under it.
        logical_plan::execution_plan_t make_remote_columns_read(std::pmr::memory_resource* resource,
                                                                const table_key_t& key) {
            OTX_ZONE_N("federation::make_remote_columns_read");
            using layout = remote_columns_layout;
            const qualified_name_t target{core::dbname_t{std::string{metadata_database}},
                                          core::relname_t{std::string{remote_columns_table}}};
            auto params = logical_plan::make_parameter_node(resource);
            auto where = expressions::make_compare_union_expression(resource, expressions::compare_type::union_and);
            const std::pair<std::size_t, const std::string*> slots[] = {{layout::uid, &key.uid},
                                                                        {layout::database_name, &key.database},
                                                                        {layout::schema_name, &key.schema},
                                                                        {layout::table_name, &key.table}};
            for (const auto& [column, value] : slots) {
                const auto id = params->add_parameter(types::logical_value_t(resource, *value));
                where->append_child(expressions::make_compare_expression(
                    resource,
                    expressions::compare_type::eq,
                    expressions::key_t{resource, layout::names[column], expressions::side_t::left},
                    id));
            }
            auto aggregate = logical_plan::make_node_aggregate(resource, target);
            aggregate->append_child(logical_plan::make_node_match(resource, target, std::move(where)));
            return logical_plan::execution_plan_t{resource, std::move(aggregate), std::move(params)};
        }

    } // namespace

    std::optional<resolved_name_t> resolve_written_name(const qualified_name_t& name, const registry_t& registry) {
        if (name.collection.t.empty()) {
            return std::nullopt;
        }
        const server_t* server = nullptr;
        std::string_view database;
        std::string_view schema;
        if (!name.unique_identifier.t.empty()) {
            server = registry.find(name.unique_identifier.t);
            database = name.database.t;
            schema = name.schema.t;
        } else if (!name.database.t.empty() && !name.schema.t.empty()) {
            // uid.db.table: the engine reads it as database.schema.table.
            server = registry.find(name.database.t);
            database = name.schema.t;
            schema = name.schema.t;
        }
        if (server == nullptr) {
            return std::nullopt;
        }
        if ((server->kind == backend_type_t::PostgreSQL && schema.empty()) ||
            (server->kind != backend_type_t::PostgreSQL && database.empty())) {
            return std::nullopt;
        }
        return resolved_name_t{
            server,
            make_table_key(server->kind, server->uid, database, schema, name.collection.t),
            remote_table_name_t{
                std::string{server->kind == backend_type_t::PostgreSQL ? std::string_view{} : database},
                std::string{server->kind == backend_type_t::PostgreSQL ? schema : std::string_view{}},
                name.collection.t}};
    }

    namespace {

        // need: one read per unresolved name the federation answers for, in the order of the names —
        // the rows of federation.remote_columns under its key, every column.
        core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>>
        need_remote_columns(std::pmr::memory_resource* resource,
                            const logical_plan::node_ptr&,
                            std::span<const qualified_name_t> unresolved) {
            OTX_ZONE_N("federation::need_remote_columns");
            const auto& servers = registry();
            std::pmr::vector<logical_plan::execution_plan_t> reads{resource};
            for (const auto& name : unresolved) {
                if (auto resolved = resolve_written_name(name, servers)) {
                    reads.push_back(make_remote_columns_read(resource, resolved->key));
                }
            }
            return reads;
        }

        // decide: for every unresolved name, the declared columns and a remote_storage_t when the read
        // found its columns; a null storage ("not mine") otherwise. The reads come in need's order. Rows
        // repeated for one ordinal (two rediscoveries racing the same table) are read once. The engine's
        // explicit-transaction flag is not read: a write into a remote table runs at once inside a
        // transaction too (the engine has no storage transaction hooks yet).
        core::result_wrapper_t<std::pmr::vector<planner::table_storage_answer_t>>
        decide_remote_storages(std::pmr::memory_resource* resource,
                               std::span<const qualified_name_t> unresolved,
                               std::span<const std::pmr::vector<vector::data_chunk_t>> read_results,
                               bool /*explicit_transaction*/) {
            OTX_ZONE_N("federation::decide_remote_storages");
            const auto& servers = registry();
            std::pmr::vector<planner::table_storage_answer_t> answers{resource};
            answers.reserve(unresolved.size());
            std::size_t next_read = 0;
            for (const auto& name : unresolved) {
                auto resolved = resolve_written_name(name, servers);
                if (!resolved) {
                    answers.push_back(
                        planner::table_storage_answer_t{std::pmr::vector<types::complex_logical_type>{resource}});
                    continue;
                }
                // The engine answers every read need asked for, in need's order.
                assert(next_read < read_results.size());
                auto answer = decide_one(resource, *resolved, read_results[next_read++]);
                if (answer.has_error()) {
                    return core::error_on(resource, answer.error());
                }
                answers.push_back(std::move(answer.value()));
            }
            assert(next_read == read_results.size());
            return answers;
        }

        constexpr planner::optimizer_rule_t federation_rules[] = {
            {planner::optimizer_stage::after_simplify, &rules::pushdown},
        };
    } // namespace

    planner::primitives_t make_primitives() noexcept {
        return planner::primitives_t{federation_rules, {&need_remote_columns, &decide_remote_storages}};
    }

} // namespace otterstax::federation
