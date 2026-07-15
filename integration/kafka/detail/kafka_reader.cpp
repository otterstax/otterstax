// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_reader.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <boost/json.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <algorithm>
#include <map>
#include <optional>

namespace otterstax::kafka::detail {
    using namespace components;
    namespace {
        // One JSON field -> logical_value_t of the column's logical_type, or
        // nullopt if the JSON kind doesn't fit (caller drops the row)
        std::optional<types::logical_value_t> json_field_to_value(std::pmr::memory_resource* resource,
                                                                  types::logical_type type,
                                                                  const boost::json::value& jv) {
            const bool is_integral = jv.is_int64() || jv.is_uint64();
            switch (type) {
                case types::logical_type::INTEGER:
                    if (is_integral) {
                        return types::logical_value_t(resource, static_cast<int32_t>(jv.to_number<int64_t>()));
                    }
                    return std::nullopt;
                case types::logical_type::BIGINT:
                    if (is_integral) {
                        return types::logical_value_t(resource, jv.to_number<int64_t>());
                    }
                    return std::nullopt;
                case types::logical_type::DOUBLE:
                    if (is_integral || jv.is_double()) {
                        return types::logical_value_t(resource, jv.to_number<double>());
                    }
                    return std::nullopt;
                case types::logical_type::STRING_LITERAL:
                    if (jv.is_string()) {
                        return types::logical_value_t(resource, std::string(jv.as_string().c_str()));
                    }
                    return std::nullopt;
                case types::logical_type::BOOLEAN:
                    if (jv.is_bool()) {
                        return types::logical_value_t(resource, jv.as_bool());
                    }
                    return std::nullopt;
                default:
                    return std::nullopt;
            }
        }

    } // namespace

    vector::data_chunk_t json_to_chunk(std::pmr::memory_resource* resource,
                                       const std::vector<kafka_column_t>& columns,
                                       const std::vector<std::string>& payloads) {
        OTX_ZONE_N("kafka::json_to_chunk");
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto& column : columns) {
            types::complex_logical_type t = column.type;
            t.set_alias(column.name);
            types.push_back(std::move(t));
        }

        // Over-allocate to the payload count; only the fully-valid prefix rows are
        // committed (cardinality), partial writes from a dropped row are overwritten
        // by the next good row or excluded by the final set_cardinality
        vector::data_chunk_t chunk(resource, types, std::max<std::size_t>(payloads.size(), 1));
        std::uint64_t row = 0;
        for (const auto& payload : payloads) {
            boost::json::value jv;
            try {
                jv = boost::json::parse(payload);
            } catch (...) {
                continue; // malformed JSON
            }
            if (!jv.is_object()) {
                continue;
            }
            const auto& obj = jv.as_object();

            bool row_ok = true;
            for (std::size_t c = 0; c < columns.size(); ++c) {
                const boost::json::value* field = obj.if_contains(columns[c].name);
                if (!field) {
                    row_ok = false;
                    break;
                }
                auto value = json_field_to_value(resource, columns[c].type.type(), *field);
                if (!value) {
                    row_ok = false;
                    break;
                }
                chunk.set_value(c, row, *value);
            }
            if (row_ok) {
                ++row;
            }
        }
        chunk.set_cardinality(row);
        return chunk;
    }

    std::vector<std::string> chunk_to_json(const vector::data_chunk_t& chunk) {
        OTX_ZONE_N("kafka::chunk_to_json");
        std::vector<std::string> out;
        out.reserve(chunk.size());
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            boost::json::object obj;
            for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
                const auto& col_type = chunk.data[col].type();
                const std::string key = col_type.alias();
                if (chunk.data[col].is_null(row)) {
                    obj[key] = nullptr;
                    continue;
                }
                const auto value = chunk.value(col, row);
                switch (col_type.type()) {
                    case types::logical_type::INTEGER:
                        obj[key] = value.value<int32_t>();
                        break;
                    case types::logical_type::BIGINT:
                        obj[key] = value.value<int64_t>();
                        break;
                    case types::logical_type::DOUBLE:
                        obj[key] = value.value<double>();
                        break;
                    case types::logical_type::BOOLEAN:
                        obj[key] = value.value<bool>();
                        break;
                    case types::logical_type::STRING_LITERAL:
                        obj[key] = std::string(value.value<std::string_view>());
                        break;
                    default:
                        // json_to_chunk handles only these 5 types; emit null
                        // rather than guess an encoding for anything else
                        obj[key] = nullptr;
                        break;
                }
            }
            out.push_back(boost::json::serialize(obj));
        }
        return out;
    }

    bool chunk_matches_columns(std::pmr::memory_resource* resource,
                               const vector::data_chunk_t& chunk,
                               const std::vector<kafka_column_t>& declared) {
        OTX_ZONE_N("kafka::chunk_matches_columns");
        if (chunk.column_count() != declared.size()) {
            return false; // wrong number of columns (partial / extra)
        }
        // Serialize the rows and re-ingest them against the declared columns — the
        // exact check the SOURCE poller applies. A row that doesn't survive (a
        // missing, mis-keyed, or wrong-typed column) means the produced JSON would
        // not round-trip into the declared schema, i.e. it is malformed for this
        // object's topic
        const auto payloads = chunk_to_json(chunk);
        return json_to_chunk(resource, declared, payloads).size() == chunk.size();
    }

    std::vector<kafka_column_t> stream_output_schema(std::pmr::memory_resource* resource,
                                                     const logical_plan::node_aggregate_t& agg,
                                                     logical_plan::parameter_node_t* params,
                                                     const std::vector<kafka_column_t>& source_columns) {
        OTX_ZONE_N("kafka::stream_output_schema");
        // Input schema = the source's columns, alias = column name (the form the
        // schema computation matches projected columns against)
        std::pmr::vector<types::complex_logical_type> schema_types(resource);
        schema_types.reserve(source_columns.size());
        for (const auto& col : source_columns) {
            types::complex_logical_type t = col.type;
            t.set_alias(col.name);
            schema_types.push_back(std::move(t));
        }
        // Apply the SELECT's projection/rename to the source schema (no data) —
        // returns a STRUCT whose fields are the output columns (alias = name)
        const types::complex_logical_type out = schema_utils::aggregate_filter_schema(agg, params, schema_types);
        std::vector<kafka_column_t> columns;
        if (out.type() != types::logical_type::STRUCT) {
            return columns;
        }
        for (const auto& field : out.child_types()) {
            // Guard alias() — an alias-less field would null-deref (schema_utils note)
            columns.push_back(kafka_column_t{field.has_alias() ? std::string{field.alias()} : std::string{}, field});
        }
        return columns;
    }

    std::vector<kafka_column_t> columns_from_cursor(const cursor::cursor_t_ptr& cursor) {
        std::vector<kafka_column_t> columns;
        if (!cursor || cursor->is_error()) {
            return columns;
        }
        // type_data() carries one alias'd complex_logical_type per column (the
        // schema the engine fills even for an empty LIMIT 0 result) — unpack each
        // into a kafka_column_t, exactly as stream_output_schema does for STRUCT
        // child fields
        for (const auto& col : cursor->type_data()) {
            columns.push_back(kafka_column_t{col.has_alias() ? std::string{col.alias()} : std::string{}, col});
        }
        return columns;
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_insert_session(actor_zeta::address_t dispatcher_address,
                                                                         std::pmr::memory_resource* resource,
                                                                         session::session_id_t session,
                                                                         const std::string& database,
                                                                         const std::string& relname,
                                                                         vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::kafka_insert_session");
        // No explicit column list → full-row insert in chunk order (the chunk holds
        // all of the table's columns, in declared order). Mirrors the engine's own
        // INSERT planning (node_insert(chunk) + catalog_resolve_table, outgoing FK
        // resolution). Sent on the caller's `session` so it can share a transaction
        auto insert = logical_plan::make_node_insert(resource, std::move(chunk));
        logical_plan::node_ptr node =
            sql::transform::maybe_wrap_with_catalog_resolve_table(resource,
                                                                  database,
                                                                  relname,
                                                                  insert,
                                                                  sql::transform::constraint_resolve_kind::outgoing);
        return actor_zeta::otterbrix::send(
                   dispatcher_address,
                   &services::dispatcher::manager_dispatcher_t::execute_plan,
                   session,
                   logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)})
            .second;
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_insert(actor_zeta::address_t dispatcher_address,
                                                                 std::pmr::memory_resource* resource,
                                                                 const std::string& database,
                                                                 const std::string& relname,
                                                                 vector::data_chunk_t chunk) {
        // Fresh session -> statement-level autocommit (the at-least-once poller path)
        return kafka_insert_session(dispatcher_address,
                                    resource,
                                    session::session_id_t(),
                                    database,
                                    relname,
                                    std::move(chunk));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> write_offsets_session(actor_zeta::address_t dispatcher_address,
                                                                          std::pmr::memory_resource* resource,
                                                                          session::session_id_t session,
                                                                          const std::string& database,
                                                                          const std::string& offsets_relname,
                                                                          const std::map<int32_t, int64_t>& offsets) {
        OTX_ZONE_N("kafka::write_offsets_session");
        std::pmr::vector<types::complex_logical_type> types(resource);
        types::complex_logical_type partition_col(types::logical_type::INTEGER);
        partition_col.set_alias("partition_id"); // "partition" is a reserved SQL keyword
        types::complex_logical_type offset_col(types::logical_type::BIGINT);
        offset_col.set_alias("committed_offset");
        types.push_back(std::move(partition_col));
        types.push_back(std::move(offset_col));

        vector::data_chunk_t chunk(resource, types, std::max<std::size_t>(offsets.size(), 1));
        std::uint64_t row = 0;
        for (const auto& [partition, offset] : offsets) {
            chunk.set_value(0, row, types::logical_value_t(resource, partition));
            chunk.set_value(1, row, types::logical_value_t(resource, offset));
            ++row;
        }
        chunk.set_cardinality(row);
        return kafka_insert_session(dispatcher_address, resource, session, database, offsets_relname, std::move(chunk));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> write_offsets(actor_zeta::address_t dispatcher_address,
                                                                  std::pmr::memory_resource* resource,
                                                                  const std::string& database,
                                                                  const std::string& offsets_relname,
                                                                  const std::map<int32_t, int64_t>& offsets) {
        // Fresh session -> statement-level autocommit
        return write_offsets_session(dispatcher_address,
                                     resource,
                                     session::session_id_t(),
                                     database,
                                     offsets_relname,
                                     offsets);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_delete_where_eq(actor_zeta::address_t dispatcher_address,
                                                                          std::pmr::memory_resource* resource,
                                                                          const std::string& database,
                                                                          const std::string& relname,
                                                                          const std::string& column,
                                                                          const std::string& value) {
        OTX_ZONE_N("kafka::kafka_delete_where_eq");
        // Build `DELETE FROM database.relname WHERE column = value` as a logical plan
        // and wrap it for catalog resolution — the exact shape transform_delete emits
        // for a live DELETE (node_delete_many over node_match(eq predicate)). A raw
        // node_delete sent without this wrap null-derefs the engine, which is why the
        // old SQL-string path through kafka_query crashed; this mirrors kafka_insert
        // The string literal is bound as a parameter rather than spliced into SQL
        auto params = logical_plan::make_parameter_node(resource);
        const auto value_param = params->add_parameter(types::logical_value_t(resource, value));
        // side_t::left: the predicate column belongs to the (single) target table
        // transform_delete reaches this via key.deduce_side(names); a hand-built key
        // defaults to side_t::undefined, which leaves the column unresolved and the
        // full_scan feeds operator_delete a chunk whose types carry no alias (crash)
        expressions::key_t column_key(resource, column, expressions::side_t::left);
        auto predicate =
            expressions::make_compare_expression(resource, expressions::compare_type::eq, column_key, value_param);
        auto match =
            logical_plan::make_node_match(resource, core::dbname_t{database}, core::relname_t{relname}, predicate);
        auto del = logical_plan::make_node_delete_many(resource, match);
        // constraint_resolve_kind::referencing mirrors transform_delete (FK cascade to
        // children); __sources has no FKs, so it is a no-op wrap here
        logical_plan::node_ptr node =
            sql::transform::maybe_wrap_with_catalog_resolve_table(resource,
                                                                  database,
                                                                  relname,
                                                                  del,
                                                                  sql::transform::constraint_resolve_kind::referencing);
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session::session_id_t(),
                                           logical_plan::execution_plan_t{resource, node, params})
            .second;
    }

    std::map<int32_t, int64_t> parse_offsets(const cursor::cursor_t_ptr& cursor) {
        OTX_ZONE_N("kafka::parse_offsets");
        std::map<int32_t, int64_t> result;
        if (!cursor || cursor->is_error()) {
            return result;
        }
        const auto& chunk = cursor->chunk_data();
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            const int32_t partition = chunk.value(0, row).value<int32_t>();
            const int64_t offset = chunk.value(1, row).value<int64_t>();
            auto it = result.find(partition);
            if (it == result.end() || offset > it->second) {
                result[partition] = offset;
            }
        }
        return result;
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    kafka_query(actor_zeta::address_t dispatcher_address, std::pmr::memory_resource* resource, const std::string& sql) {
        // Fresh session -> statement-level autocommit
        return kafka_query_session(dispatcher_address, resource, session::session_id_t(), sql);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_query_session(actor_zeta::address_t dispatcher_address,
                                                                        std::pmr::memory_resource* resource,
                                                                        session::session_id_t session,
                                                                        const std::string& sql) {
        OTX_ZONE_N("kafka::kafka_query_session");
        // Parse core SQL to an execution_plan_t (no kafka extension needed) and
        // hand off to the dispatcher on `session` — mirrors OtterbrixDataManager::
        // execute_sql, but the caller owns the session so a sequence of calls on the
        // same session runs inside one engine transaction
        using namespace components::sql::transform;
        components::sql::parser::parser_extension_registry_t registry;
        std::pmr::monotonic_buffer_resource arena(resource);
        void* parse_result;
        try {
            parse_result = linitial(raw_parser(&arena, sql.c_str(), registry));
        } catch (const std::exception& exception) {
            return actor_zeta::make_ready_future<cursor::cursor_t_ptr>(
                resource,
                cursor::make_cursor(
                    resource,
                    core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{exception.what(), resource})));
        }
        if (!parse_result) {
            return actor_zeta::make_ready_future<cursor::cursor_t_ptr>(
                resource,
                cursor::make_cursor(resource,
                                    core::error_t(core::error_code_t::sql_parse_error,
                                                  std::pmr::string{"unknown parser error", resource})));
        }
        transformer local_transformer(resource, sql.c_str(), &registry);
        auto result = local_transformer.transform(pg_cell_to_node_cast(parse_result)).finalize();
        if (result.has_error()) {
            return actor_zeta::make_ready_future<cursor::cursor_t_ptr>(resource,
                                                                       cursor::make_cursor(resource, result.error()));
        }
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session,
                                           std::move(result.value()))
            .second;
    }

    core::result_wrapper_t<logical_plan::execution_plan_t> kafka_parse_plan(std::pmr::memory_resource* resource,
                                                                            const std::string& sql) {
        OTX_ZONE_N("kafka::kafka_parse_plan");
        using namespace components::sql::transform;
        components::sql::parser::parser_extension_registry_t registry;
        std::pmr::monotonic_buffer_resource arena(resource);
        void* parse_result = nullptr;
        try {
            parse_result = linitial(raw_parser(&arena, sql.c_str(), registry));
        } catch (const std::exception& exception) {
            return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{exception.what(), resource});
        }
        if (!parse_result) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"kafka: parser returned null", resource});
        }
        transformer local_transformer(resource, sql.c_str(), &registry);
        auto result = local_transformer.transform(pg_cell_to_node_cast(parse_result)).finalize();
        if (result.has_error()) {
            return result.error();
        }
        return std::move(result.value());
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_execute(actor_zeta::address_t dispatcher_address,
                                                                  std::pmr::memory_resource* /*resource*/,
                                                                  logical_plan::execution_plan_t plan) {
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session::session_id_t(),
                                           std::move(plan))
            .second;
    }
} // namespace otterstax::kafka::detail
