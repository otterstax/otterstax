// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_reader.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <boost/json.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
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
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <optional>

namespace otterstax::kafka::detail {
    using namespace components;
    namespace {
        // A JSON number as int64 when it has an int64 image: a uint64 above
        // INT64_MAX has none and is rejected, never wrapped
        std::optional<std::int64_t> json_integral(const boost::json::value& jv) {
            if (jv.is_int64()) {
                return jv.get_int64();
            }
            if (jv.is_uint64()) {
                const std::uint64_t u = jv.get_uint64();
                if (u > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
                    return std::nullopt;
                }
                return static_cast<std::int64_t>(u);
            }
            return std::nullopt;
        }

        // One JSON field -> logical_value_t of the column's logical_type, or nullopt
        // with the reason when the value cannot be stored (the caller drops the
        // row). A JSON null is a SQL NULL for every type. Non-throwing: every
        // boost::json accessor is called only after its kind check
        struct field_value_t {
            std::optional<types::logical_value_t> value;
            const char* reject{""};
        };

        field_value_t json_field_to_value(std::pmr::memory_resource* resource,
                                          types::logical_type type,
                                          const boost::json::value& jv) {
            if (jv.is_null()) {
                return {types::logical_value_t(resource, nullptr), ""};
            }
            switch (type) {
                case types::logical_type::INTEGER:
                case types::logical_type::BIGINT: {
                    if (!jv.is_int64() && !jv.is_uint64()) {
                        return {std::nullopt, "not an integer"};
                    }
                    const auto integral = json_integral(jv);
                    if (!integral) {
                        return {std::nullopt, "integer out of BIGINT range"};
                    }
                    if (type == types::logical_type::BIGINT) {
                        return {types::logical_value_t(resource, *integral), ""};
                    }
                    if (*integral < std::numeric_limits<std::int32_t>::min() ||
                        *integral > std::numeric_limits<std::int32_t>::max()) {
                        return {std::nullopt, "integer out of INTEGER range"};
                    }
                    return {types::logical_value_t(resource, static_cast<std::int32_t>(*integral)), ""};
                }
                case types::logical_type::UBIGINT: {
                    // The unsigned counterpart of the case above. A stream projecting a
                    // COUNT is declared with a UBIGINT column — that is the type the
                    // engine's count kernel produces — so its own records have to ingest
                    // back into one. A negative number has no reading as UBIGINT and is
                    // rejected, never wrapped around
                    if (!jv.is_int64() && !jv.is_uint64()) {
                        return {std::nullopt, "not an integer"};
                    }
                    if (jv.is_int64() && jv.get_int64() < 0) {
                        return {std::nullopt, "negative integer out of UBIGINT range"};
                    }
                    const std::uint64_t unsigned_value =
                        jv.is_uint64() ? jv.get_uint64() : static_cast<std::uint64_t>(jv.get_int64());
                    return {types::logical_value_t(resource, unsigned_value), ""};
                }
                case types::logical_type::DOUBLE: {
                    if (!jv.is_number()) {
                        return {std::nullopt, "not a number"};
                    }
                    boost::system::error_code ec;
                    const double d = jv.to_number<double>(ec);
                    if (ec) {
                        return {std::nullopt, "number not representable as DOUBLE"};
                    }
                    return {types::logical_value_t(resource, d), ""};
                }
                case types::logical_type::STRING_LITERAL:
                    if (!jv.is_string()) {
                        return {std::nullopt, "not a string"};
                    }
                    return {types::logical_value_t(resource, std::string(jv.as_string().c_str())), ""};
                case types::logical_type::BOOLEAN:
                    if (!jv.is_bool()) {
                        return {std::nullopt, "not a boolean"};
                    }
                    return {types::logical_value_t(resource, jv.as_bool()), ""};
                default:
                    return {std::nullopt, "column type has no JSON mapping"};
            }
        }

        // One chunk's rows as JSON objects keyed by column alias. A column without
        // an alias gets a "colN" key, which can never name a declared column, so
        // such a batch fails the chunk_matches_columns round-trip instead of
        // dereferencing a null alias extension here.
        //
        // A column whose type this writer has no encoding for is a conversion_failure
        // naming it, never a JSON null in its place: the reader below ingests a null
        // back as a SQL NULL, so a record written that way round-trips, passes the
        // write-path guard, and reaches every consumer of the topic with the value
        // simply gone and nothing logged. The types carried are the five a SOURCE can
        // declare plus UBIGINT, which the engine's count kernel introduces into a
        // STREAM's output schema (components/compute/kernels/aggregate.cpp:503, :508)
        core::result_wrapper_t<std::vector<std::string>> chunk_to_json(std::pmr::memory_resource* resource,
                                                                       const vector::data_chunk_t& chunk) {
            OTX_ZONE_N("kafka::chunk_to_json");
            std::vector<std::string> out;
            out.reserve(chunk.size());
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                boost::json::object obj;
                for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
                    const auto& col_type = chunk.data[col].type();
                    const std::string key =
                        col_type.has_alias() ? std::string{col_type.alias()} : "col" + std::to_string(col);
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
                        case types::logical_type::UBIGINT:
                            obj[key] = value.value<std::uint64_t>();
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
                            return core::error_t(
                                core::error_code_t::conversion_failure,
                                std::pmr::string{"kafka: column '" + key + "' (logical type " +
                                                     std::to_string(static_cast<int>(col_type.type())) +
                                                     ") has no JSON encoding",
                                                 resource});
                    }
                }
                out.push_back(boost::json::serialize(obj));
            }
            return out;
        }

        // Serialize the chunk's rows and re-ingest them against the declared
        // columns — the exact check the SOURCE poller applies. A row that doesn't
        // survive (a missing, mis-keyed, or wrong-typed column) would not round-trip
        // into the declared schema, i.e. it is malformed for this object's topic.
        // A chunk the writer cannot serialize at all does not round-trip either, and
        // the callers answer with their own error rather than publishing it
        bool chunk_matches_columns(std::pmr::memory_resource* resource,
                                   const vector::data_chunk_t& chunk,
                                   const std::vector<kafka_column_t>& declared) {
            OTX_ZONE_N("kafka::chunk_matches_columns");
            if (chunk.column_count() != declared.size()) {
                return false; // wrong number of columns (partial / extra)
            }
            auto payloads = chunk_to_json(resource, chunk);
            if (payloads.has_error()) {
                return false; // a column with no JSON encoding: nothing to round-trip
            }
            return json_to_chunk(resource, declared, payloads.value()).size() == chunk.size();
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
        // A dropped row is an operational signal (a producer emitting rows the
        // declared schema cannot hold), so every drop is logged with its reason.
        // The logger is absent in logger-free contexts (microbenchmarks); the
        // conversion itself does not depend on it
        auto log = get_logger(logger_tag::KAFKA_MANAGER);
        auto reject = [&](std::size_t index, const std::string& why) {
            if (log.is_valid()) {
                log->error("kafka: message {} of {} rejected: {}", index, payloads.size(), why);
            }
        };

        // Over-allocate to the payload count; only the fully-valid prefix rows are
        // committed (cardinality), partial writes from a dropped row are overwritten
        // by the next good row or excluded by the final set_cardinality
        vector::data_chunk_t chunk(resource, types, std::max<std::size_t>(payloads.size(), 1));
        std::uint64_t row = 0;
        for (std::size_t index = 0; index < payloads.size(); ++index) {
            boost::system::error_code ec;
            const boost::json::value jv = boost::json::parse(payloads[index], ec);
            if (ec) {
                reject(index, "malformed JSON: " + ec.message());
                continue;
            }
            if (!jv.is_object()) {
                reject(index, "payload is not a JSON object");
                continue;
            }
            const auto& obj = jv.as_object();

            bool row_ok = true;
            for (std::size_t c = 0; c < columns.size(); ++c) {
                const boost::json::value* field = obj.if_contains(columns[c].name);
                if (!field) {
                    reject(index, "missing column '" + columns[c].name + "'");
                    row_ok = false;
                    break;
                }
                auto converted = json_field_to_value(resource, columns[c].type.type(), *field);
                if (!converted.value) {
                    reject(index, "column '" + columns[c].name + "': " + converted.reject);
                    row_ok = false;
                    break;
                }
                chunk.set_value(c, row, *converted.value);
            }
            if (row_ok) {
                ++row;
            }
        }
        chunk.set_cardinality(row);
        return chunk;
    }

    core::result_wrapper_t<std::vector<std::string>>
    chunk_to_json(std::pmr::memory_resource* resource, const std::pmr::vector<vector::data_chunk_t>& chunks) {
        OTX_ZONE_N("kafka::chunk_to_json_multi");
        std::vector<std::string> out;
        for (const auto& chunk : chunks) {
            auto part = chunk_to_json(resource, chunk);
            if (part.has_error()) {
                return part.error();
            }
            auto& rows = part.value();
            out.insert(out.end(), std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end()));
        }
        return out;
    }

    bool chunk_matches_columns(std::pmr::memory_resource* resource,
                               const std::pmr::vector<vector::data_chunk_t>& chunks,
                               const std::vector<kafka_column_t>& declared) {
        OTX_ZONE_N("kafka::chunk_matches_columns_multi");
        for (const auto& chunk : chunks) {
            if (!chunk_matches_columns(resource, chunk, declared)) {
                return false;
            }
        }
        return true;
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

    actor_zeta::unique_future<cursor::cursor_t_ptr> kafka_insert_session(actor_zeta::address_t dispatcher_address,
                                                                         std::pmr::memory_resource* resource,
                                                                         session::session_id_t session,
                                                                         const std::string& database,
                                                                         const std::string& relname,
                                                                         vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::kafka_insert_session");
        // No explicit column list → full-row insert in chunk order (the chunk holds
        // all of the table's columns, in declared order). Mirrors the engine's own
        // INSERT planning (node_insert(chunk) naming its target + a table resolve
        // with the outgoing FK gather). Sent on the caller's `session` so it can
        // share a transaction
        auto insert = logical_plan::make_node_insert(resource, std::move(chunk));
        logical_plan::execution_plan_t plan{resource,
                                            sql::transform::name_catalog_target(database, relname, insert),
                                            logical_plan::make_parameter_node(resource)};
        sql::transform::register_catalog_resolve_table(resource,
                                                       &plan.catalog_resolves,
                                                       database,
                                                       relname,
                                                       sql::transform::constraint_resolve_kind::outgoing);
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session,
                                           std::move(plan))
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
        // `DELETE FROM database.relname WHERE column = value` as the plan shape
        // transform_delete emits for a live DELETE: node_delete over node_match(eq
        // predicate), naming its target table (a node_delete without the name has
        // no resolved target). The value is bound as a parameter, never spliced
        // into SQL text
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
        // unlimit() keeps the delete-all-matching semantics
        auto del = logical_plan::make_node_delete(resource,
                                                  match,
                                                  logical_plan::make_node_limit(resource,
                                                                                core::dbname_t{database},
                                                                                core::relname_t{relname},
                                                                                logical_plan::limit_t::unlimit()));
        logical_plan::execution_plan_t plan{resource,
                                            sql::transform::name_catalog_target(database, relname, del),
                                            std::move(params)};
        // constraint_resolve_kind::referencing mirrors transform_delete (FK cascade to
        // children); __sources has no FKs, so the gather finds nothing here
        sql::transform::register_catalog_resolve_table(resource,
                                                       &plan.catalog_resolves,
                                                       database,
                                                       relname,
                                                       sql::transform::constraint_resolve_kind::referencing);
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session::session_id_t(),
                                           std::move(plan))
            .second;
    }

    std::map<int32_t, int64_t> parse_offsets(const cursor::cursor_t_ptr& cursor) {
        OTX_ZONE_N("kafka::parse_offsets");
        std::map<int32_t, int64_t> result;
        if (!cursor || cursor->is_error()) {
            return result;
        }
        // The result is a batch of <=1024-row chunks; walk each chunk's own row
        // space (a global-row lookup through the cursor is a chunk scan per cell)
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                const int32_t partition = chunk.value(0, row).value<int32_t>();
                const int64_t offset = chunk.value(1, row).value<int64_t>();
                auto it = result.find(partition);
                if (it == result.end() || offset > it->second) {
                    result[partition] = offset;
                }
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
        OTX_ZONE_N("kafka::kafka_execute");
        return actor_zeta::otterbrix::send(dispatcher_address,
                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                           session::session_id_t(),
                                           std::move(plan))
            .second;
    }
} // namespace otterstax::kafka::detail
