// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "remote_metadata.hpp"

#include "otterbrix/federation/remote_storage.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/operators/schema_probe.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <set>
#include <span>
#include <utility>

using namespace components;
namespace fed = otterstax::federation;

namespace otterstax::catalog {

    namespace {

        using plans_t = std::pmr::vector<logical_plan::execution_plan_t>;
        using row_t = std::vector<std::string>; // one value per column; the ordinal as decimal text

        using tsl::make_error;

        const std::string& database() {
            static const std::string name{fed::metadata_database};
            return name;
        }

        // The manifest collection `<uid>.__otterstax_tables` a previous OtterStax kept in its mirror
        // database of a connection (make_legacy_mirror_probe).
        constexpr std::string_view legacy_mirror_manifest = "__otterstax_tables";

        // Every metadata column is TEXT but remote_columns' ordinal and key_position, which are BIGINT.
        template<typename Layout>
        std::pmr::vector<types::complex_logical_type> column_types(std::pmr::memory_resource* resource,
                                                                    std::string_view table) {
            std::pmr::vector<types::complex_logical_type> out{resource};
            for (std::size_t i = 0; i < Layout::count; ++i) {
                const bool bigint =
                    table == fed::remote_columns_table &&
                    (i == fed::remote_columns_layout::ordinal || i == fed::remote_columns_layout::key_position);
                out.emplace_back(bigint ? types::logical_type::BIGINT : types::logical_type::STRING_LITERAL,
                                 std::string{Layout::names[i]});
            }
            return out;
        }

        template<typename Layout>
        logical_plan::execution_plan_t make_create_table(std::pmr::memory_resource* resource, std::string_view table) {
            std::vector<components::table::column_definition_t> columns;
            for (auto& type : column_types<Layout>(resource, table)) {
                columns.emplace_back(type.alias(), type);
            }
            const std::string relname{table};
            auto create = logical_plan::make_node_create_collection(resource,
                                                                    core::relname_t{relname},
                                                                    std::move(columns),
                                                                    {},
                                                                    /*if_not_exists*/ true);
            auto node = sql::transform::name_catalog_target(core::dbname_t{database()}, core::relname_t{relname}, create);
            logical_plan::execution_plan_t plan{resource, node, logical_plan::make_parameter_node(resource)};
            sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, database());
            return plan;
        }

        // INSERT of full rows; the rows go in as chunks within DEFAULT_VECTOR_CAPACITY.
        template<typename Layout>
        logical_plan::execution_plan_t
        make_insert(std::pmr::memory_resource* resource, std::string_view table, const std::vector<row_t>& rows) {
            const auto types = column_types<Layout>(resource, table);
            std::pmr::vector<vector::data_chunk_t> chunks{resource};
            for (std::size_t first = 0; first < rows.size(); first += vector::DEFAULT_VECTOR_CAPACITY) {
                const auto count = std::min<std::size_t>(rows.size() - first, vector::DEFAULT_VECTOR_CAPACITY);
                vector::data_chunk_t chunk(resource, types, count);
                chunk.set_cardinality(count);
                for (std::size_t row = 0; row < count; ++row) {
                    const auto& values = rows[first + row];
                    for (std::size_t column = 0; column < Layout::count; ++column) {
                        if (types[column].type() == types::logical_type::BIGINT) {
                            chunk.set_value(column,
                                            row,
                                            types::logical_value_t(resource, std::int64_t{std::stoll(values[column])}));
                        } else {
                            chunk.set_value(column, row, types::logical_value_t(resource, values[column]));
                        }
                    }
                }
                chunks.push_back(std::move(chunk));
            }
            const std::string relname{table};
            auto insert = logical_plan::make_node_insert(resource,
                                                         std::move(chunks),
                                                         std::pmr::vector<expressions::key_t>(resource));
            auto node = sql::transform::name_catalog_target(core::dbname_t{database()}, core::relname_t{relname}, insert);
            logical_plan::execution_plan_t plan{resource, node, logical_plan::make_parameter_node(resource)};
            sql::transform::register_catalog_resolve_table(resource,
                                                           &plan.catalog_resolves,
                                                           database(),
                                                           relname,
                                                           sql::transform::constraint_resolve_kind::outgoing);
            return plan;
        }

        // DELETE FROM federation.<table> WHERE <column> = <value> AND ...: the engine requests' one
        // builder of such a plan, every value a bound parameter.
        template<typename Layout>
        logical_plan::execution_plan_t
        make_delete(std::pmr::memory_resource* resource,
                    std::string_view table,
                    std::span<const std::pair<std::size_t, std::string>> where) {
            std::vector<otterstax::engine::column_equals_t> equals;
            equals.reserve(where.size());
            for (const auto& [column, value] : where) {
                equals.push_back(otterstax::engine::column_equals_t{std::string{Layout::names[column]},
                                                                    types::logical_value_t(resource, value)});
            }
            return otterstax::engine::delete_rows_plan(resource, database(), std::string{table}, equals);
        }

        using where_t = std::vector<std::pair<std::size_t, std::string>>;

        // The key columns of a table key, for the two tables keyed alike.
        template<typename Layout>
        where_t key_where(const fed::table_key_t& key) {
            return where_t{{Layout::uid, key.uid},
                           {Layout::database_name, key.database},
                           {Layout::schema_name, key.schema},
                           {Layout::table_name, key.table}};
        }

        void append_removal(std::pmr::memory_resource* resource, plans_t& plans, const where_t& tables_where, const where_t& columns_where) {
            plans.push_back(make_delete<fed::remote_columns_layout>(resource, fed::remote_columns_table, columns_where));
            plans.push_back(make_delete<fed::remote_tables_layout>(resource, fed::remote_tables_table, tables_where));
        }

        // The column_type text: the engine's own type spec (pg_attribute.atttypspec,
        // components::catalog::encode_type_spec), which leaves a built-in scalar empty — pg_attribute
        // keys it by atttypid — so a scalar is written as its pg_type name, the spelling the spec
        // itself uses for a nested scalar. decode_type_spec reads both. A type the spec cannot
        // carry does not decode back and is refused.
        core::result_wrapper_t<std::string> column_type_text(std::pmr::memory_resource* resource,
                                                             const types::complex_logical_type& type) {
            auto text = components::catalog::encode_type_spec(type);
            if (text.empty()) {
                text = std::string{components::catalog::logical_type_to_pg_name(type.type())};
            }
            if (components::catalog::decode_type_spec(resource, text).has_error()) {
                return make_error(resource,
                                  core::error_code_t::conversion_failure,
                                  "column type " + std::to_string(static_cast<int>(type.type())) +
                                      " has no text form in federation.remote_columns");
            }
            return text;
        }

    } // namespace

    plans_t make_metadata_bootstrap(std::pmr::memory_resource* resource) {
        OTX_ZONE_N("catalog::make_metadata_bootstrap");
        plans_t plans{resource};
        auto create_db = logical_plan::make_node_create_database(resource, core::dbname_t{database()}, /*if_not_exists*/ true);
        plans.emplace_back(resource, create_db, logical_plan::make_parameter_node(resource));
        plans.push_back(make_create_table<fed::remote_tables_layout>(resource, fed::remote_tables_table));
        plans.push_back(make_create_table<fed::remote_columns_layout>(resource, fed::remote_columns_table));
        return plans;
    }

    plans_t make_other_kind_removal(std::pmr::memory_resource* resource, std::string_view uid, backend_type_t kind) {
        OTX_ZONE_N("catalog::make_other_kind_removal");
        // Keyed by the folded uid, as remote_tables / remote_columns are.
        const auto folded = fed::fold_identifier(uid);
        plans_t plans{resource};
        // The alias now names a server of this kind: rows a discovery wrote while it named another
        // kind are keyed another way (a PostgreSQL table by schema, a MySQL / ClickHouse one by
        // database), so no discovery of this kind replaces them. They go here.
        for (const auto other : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
            if (other == kind) {
                continue;
            }
            const std::string other_name{fed::backend_kind_name(other)};
            append_removal(resource,
                           plans,
                           where_t{{fed::remote_tables_layout::uid, folded}, {fed::remote_tables_layout::backend, other_name}},
                           where_t{{fed::remote_columns_layout::uid, folded}, {fed::remote_columns_layout::backend, other_name}});
        }
        return plans;
    }

    core::result_wrapper_t<plans_t> make_discovery_upsert(std::pmr::memory_resource* resource,
                                                          backend_type_t kind,
                                                          const qualified_name_t& scope,
                                                          const fed::discovered_tables_t& tables) {
        OTX_ZONE_N("catalog::make_discovery_upsert");
        const std::string backend{fed::backend_kind_name(kind)};
        plans_t plans{resource};

        std::vector<row_t> table_rows;
        std::vector<row_t> column_rows;
        std::vector<fed::table_key_t> keys;
        for (const auto& table : tables) {
            const auto& name = table.name;
            auto key = fed::make_table_key(kind,
                                           name.unique_identifier.t,
                                           name.database.t,
                                           name.schema.t,
                                           name.collection.t);
            if (std::find(keys.begin(), keys.end(), key) != keys.end()) {
                continue;
            }
            table_rows.push_back(row_t{key.uid,
                                       key.database,
                                       key.schema,
                                       key.table,
                                       name.database.t,
                                       name.schema.t,
                                       name.collection.t,
                                       backend});
            const auto& fields = table.schema.child_types();
            // Every key column must be a column of the table.
            for (const auto& key_column : table.primary_key) {
                const bool known = std::any_of(fields.begin(), fields.end(), [&key_column](const auto& field) {
                    return field.has_alias() && field.alias() == key_column;
                });
                if (!known) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "Discovered table '" + fed::display_name(key) + "' has primary key column '" +
                                          key_column + "' that is not one of its columns");
                }
            }
            std::set<std::string> seen;
            for (std::size_t i = 0; i < fields.size(); ++i) {
                const auto& field = fields[i];
                if (!field.has_alias() || field.alias().empty()) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "Discovered table '" + fed::display_name(key) +
                                          "' has a column without a name at position " + std::to_string(i));
                }
                auto column_name = fed::fold_identifier(field.alias());
                if (!seen.insert(column_name).second) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "Discovered table '" + fed::display_name(key) + "' has two columns named '" +
                                          column_name + "' once folded to lower case");
                }
                auto type_text = column_type_text(resource, field);
                if (type_text.has_error()) {
                    return make_error(resource,
                                      type_text.error().type,
                                      "column '" + field.alias() + "' of discovered table '" + fed::display_name(key) +
                                          "': " + type_text.error().what.c_str());
                }
                const auto in_key = std::find(table.primary_key.begin(), table.primary_key.end(), field.alias());
                const auto key_position =
                    in_key == table.primary_key.end() ? 0 : (in_key - table.primary_key.begin()) + 1;
                column_rows.push_back(row_t{key.uid,
                                            key.database,
                                            key.schema,
                                            key.table,
                                            std::move(column_name),
                                            field.alias(),
                                            std::move(type_text.value()),
                                            std::to_string(i + 1),
                                            backend,
                                            name.database.t,
                                            name.schema.t,
                                            name.collection.t,
                                            std::to_string(key_position)});
            }
            keys.push_back(std::move(key));
        }

        // What the reply replaces: the whole database / schema, or the one table asked for.
        if (scope.collection.t.empty()) {
            auto scope_key =
                fed::make_table_key(kind, scope.unique_identifier.t, scope.database.t, scope.schema.t, std::string_view{});
            const bool by_schema = kind == backend_type_t::PostgreSQL;
            const where_t tables_where{
                {fed::remote_tables_layout::uid, scope_key.uid},
                by_schema ? std::pair{std::size_t{fed::remote_tables_layout::schema_name}, scope_key.schema}
                          : std::pair{std::size_t{fed::remote_tables_layout::database_name}, scope_key.database}};
            const where_t columns_where{
                {fed::remote_columns_layout::uid, scope_key.uid},
                by_schema ? std::pair{std::size_t{fed::remote_columns_layout::schema_name}, scope_key.schema}
                          : std::pair{std::size_t{fed::remote_columns_layout::database_name}, scope_key.database}};
            append_removal(resource, plans, tables_where, columns_where);
        } else {
            const auto scope_key = fed::make_table_key(kind,
                                                       scope.unique_identifier.t,
                                                       scope.database.t,
                                                       scope.schema.t,
                                                       scope.collection.t);
            append_removal(resource,
                           plans,
                           key_where<fed::remote_tables_layout>(scope_key),
                           key_where<fed::remote_columns_layout>(scope_key));
            // A reply for one table may name it in the backend's spelling under another key only
            // if it folds the same way; anything else it holds is replaced too.
            for (const auto& key : keys) {
                if (key != scope_key) {
                    append_removal(resource,
                                   plans,
                                   key_where<fed::remote_tables_layout>(key),
                                   key_where<fed::remote_columns_layout>(key));
                }
            }
        }

        if (!table_rows.empty()) {
            plans.push_back(make_insert<fed::remote_tables_layout>(resource, fed::remote_tables_table, table_rows));
        }
        if (!column_rows.empty()) {
            plans.push_back(make_insert<fed::remote_columns_layout>(resource, fed::remote_columns_table, column_rows));
        }
        return plans;
    }

    logical_plan::execution_plan_t make_remote_table_read(std::pmr::memory_resource* resource,
                                                          const fed::table_key_t& key) {
        OTX_ZONE_N("catalog::make_remote_table_read");
        using layout = fed::remote_tables_layout;
        const qualified_name_t target{core::dbname_t{database()},
                                      core::relname_t{std::string{fed::remote_tables_table}}};
        auto params = logical_plan::make_parameter_node(resource);
        auto where = expressions::make_compare_union_expression(resource, expressions::compare_type::union_and);
        for (const auto& [column, value] : key_where<layout>(key)) {
            const auto id = params->add_parameter(types::logical_value_t(resource, value));
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

    core::result_wrapper_t<logical_plan::execution_plan_t> make_legacy_mirror_probe(std::pmr::memory_resource* resource,
                                                                                    std::string_view uid) {
        OTX_ZONE_N("catalog::make_legacy_mirror_probe");
        auto probe = schema_probe::make_table_probe(resource, std::string{uid}, std::string{legacy_mirror_manifest});
        return probe.execution_plan(resource);
    }

    core::result_wrapper_t<logical_plan::execution_plan_t> make_legacy_mirror_drop(std::pmr::memory_resource* resource,
                                                                                   std::string_view uid) {
        OTX_ZONE_N("catalog::make_legacy_mirror_drop");
        // The uid as a quoted identifier (the engine reads PostgreSQL's), as the mirror spelled it.
        return otterstax::engine::sql_plan(
            resource,
            "DROP DATABASE " + sql_gen::quote_identifier(uid, backend_type_t::PostgreSQL) + ";");
    }

} // namespace otterstax::catalog
