// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "execute_plan.hpp"

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/tracy_profiler.hpp"
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components;

static std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

OtterbrixDataManager::OtterbrixDataManager(otterbrix::base_otterbrix_t* engine)
    : otterbrix_(engine) {}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_plan(OtterbrixStatementPtr& otterbrix_params) {
    OTX_ZONE_N("otterbrix::execute_plan");

    return otterbrix_->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        components::logical_plan::execution_plan_t(otterbrix_params->node->resource(),
                                                   otterbrix_params->node,
                                                   otterbrix_params->params_node));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::get_schema(const OtterbrixSchemaParams& otterbrix_params) {
    OTX_ZONE_N("otterbrix::get_schema_impl");
    // Otterbrix get internal collection schema.
    //
    // a13 removed wrapper_dispatcher_t::get_schema(); instead probe every
    // dependency with a bounded query (LIMIT 1, falling back to an unlimited
    // read for empty tables — see the probe below). The input vector is
    // already in dependency-index order (built by OtterbrixManager), so
    // result->type_data()[i] is the STRUCT schema of dependency i — the
    // layout schema_utils::compute_* consumers index into.
    auto* resource = otterbrix_->dispatcher()->resource();
    std::pmr::vector<types::complex_logical_type> table_schemas(resource);
    table_schemas.reserve(otterbrix_params.size());
    for (const auto& [database, collection] : otterbrix_params) {
        if (collection.empty()) {
            // External table or unnamed wrapper aggregate — not probeable
            // locally; keep its positional slot with an empty schema.
            table_schemas.emplace_back(
                types::complex_logical_type::create_struct("",
                                                           std::pmr::vector<types::complex_logical_type>(resource)));
            continue;
        }
        // The engine's LIMIT plans over an EMPTY table short-circuit to a bare
        // cursor (no type_data) — an empty local dependency would contribute a
        // zero-column schema to mixed-query JOIN typing. LIMIT 1 bounds the
        // probe on populated tables; bare-but-not-error means the table is
        // empty, so the unlimited re-read is free.
        auto cursor =
            otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(),
                                                  "SELECT * FROM " + database + "." + collection + " LIMIT 1;");
        if (cursor && !cursor->is_error() && cursor->type_data().empty()) {
            cursor = otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(),
                                                           "SELECT * FROM " + database + "." + collection + ";");
        }
        if (!cursor) {
            return cursor::make_cursor(
                resource,
                core::error_t(
                    core::error_code_t::other_error,
                    std::pmr::string{"get_schema: null cursor for " + database + "." + collection, resource}));
        }
        if (cursor->is_error()) {
            return cursor;
        }
        table_schemas.emplace_back(types::complex_logical_type::create_struct("", cursor->type_data()));
    }
    return cursor::make_cursor(resource, std::move(table_schemas));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_sql(const std::string& query) {
    OTX_ZONE_N("otterbrix::execute_sql");

    return otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(), query);
}

components::cursor::cursor_t_ptr
OtterbrixDataManager::create_collection(const std::string& database,
                                        const std::string& collection,
                                        std::vector<components::table::column_definition_t> columns,
                                        components::catalog::oid_t& out_oid) {
    OTX_ZONE_N("otterbrix::create_collection");

    // Same plan shape as wrapper_dispatcher::create_collection, but built here
    // so we keep the node and can read the planner-stamped table_oid off it —
    // pg_catalog is not reachable via plain SQL SELECT.
    out_oid = components::catalog::INVALID_OID;
    auto* resource = otterbrix_->dispatcher()->resource();
    auto create =
        logical_plan::make_node_create_collection(resource, core::relname_t{collection}, std::move(columns), {});
    logical_plan::node_ptr node = sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource, database, create);
    auto cursor = otterbrix_->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)});
    if (cursor && !cursor->is_error()) {
        out_oid = create->table_oid();
    }
    return cursor;
}

components::cursor::cursor_t_ptr OtterbrixDataManager::create_database(const std::string& database) {
    OTX_ZONE_N("otterbrix::create_database");

    // a13 removed wrapper_dispatcher_t::create_database(); route CREATE DATABASE
    // through execute_sql (the same path as register_external_database) so the
    // transformer applies the catalog-resolve wrapping.
    auto cur =
        otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE " + to_lower(database) + ";");
    if (cur && cur->is_error() && cur->get_error().type == core::error_code_t::database_already_exists) {
        return cursor::make_cursor(otterbrix_->dispatcher()->resource());
    }
    return cur;
}

components::cursor::cursor_t_ptr
OtterbrixDataManager::insert_data(const std::string& database,
                                  const std::string& collection,
                                  std::vector<components::table::column_definition_t> columns,
                                  components::vector::data_chunk_t data) {
    OTX_ZONE_N("otterbrix::insert_data");

    const std::string db = to_lower(database);
    const std::string col = to_lower(collection);
    auto* res = otterbrix_->dispatcher()->resource();
    const otterbrix::session_id_t session;

    // a13: create_collection carries only the relname; the database is applied by
    // wrapping with a catalog_resolve_namespace node (mirrors create_collection()).
    auto create = logical_plan::make_node_create_collection(res, core::relname_t{col}, std::move(columns), {});
    auto cc_node = sql::transform::maybe_wrap_with_catalog_resolve_namespace(res, db, create);
    otterbrix_->dispatcher()->execute_plan(
        session,
        logical_plan::execution_plan_t{res, cc_node, logical_plan::make_parameter_node(res)});

    // a13: node_insert no longer carries the target name; wrap with a
    // catalog_resolve_table node so the insert binds to db.col.
    auto insert = logical_plan::make_node_insert(res, std::move(data));
    auto insert_node = sql::transform::maybe_wrap_with_catalog_resolve_table(res, db, col, insert);
    return otterbrix_->dispatcher()->execute_plan(
        session,
        logical_plan::execution_plan_t{res, insert_node, logical_plan::make_parameter_node(res)});
}