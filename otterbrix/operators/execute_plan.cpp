// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "execute_plan.hpp"

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components;
OtterbrixDataManager::OtterbrixDataManager(otterbrix::otterbrix_ptr otterbrix)
    : otterbrix_(otterbrix) {}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_plan(OtterbrixStatementPtr& otterbrix_params) {
    // Otterbrix get response and parse

    return otterbrix_->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        components::logical_plan::execution_plan_t(otterbrix_params->node->resource(),
                                                   otterbrix_params->node,
                                                   otterbrix_params->params_node));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::get_schema(const OtterbrixSchemaParams& otterbrix_params) {
    // Otterbrix get internal collection schema.
    //
    // a13 removed wrapper_dispatcher_t::get_schema(); instead probe every
    // dependency with a `LIMIT 0` query — the engine fills the cursor's
    // type_data() even when the result set is empty. The input vector is
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
        auto cursor = otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(),
                                                            "SELECT * FROM " + database + "." + collection +
                                                                " LIMIT 0;");
        if (!cursor) {
            return cursor::make_cursor(
                resource,
                core::error_t(core::error_code_t::other_error,
                              std::pmr::string{"get_schema: null cursor for " + database + "." + collection,
                                               resource}));
        }
        if (cursor->is_error()) {
            return cursor;
        }
        table_schemas.emplace_back(types::complex_logical_type::create_struct("", cursor->type_data()));
    }
    return cursor::make_cursor(resource, std::move(table_schemas));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_sql(const std::string& query) {
    return otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(), query);
}

components::cursor::cursor_t_ptr
OtterbrixDataManager::create_collection(const std::string& database,
                                        const std::string& collection,
                                        std::vector<components::table::column_definition_t> columns,
                                        components::catalog::oid_t& out_oid) {
    // Same plan shape as wrapper_dispatcher::create_collection, but built here
    // so we keep the node and can read the planner-stamped table_oid off it —
    // pg_catalog is not reachable via plain SQL SELECT.
    out_oid = components::catalog::INVALID_OID;
    auto* resource = otterbrix_->dispatcher()->resource();
    auto create = logical_plan::make_node_create_collection(resource,
                                                            core::relname_t{collection},
                                                            std::move(columns),
                                                            {});
    logical_plan::node_ptr node =
        sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource, database, create);
    auto cursor = otterbrix_->dispatcher()->execute_plan(
        otterbrix::session_id_t(),
        logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)});
    if (cursor && !cursor->is_error()) {
        out_oid = create->table_oid();
    }
    return cursor;
}