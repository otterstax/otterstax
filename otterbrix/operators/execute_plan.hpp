// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <otterbrix/otterbrix.hpp>

#include "integration/otterbrix/otterbrix_engine.hpp" // db::otterbrix_engine_ptr + otterbrix::base_otterbrix_t
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "types/otterbrix.hpp"

#include <string>
#include <vector>

class IDataManager {
public:
    virtual ~IDataManager() = default;

    virtual components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) = 0;
    virtual components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) = 0;
    // Output columns of a row-producing statement as the engine's plan
    // validation stamps them (plan-only EXPLAIN: resolve + validate, no scan).
    // The cursor carries one STRUCT in type_data() — the statement's columns,
    // alias = column name, duplicates kept — or the engine's error.
    virtual components::cursor::cursor_t_ptr plan_output_schema(OtterbrixStatementPtr& otterbrix_params) = 0;
    virtual components::cursor::cursor_t_ptr execute_sql(const std::string& query) = 0;
    // On success `out_oid` carries the pg_class oid the engine planner stamped
    // on the create node (pg_catalog is not reachable via plain SQL SELECT).
    virtual components::cursor::cursor_t_ptr
    create_collection(const std::string& database,
                      const std::string& collection,
                      std::vector<components::table::column_definition_t> columns,
                      components::catalog::oid_t& out_oid) = 0;
    virtual components::cursor::cursor_t_ptr create_database(const std::string& database) = 0;
    virtual components::cursor::cursor_t_ptr insert_data(const std::string& database,
                                                         const std::string& collection,
                                                         std::vector<components::table::column_definition_t> columns,
                                                         components::vector::data_chunk_t data) = 0;
    // Column types (alias = column name) and pg_class oid of an existing engine
    // base relation, read off the plan-driven schema probe: one STRUCT in
    // type_data(), the oid in `out_oid`. A relation the engine does not hold
    // carries the engine's own code (table_not_exists / database_not_exists);
    // a VIEW under the name is a schema_error — a mirrored external table is a
    // base relation.
    virtual components::cursor::cursor_t_ptr describe_collection(const std::string& database,
                                                                 const std::string& collection,
                                                                 components::catalog::oid_t& out_oid) = 0;
    // Appends `data` to an EXISTING collection (node_insert under the
    // catalog-resolve wrap); the engine's verdict on a missing collection or a
    // shape mismatch is the answer. Names are taken as given — not lower-cased.
    virtual components::cursor::cursor_t_ptr insert_rows(const std::string& database,
                                                         const std::string& collection,
                                                         components::vector::data_chunk_t data) = 0;
    // DELETE FROM database.collection WHERE column = value; the value is bound
    // as a plan parameter, never spliced into SQL text.
    virtual components::cursor::cursor_t_ptr delete_rows(const std::string& database,
                                                         const std::string& collection,
                                                         const std::string& column,
                                                         components::types::logical_value_t value) = 0;
};

class OtterbrixDataManager : public IDataManager {
public:
    explicit OtterbrixDataManager(otterbrix::base_otterbrix_t* engine);

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override;
    components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) override;
    components::cursor::cursor_t_ptr plan_output_schema(OtterbrixStatementPtr& otterbrix_params) override;
    components::cursor::cursor_t_ptr execute_sql(const std::string& query) override;
    components::cursor::cursor_t_ptr create_collection(const std::string& database,
                                                       const std::string& collection,
                                                       std::vector<components::table::column_definition_t> columns,
                                                       components::catalog::oid_t& out_oid) override;
    components::cursor::cursor_t_ptr create_database(const std::string& database) override;
    components::cursor::cursor_t_ptr insert_data(const std::string& database,
                                                 const std::string& collection,
                                                 std::vector<components::table::column_definition_t> columns,
                                                 components::vector::data_chunk_t data) override;
    components::cursor::cursor_t_ptr describe_collection(const std::string& database,
                                                         const std::string& collection,
                                                         components::catalog::oid_t& out_oid) override;
    components::cursor::cursor_t_ptr insert_rows(const std::string& database,
                                                 const std::string& collection,
                                                 components::vector::data_chunk_t data) override;
    components::cursor::cursor_t_ptr delete_rows(const std::string& database,
                                                 const std::string& collection,
                                                 const std::string& column,
                                                 components::types::logical_value_t value) override;

private:
    // Non-owning. Held as base_otterbrix_t*; the owning intrusive_ptr is kept by
    // the caller (ComponentManager::engine_ / the test's local) which outlives
    // this manager.
    otterbrix::base_otterbrix_t* otterbrix_;
};

using data_manager_ptr = std::unique_ptr<OtterbrixDataManager>;

// The engine handle is db::otterbrix_engine_t (base_otterbrix_t plus the
// dispatcher-address accessor for the kafka runtime); build it via
// db::make_otterbrix_engine.
inline data_manager_ptr make_otterbrix_manager(const db::otterbrix_engine_ptr& engine) {
    return std::make_unique<OtterbrixDataManager>(engine.get());
}
