// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <otterbrix/otterbrix.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "types/otterbrix.hpp"

#include <string>
#include <vector>

class IDataManager {
public:
    virtual ~IDataManager() = default;

    virtual components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) = 0;
    virtual components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) = 0;
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
};

class OtterbrixDataManager : public IDataManager {
public:
    explicit OtterbrixDataManager(otterbrix::otterbrix_ptr otterbrix);

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override;
    components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) override;
    components::cursor::cursor_t_ptr execute_sql(const std::string& query) override;
    components::cursor::cursor_t_ptr
    create_collection(const std::string& database,
                      const std::string& collection,
                      std::vector<components::table::column_definition_t> columns,
                      components::catalog::oid_t& out_oid) override;
    components::cursor::cursor_t_ptr create_database(const std::string& database) override;
    components::cursor::cursor_t_ptr insert_data(const std::string& database,
                                                 const std::string& collection,
                                                 std::vector<components::table::column_definition_t> columns,
                                                 components::vector::data_chunk_t data) override;

private:
    otterbrix::otterbrix_ptr otterbrix_;
};

using data_manager_ptr = std::unique_ptr<OtterbrixDataManager>;

inline data_manager_ptr make_otterbrix_manager(otterbrix::otterbrix_ptr otterbrix) {
    return std::make_unique<OtterbrixDataManager>(std::move(otterbrix));
}
