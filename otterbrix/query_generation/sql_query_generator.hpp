// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"

#include <memory_resource>
#include <vector>

namespace sql_gen {

    void generate_values(std::stringstream& stream,
                         const components::vector::data_chunk_t& chunk,
                         backend_type_t backend = backend_type_t::MySQL);

    // Generate table reference string based on backend type
    // MySQL: database.collection
    // PostgreSQL: schema.collection (e.g., public.products)
    std::string table_reference(const qualified_name_t& name, backend_type_t backend = backend_type_t::MySQL);

    std::string replace_qualifiers(std::string raw_sql,
                                   const std::vector<otterstax::parser::qualifier_rewrite_t>& quals,
                                   backend_type_t backend);

    // Generate backend SQL for an external logical-plan node.
    //
    // `target` is THE resolved entry for `node`:
    //   - target.name      — full alias.db.schema.table the statement operates on
    //   - target.from_name — secondary table for UPDATE ... FROM / DELETE ... USING
    //                        (empty otherwise; the clause is omitted when empty)
    // `batch` carries the whole batch's external entries with their resolved
    // targets (oids stamped by CatalogManager before SQL generation). It is
    // used to resolve the inner SELECT table of INSERT ... SELECT via the
    // child aggregate's table_oid(); it may be empty for probe queries that
    // have no INSERT ... SELECT shape.
    void generate_query(std::stringstream& stream,
                        const components::logical_plan::node_ptr& node,
                        const components::logical_plan::storage_parameters* parameters,
                        backend_type_t backend,
                        const otterstax::names::resolved_target_t& target,
                        const std::pmr::vector<external_entry_t>& batch);
    std::string generate_query(const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* parameters,
                               backend_type_t backend,
                               const otterstax::names::resolved_target_t& target,
                               const std::pmr::vector<external_entry_t>& batch);

    // Engine-dialect (Otterbrix, double-quoted identifiers) database-level
    // statements used by OtterbrixManager when mirroring external connections
    // into the engine catalog.
    std::string create_database_statement(const std::string& db);
    std::string drop_database_statement(const std::string& db);

} // namespace sql_gen