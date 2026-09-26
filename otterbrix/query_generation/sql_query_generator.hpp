// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"

#include <memory_resource>
#include <sstream>
#include <vector>

namespace sql_gen {

    // VALUES (...), (...) for every row of `chunk`, literals in the dialect of
    // `backend`. `resource` owns the returned error's message.
    core::error_t generate_values(std::stringstream& stream,
                                  const components::vector::data_chunk_t& chunk,
                                  backend_type_t backend,
                                  std::pmr::memory_resource* resource);

    // Table reference in the dialect of `backend`:
    //   MySQL / ClickHouse: database.collection
    //   PostgreSQL: schema.collection (e.g., public.products; an empty schema is public)
    // Only those three backends have a dialect: any other backend value, and an
    // empty name, is invalid_parameter. `resource` owns the error's message.
    core::result_wrapper_t<std::string>
    table_reference(const qualified_name_t& name, backend_type_t backend, std::pmr::memory_resource* resource);

    // Rewrites every 4-part qualifier slot of a raw subquery into the backend's
    // table reference. A slot outside the SQL text (negative start, non-positive
    // length, or an end past the text) breaks the extractor's contract and is
    // invalid_parameter — a qualifier left in place would reach the backend as
    // an unknown name. A slot table_reference refuses (a backend without a
    // dialect, an empty name) fails the rewrite with that error. Error messages
    // are owned by `resource`.
    core::result_wrapper_t<std::string>
    replace_qualifiers(std::string_view raw_sql,
                       const std::pmr::vector<otterstax::parser::qualifier_rewrite_t>& quals,
                       backend_type_t backend,
                       std::pmr::memory_resource* resource);

    // Generate backend SQL for an external logical-plan node.
    //
    // `target` is THE resolved entry for `node`:
    //   - target.name      — full alias.db.schema.table the statement operates on
    //   - target.from_name — secondary table for UPDATE ... FROM / DELETE ... USING
    //                        and the index of DROP INDEX (empty otherwise; the
    //                        clause is omitted when empty)
    // `batch` carries the whole batch's external entries with their resolved
    // targets (oids stamped by CatalogManager before SQL generation). It is
    // used to resolve the inner SELECT table of INSERT ... SELECT via the
    // child aggregate's table_oid(); it may be empty for probe queries that
    // have no INSERT ... SELECT shape.
    // `resource` owns the returned error's message. Nothing in this module throws
    // across its API: an unsupported dialect feature comes back as
    // unimplemented_yet, a violated pipeline invariant (unbound parameter,
    // unstamped oid, malformed node) as invalid_parameter, so a calling actor
    // coroutine can co_return the error directly instead of catching and
    // re-wrapping. Only MySQL, PostgreSQL and ClickHouse are dialects the
    // generator emits; any other backend value is invalid_parameter.
    core::error_t generate_query(std::stringstream& stream,
                                 const components::logical_plan::node_ptr& node,
                                 const components::logical_plan::storage_parameters* parameters,
                                 backend_type_t backend,
                                 const otterstax::names::resolved_target_t& target,
                                 const std::pmr::vector<external_entry_t>& batch,
                                 std::pmr::memory_resource* resource);
    core::result_wrapper_t<std::string> generate_query(const components::logical_plan::node_ptr& node,
                                                       const components::logical_plan::storage_parameters* parameters,
                                                       backend_type_t backend,
                                                       const otterstax::names::resolved_target_t& target,
                                                       const std::pmr::vector<external_entry_t>& batch,
                                                       std::pmr::memory_resource* resource);

    // Engine-dialect (Otterbrix, double-quoted identifiers) statements used by
    // OtterbrixManager when mirroring external connections into the engine
    // catalog: the per-uid database, the drop of one mirrored collection
    // (`collection` is the already-encoded engine-side name), and the read of
    // one column of a collection (the per-uid manifest of mirrored tables).
    std::string create_database_statement(const std::string& db);
    std::string drop_table_statement(const std::string& db, const std::string& collection);
    std::string
    select_column_statement(const std::string& db, const std::string& collection, const std::string& column);

} // namespace sql_gen
