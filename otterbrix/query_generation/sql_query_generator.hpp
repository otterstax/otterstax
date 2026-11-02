// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// The SQL OtterStax sends to a remote backend (MySQL, PostgreSQL, ClickHouse), written in the
// backend's dialect. The caller is the host's backend actor (otterbrix/federation/): it generates
// from the blocks otterbrix offers it (storage_offer: a detached copy of a validated sub-tree, whose
// nodes read their table through table_oid() and carry no table metadata — every name and column
// reaches the generator through statement_target_t / relation_t), the scans of a table
// (generate_table_read), the rows of an INSERT (generate_insert_rows) and a client's DDL of a remote
// table (generate_create_table / drop / add / drop column). The discovery probe
// (catalog/discovery.hpp) generates a SELECT from a hand-built node.
namespace sql_gen {

    // One column of a remote table: its spelling on the backend and its engine type (the declared
    // type, whole: a DECIMAL with its width and scale).
    struct remote_column_t {
        std::string name;
        components::types::complex_logical_type type{components::types::logical_type::NA};
    };

    // engine column name (lower case, as otterbrix binds it) -> the column.
    using column_map_t = std::unordered_map<std::string, remote_column_t>;

    // One table of a query over several tables of one server (a JOIN, a derived table): the block's
    // node that reads it (an aggregate naming the table by table_oid()), its name in the backend's
    // spelling, how its columns are spelled there and every column, backend spelling, in the
    // declared order (detached_table_t::columns). A leaf whose projected_cols() is not empty outputs
    // only those columns, in that order.
    struct relation_t {
        const components::logical_plan::node_t* node{nullptr};
        qualified_name_t name;
        const column_map_t* columns{nullptr};
        std::span<const std::string> all_columns{};
    };

    // The table(s) one statement names, in the backend's spelling: MySQL / ClickHouse
    // {"", database, "", table}, PostgreSQL {"", "", schema, table}.
    struct statement_target_t {
        // The table the statement reads or writes.
        qualified_name_t name;
        // The second name: the FROM / USING source of UPDATE ... FROM / DELETE ... USING, the source
        // of INSERT ... SELECT, the index of DROP INDEX. Empty otherwise.
        qualified_name_t from_name;
        // How the engine's column names are spelled on the backend, per table. A column reference
        // the map does not hold is written as the plan names it. Null: every name as the plan
        // names it.
        const column_map_t* columns{nullptr};
        const column_map_t* from_columns{nullptr};
        // Every column of `name`, backend spelling, declared order: what a SELECT without a
        // projection (or `SELECT *`) is written as, and what RETURNING lists. Empty: `*`.
        std::span<const std::string> all_columns{};
        // The tables of a query whose FROM is a JOIN or a derived table: each table read in it,
        // found by its node. Such a query names no `name`; its tables are written `<table> AS tN`,
        // a derived table `(SELECT ...) AS dN`, and every column qualified by the alias of the
        // relation it comes from.
        std::span<const relation_t> relations{};
    };

    struct generation_options_t {
        // The statement's parameter values are not known yet: every parameter is written as `?`.
        // The actor's storage_offer uses it to decide whether a node is expressible as one remote
        // statement; the text is never sent.
        bool placeholders{false};
        // The tree is the one otterbrix validated (a detached block). Its numeric casts — the
        // validator's and the user's are the same node — are written in the dialect's syntax where
        // its CAST does to the value what the engine's does and refused (unimplemented_yet) where
        // none does; only a lossless one that is a comparison's operand is written as its operand.
        // A SELECT without a projection lists target.all_columns. A two-table statement trusts the
        // caller for its source table.
        bool validated{true};
        // UPDATE / DELETE: ` RETURNING <target.all_columns>` (every column of each changed row, the
        // declared order) is appended. PostgreSQL only; unimplemented_yet elsewhere.
        bool returning{false};
    };

    // One identifier quoted for `backend` (`...` MySQL / ClickHouse, "..." PostgreSQL; the quote
    // character doubled inside, and for ClickHouse, which reads a backslash inside a quoted
    // identifier as an escape, the backslash doubled too).
    std::string quote_identifier(std::string_view identifier, backend_type_t backend);

    // Backend SQL for one logical-plan node: a SELECT (an aggregate node and its clause children —
    // over one table, or over a JOIN tree or a derived table of `target.relations`; a JOIN node
    // alone is the SELECT of every column it outputs), INSERT, UPDATE, DELETE, CREATE TABLE / INDEX
    // / DATABASE, DROP TABLE / INDEX / DATABASE — over `target`.
    //
    // A JOIN is written as the dialect runs it with the engine's semantics or refused
    // (unimplemented_yet): INNER / LEFT / RIGHT / CROSS everywhere, FULL on PostgreSQL and
    // ClickHouse (MySQL has none); no LATERAL, semi- or anti-join; on ClickHouse the ON condition
    // must be equalities between the two sides AND-ed with conditions on the side whose rows the
    // join does not preserve, and an outer join adds `SETTINGS join_use_nulls = 1`.
    //
    // Nothing in this module throws across its API: an unsupported dialect feature comes back as
    // unimplemented_yet, a violated plan invariant (unbound parameter, malformed node) as
    // invalid_parameter. `resource` owns the returned error's message. The statement is terminated
    // by `;`.
    core::result_wrapper_t<std::string> generate_query(const components::logical_plan::node_ptr& node,
                                                       const components::logical_plan::storage_parameters* parameters,
                                                       backend_type_t backend,
                                                       const statement_target_t& target,
                                                       std::pmr::memory_resource* resource,
                                                       generation_options_t options = {});

    // A read of one table: SELECT <columns> FROM <target.name> [WHERE w1 AND w2 ...] [LIMIT n];
    // `columns` are positions in target.all_columns (empty: every column); `where` the conjuncts of a
    // block's WHERE (their columns named through target.columns); `limit` < 0: no LIMIT.
    core::result_wrapper_t<std::string>
    generate_table_read(backend_type_t backend,
                        const statement_target_t& target,
                        std::span<const std::size_t> columns,
                        std::span<const components::expressions::expression_ptr> where,
                        const components::logical_plan::storage_parameters* parameters,
                        std::int64_t limit,
                        std::pmr::memory_resource* resource,
                        generation_options_t options = {});

    // The rows of an INSERT into a remote table: INSERT INTO <table> (<columns>) VALUES (...), ...
    // — `columns[i]` (backend spelling) receives column `column_indexes[i]` of `rows`. `rows` must
    // hold a row and a column. `returning_columns` not empty: ` RETURNING <them>` (PostgreSQL only,
    // unimplemented_yet elsewhere).
    core::result_wrapper_t<std::string> generate_insert_rows(backend_type_t backend,
                                                             const qualified_name_t& table,
                                                             std::span<const std::string> columns,
                                                             std::span<const std::size_t> column_indexes,
                                                             const components::vector::data_chunk_t& rows,
                                                             std::pmr::memory_resource* resource,
                                                             std::span<const std::string> returning_columns = {});

    // A client's DDL of a remote table, in the backend's dialect (ClickHouse tables are MergeTree
    // ordered by tuple()).
    core::result_wrapper_t<std::string> generate_create_table(backend_type_t backend,
                                                              const qualified_name_t& table,
                                                              std::span<const remote_column_t> columns,
                                                              std::pmr::memory_resource* resource);
    core::result_wrapper_t<std::string>
    generate_drop_table(backend_type_t backend, const qualified_name_t& table, std::pmr::memory_resource* resource);
    core::result_wrapper_t<std::string> generate_add_column(backend_type_t backend,
                                                            const qualified_name_t& table,
                                                            const remote_column_t& column,
                                                            std::pmr::memory_resource* resource);
    core::result_wrapper_t<std::string> generate_drop_column(backend_type_t backend,
                                                             const qualified_name_t& table,
                                                             std::string_view column,
                                                             std::pmr::memory_resource* resource);

} // namespace sql_gen
