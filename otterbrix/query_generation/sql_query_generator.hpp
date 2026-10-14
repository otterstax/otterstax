// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include "otterbrix/parser/parser.hpp"

#include <memory_resource>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

// The SQL OtterStax sends to a remote backend (MySQL, PostgreSQL, ClickHouse),
// written from an otterbrix logical-plan node in the backend's dialect. Two
// callers: the federation's pushdown rule and storage operators
// (otterbrix/federation/), which generate from the tree the engine validated,
// and the query session's remote-DDL route and the discovery probe, which
// generate from the parser's tree.
namespace sql_gen {

    // One column of a remote table: its spelling on the backend and its engine
    // type (the declared type of the federation metadata, whole: a DECIMAL with
    // its width and scale).
    struct remote_column_t {
        std::string name;
        components::types::complex_logical_type type{components::types::logical_type::NA};
    };

    // engine column name (lower case, as the engine binds it) -> the column.
    using column_map_t = std::unordered_map<std::string, remote_column_t>;

    // One table of a query over several tables of one server (a JOIN, a derived
    // table): the plan node that reads it (an aggregate naming the table), its
    // name in the backend's spelling, how its columns are spelled there and every
    // column, backend spelling, in the declared order (the node's output columns).
    struct relation_t {
        const components::logical_plan::node_t* node{nullptr};
        qualified_name_t name;
        const column_map_t* columns{nullptr};
        std::span<const std::string> all_columns{};
    };

    // The table(s) one statement names, in the backend's spelling (MySQL /
    // ClickHouse: database.collection, PostgreSQL: schema.collection).
    struct statement_target_t {
        // The table the statement reads or writes.
        qualified_name_t name;
        // The second name: the FROM / USING source of UPDATE ... FROM /
        // DELETE ... USING, the source of INSERT ... SELECT, the index of
        // DROP INDEX. Empty otherwise.
        qualified_name_t from_name;
        // How the engine's column names are spelled on the backend, per table.
        // A column reference the map does not hold is written as the plan names
        // it. Null: every name as the plan names it.
        const column_map_t* columns{nullptr};
        const column_map_t* from_columns{nullptr};
        // Every column of `name`, backend spelling, declared order: what a
        // SELECT without a projection (or `SELECT *`) is written as when the
        // tree was validated. Empty: `*`.
        std::span<const std::string> all_columns{};
        // The tables of a query whose FROM is a JOIN or a derived table (only
        // over the validated tree): each table read in it, found by its node.
        // Such a query names no `name`; its tables are written `<table> AS tN`,
        // a derived table `(SELECT ...) AS dN`, and every column qualified by the
        // alias of the relation it comes from.
        std::span<const relation_t> relations{};
    };

    struct generation_options_t {
        // The statement's parameter values are not known yet: every parameter is
        // written as `?`. A host optimizer rule uses it to decide whether a
        // subtree is expressible as one remote statement; the text is never sent.
        bool placeholders{false};
        // The tree is the one the engine validated (the federation's pushdown).
        // Its numeric casts — the validator's and the user's are the same node —
        // are written in the dialect's syntax where its CAST does to the value
        // what the engine's does and refused (unimplemented_yet) where none does;
        // only a lossless one (an integer widened, FLOAT into DOUBLE, ...) that is
        // a comparison's operand is written as its operand. A SELECT without a
        // projection lists target.all_columns. A two-table statement trusts the
        // caller for its source table.
        bool validated{false};
    };

    // One identifier quoted for `backend` (`...` MySQL / ClickHouse, "..."
    // PostgreSQL; the quote character doubled inside, and for ClickHouse, which
    // reads a backslash inside a quoted identifier as an escape, the backslash
    // doubled too).
    std::string quote_identifier(std::string_view identifier, backend_type_t backend);

    // Backend SQL for one logical-plan node: a SELECT (an aggregate node and its
    // clause children — over one table, or, over the validated tree, over a JOIN
    // tree or a derived table of `target.relations`; a JOIN node alone is the
    // SELECT of every column it outputs), INSERT, UPDATE, DELETE, CREATE TABLE /
    // INDEX / DATABASE, DROP TABLE / INDEX / DATABASE — over `target`.
    //
    // A JOIN is written as the dialect runs it with the engine's semantics or
    // refused (unimplemented_yet): INNER / LEFT / RIGHT / CROSS everywhere, FULL
    // on PostgreSQL and ClickHouse (MySQL has none); no LATERAL, semi- or
    // anti-join; on ClickHouse the ON condition must be equalities between the
    // two sides AND-ed with conditions on the side whose rows the join does not
    // preserve, and an outer join adds `SETTINGS join_use_nulls = 1` (its
    // default pads with the type's default value, not NULL).
    //
    // Nothing in this module throws across its API: an unsupported dialect
    // feature comes back as unimplemented_yet, a violated pipeline invariant
    // (unbound parameter, malformed node) as invalid_parameter, so a calling
    // coroutine can co_return the error directly. `resource` owns the returned
    // error's message. The statement is terminated by `;`.
    core::result_wrapper_t<std::string> generate_query(const components::logical_plan::node_ptr& node,
                                                       const components::logical_plan::storage_parameters* parameters,
                                                       backend_type_t backend,
                                                       const statement_target_t& target,
                                                       std::pmr::memory_resource* resource,
                                                       generation_options_t options = {});

    // The rows of an INSERT into a remote table: INSERT INTO <table> (<columns>)
    // VALUES (...), ... — `columns[i]` (backend spelling) receives column
    // `column_indexes[i]` of `rows`. `rows` must hold a row and a column.
    core::result_wrapper_t<std::string> generate_insert_rows(backend_type_t backend,
                                                             const qualified_name_t& table,
                                                             std::span<const std::string> columns,
                                                             std::span<const std::size_t> column_indexes,
                                                             const components::vector::data_chunk_t& rows,
                                                             std::pmr::memory_resource* resource);

    // One row of a remote table, changed or removed by its primary key (the
    // batch path of UPDATE / DELETE: the engine picked the rows over the
    // storage's scan):
    //
    //   UPDATE t SET c = v, ... WHERE k1 = o1 AND k2 = o2 ...
    //   DELETE FROM t WHERE k1 = o1 AND k2 = o2 ...
    //
    // `columns` is every column of the table (backend spelling) in the order of
    // `before` / `after`; `key` the positions of the primary key's columns among
    // them. A primary key is unique under its columns' own comparison (collation
    // and pad rules included), so `=` on them names the row the scan read and no
    // other: no LIMIT, no physical row address. SET lists the columns whose value
    // changed; when none did, the key's first column is set to its own value, so
    // the statement still names the row and the backend reports it matched.
    // ClickHouse has no key that names one row: unimplemented_yet. An empty key,
    // a position past the row, or a key value that is NULL: invalid_parameter.
    core::result_wrapper_t<std::string> generate_update_row(backend_type_t backend,
                                                            const qualified_name_t& table,
                                                            std::span<const std::string> columns,
                                                            std::span<const std::size_t> key,
                                                            const std::vector<components::types::logical_value_t>& before,
                                                            const std::vector<components::types::logical_value_t>& after,
                                                            std::pmr::memory_resource* resource);
    core::result_wrapper_t<std::string> generate_delete_row(backend_type_t backend,
                                                            const qualified_name_t& table,
                                                            std::span<const std::string> columns,
                                                            std::span<const std::size_t> key,
                                                            const std::vector<components::types::logical_value_t>& before,
                                                            std::pmr::memory_resource* resource);

} // namespace sql_gen
