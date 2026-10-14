// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>

#include <memory_resource>

// The federation's optimizer rule (components/planner/host_hooks.hpp, stage after_simplify): where
// a subtree of the validated tree touches only tables of ONE remote server and the dialect can say
// it, the subtree becomes one remote statement — a host node (node_extension_t) whose operator
// generates the statement (otterbrix/query_generation/sql_query_generator.hpp) with the statement's
// parameter values when it runs and sends it through the server's backend_t.
//
//  - a query block over one remote table: the aggregate node of the table and its clause children —
//    WHERE, GROUP BY / aggregates, HAVING, ORDER BY, LIMIT / OFFSET, DISTINCT, the projection.
//    Replaced by a source whose columns are the block's output columns as the engine typed them (an
//    integer sum BIGINT, avg DOUBLE, count(*) UBIGINT); the backend's answer is converted to them
//    (remote_storage.hpp, conform_to_result), and an answer without rows is one typed chunk without
//    rows. A table read with no clause stays the storage's own scan.
//  - a query block over a JOIN tree or a derived table, or a JOIN alone, every table of which is a
//    remote table of the same server: the same source, its statement generated over all of the
//    tables (sql_gen::statement_target_t::relations) — the JOINs (INNER / LEFT / RIGHT / CROSS, FULL
//    where the dialect has it) and their ON conditions, the clauses and aggregates above them, the
//    derived tables under them. Tried top down: what the dialect cannot say of a block leaves the
//    block to the engine and its children are tried in turn (a JOIN under an unsayable block is still
//    one statement).
//  - UPDATE / DELETE of a remote table without RETURNING, its WHERE and LIMIT written as the
//    backend can, optionally FROM / USING one plain table of the same server other than the target
//    itself: one remote statement, its count the backend's.
//  - INSERT into a remote table without RETURNING from VALUES or from a query block over a table of
//    the same server: one remote statement naming the columns the statement wrote (the backend's
//    defaults fill the rest).
//
// Whether a subtree can be one statement is decided by generating it with every parameter written
// as `?` (sql_gen::generation_options_t::placeholders); a subtree the dialect cannot say (a function
// the backend has no spelling for, a sub-query result) stays with the engine over the storage's
// scan and sinks. The rule cannot see the parameter values: the host node's operator factory, which
// runs with them, generates the statement and — where a value has no literal in the dialect —
// builds the engine's own plan of the replaced subtree instead (services::planner::create_plan).
// Everything else — a query over two servers or a remote and a local table — runs in the engine over
// the storages (each server's own subtrees pushed). For the INSERT the rule cannot take, it records the
// statement's column list on the storage, so the insert sink names only those columns.
//
// Stateless and reentrant (the engine runs it from several executors at once); it recognizes its
// tables by the storage owner tag (remote_storage.hpp, storage_owner()).
namespace otterstax::federation::rules {

    components::logical_plan::node_ptr pushdown(std::pmr::memory_resource* resource,
                                                components::logical_plan::node_ptr node);

} // namespace otterstax::federation::rules
