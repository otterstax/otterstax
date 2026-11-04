// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

// Requests to the embedded engine, sent straight to its dispatcher
// (otterstax::runtime::engine_t::dispatcher_address(),
// services::dispatcher::manager_dispatcher_t). There is no engine actor of
// otterstax's own in between: every function here either builds a plan and
// sends it, or is a coroutine that sends several and reads their replies.
//
// The coroutines take the memory resource first (it carries the coroutine
// frame) and must be awaited from an actor coroutine — a query session — or
// driven by a test's drive loop: in actor-zeta 2.0 a future
// is co_awaited only inside an actor's await chain. Every one names the engine
// SESSION it runs on: a query session passes its connection's session, so the
// statement joins that connection's transaction.
//
// Nothing here throws. An engine refusal is an error cursor or an error value
// carrying the engine's own code.
namespace otterstax::engine {

    using components::session::session_id_t;
    using cursor_future_t = actor_zeta::unique_future<components::cursor::cursor_t_ptr>;
    using columns_t = std::pmr::vector<components::types::complex_logical_type>;

    // One SQL statement of otterstax's own bookkeeping (CREATE DATABASE, DROP
    // TABLE, a manifest read) as an engine plan: the engine's raw parser and
    // transformer, finalized. Never a client's statement — those go through the
    // query session's parser and its grammar extensions. An unparsable text is
    // sql_parse_error.
    [[nodiscard]] core::result_wrapper_t<components::logical_plan::execution_plan_t>
    sql_plan(std::pmr::memory_resource* resource, const std::string& sql);

    // Sends one plan on `session`. A plain send: the future settles when the
    // dispatcher answers.
    [[nodiscard]] cursor_future_t
    send_plan(actor_zeta::address_t dispatcher, session_id_t session, components::logical_plan::execution_plan_t plan);

    // Runs a parsed statement and holds the engine's answer to the shape its
    // root owes. A DML without RETURNING answers its count on a write cursor
    // (is_write(), affected_rows()) — a write into a remote table too: the
    // engine counts the rows it hands the table's storage, or takes the count
    // the backend reported for a statement the federation sent whole. A
    // row-producing statement (SELECT, set operation, DML with RETURNING) whose
    // result has no columns is an internal_error naming the statement: every
    // frontend reads a column-less result as an affected count. Never a null
    // cursor; every cursor it answers carries the engine's transaction status
    // (cursor_t::transaction_status()) and window (portal_id(), has_more()).
    [[nodiscard]] cursor_future_t execute_statement(std::pmr::memory_resource* resource,
                                                    actor_zeta::address_t dispatcher,
                                                    session_id_t session,
                                                    ExecutionPlanPtr statement);

    // CREATE TABLE `target` (the columns of the run's chunks — a loader's run, every
    // chunk of one shape — each named by its alias) as a
    // plan: the name as written, its empty slots the session's place, as for a
    // CREATE TABLE of SQL — the database is not created, a missing schema is the
    // engine's refusal, an existing relation table_already_exists. A column
    // without a name, and an empty run, are invalid_parameter.
    [[nodiscard]] core::result_wrapper_t<components::logical_plan::execution_plan_t>
    create_table_plan(std::pmr::memory_resource* resource,
                      const qualified_name_t& target,
                      const std::pmr::vector<components::vector::data_chunk_t>& rows);

    // The load of CREATE EXTERNAL TABLE on `session`: `create` (create_table_plan
    // of `target`), then the run's rows inserted into `target` (each chunk within
    // the chunk contract, as the loaders cut them). `own_block`: the load is one transaction of its own — BEGIN
    // before, COMMIT after, ROLLBACK when a step fails; otherwise the steps run as
    // the session stands (in its open block, which a failure fails, or each in
    // autocommit). Answers the failed step's cursor, or the last one; the cursor
    // carries where the session stands (transaction_status()).
    [[nodiscard]] cursor_future_t load_table(std::pmr::memory_resource* resource,
                                             actor_zeta::address_t dispatcher,
                                             session_id_t session,
                                             components::logical_plan::execution_plan_t create,
                                             qualified_name_t target,
                                             std::pmr::vector<components::vector::data_chunk_t> rows,
                                             bool own_block);

} // namespace otterstax::engine
