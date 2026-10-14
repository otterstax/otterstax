// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <map>
#include <memory_resource>
#include <span>
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
// frame) and must be awaited from an actor coroutine — a query session, the
// file manager — or driven by a test's drive loop: in actor-zeta 2.0 a future
// is co_awaited only inside an actor's await chain. Every one names the engine
// SESSION it runs on: a query session passes its connection's session, so the
// statement joins that connection's transaction; the file manager passes its
// own.
//
// Nothing here throws. An engine refusal is an error cursor or an error value
// carrying the engine's own code.
namespace otterstax::engine {

    using components::session::session_id_t;
    using cursor_future_t = actor_zeta::unique_future<components::cursor::cursor_t_ptr>;
    using columns_t = std::pmr::vector<components::types::complex_logical_type>;
    using dependencies_t = std::pmr::map<qualified_name_t, size_t>;
    // A describe's answer: the cursor whose type_data()[0] is the result STRUCT
    // (none for a statement without rows), and the statement handed back so the
    // caller keeps it for execute.
    using describe_answer_t = core::result_wrapper_t<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>;

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
    // result has no columns is a schema_error naming the statement: every
    // frontend reads a column-less result as an affected count. Never a null
    // cursor.
    [[nodiscard]] cursor_future_t execute_statement(std::pmr::memory_resource* resource,
                                                    actor_zeta::address_t dispatcher,
                                                    session_id_t session,
                                                    OtterbrixStatementPtr statement);

    // The result schema of `data` without running it: the engine's own plan
    // validation (plan-only EXPLAIN) for a statement it can validate — every
    // parameter bound, not a set operation; remote tables are resolved and
    // typed by the federation without a backend round trip — and the federated
    // computation over `dependencies` otherwise (one probe per relation,
    // positional: a LIMIT 0 read of a local one, probe_declared_columns of a
    // remote one). A root that produces no rows answers no schema.
    [[nodiscard]] actor_zeta::unique_future<describe_answer_t> describe(std::pmr::memory_resource* resource,
                                                                        actor_zeta::address_t dispatcher,
                                                                        session_id_t session,
                                                                        dependencies_t dependencies,
                                                                        ParsedQueryDataPtr data);

    // The declared columns of the relation `name` designates, as the engine's
    // plan-only validation of a read of it types them (alias = column name):
    // for a remote table the columns federation.remote_columns declares, with no
    // backend round trip. A name nobody resolves carries the engine's code.
    [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<columns_t>>
    probe_declared_columns(std::pmr::memory_resource* resource,
                           actor_zeta::address_t dispatcher,
                           session_id_t session,
                           qualified_name_t name);

    // One `column = value` term of a DELETE's WHERE.
    struct column_equals_t {
        std::string column;
        components::types::logical_value_t value;
    };

    // The plan of DELETE FROM database.collection WHERE column1 = value1 AND
    // ..., every value bound as a plan parameter, never spliced into SQL text,
    // the column keyed on the target; the constraints that reference the
    // target looked up. `where` is not empty. The one builder of such a plan: a
    // caller sends it itself (the startup's federation metadata, inside its
    // transaction; the metadata plans of catalog/remote_metadata).
    [[nodiscard]] components::logical_plan::execution_plan_t
    delete_rows_plan(std::pmr::memory_resource* resource,
                     const std::string& database,
                     const std::string& collection,
                     std::span<const column_equals_t> where);

    // The file / s3 load: makes `database` exist (an existing one is the desired
    // end state), refuses a relation already under the name as
    // table_already_exists (nothing created or appended), creates the table from
    // the chunk's named columns and inserts its rows. Names are lower-cased.
    [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<bool>>
    create_table(std::pmr::memory_resource* resource,
                 actor_zeta::address_t dispatcher,
                 session_id_t session,
                 std::string database,
                 std::string table,
                 components::vector::data_chunk_t chunk);

} // namespace otterstax::engine
