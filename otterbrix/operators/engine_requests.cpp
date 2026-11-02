// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "engine_requests.hpp"

#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <string>
#include <string_view>
#include <utility>

namespace otterstax::engine {

    using namespace components;

    namespace {

        using tsl::make_error;

        // Names a hand-built plan node's target in full: name_catalog_target
        // names schema and relation only, the database coming from the session's
        // place, and these plans name a database of their own.
        logical_plan::node_ptr name_target(qualified_name_t name, logical_plan::node_ptr node) {
            node->set_target(std::move(name));
            return node;
        }

        // BEGIN / COMMIT / ROLLBACK as a plan.
        logical_plan::execution_plan_t transaction_plan(std::pmr::memory_resource* resource, const char* sql);

    } // namespace

    core::result_wrapper_t<logical_plan::execution_plan_t> sql_plan(std::pmr::memory_resource* resource,
                                                                    const std::string& sql) {
        OTX_ZONE_N("engine::sql_plan");
        using namespace components::sql::transform;
        core::pmr::arena_resource_t parser_arena(resource);
        auto parsed = raw_parser(&parser_arena, sql.c_str());
        if (parsed.has_error()) {
            // The error lives on the arena, which dies with this call.
            return core::error_on(resource, parsed.error());
        }
        List* parse_tree = parsed.value();
        if (list_length(parse_tree) != 1 || linitial(parse_tree) == nullptr) {
            return make_error(resource,
                              core::error_code_t::sql_parse_error,
                              "sql_plan: expected exactly one statement in: " + sql);
        }
        transformer local_transformer(resource, sql.c_str());
        return local_transformer.transform(pg_cell_to_node_cast(linitial(parse_tree))).finalize();
    }

    namespace {
        logical_plan::execution_plan_t transaction_plan(std::pmr::memory_resource* resource, const char* sql) {
            auto plan = sql_plan(resource, sql);
            assert(!plan.has_error() && "a transaction statement parses");
            return std::move(plan.value());
        }
    } // namespace

    cursor_future_t
    send_plan(actor_zeta::address_t dispatcher, session_id_t session, logical_plan::execution_plan_t plan) {
        auto [needs_sched, future] = actor_zeta::otterbrix::send(std::move(dispatcher),
                                                                 &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                                 session,
                                                                 std::move(plan));
        // The dispatcher runs its own loop: a send only feeds its inbox.
        static_cast<void>(needs_sched);
        return std::move(future);
    }

    cursor_future_t execute_statement(std::pmr::memory_resource* resource,
                                      actor_zeta::address_t dispatcher,
                                      session_id_t session,
                                      ExecutionPlanPtr statement) {
        cursor::cursor_t_ptr cursor;
        logical_plan::execution_plan_t plan{resource};
        logical_plan::node_ptr root;
        {
            OTX_ZONE_N("engine::execute_statement");
            root = plan_root(*statement);
            plan = std::move(*statement);
        }
        {
            auto future = send_plan(dispatcher, session, std::move(plan));
            cursor = co_await std::move(future);
        }
        assert(cursor);
        if (cursor->is_error()) {
            co_return cursor;
        }
        OTX_ZONE_N("engine::execute_statement::shape");
        // The result shape every frontend reads: columns are a resultset, no
        // columns is an affected count. The plan root says which one the
        // statement owes, and the engine's result is held to it both ways.
        if (dml_without_returning(root)) {
            // A no-RETURNING DML reports only its affected-row count, which the
            // engine answers on the cursor (affected_rows(), is_write()). Any
            // columns it attached are dropped: they would be serialised as rows of
            // never-initialised values. A cursor that does not say it is a write
            // reports the rows it carries.
            const auto affected = cursor->is_write() ? cursor->affected_rows() : cursor->size();
            auto written = cursor::make_cursor(resource);
            written->set_written(affected);
            written->set_transaction_status(cursor->transaction_status());
            co_return written;
        }
        if (const char* kind = row_producing_statement(root);
            kind != nullptr && cursor->column_count() == 0) {
            // A SELECT / RETURNING whose result has no columns would reach the
            // client as "N rows affected". The engine produces such a result for
            // a column it cannot type (a `hugeint` column is read back as
            // UNKNOWN); no column is invented for it.
            std::pmr::string what{kind, resource};
            what.append(" returned a result without columns: the engine produced no typed output column for the "
                        "statement");
            auto refused = cursor::make_cursor(resource, core::error_t(core::error_code_t::internal_error, std::move(what)));
            refused->set_transaction_status(cursor->transaction_status());
            co_return refused;
        }
        co_return cursor;
    }

    core::result_wrapper_t<logical_plan::execution_plan_t> create_table_plan(std::pmr::memory_resource* resource,
                                                                             const qualified_name_t& target,
                                                                             const std::pmr::vector<vector::data_chunk_t>& rows) {
        OTX_ZONE_N("engine::create_table_plan");
        // Every chunk of a loaded run shares one column shape (an empty file is one
        // chunk of its columns and no row), so the first one names the columns.
        if (rows.empty()) {
            return make_error(resource,
                              core::error_code_t::invalid_parameter,
                              "CREATE TABLE " + target.to_string() + ": the input has no columns");
        }
        std::vector<table::column_definition_t> columns;
        const auto types = rows.front().types();
        columns.reserve(types.size());
        for (size_t i = 0; i < types.size(); ++i) {
            // A column definition needs a name; the loaded chunk is the only
            // source of one, so a nameless column cannot become part of the table.
            if (!types[i].has_alias()) {
                return make_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "CREATE TABLE " + target.to_string() + ": column " + std::to_string(i) +
                                      " has no name");
            }
            columns.emplace_back(types[i].alias(), types[i]);
        }
        // What transform_create_table builds for CREATE TABLE <target> (...): the
        // node keeps every written slot (the executor completes the empty ones
        // with the session's place) and the target's namespace is looked up; a
        // relation already under the name is the engine's table_already_exists.
        auto create = logical_plan::make_node_create_collection(resource, target.collection, std::move(columns), {});
        logical_plan::node_ptr node = name_target(target, create);
        logical_plan::execution_plan_t plan{resource, node, logical_plan::make_parameter_node(resource)};
        sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, target);
        return plan;
    }

    cursor_future_t load_table(std::pmr::memory_resource* resource,
                               actor_zeta::address_t dispatcher,
                               session_id_t session,
                               logical_plan::execution_plan_t create,
                               qualified_name_t target,
                               std::pmr::vector<vector::data_chunk_t> rows,
                               bool own_block) {
        logical_plan::execution_plan_t begin{resource};
        {
            OTX_ZONE_N("engine::load_table");
            if (own_block) {
                begin = transaction_plan(resource, "BEGIN");
            }
        }
        if (own_block) {
            auto began = co_await send_plan(dispatcher, session, std::move(begin));
            assert(began);
            if (began->is_error()) {
                co_return began;
            }
        }
        cursor::cursor_t_ptr failed_step;
        auto created = co_await send_plan(dispatcher, session, std::move(create));
        assert(created);
        if (created->is_error()) {
            failed_step = std::move(created);
        } else {
            logical_plan::execution_plan_t insert_plan{resource};
            {
                OTX_ZONE_N("engine::load_table::insert");
                // What transform_insert builds for INSERT INTO <target> VALUES ...:
                // the node keeps the written name, the write target is looked up
                // (with the constraints the engine enforces on the rows). The rows
                // are the loader's run of chunks within DEFAULT_VECTOR_CAPACITY.
                auto insert = logical_plan::make_node_insert(resource,
                                                             std::move(rows),
                                                             std::pmr::vector<expressions::key_t>(resource));
                logical_plan::node_ptr node = name_target(target, insert);
                insert_plan =
                    logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)};
                sql::transform::register_catalog_resolve_table(resource, &insert_plan.catalog_resolves, target);
            }
            auto inserted = co_await send_plan(dispatcher, session, std::move(insert_plan));
            assert(inserted);
            if (!inserted->is_error()) {
                if (!own_block) {
                    co_return inserted;
                }
                auto committed = co_await send_plan(dispatcher, session, transaction_plan(resource, "COMMIT"));
                assert(committed);
                co_return committed;
            }
            failed_step = std::move(inserted);
        }
        if (own_block) {
            // The load's own block is rolled back: the failure's code and message,
            // with the status the ROLLBACK left.
            auto ended = co_await send_plan(dispatcher, session, transaction_plan(resource, "ROLLBACK"));
            assert(ended);
            failed_step->set_transaction_status(ended->transaction_status());
        }
        co_return failed_step;
    }

} // namespace otterstax::engine
