// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "engine_requests.hpp"

#include "otterbrix/operators/schema_probe.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
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
#include <exception>
#include <string>
#include <string_view>
#include <utility>

namespace otterstax::engine {

    using namespace components;

    namespace {

        std::string to_lower(std::string s) {
            std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
            return s;
        }

        using tsl::make_error;

        cursor::cursor_t_ptr error_cursor(std::pmr::memory_resource* resource, const core::error_t& error) {
            return cursor::make_cursor(resource,
                                       core::error_t(error.type, std::pmr::string{error.what.c_str(), resource}));
        }

        // A failed engine call as an error_t on `resource`, keeping the engine's
        // own code.
        core::error_t engine_failure(std::pmr::memory_resource* resource,
                                     const cursor::cursor_t_ptr& cursor,
                                     const std::string& context) {
            assert(cursor);
            return core::error_t(cursor->get_error().type,
                                 std::pmr::string{(context + ": " + cursor->get_error().what.c_str()).c_str(), resource});
        }

        bool failed(const cursor::cursor_t_ptr& cursor) {
            assert(cursor);
            return cursor->is_error();
        }

        // The branch a set operation names its result after. SQL takes a UNION's
        // column names from its FIRST SELECT, so the first aggregate in plan order
        // is what such a statement is described from. Null when no aggregate sits
        // under the root at all, which is the honest "nothing to describe".
        const logical_plan::node_t* first_aggregate(const logical_plan::node_t* root) {
            if (root == nullptr) {
                return nullptr;
            }
            if (root->type() == logical_plan::node_type::aggregate_t) {
                return root;
            }
            for (const auto& child : root->children()) {
                if (const auto* found = first_aggregate(child.get())) {
                    return found;
                }
            }
            return nullptr;
        }

        // The engine plan of a statement: its sub-queries, in the order the engine
        // runs them, ahead of its root, and the catalog lookups the transformer
        // registered beside the root. Copies, not moves: one statement is planned
        // for its output schema and executed, and both plans need them.
        logical_plan::execution_plan_t statement_plan(std::pmr::memory_resource* resource,
                                                      const OtterbrixStatement& statement) {
            OTX_ZONE_N("engine::statement_plan");
            logical_plan::execution_plan_t plan{resource, statement.node, statement.params_node};
            if (!statement.sub_queries.empty()) {
                plan.sub_queries.insert(plan.sub_queries.begin(),
                                        statement.sub_queries.begin(),
                                        statement.sub_queries.end());
                plan.sub_query_results.assign(statement.sub_query_results.begin(), statement.sub_query_results.end());
            }
            plan.catalog_resolves = statement.catalog_resolves;
            return plan;
        }

        // The statement's output columns as the engine's plan validation stamps
        // them (plan-only EXPLAIN: resolve + validate, no scan): one STRUCT in
        // type_data(), alias = column name, duplicates kept.
        cursor_future_t
        plan_output_schema(std::pmr::memory_resource* resource,
                           actor_zeta::address_t dispatcher,
                           session_id_t session,
                           const OtterbrixStatement& statement) {
            logical_plan::node_ptr consumer_root = statement.node;
            cursor_future_t future;
            {
                OTX_ZONE_N("engine::plan_output_schema");
                // The statement's catalog lookups and sub-queries ride along:
                // validation resolves the same targets the execution will, and a
                // constraint the plan does not name is one the validator cannot see.
                auto plan = statement_plan(resource, statement);
                plan.explain = logical_plan::explain_type::plan;
                future = send_plan(dispatcher, session, std::move(plan));
            }
            // Awaited outside the zone: the coroutine may resume on another thread.
            components::cursor::cursor_t_ptr cursor = co_await std::move(future);
            assert(cursor);
            if (cursor->is_error()) {
                co_return cursor;
            }
            // A statement is not a relation: it cannot be re-read with LIMIT 0 (a
            // DML would run twice), so this one path reads the stamp the validator
            // left on the plan's own consumer node, after the engine replied. A VIEW
            // needs no branch of its own — the engine splices the body into that
            // node and stamps it there.
            const logical_plan::node_t* consumer = plan_statement_node(consumer_root.get());
            if (consumer == nullptr || !consumer->has_output_types()) {
                co_return error_cursor(resource,
                                       make_error(resource,
                                                  core::error_code_t::schema_error,
                                                  "plan_output_schema: the engine stamped no output types for the "
                                                  "statement"));
            }
            columns_t columns(consumer->output_types().begin(), consumer->output_types().end(), resource);
            std::pmr::vector<types::complex_logical_type> schema(resource);
            schema.emplace_back(types::complex_logical_type::create_struct("", columns));
            co_return cursor::make_cursor(resource, std::move(schema));
        }

        // Output columns of database.collection (alias = column name), from a LIMIT 0
        // read; a VIEW answers with its body's columns. A relation the engine does
        // not hold carries the engine's code (table_not_exists / database_not_exists).
        actor_zeta::unique_future<core::result_wrapper_t<columns_t>> probe_relation(std::pmr::memory_resource* resource,
                                                                                    actor_zeta::address_t dispatcher,
                                                                                    session_id_t session,
                                                                                    std::string database,
                                                                                    std::string collection) {
            cursor::cursor_t_ptr cursor;
            schema_probe::probe_t probe;
            {
                OTX_ZONE_N("engine::probe_relation");
                probe = schema_probe::make_table_probe(resource, database, collection);
            }
            {
                auto future = send_plan(dispatcher, session, probe.execution_plan(resource));
                cursor = co_await std::move(future);
            }
            auto read = schema_probe::read_columns(probe, cursor, resource);
            if (read.has_error()) {
                co_return read.convert_error<columns_t>();
            }
            co_return std::move(read.value().columns);
        }

        // CREATE TABLE database.collection (columns) built as a plan.
        actor_zeta::unique_future<core::result_wrapper_t<bool>>
        create_collection(std::pmr::memory_resource* resource,
                          actor_zeta::address_t dispatcher,
                          session_id_t session,
                          std::string database,
                          std::string collection,
                          std::vector<table::column_definition_t> columns) {
            logical_plan::execution_plan_t plan{resource};
            {
                OTX_ZONE_N("engine::create_collection");
                auto create = logical_plan::make_node_create_collection(resource,
                                                                        core::relname_t{collection},
                                                                        std::move(columns),
                                                                        {});
                // The database is named on the node's target together with the
                // relname, and registered as the plan's namespace lookup.
                logical_plan::node_ptr node =
                    sql::transform::name_catalog_target(core::dbname_t{database}, core::relname_t{collection}, create);
                plan = logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)};
                sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, database);
            }
            auto future = send_plan(dispatcher, session, std::move(plan));
            cursor::cursor_t_ptr cursor = co_await std::move(future);
            assert(cursor);
            if (cursor->is_error()) {
                co_return core::error_t(cursor->get_error().type,
                                        std::pmr::string{cursor->get_error().what.c_str(), resource});
            }
            co_return true;
        }

        // Appends `data` to an EXISTING collection (node_insert named on the node,
        // the outgoing constraints looked up), cut into chunks within
        // DEFAULT_VECTOR_CAPACITY rows. Names are taken as given.
        cursor_future_t insert_rows(std::pmr::memory_resource* resource,
                                    actor_zeta::address_t dispatcher,
                                    session_id_t session,
                                    std::string database,
                                    std::string collection,
                                    vector::data_chunk_t data) {
            logical_plan::execution_plan_t plan{resource};
            {
                OTX_ZONE_N("engine::insert_rows");
                // node_insert is built without a target name; naming it binds the
                // insert to database.collection, and the plan's table lookup carries
                // the outgoing constraints the engine enforces on the rows. The rows
                // go in as a run of chunks within DEFAULT_VECTOR_CAPACITY: a file
                // loader hands over the whole file as one chunk.
                auto insert = logical_plan::make_node_insert(resource,
                                                             tsl::split_to_capacity(resource, std::move(data)),
                                                             std::pmr::vector<expressions::key_t>(resource));
                auto insert_node =
                    sql::transform::name_catalog_target(core::dbname_t{database}, core::relname_t{collection}, insert);
                plan = logical_plan::execution_plan_t{resource, insert_node, logical_plan::make_parameter_node(resource)};
                sql::transform::register_catalog_resolve_table(resource,
                                                               &plan.catalog_resolves,
                                                               database,
                                                               collection,
                                                               sql::transform::constraint_resolve_kind::outgoing);
            }
            auto future = send_plan(dispatcher, session, std::move(plan));
            co_return co_await std::move(future);
        }

    } // namespace

    core::result_wrapper_t<logical_plan::execution_plan_t> sql_plan(std::pmr::memory_resource* resource,
                                                                    const std::string& sql) {
        OTX_ZONE_N("engine::sql_plan");
        using namespace components::sql::transform;
        core::pmr::arena_resource_t parser_arena(resource);
        List* parse_tree = nullptr;
        try {
            parse_tree = raw_parser(&parser_arena, sql.c_str());
        } catch (const std::exception& e) {
            return make_error(resource, core::error_code_t::sql_parse_error, e.what());
        }
        if (list_length(parse_tree) != 1 || linitial(parse_tree) == nullptr) {
            return make_error(resource,
                              core::error_code_t::sql_parse_error,
                              "sql_plan: expected exactly one statement in: " + sql);
        }
        transformer local_transformer(resource, sql.c_str());
        return local_transformer.transform(pg_cell_to_node_cast(linitial(parse_tree))).finalize();
    }

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
                                      OtterbrixStatementPtr statement) {
        cursor::cursor_t_ptr cursor;
        logical_plan::execution_plan_t plan{resource};
        {
            OTX_ZONE_N("engine::execute_statement");
            plan = statement_plan(statement->node->resource(), *statement);
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
        if (dml_without_returning(statement->node)) {
            // A no-RETURNING DML reports only its affected-row count, which the
            // engine answers on the cursor (affected_rows(), is_write()). Any
            // columns it attached are dropped: they would be serialised as rows of
            // never-initialised values. A cursor that does not say it is a write
            // reports the rows it carries.
            const auto affected = cursor->is_write() ? cursor->affected_rows() : cursor->size();
            auto written = cursor::make_cursor(resource);
            written->set_written(affected);
            co_return written;
        }
        if (const char* kind = row_producing_statement(statement->node);
            kind != nullptr && cursor->column_count() == 0) {
            // A SELECT / RETURNING whose result has no columns would reach the
            // client as "N rows affected". The engine produces such a result for
            // a column it cannot type (a `hugeint` column is read back as
            // UNKNOWN); no column is invented for it.
            std::pmr::string what{kind, resource};
            what.append(" returned a result without columns: the engine produced no typed output column for the "
                        "statement");
            co_return cursor::make_cursor(resource, core::error_t(core::error_code_t::schema_error, std::move(what)));
        }
        co_return cursor;
    }

    actor_zeta::unique_future<describe_answer_t> describe(std::pmr::memory_resource* resource,
                                                          actor_zeta::address_t dispatcher,
                                                          session_id_t session,
                                                          dependencies_t dependencies,
                                                          ParsedQueryDataPtr data) {
        const logical_plan::node_t* schema_root = nullptr;
        bool set_operation = false;
        bool engine_validated = false;
        {
            OTX_ZONE_N("engine::describe");
            schema_root = data->otterbrix_params->node.get();
            // A set operation answers rows as much as an aggregate does, and its
            // columns are its first branch's. It cannot take the engine's plan
            // validation — that pass stamps the aggregate consumer, not a union
            // root — so it is computed from that branch instead. Answering it "no
            // schema" is what must not happen: a frontend can only send that as
            // NoData, which a client reads as "this statement returns no rows".
            set_operation = schema_root->type() == logical_plan::node_type::union_t;

            // Only an aggregate consumer or that set operation produces a result
            // schema; every other root (DDL, DML, inlined raw data) has none.
            if (schema_root->type() != logical_plan::node_type::aggregate_t && !set_operation) {
                co_return std::make_pair(cursor::make_cursor(resource), std::move(data));
            }

            // A statement with every parameter bound is validated by the engine,
            // remote tables included (the plan-only pass resolves their names
            // through the federation and builds the plan without opening a
            // source), so its output columns are the ones that validation stamps
            // — the types the execution will produce, duplicate names included
            // (both key columns of a JOIN). A parameterized statement is not
            // validatable at all (the plan-only pass refuses a plan that still
            // carries `$n`), so it takes the federated computation below over its
            // own relations' probes.
            engine_validated = data->otterbrix_params->parameters_count == 0 && !set_operation;
        }
        if (engine_validated) {
            auto future = plan_output_schema(resource, dispatcher, session, *data->otterbrix_params);
            auto cursor = co_await std::move(future);
            if (failed(cursor)) {
                co_return engine_failure(resource, cursor, "get_schema");
            }
            co_return std::make_pair(std::move(cursor), std::move(data));
        }

        // A set operation is described from the branch its result is named after;
        // an aggregate root describes itself.
        if (set_operation) {
            schema_root = first_aggregate(schema_root);
            if (schema_root == nullptr) {
                co_return std::make_pair(cursor::make_cursor(resource), std::move(data));
            }
        }

        // Dependency-map values are indices 0..N-1 and the schema cursor is
        // positional (type_data()[i] = schema of dependency i). A local table is
        // probed by a LIMIT 0 read; a name with a uid or a schema slot — a
        // remote table — by the engine's plan-only validation of a read of it,
        // which answers the declared columns of federation.remote_columns and
        // reaches no backend. An unnamed wrapper aggregate keeps its positional
        // slot with an empty schema.
        std::pmr::vector<types::complex_logical_type> table_schemas(resource);
        table_schemas.resize(dependencies.size(),
                             types::complex_logical_type::create_struct("", columns_t(resource)));
        for (const auto& [name, index] : dependencies) {
            if (name.collection.t.empty() || index >= table_schemas.size()) {
                continue;
            }
            const bool remote = !name.unique_identifier.t.empty() || !name.schema.t.empty();
            auto probe_future = remote ? probe_declared_columns(resource, dispatcher, session, name)
                                       : probe_relation(resource, dispatcher, session, name.database.t, name.collection.t);
            auto columns = co_await std::move(probe_future);
            if (columns.has_error()) {
                co_return core::error_t(columns.error().type,
                                        std::pmr::string{("get_schema: " + std::string{columns.error().what.c_str()})
                                                             .c_str(),
                                                         resource});
            }
            table_schemas[index] = types::complex_logical_type::create_struct("", columns.value());
        }

        OTX_ZONE_N("engine::describe::compute");
        auto schema = schema_utils::compute_otterbrix_schema(
            static_cast<const logical_plan::node_aggregate_t&>(*schema_root),
            data->otterbrix_params->params_node.get(),
            cursor::make_cursor(resource, std::move(table_schemas)),
            std::move(dependencies));
        co_return std::make_pair(std::move(schema), std::move(data));
    }

    actor_zeta::unique_future<core::result_wrapper_t<columns_t>>
    probe_declared_columns(std::pmr::memory_resource* resource,
                           actor_zeta::address_t dispatcher,
                           session_id_t session,
                           qualified_name_t name) {
        logical_plan::node_ptr root;
        cursor_future_t future;
        std::string subject;
        {
            OTX_ZONE_N("engine::probe_declared_columns");
            subject = name.to_string();
            root = logical_plan::make_node_aggregate(resource, std::move(name));
            logical_plan::execution_plan_t plan{resource, root, logical_plan::make_parameter_node(resource)};
            plan.explain = logical_plan::explain_type::plan;
            future = send_plan(dispatcher, session, std::move(plan));
        }
        // Awaited outside the zone: the coroutine may resume on another thread.
        cursor::cursor_t_ptr cursor = co_await std::move(future);
        assert(cursor);
        if (cursor->is_error()) {
            co_return core::error_t(cursor->get_error().type,
                                    std::pmr::string{cursor->get_error().what.c_str(), resource});
        }
        // The output types the validator stamped on the probe's own aggregate, read
        // after the engine replied (plan_output_schema reads the same stamp).
        if (!root->has_output_types()) {
            co_return make_error(resource,
                                 core::error_code_t::schema_error,
                                 "probe of " + subject + ": the engine stamped no output types");
        }
        co_return columns_t(root->output_types().begin(), root->output_types().end(), resource);
    }

    logical_plan::execution_plan_t delete_rows_plan(std::pmr::memory_resource* resource,
                                                    const std::string& database,
                                                    const std::string& collection,
                                                    std::span<const column_equals_t> where) {
        OTX_ZONE_N("engine::delete_rows_plan");
        assert(!where.empty() && "a DELETE without a WHERE term is not built here");
        // The plan shape transform_delete emits for `DELETE ... WHERE column =
        // value [AND ...]`: node_delete over node_match(eq, or an AND of eqs). The
        // key is on side_t::left — the (single) target table; a key of undefined
        // side leaves the column unresolved.
        auto params = logical_plan::make_parameter_node(resource);
        auto term = [&](const column_equals_t& equals) {
            const auto value_param = params->add_parameter(types::logical_value_t(resource, equals.value));
            expressions::key_t column_key(resource, equals.column, expressions::side_t::left);
            return expressions::make_compare_expression(resource, expressions::compare_type::eq, column_key, value_param);
        };
        expressions::expression_ptr predicate;
        if (where.size() == 1) {
            predicate = term(where.front());
        } else {
            auto all = expressions::make_compare_union_expression(resource, expressions::compare_type::union_and);
            for (const auto& equals : where) {
                all->append_child(term(equals));
            }
            predicate = std::move(all);
        }
        const qualified_name_t target{core::dbname_t{database}, core::relname_t{collection}};
        auto match = logical_plan::make_node_match(resource, target, std::move(predicate));
        auto del = logical_plan::make_node_delete(resource,
                                                  match,
                                                  logical_plan::make_node_limit(resource, logical_plan::limit_t::unlimit()));
        logical_plan::node_ptr node = sql::transform::name_catalog_target(target.database, target.collection, del);
        logical_plan::execution_plan_t plan{resource, node, std::move(params)};
        // DELETE is checked against the constraints that REFERENCE the target.
        sql::transform::register_catalog_resolve_table(resource,
                                                       &plan.catalog_resolves,
                                                       database,
                                                       collection,
                                                       sql::transform::constraint_resolve_kind::referencing);
        return plan;
    }

    actor_zeta::unique_future<core::result_wrapper_t<bool>> create_table(std::pmr::memory_resource* resource,
                                                                         actor_zeta::address_t dispatcher,
                                                                         session_id_t session,
                                                                         std::string database,
                                                                         std::string table,
                                                                         vector::data_chunk_t chunk) {
        std::string context;
        std::string db;
        std::string tbl;
        std::vector<table::column_definition_t> columns;
        {
            OTX_ZONE_N("engine::create_table");
            context = "create_table " + database + "." + table;
            db = to_lower(database);
            tbl = to_lower(table);
            const auto types = chunk.types();
            columns.reserve(types.size());
            for (size_t i = 0; i < types.size(); ++i) {
                // A column definition needs a name; the loaded chunk is the only
                // source of one, so a nameless column cannot become part of the table.
                if (!types[i].has_alias()) {
                    co_return make_error(resource,
                                         core::error_code_t::invalid_parameter,
                                         context + ": column " + std::to_string(i) + " has no name");
                }
                columns.emplace_back(types[i].alias(), types[i]);
            }
        }

        // CREATE DATABASE through the transformer, so the catalog-resolve wrapping
        // applies. An existing database is the desired end state, not a failure.
        // The name is spelled as a quoted identifier (the engine reads PostgreSQL's),
        // so no name reads as SQL; quoting keeps the case, and `db` is already
        // folded the way the engine folds an unquoted one.
        auto db_plan =
            sql_plan(resource, "CREATE DATABASE " + sql_gen::quote_identifier(db, backend_type_t::PostgreSQL) + ";");
        if (db_plan.has_error()) {
            const std::string what = context + ": create_database: " + db_plan.error().what.c_str();
            co_return core::error_t(db_plan.error().type, std::pmr::string{what.c_str(), resource});
        }
        auto db_future = send_plan(dispatcher, session, std::move(db_plan.value()));
        auto db_cursor = co_await std::move(db_future);
        if (failed(db_cursor) && db_cursor->get_error().type != core::error_code_t::database_already_exists) {
            co_return engine_failure(resource, db_cursor, context + ": create_database");
        }

        // A relation that already carries the name is refused before anything is
        // created or inserted: the create resolves only the namespace, so a create
        // over an existing table is accepted and the insert would append to it. A
        // free name probes as table_not_exists (database_not_exists when the
        // database is missing too) — the create and insert answer that one.
        auto probe_future = probe_relation(resource, dispatcher, session, db, tbl);
        auto probed = co_await std::move(probe_future);
        if (!probed.has_error()) {
            co_return make_error(resource,
                                 core::error_code_t::table_already_exists,
                                 context + ": insert_data: " + db + "." + tbl + " already exists");
        }
        if (probed.error().type != core::error_code_t::table_not_exists &&
            probed.error().type != core::error_code_t::database_not_exists) {
            co_return core::error_t(probed.error().type,
                                    std::pmr::string{(context + ": insert_data: " + probed.error().what.c_str()).c_str(),
                                                     resource});
        }

        auto create_future = create_collection(resource, dispatcher, session, db, tbl, std::move(columns));
        auto created = co_await std::move(create_future);
        if (created.has_error()) {
            co_return core::error_t(created.error().type,
                                    std::pmr::string{(context + ": insert_data: " + created.error().what.c_str()).c_str(),
                                                     resource});
        }

        auto insert_future = insert_rows(resource, dispatcher, session, db, tbl, std::move(chunk));
        auto inserted = co_await std::move(insert_future);
        if (failed(inserted)) {
            co_return engine_failure(resource, inserted, context + ": insert_data");
        }
        co_return true;
    }

} // namespace otterstax::engine
