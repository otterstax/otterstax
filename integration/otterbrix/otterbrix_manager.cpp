// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/result_wrapper.hpp>

#include <algorithm>
#include <exception>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

using namespace db;

namespace {

    // The branch a set operation names its result after. SQL takes a UNION's
    // column names from its FIRST SELECT, so the first aggregate in plan order
    // is what such a statement is described from. Null when no aggregate sits
    // under the root at all, which is the honest "nothing to describe".
    const components::logical_plan::node_t* first_aggregate(const components::logical_plan::node_t* root) {
        if (root == nullptr) {
            return nullptr;
        }
        if (root->type() == components::logical_plan::node_type::aggregate_t) {
            return root;
        }
        for (const auto& child : root->children()) {
            if (const auto* found = first_aggregate(child.get())) {
                return found;
            }
        }
        return nullptr;
    }

    // The engine boundary. IDataManager calls are the only statements in this
    // actor that can raise: the engine and its test doubles throw, nothing else
    // here does. An exception escaping an actor coroutine reaches actor-zeta's
    // unhandled_exception(), which only asserts, so it is converted to an error
    // cursor right where it can originate and nowhere else.
    template<typename Call>
    components::cursor::cursor_t_ptr at_engine_boundary(std::pmr::memory_resource* resource, Call&& call) {
        try {
            return call();
        } catch (const std::exception& e) {
            return cursor::make_cursor(resource,
                                       core::error_t(core::error_code_t::other_error,
                                                     std::pmr::string{e.what(), resource}));
        } catch (...) {
            return cursor::make_cursor(
                resource,
                core::error_t(core::error_code_t::other_error,
                              std::pmr::string{"engine raised an exception of unknown type", resource}));
        }
    }

    // A failed engine call as an error_t on `resource`, keeping the engine's own
    // code; a null cursor is reported as such rather than dereferenced.
    core::error_t engine_failure(std::pmr::memory_resource* resource,
                                 const components::cursor::cursor_t_ptr& cursor,
                                 const std::string& context) {
        if (!cursor) {
            return core::error_t(core::error_code_t::other_error,
                                 std::pmr::string{(context + ": engine returned a null cursor").c_str(), resource});
        }
        return core::error_t(cursor->get_error().type,
                             std::pmr::string{(context + ": " + cursor->get_error().what.c_str()).c_str(), resource});
    }

    bool failed(const components::cursor::cursor_t_ptr& cursor) { return !cursor || cursor->is_error(); }

    // Encodes the engine-side collection name for an external table registered
    // in the Otterbrix catalog. The per-connection uid becomes the engine
    // database name; the collection name folds the remaining qualifiers as
    //   <db> ':' <schema> ':' <collection>
    // The encoding is one-way by design — the OID-keyed schema_store_t holds
    // the original qualified name, so no decode function exists.
    std::string encode_external_collection(const qualified_name_t& name) {
        std::string encoded;
        encoded.reserve(name.database.size() + name.schema.size() + name.collection.size() + 2);
        encoded += name.database;
        encoded += ':';
        encoded += name.schema;
        encoded += ':';
        encoded += name.collection;
        return encoded;
    }

} // namespace

OtterbrixManager::OtterbrixManager(std::pmr::memory_resource* res, std::unique_ptr<IDataManager> data_manager)
    : resource_(res)
    , data_manager_(std::move(data_manager))
    , log_(get_logger(logger_tag::OTTERBRIX_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(data_manager_ != nullptr);
    log_->info("OtterbrixManager initialized successfully");
}

std::pair<bool, actor_zeta::detail::enqueue_result>
OtterbrixManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    OTX_ZONE_N("OtterbrixManager::enqueue_impl");
    std::lock_guard guard(mutex_);
    current_behavior_ = behavior(msg.get());

    while (current_behavior_.is_busy()) {
        if (current_behavior_.is_awaited_ready()) {
            auto cont = current_behavior_.take_awaited_continuation();
            if (cont) {
                cont.resume();
            }
        } else {
            std::this_thread::yield();
        }
    }

    return {false, actor_zeta::detail::enqueue_result::success};
}

actor_zeta::behavior_t OtterbrixManager::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("OtterbrixManager::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::execute>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::execute, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::get_schema>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::get_schema, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::register_external_database>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::register_external_database, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::register_external_table>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::register_external_table, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::drop_external_table>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::drop_external_table, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::drop_stale_external_tables>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::drop_stale_external_tables, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::create_table>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::create_table, msg);
    }
}

namespace {

    // The manifest's single column: the encoded engine-side collection name.
    std::pmr::vector<components::types::complex_logical_type> manifest_row_types(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::STRING_LITERAL);
        types.back().set_alias(external_manifest_column);
        return types;
    }

    // True when the engine collection carries exactly the requested columns:
    // same count, and per position the same name and the same type.
    bool same_columns(const std::vector<components::table::column_definition_t>& requested,
                      const std::pmr::vector<components::types::complex_logical_type>& present) {
        if (requested.size() != present.size()) {
            return false;
        }
        for (size_t i = 0; i < requested.size(); ++i) {
            if (!present[i].has_alias() || present[i].alias() != requested[i].name() ||
                !(requested[i].type() == present[i])) {
                return false;
            }
        }
        return true;
    }

} // namespace

core::error_t OtterbrixManager::ensure_manifest(const std::string& db_name) {
    OTX_ZONE_N("OtterbrixManager::ensure_manifest");
    // Probed before it is created: on rc-2 a repeated CREATE TABLE of an
    // existing collection is not an error, so the engine's create verdict
    // cannot tell a fresh manifest from a restored one.
    components::catalog::oid_t oid = components::catalog::INVALID_OID;
    auto described = at_engine_boundary(resource(), [&] {
        return data_manager_->describe_collection(db_name, external_manifest_collection, oid);
    });
    if (!failed(described)) {
        return core::error_t::no_error();
    }
    if (!described || described->get_error().type != core::error_code_t::table_not_exists) {
        return engine_failure(resource(),
                              described,
                              "Failed to probe the mirror manifest of database '" + db_name + "'");
    }
    std::vector<components::table::column_definition_t> columns;
    columns.emplace_back(external_manifest_column,
                         components::types::complex_logical_type(components::types::logical_type::STRING_LITERAL));
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->create_collection(db_name, external_manifest_collection, std::move(columns), oid);
    });
    if (failed(cursor)) {
        return engine_failure(resource(),
                              cursor,
                              "Failed to create the mirror manifest of database '" + db_name + "'");
    }
    log_->debug("ensure_manifest: created {}.{}", db_name, external_manifest_collection);
    return core::error_t::no_error();
}

core::result_wrapper_t<std::pmr::vector<std::pmr::string>> OtterbrixManager::read_manifest(const std::string& db_name) {
    OTX_ZONE_N("OtterbrixManager::read_manifest");
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->execute_sql(
            sql_gen::select_column_statement(db_name, external_manifest_collection, external_manifest_column));
    });
    if (failed(cursor)) {
        return engine_failure(resource(), cursor, "Failed to read the mirror manifest of database '" + db_name + "'");
    }
    std::pmr::vector<std::pmr::string> collections(resource());
    while (cursor->has_next()) {
        cursor->advance();
        const auto value = cursor->value(0);
        // The column is NOT NULL by construction (every row is written here);
        // a NULL is a manifest the engine no longer reads back faithfully.
        if (value.is_null()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{("Mirror manifest of database '" + db_name +
                                                   "' holds a NULL collection name")
                                                      .c_str(),
                                                  resource()});
        }
        collections.emplace_back();
        collections.back().assign(value.value<std::string_view>());
    }
    return std::move(collections);
}

core::error_t OtterbrixManager::write_manifest_row(const std::string& db_name, const std::string& collection) {
    OTX_ZONE_N("OtterbrixManager::write_manifest_row");
    if (auto err = delete_manifest_row(db_name, collection); err.contains_error()) {
        return std::move(err);
    }
    components::vector::data_chunk_t row{resource(), manifest_row_types(resource()), 1};
    row.set_value(0, 0, components::types::logical_value_t(resource(), std::string_view{collection}));
    row.set_cardinality(1);
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->insert_rows(db_name, external_manifest_collection, std::move(row));
    });
    if (failed(cursor)) {
        return engine_failure(resource(),
                              cursor,
                              "Failed to record '" + collection + "' in the mirror manifest of database '" + db_name +
                                  "'");
    }
    return core::error_t::no_error();
}

core::error_t OtterbrixManager::delete_manifest_row(const std::string& db_name, const std::string& collection) {
    OTX_ZONE_N("OtterbrixManager::delete_manifest_row");
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->delete_rows(db_name,
                                          external_manifest_collection,
                                          external_manifest_column,
                                          components::types::logical_value_t(resource(), std::string_view{collection}));
    });
    if (failed(cursor)) {
        return engine_failure(resource(),
                              cursor,
                              "Failed to remove '" + collection + "' from the mirror manifest of database '" +
                                  db_name + "'");
    }
    return core::error_t::no_error();
}

core::error_t OtterbrixManager::drop_collection(const std::string& db_name, const std::string& collection) {
    OTX_ZONE_N("OtterbrixManager::drop_collection");
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->execute_sql(sql_gen::drop_table_statement(db_name, collection));
    });
    if (failed(cursor)) {
        return engine_failure(resource(),
                              cursor,
                              "Failed to drop engine collection '" + db_name + "." + collection + "'");
    }
    return core::error_t::no_error();
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
OtterbrixManager::register_external_database(std::string db_name) {
    OTX_ZONE_N("OtterbrixManager::register_external_database");
    log_->debug("register_external_database: creating engine database {}", db_name);
    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->execute_sql(sql_gen::create_database_statement(db_name));
    });
    bool created = true;
    if (failed(cursor)) {
        // The name is guarded against user DDL, so the database the engine
        // already holds is the mirror a previous run left on this data dir.
        if (!cursor || cursor->get_error().type != core::error_code_t::database_already_exists) {
            auto error = engine_failure(resource(), cursor, "Failed to create engine database '" + db_name + "'");
            log_->error("register_external_database: {}", error.what.c_str());
            co_return std::move(error);
        }
        log_->info("register_external_database: engine database {} exists from a previous run, reusing it", db_name);
        created = false;
    }
    if (auto err = ensure_manifest(db_name); err.contains_error()) {
        log_->error("register_external_database: {}", err.what.c_str());
        co_return std::move(err);
    }
    co_return created;
}

actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
OtterbrixManager::register_external_table(qualified_name_t name,
                                          std::vector<components::table::column_definition_t> columns) {
    OTX_ZONE_N("OtterbrixManager::register_external_table");
    const std::string& encoded_db = name.unique_identifier;
    const std::string encoded_collection = encode_external_collection(name);
    log_->debug("register_external_table: registering {} as {}.{}", name.to_string(), encoded_db, encoded_collection);

    // Manifest first: a crash between the two leaves a row without a
    // collection, which the next run resolves; the reverse would leave a
    // mirror no run can find again.
    if (auto err = write_manifest_row(encoded_db, encoded_collection); err.contains_error()) {
        log_->error("register_external_table: {}", err.what.c_str());
        co_return std::move(err);
    }

    auto create = [&]() -> core::result_wrapper_t<components::catalog::oid_t> {
        components::catalog::oid_t oid = components::catalog::INVALID_OID;
        auto create_cursor = at_engine_boundary(resource(), [&] {
            return data_manager_->create_collection(encoded_db, encoded_collection, std::move(columns), oid);
        });
        if (failed(create_cursor)) {
            return engine_failure(resource(),
                                  create_cursor,
                                  "Failed to register external table '" + name.to_string() + "'");
        }
        if (oid == components::catalog::INVALID_OID) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{("Engine did not assign an oid for external table '" + name.to_string() + "'").c_str(),
                                 resource()});
        }
        return oid;
    };

    // The engine is probed BEFORE anything is created: on rc-2 a repeated
    // CREATE TABLE of an existing collection is not an error and stamps a
    // fresh oid the catalog never holds, so the create verdict cannot tell an
    // absent mirror from a restored one. Absent: created. Present: a previous
    // run's mirror — kept under its restored OID when it carries exactly the
    // discovered columns, replaced otherwise.
    components::catalog::oid_t present_oid = components::catalog::INVALID_OID;
    auto describe_cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->describe_collection(encoded_db, encoded_collection, present_oid);
    });
    if (failed(describe_cursor)) {
        if (!describe_cursor || describe_cursor->get_error().type != core::error_code_t::table_not_exists) {
            auto error = engine_failure(resource(),
                                        describe_cursor,
                                        "Failed to probe the engine for external table '" + name.to_string() + "'");
            log_->error("register_external_table: {}", error.what.c_str());
            co_return std::move(error);
        }
        auto created = create();
        if (created.has_error()) {
            log_->error("register_external_table: {}", created.error().what.c_str());
            co_return std::move(created);
        }
        log_->debug("register_external_table: {} registered with oid {}", name.to_string(), created.value());
        co_return created.value();
    }
    if (describe_cursor->type_data().size() != 1 || present_oid == components::catalog::INVALID_OID) {
        log_->error("register_external_table: the engine described {} without a schema or an oid", encoded_collection);
        co_return core::error_t(core::error_code_t::schema_error,
                                std::pmr::string{("Engine described the existing mirror of external table '" +
                                                  name.to_string() + "' without a schema or an oid")
                                                     .c_str(),
                                                 resource()});
    }
    if (same_columns(columns, describe_cursor->type_data().front().child_types())) {
        log_->info("register_external_table: {} exists from a previous run with the same columns, reusing oid {}",
                   name.to_string(),
                   present_oid);
        co_return present_oid;
    }
    log_->warn("register_external_table: {} exists from a previous run with another schema, recreating it",
               name.to_string());
    if (auto err = drop_collection(encoded_db, encoded_collection); err.contains_error()) {
        log_->error("register_external_table: {}", err.what.c_str());
        co_return std::move(err);
    }
    auto recreated = create();
    if (recreated.has_error()) {
        log_->error("register_external_table: {}", recreated.error().what.c_str());
        co_return std::move(recreated);
    }
    log_->debug("register_external_table: {} recreated with oid {}", name.to_string(), recreated.value());
    co_return recreated.value();
}

actor_zeta::unique_future<core::result_wrapper_t<bool>> OtterbrixManager::drop_external_table(qualified_name_t name) {
    OTX_ZONE_N("OtterbrixManager::drop_external_table");
    const std::string& encoded_db = name.unique_identifier;
    const std::string encoded_collection = encode_external_collection(name);
    log_->debug("drop_external_table: dropping {} registered as {}.{}",
                name.to_string(),
                encoded_db,
                encoded_collection);
    if (auto err = drop_collection(encoded_db, encoded_collection); err.contains_error()) {
        auto error = core::error_t(err.type,
                                   std::pmr::string{("Failed to drop external table '" + name.to_string() + "': " +
                                                     err.what.c_str())
                                                        .c_str(),
                                                    resource()});
        log_->error("drop_external_table: {}", error.what.c_str());
        co_return std::move(error);
    }
    if (auto err = delete_manifest_row(encoded_db, encoded_collection); err.contains_error()) {
        log_->error("drop_external_table: {}", err.what.c_str());
        co_return std::move(err);
    }
    co_return true;
}

actor_zeta::unique_future<core::result_wrapper_t<size_t>>
OtterbrixManager::drop_stale_external_tables(std::string db_name, std::pmr::vector<qualified_name_t> live) {
    OTX_ZONE_N("OtterbrixManager::drop_stale_external_tables");
    auto manifest = read_manifest(db_name);
    if (manifest.has_error()) {
        log_->error("drop_stale_external_tables: {}", manifest.error().what.c_str());
        co_return core::error_t(manifest.error().type,
                                std::pmr::string{manifest.error().what.c_str(), resource()});
    }
    std::pmr::vector<std::pmr::string> live_collections(resource());
    live_collections.reserve(live.size());
    for (const auto& name : live) {
        live_collections.emplace_back();
        live_collections.back().assign(encode_external_collection(name));
    }

    size_t removed = 0;
    for (const auto& collection : manifest.value()) {
        if (std::find(live_collections.begin(), live_collections.end(), collection) != live_collections.end()) {
            continue;
        }
        const std::string encoded{collection.c_str(), collection.size()};
        auto cursor = at_engine_boundary(resource(), [&] {
            return data_manager_->execute_sql(sql_gen::drop_table_statement(db_name, encoded));
        });
        if (failed(cursor)) {
            // A row without a collection is what a crash between a manifest
            // write and the create leaves; the row alone is removed.
            if (!cursor || cursor->get_error().type != core::error_code_t::table_not_exists) {
                auto error = engine_failure(resource(),
                                            cursor,
                                            "Failed to drop stale mirror '" + db_name + "." + encoded + "'");
                log_->error("drop_stale_external_tables: {}", error.what.c_str());
                co_return std::move(error);
            }
            log_->info("drop_stale_external_tables: manifest of {} names {} which the engine does not hold",
                       db_name,
                       encoded);
        } else {
            log_->info("drop_stale_external_tables: dropped {}.{}: the backend no longer has it", db_name, encoded);
        }
        if (auto err = delete_manifest_row(db_name, encoded); err.contains_error()) {
            log_->error("drop_stale_external_tables: {}", err.what.c_str());
            co_return std::move(err);
        }
        ++removed;
    }
    co_return removed;
}

actor_zeta::unique_future<components::cursor::cursor_t_ptr> OtterbrixManager::execute(session_hash_t id,
                                                                                      OtterbrixStatementPtr params) {
    OTX_ZONE_N("OtterbrixManager::execute");
    log_->trace("execute id hash: {}", id);

    // A fully-remote DML already ran on the backend. What is left is a no-op
    // plan whose only non-resolve child is the substituted data node, and
    // running it would DESTROY the count: the engine's pipeline pump reads a
    // zero-column batch as its drain sentinel. Answer from the number captured
    // at substitution time, in the carrier shape a local DML produces (see
    // OtterbrixStatement::remote_affected_rows).
    if (params->remote_affected_rows != OtterbrixStatement::no_remote_dml) {
        const size_t affected = params->remote_affected_rows;
        log_->trace("execute: remote DML, {} affected row(s), engine bypassed", affected);
        co_return cursor::make_cursor(resource(), make_affected_count_carrier(resource(), affected));
    }

    auto cursor_data = at_engine_boundary(resource(), [&] { return data_manager_->execute_plan(params); });
    log_->trace("execute: execute_plan done");
    if (cursor_data && !cursor_data->is_error()) {
        // The result shape every frontend reads: columns are a resultset, no
        // columns is an affected count. The plan root says which one the
        // statement owes, and the engine's result is held to it both ways.
        const bool has_columns = cursor_data->column_count() > 0;
        if (has_columns && dml_without_returning(params->node)) {
            // A no-RETURNING DML reports only its affected-row count, and the engine
            // encodes it as the CARDINALITY of a carrier: insert/update build that
            // carrier column-less, DELETE builds it from the table's storage types
            // without ever writing a value into them. Those columns are allocated
            // vectors, so every frontend would branch on column_count() > 0 and
            // serialise cardinality-many rows of never-initialised heap. Keep the
            // count, drop the columns.
            cursor_data =
                cursor::make_cursor(resource(), make_affected_count_carrier(resource(), cursor_data->size()));
        } else if (const char* kind = row_producing_statement(params->node); !has_columns && kind != nullptr) {
            // A SELECT / RETURNING whose result has no columns would reach the
            // client as "N rows affected". The engine produces such a result for
            // a column it cannot type (a `hugeint` column has no pg_type row and
            // is read back as UNKNOWN); no column is invented for it.
            std::pmr::string what{kind, resource()};
            what.append(" returned a result without columns: the engine produced no typed output column for the "
                        "statement");
            log_->error("execute: {}", what.c_str());
            cursor_data =
                cursor::make_cursor(resource(), core::error_t(core::error_code_t::schema_error, std::move(what)));
        }
    }
    log_->trace("execute finish");
    co_return std::move(cursor_data);
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>>
OtterbrixManager::get_schema(session_hash_t id,
                             std::pmr::map<qualified_name_t, size_t> dependencies,
                             ParsedQueryDataPtr data) {
    OTX_ZONE_N("OtterbrixManager::get_schema");
    log_->trace("get_schema id hash: {}", id);

    // The transformer wraps table-referencing statements in a node_sequence_t
    // whose data-producing node is the LAST child; planner-emitted sequences
    // order children differently but never reach this path. Unwrap before the
    // aggregate check below.
    const logical_plan::node_t* schema_root = data->otterbrix_params->node.get();
    if (schema_root->type() == logical_plan::node_type::sequence_t) {
        if (schema_root->children().empty()) {
            log_->error("get_schema: sequence node has no children, cannot compute schema");
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"Sequence node has no children, cannot compute schema", resource()});
        }
        schema_root = schema_root->children().back().get();
    }
    // A set operation answers rows as much as an aggregate does, and its
    // columns are its first branch's. It cannot take the engine's plan
    // validation below — that pass stamps the aggregate consumer, not a union
    // root — so it is computed from that branch instead. Answering it "no
    // schema" is what must not happen: a frontend can only send that as NoData,
    // the message that states the statement returns no rows, and a client that
    // believes it does not get an error but a row it reads as zero values
    // (lib/pq). A statement that answers rows is therefore never described that
    // way.
    const bool set_operation = schema_root->type() == logical_plan::node_type::union_t;

    // Only an aggregate consumer or that set operation produces a result
    // schema. Every other root (DDL, DML, inlined raw data) has none, and that
    // is an empty answer, not a failure — whichever side, engine or federated,
    // would otherwise describe it.
    if (schema_root->type() != logical_plan::node_type::aggregate_t && !set_operation) {
        co_return std::make_pair(cursor::make_cursor(resource()), std::move(data));
    }

    // A statement with no external slot and every parameter bound runs entirely
    // in the engine, so its output columns are the ones the engine's own plan
    // validation stamps — the same types the execution will produce, duplicate
    // names included (both key columns of a JOIN). The federated computation
    // below dedups by name and cannot describe such a result.
    //
    // A parameterized statement is not validatable at all — the plan-only pass
    // refuses a plan that still carries `$n`, and the binder holds every
    // parameter until finalize — so it takes that computation instead, over the
    // columns the probes answer for its own relations. That is what expands its
    // `*` before Bind; where the computation dedups a name or cannot type a
    // call, the frontend's own check on the executed result is what catches the
    // difference.
    if (data->otterbrix_params->external_nodes_count == 0 && data->otterbrix_params->parameters_count == 0 &&
        !set_operation) {
        auto cursor_data =
            at_engine_boundary(resource(), [&] { return data_manager_->plan_output_schema(data->otterbrix_params); });
        if (failed(cursor_data)) {
            co_return engine_failure(resource(), cursor_data, "get_schema");
        }
        log_->trace("get_schema finish (engine-validated plan)");
        co_return std::make_pair(std::move(cursor_data), std::move(data));
    }

    // A set operation is described from the branch its result is named after;
    // an aggregate root describes itself.
    if (set_operation) {
        schema_root = first_aggregate(schema_root);
        if (schema_root == nullptr) {
            co_return std::make_pair(cursor::make_cursor(resource()), std::move(data));
        }
    }

    // Dependency-map values are indices 0..N-1; the schema cursor returned by
    // IDataManager::get_schema is positional (type_data()[i] = schema of
    // dependency i), so invert the map into index order here.
    OtterbrixSchemaParams params(resource());
    params.resize(dependencies.size());

    for (auto& [name, index] : dependencies) {
        assert(index < params.size());
        // Only local named tables are probed against the engine. External
        // tables (non-empty uid) are resolved through the CatalogManager, and
        // unnamed wrapper aggregates carry no table at all — both keep their
        // positional slot as an empty entry.
        if (name.unique_identifier.empty() && !name.collection.empty()) {
            params[index] = std::make_pair(name.database, name.collection);
        }
    }

    auto cursor_data = cursor::make_cursor(resource());
    if (params.size()) {
        cursor_data = at_engine_boundary(resource(), [&] { return data_manager_->get_schema(params); });
        log_->trace("get_schema: get_schema done");
        if (failed(cursor_data)) {
            co_return engine_failure(resource(), cursor_data, "get_schema");
        }
    }

    auto schema = schema_utils::compute_otterbrix_schema(
        static_cast<const logical_plan::node_aggregate_t&>(*schema_root),
        data->otterbrix_params->params_node.get(),
        std::move(cursor_data),
        std::move(dependencies));

    log_->trace("get_schema finish");
    co_return std::make_pair(std::move(schema), std::move(data));
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
OtterbrixManager::create_table(session_hash_t id, std::string database, std::string table,
                               components::vector::data_chunk_t chunk) {
    OTX_ZONE_N("OtterbrixManager::create_table");
    log_->trace("create_table id hash: {}, target {}.{}", id, database, table);
    const std::string context = "create_table " + database + "." + table;

    auto db_cursor = at_engine_boundary(resource(), [&] { return data_manager_->create_database(database); });
    if (failed(db_cursor)) {
        auto error = engine_failure(resource(), db_cursor, context + ": create_database");
        log_->error("{}", error.what.c_str());
        co_return std::move(error);
    }

    const auto types = chunk.types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
        // A column definition needs a name; the loaded chunk is the only source
        // of one, so a nameless column cannot become part of the table.
        if (!types[i].has_alias()) {
            co_return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{(context + ": column " + std::to_string(i) + " has no name").c_str(), resource()});
        }
        columns.emplace_back(types[i].alias(), types[i]);
    }

    auto cursor = at_engine_boundary(resource(), [&] {
        return data_manager_->insert_data(database, table, std::move(columns), std::move(chunk));
    });
    if (failed(cursor)) {
        auto error = engine_failure(resource(), cursor, context + ": insert_data");
        log_->error("{}", error.what.c_str());
        co_return std::move(error);
    }
    co_return true;
}
