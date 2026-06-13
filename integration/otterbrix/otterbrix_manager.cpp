// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix_manager.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <core/result_wrapper.hpp>

#include <ranges>
#include <thread>

using namespace db;

namespace {

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
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::execute>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::execute, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::get_schema>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::get_schema, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::register_external_database>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::register_external_database, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::register_external_table>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::register_external_table, msg);
    } else if (cmd == actor_zeta::msg_id<OtterbrixManager, &OtterbrixManager::drop_external_database>) {
        co_await actor_zeta::dispatch(this, &OtterbrixManager::drop_external_database, msg);
    }
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
OtterbrixManager::register_external_database(std::string db_name) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
        log_->debug("register_external_database: creating engine database {}", db_name);
        auto cursor = data_manager_->execute_sql(sql_gen::create_database_statement(db_name));
        if (!cursor || cursor->is_error()) {
            std::string what = cursor ? std::string{cursor->get_error().what.c_str()} : std::string{"null cursor"};
            log_->error("register_external_database: CREATE DATABASE {} failed: {}", db_name, what);
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{("Failed to create engine database '" + db_name + "': " + what).c_str(), resource()});
        }
        co_return true;
    } catch (const std::exception& e) {
        log_->error("register_external_database caught exception: {}", e.what());
        co_return core::error_t(
            core::error_code_t::schema_error,
            std::pmr::string{("register_external_database failed for '" + db_name + "': " + e.what()).c_str(),
                             resource()});
    }
}

actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
OtterbrixManager::register_external_table(qualified_name_t name,
                                          std::vector<components::table::column_definition_t> columns) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
        const std::string& encoded_db = name.unique_identifier;
        const std::string encoded_collection = encode_external_collection(name);
        log_->debug("register_external_table: registering {} as {}.{}",
                    name.to_string(),
                    encoded_db,
                    encoded_collection);

        components::catalog::oid_t oid = components::catalog::INVALID_OID;
        auto create_cursor =
            data_manager_->create_collection(encoded_db, encoded_collection, std::move(columns), oid);
        if (!create_cursor || create_cursor->is_error()) {
            std::string what =
                create_cursor ? std::string{create_cursor->get_error().what.c_str()} : std::string{"null cursor"};
            log_->error("register_external_table: create_collection {}.{} failed: {}",
                        encoded_db,
                        encoded_collection,
                        what);
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{("Failed to register external table '" + name.to_string() + "': " + what).c_str(),
                                 resource()});
        }
        if (oid == components::catalog::INVALID_OID) {
            log_->error("register_external_table: engine did not stamp an oid for {}", encoded_collection);
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{("Engine did not assign an oid for external table '" + name.to_string() + "'").c_str(),
                                 resource()});
        }
        log_->debug("register_external_table: {} registered with oid {}", name.to_string(), oid);
        co_return oid;
    } catch (const std::exception& e) {
        log_->error("register_external_table caught exception: {}", e.what());
        co_return core::error_t(
            core::error_code_t::schema_error,
            std::pmr::string{("register_external_table failed for '" + name.to_string() + "': " + e.what()).c_str(),
                             resource()});
    }
}

actor_zeta::unique_future<core::result_wrapper_t<bool>> OtterbrixManager::drop_external_database(std::string db_name) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
        log_->debug("drop_external_database: dropping engine database {}", db_name);
        auto cursor = data_manager_->execute_sql(sql_gen::drop_database_statement(db_name));
        if (!cursor || cursor->is_error()) {
            std::string what = cursor ? std::string{cursor->get_error().what.c_str()} : std::string{"null cursor"};
            log_->error("drop_external_database: DROP DATABASE {} failed: {}", db_name, what);
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{("Failed to drop engine database '" + db_name + "': " + what).c_str(), resource()});
        }
        co_return true;
    } catch (const std::exception& e) {
        log_->error("drop_external_database caught exception: {}", e.what());
        co_return core::error_t(
            core::error_code_t::schema_error,
            std::pmr::string{("drop_external_database failed for '" + db_name + "': " + e.what()).c_str(),
                             resource()});
    }
}

actor_zeta::unique_future<components::cursor::cursor_t_ptr> OtterbrixManager::execute(session_hash_t id,
                                                                                      OtterbrixStatementPtr params) {
    OTX_ZONE_N("otterbrix::execute");
    try {
        log_->trace("execute id hash: {}", id);

        auto cursor_data = this->data_manager_->execute_plan(params);
        log_->trace("execute: execute_plan done");
        log_->trace("execute finish");
        co_return std::move(cursor_data);
    } catch (const std::exception& e) {
        log_->error("execute caught exception: {}", e.what());
        co_return cursor::make_cursor(
            resource(),
            core::error_t(core::error_code_t::other_error, std::pmr::string{e.what(), resource()}));
    } catch (...) {
        log_->error("execute caught unknown exception");
        co_return cursor::make_cursor(
            resource(),
            core::error_t(core::error_code_t::other_error,
                          std::pmr::string{"OtterbrixManager::execute caught unknown exception", resource()}));
    }
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>>
OtterbrixManager::get_schema(session_hash_t id,
                             std::pmr::map<qualified_name_t, size_t> dependencies,
                             ParsedQueryDataPtr data) {
    OTX_ZONE_N("otterbrix::get_schema");
    log_->trace("get_schema id hash: {}", id);

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

    // a13 transformer output wraps table-referencing statements in a
    // node_sequence_t whose data-producing node is the LAST child;
    // planner-emitted sequences order children differently but never reach
    // this path. Unwrap before the aggregate check below.
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
    if (schema_root->type() != logical_plan::node_type::aggregate_t) {
        co_return std::make_pair(cursor::make_cursor(resource()), std::move(data));
    }

    auto cursor_data = cursor::make_cursor(resource());
    if (params.size()) {
        cursor_data = this->data_manager_->get_schema(params);
        log_->trace("get_schema: get_schema done");
        if (cursor_data->is_error()) {
            co_return std::make_pair(std::move(cursor_data), std::move(data));
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
