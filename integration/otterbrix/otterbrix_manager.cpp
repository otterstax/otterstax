// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix_manager.hpp"

#include "scheduler/schema_utils.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"

#include <core/result_wrapper.hpp>

#include <ranges>
#include <thread>

using namespace db;
using otterstax::error_code_t;
using otterstax::error_tag_t;
using otterstax::pipeline_error;

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
    std::lock_guard<std::mutex> guard(mutex_);
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

namespace {
    // Double-quoted SQL identifier; embedded double quotes are doubled.
    std::string quote_identifier(const std::string& ident) {
        std::string quoted;
        quoted.reserve(ident.size() + 2);
        quoted.push_back('"');
        for (char c : ident) {
            if (c == '"') {
                quoted.push_back('"');
            }
            quoted.push_back(c);
        }
        quoted.push_back('"');
        return quoted;
    }
} // namespace

actor_zeta::unique_future<otterstax::result<bool>>
OtterbrixManager::register_external_database(std::string db_name) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
        log_->debug("register_external_database: creating engine database {}", db_name);
        auto cursor = data_manager_->execute_sql("CREATE DATABASE " + quote_identifier(db_name));
        if (!cursor || cursor->is_error()) {
            std::string what = cursor ? std::string{cursor->get_error().what.c_str()} : std::string{"null cursor"};
            log_->error("register_external_database: CREATE DATABASE {} failed: {}", db_name, what);
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::otterbrix_manager,
                                     "Failed to create engine database '" + db_name + "': " + what);
        }
        co_return true;
    } catch (const std::exception& e) {
        log_->error("register_external_database caught exception: {}", e.what());
        co_return pipeline_error(error_code_t::catalog_error,
                                 error_tag_t::otterbrix_manager,
                                 "register_external_database failed for '" + db_name + "': " + e.what());
    }
}

actor_zeta::unique_future<otterstax::result<components::catalog::oid_t>>
OtterbrixManager::register_external_table(qualified_name_t name,
                                          std::string encoded_db,
                                          std::string encoded_collection,
                                          std::vector<components::table::column_definition_t> columns) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
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
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::otterbrix_manager,
                                     "Failed to register external table '" + name.to_string() + "': " + what);
        }
        if (oid == components::catalog::INVALID_OID) {
            log_->error("register_external_table: engine did not stamp an oid for {}", encoded_collection);
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::otterbrix_manager,
                                     "Engine did not assign an oid for external table '" + name.to_string() + "'");
        }
        log_->debug("register_external_table: {} registered with oid {}", name.to_string(), oid);
        co_return oid;
    } catch (const std::exception& e) {
        log_->error("register_external_table caught exception: {}", e.what());
        co_return pipeline_error(error_code_t::catalog_error,
                                 error_tag_t::otterbrix_manager,
                                 "register_external_table failed for '" + name.to_string() + "': " + e.what());
    }
}

actor_zeta::unique_future<otterstax::result<bool>> OtterbrixManager::drop_external_database(std::string db_name) {
    // try/catch only as containment at the engine boundary — mirrors execute().
    try {
        log_->debug("drop_external_database: dropping engine database {}", db_name);
        auto cursor = data_manager_->execute_sql("DROP DATABASE " + quote_identifier(db_name));
        if (!cursor || cursor->is_error()) {
            std::string what = cursor ? std::string{cursor->get_error().what.c_str()} : std::string{"null cursor"};
            log_->error("drop_external_database: DROP DATABASE {} failed: {}", db_name, what);
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::otterbrix_manager,
                                     "Failed to drop engine database '" + db_name + "': " + what);
        }
        co_return true;
    } catch (const std::exception& e) {
        log_->error("drop_external_database caught exception: {}", e.what());
        co_return pipeline_error(error_code_t::catalog_error,
                                 error_tag_t::otterbrix_manager,
                                 "drop_external_database failed for '" + db_name + "': " + e.what());
    }
}

actor_zeta::unique_future<components::cursor::cursor_t_ptr> OtterbrixManager::execute(session_hash_t id,
                                                                                      OtterbrixStatementPtr params) {
    try {
        Timer timer("OtterbrixManager::execute", log_);

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

actor_zeta::unique_future<otterstax::result<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>>
OtterbrixManager::get_schema(session_hash_t id,
                             std::pmr::map<qualified_name_t, size_t> dependencies,
                             ParsedQueryDataPtr data) {
    Timer timer("OtterbrixManager::get_schema", log_);

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

    if (data->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
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
        static_cast<const logical_plan::node_aggregate_t&>(*data->otterbrix_params->node),
        data->otterbrix_params->params_node.get(),
        std::move(cursor_data),
        std::move(dependencies));

    log_->trace("get_schema finish");
    co_return std::make_pair(std::move(schema), std::move(data));
}
