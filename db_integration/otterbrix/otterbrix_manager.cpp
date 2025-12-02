// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "otterbrix_manager.hpp"

#include "../../routes/otterbrix_manager.hpp"
#include "../../routes/scheduler.hpp"
#include "../../scheduler/schema_utils.hpp"
#include "../../utility/timer.hpp"

#include <iostream>
#include <ranges>
#include <thread>

using namespace db_conn;

OtterbrixManager::OtterbrixManager(std::pmr::memory_resource* res, std::unique_ptr<IDataManager> data_manager)
    : actor_zeta::cooperative_supervisor<OtterbrixManager>(res)
    , data_manager_(std::move(data_manager))
    , execute_(actor_zeta::make_behavior(resource(),
                                         otterbrix_manager::handler_id(otterbrix_manager::route::execute),
                                         this,
                                         &OtterbrixManager::execute))
    , get_schema_(actor_zeta::make_behavior(resource(),
                                            otterbrix_manager::handler_id(otterbrix_manager::route::get_schema),
                                            this,
                                            &OtterbrixManager::get_schema)) {
    worker_.start(); // Start the worker thread manager
}

auto OtterbrixManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("OtterbrixManager::make_scheduler");
    return nullptr; // This should be implemented to return a valid scheduler
}

auto OtterbrixManager::make_type() const noexcept -> const char* const { return "OtterbrixManager"; }

auto OtterbrixManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    std::unique_lock<std::mutex> _(input_mtx_);
    set_current_message(std::move(msg));
    behavior()(current_message());
}

actor_zeta::behavior_t OtterbrixManager::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        switch (msg->command()) {
            case otterbrix_manager::handler_id(otterbrix_manager::route::execute): {
                execute_(msg);
                break;
            }
            case otterbrix_manager::handler_id(otterbrix_manager::route::get_schema): {
                get_schema_(msg);
                break;
            }
        }
    });
}

auto OtterbrixManager::execute(session_hash_t id, OtterbrixStatementPtr&& params) -> void {
    try {
        Timer timer("OtterbrixManager::execute");

        std::cout << "OtterbrixManager::execute id hash: " << id << std::endl;

        auto cursor_data = this->data_manager_->execute_plan(params);
        std::cout << "OtterbrixManager::execute: execute_plan done" << std::endl;
        send_result(id, std::move(cursor_data));
        std::cout << "OtterbrixManager::execute finish" << std::endl;
    } catch (const std::exception& e) {
        send_error(id, e.what());
    } catch (...) {
        send_error(id, "OtterbrixManager::execute caught unknown exception");
    }
}

auto OtterbrixManager::get_schema(session_hash_t id,
                                  std::pmr::map<collection_full_name_t, size_t> dependencies,
                                  ParsedQueryDataPtr&& data) -> void {
    Timer timer("OtterbrixManager::get_schema");

    std::cout << "OtterbrixManager::get_schema id hash: " << id << std::endl;

    // we finally have everything to compute schema
    // joins - resulting schema does not depend on type of join - it's always union of aggregated fields
    // (nullability does depend on join type, however, all fields are nullable in arrow::schema by default)
    OtterbrixSchemaParams params(resource());
    params.reserve(dependencies.size());

    for (auto& [name, _] : dependencies) {
        params.emplace_back(std::make_pair(name.database, name.collection));
    }

    if (data->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
        send_schema(id, cursor::make_cursor(resource()), std::move(data));
        return;
    }

    auto cursor_data = cursor::make_cursor(resource());
    if (params.size()) {
        cursor_data = this->data_manager_->get_schema(params);
        std::cout << "OtterbrixManager::get_schema: get_schema done" << std::endl;
        if (cursor_data->is_error()) {
            send_schema(id, std::move(cursor_data), std::move(data));
            return;
        }
    }

    auto schema = schema_utils::compute_otterbrix_schema(
        static_cast<const logical_plan::node_aggregate_t&>(*data->otterbrix_params->node),
        data->otterbrix_params->params_node.get(),
        std::move(cursor_data),
        std::move(dependencies));
    send_schema(id, std::move(schema), std::move(data));
    std::cout << "OtterbrixManager::get_schema finish" << std::endl;
}

void OtterbrixManager::send_schema(session_hash_t id,
                                   components::cursor::cursor_t_ptr cursor,
                                   ParsedQueryDataPtr&& data) {
    auto send_task = [this, id, cursor_data = std::move(cursor), &data]() mutable {
        std::cout << "OtterbrixManager::get_schema send task" << std::endl;
        actor_zeta::send(current_message()->sender(),
                         address(),
                         scheduler::handler_id(scheduler::route::get_otterbrix_schema_finish),
                         id,
                         std::move(cursor_data),
                         std::move(data));
    };
    if (!worker_.addTask(std::move(send_task))) {
        std::cerr << "OtterbrixManager::get_schema failed to add task to worker" << std::endl;
    } else {
        std::cout << "OtterbrixManager::get_schema added task to worker" << std::endl;
    }
}

void OtterbrixManager::send_result(session_hash_t id, components::cursor::cursor_t_ptr cursor) {
    auto send_task = [this, id, cursor_data = std::move(cursor)]() mutable {
        std::cout << "OtterbrixManager::execute send task" << std::endl;
        actor_zeta::send(current_message()->sender(),
                         address(),
                         scheduler::handler_id(scheduler::route::execute_otterbrix_finish),
                         id,
                         std::move(cursor_data));
    };
    if (!worker_.addTask(std::move(send_task))) {
        std::cerr << "OtterbrixManager::execute failed to add task to worker" << std::endl;
    } else {
        std::cout << "OtterbrixManager::execute added task to worker" << std::endl;
    }
}

void OtterbrixManager::send_error(session_hash_t id, std::string error_msg) {
    std::cerr << "OtterbrixManager::execute caught exception: " << error_msg << std::endl;
    auto send_task = [this, id, msg = std::move(error_msg)]() mutable {
        actor_zeta::send(current_message()->sender(),
                         address(),
                         scheduler::handler_id(scheduler::route::execute_failed),
                         id,
                         std::move(msg));
    };
    if (!worker_.addTask(std::move(send_task))) {
        std::cerr << "OtterbrixManager::execute failed to add task to worker" << std::endl;
    } else {
        std::cout << "OtterbrixManager::execute added task to worker" << std::endl;
    }
}