// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "connection_manager.hpp"

#include "../../routes/nosql_connection_manager.hpp"
#include "../../routes/scheduler.hpp"

#include <iostream>
#include <ranges>
#include <thread>

using namespace db_conn;

NoSqlConnectionManager::NoSqlConnectionManager(std::pmr::memory_resource* res)
    : actor_zeta::cooperative_supervisor<NoSqlConnectionManager>(res)
    , execute_(actor_zeta::make_behavior(resource(),
                                         nosql_connection_manager::handler_id(nosql_connection_manager::route::execute),
                                         this,
                                         &NoSqlConnectionManager::execute)) {}

auto NoSqlConnectionManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
    assert("NoSqlConnectionManager::make_scheduler");
    return nullptr; // This should be implemented to return a valid scheduler
}

auto NoSqlConnectionManager::make_type() const noexcept -> const char* const { return "SQLConnectionManager"; }

auto NoSqlConnectionManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
    std::unique_lock<std::mutex> _(input_mtx_);
    set_current_message(std::move(msg));
    behavior()(current_message());
}

actor_zeta::behavior_t NoSqlConnectionManager::behavior() {
    return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
        switch (msg->command()) {
            case nosql_connection_manager::handler_id(nosql_connection_manager::route::execute): {
                execute_(msg);
                break;
            }
        }
    });
}

auto NoSqlConnectionManager::execute(size_t id,
                                     OtterbrixExecuteParamsPtr params,
                                     std::vector<components::vector::data_chunk_t> data) -> void {
    std::cout << "NoSqlConnectionManager::execute id: " << id << std::endl;
}