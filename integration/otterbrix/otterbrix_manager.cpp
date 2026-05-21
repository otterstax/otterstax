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
                             std::pmr::map<collection_full_name_t, size_t> dependencies,
                             ParsedQueryDataPtr data) {
    Timer timer("OtterbrixManager::get_schema", log_);

    log_->trace("get_schema id hash: {}", id);

    OtterbrixSchemaParams params(resource());
    params.reserve(dependencies.size());

    for (auto& [name, _] : dependencies) {
        params.emplace_back(std::make_pair(name.database, name.collection));
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
