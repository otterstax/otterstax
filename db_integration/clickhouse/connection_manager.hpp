// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "connectors/clickhouse/manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/session.hpp"
#include "utility/worker.hpp"

#include <functional>
#include <memory_resource>
#include <string>

namespace db_conn {
    // ChConnectionManager ONLY fetches data from ClickHouse, NO JOIN operations
    class ChConnectionManager final : public actor_zeta::cooperative_supervisor<ChConnectionManager> {
    public:
        ChConnectionManager(std::pmr::memory_resource* res,
                            std::shared_ptr<chc::ConnectorManager> connector_manager);
        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        std::shared_ptr<chc::ConnectorManager> connector_manager_;
        // Behaviors
        actor_zeta::behavior_t execute_;
        log_t log_;

        /// async method - ONLY fetches data, does NOT perform JOIN
        auto execute(session_hash_t id, ParsedQueryDataPtr&& data, actor_zeta::address_t scheduler) -> void;

        void send_error(session_hash_t id, std::string error_msg, actor_zeta::address_t scheduler);
        void send_result(session_hash_t id, ParsedQueryDataPtr&& data, actor_zeta::address_t scheduler);

        std::mutex input_mtx_;
        TaskManager<std::packaged_task<void()>> worker_;
    };
} // namespace db_conn
