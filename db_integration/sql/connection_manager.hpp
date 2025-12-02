// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <actor-zeta.hpp>

#include "../../connectors/mysql_manager.hpp"
#include "../../otterbrix/parser/parser.hpp"
#include "../../scheduler/schema_utils.hpp"
#include "../../utility/session.hpp"
#include "../../utility/worker.hpp"

#include <functional>
#include <memory_resource>
#include <string>

namespace db_conn {
    class SqlConnectionManager final : public actor_zeta::cooperative_supervisor<SqlConnectionManager> {
    public:
        SqlConnectionManager(std::pmr::memory_resource* res,
                             std::shared_ptr<mysqlc::ConnectorManager> connector_manager);
        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        std::shared_ptr<mysqlc::ConnectorManager> connector_manager_;
        // Behaviors
        actor_zeta::behavior_t execute_;

        /// async method
        auto execute(session_hash_t id, ParsedQueryDataPtr&& data) -> void;

        void send_error(session_hash_t id, std::string error_msg);
        void send_result(session_hash_t id, ParsedQueryDataPtr&& data);

        std::mutex input_mtx_;
        TaskManager<std::function<void()>> worker_;
    };
} // namespace db_conn