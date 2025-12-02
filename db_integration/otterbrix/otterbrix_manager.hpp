// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../../otterbrix/operators/execute_plan.hpp"
#include "../../otterbrix/parser/parser.hpp"
#include "../../types/otterbrix.hpp"
#include "../../utility/session.hpp"
#include "../../utility/worker.hpp"
#include <actor-zeta.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace db_conn {
    class OtterbrixManager final : public actor_zeta::cooperative_supervisor<OtterbrixManager> {
    public:
        OtterbrixManager(std::pmr::memory_resource* res, std::unique_ptr<IDataManager> data_manager);

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        std::unique_ptr<IDataManager> data_manager_;
        // Behaviors
        actor_zeta::behavior_t execute_;
        actor_zeta::behavior_t get_schema_;

        /// async method
        auto execute(session_hash_t id, OtterbrixStatementPtr&& params) -> void;
        auto get_schema(session_hash_t id,
                        std::pmr::map<collection_full_name_t, size_t> dependencies,
                        ParsedQueryDataPtr&& data) -> void;
        void send_result(session_hash_t id, components::cursor::cursor_t_ptr cursor);
        void send_schema(session_hash_t id, components::cursor::cursor_t_ptr cursor, ParsedQueryDataPtr&& data);
        void send_error(session_hash_t id, std::string error_msg);

        std::mutex input_mtx_;
        TaskManager<std::function<void()>> worker_;
    };
} // namespace db_conn