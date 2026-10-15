// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once

#include "scheduler/scheduler.hpp"
#include <optional>

namespace frontend {
    // Owned by one native connection and used only on its executor. Actor
    // messages carry a copy of context_; no actor observes this mutable state.
    class sql_session {
    public:
        enum class state { idle, active, failed };
        using result = Scheduler::session_result;
        sql_session(std::pmr::memory_resource* resource, actor_zeta::address_t scheduler, bool mysql);
        result execute(session_hash_t request, std::string sql);
        result prepare(session_hash_t request, std::string sql);
        result execute_prepared(session_hash_t request, const std::string& sql,
                                std::pmr::vector<components::types::logical_value_t> parameters);
        result close();
        // PostgreSQL extended-query batches hold a transaction until Sync.
        // BEGIN promotes that transaction to an explicit block.
        result begin_implicit();
        result finish_implicit();
        void mark_failed();
        state status() const { return state_; }
        bool autocommit() const { return autocommit_; }
        bool broken() const { return broken_; }

    private:
        std::optional<result> before_execute(const std::string& sql);
        result control(const char* sql, const char* command, state next);
        result submit(std::string sql);
        result error(core::error_code_t code, const char* message);
        result await(actor_zeta::unique_future<result> future);
        execution_context context() const;

        std::pmr::memory_resource* resource_;
        actor_zeta::address_t scheduler_;
        execution_context context_;
        state state_{state::idle};
        bool mysql_;
        bool autocommit_{true};
        bool broken_{false};
        bool closed_{false};
        bool implicit_{false};
    };
}
