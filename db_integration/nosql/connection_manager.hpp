// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>
// #include <core/spinlock/spinlock.hpp>
// #include <integration/cpp/impl/session_blocker.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "types/otterbrix.hpp"

#include <memory_resource>
#include <string>
#include <vector>

namespace db_conn {
    class NoSqlConnectionManager final : public actor_zeta::cooperative_supervisor<NoSqlConnectionManager> {
    public:
        [[deprecated("Not implemented")]] NoSqlConnectionManager(std::pmr::memory_resource* resource);

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        log_t log_;
        // Behaviors
        actor_zeta::behavior_t execute_;

        /// async method
        auto execute(size_t id, OtterbrixExecuteParamsPtr params, std::vector<components::vector::data_chunk_t> data)
            -> void;

        std::mutex input_mtx_;
    };
} // namespace db_conn
