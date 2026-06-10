// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "connectors/postgresql/manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <functional>
#include <memory_resource>
#include <mutex>
#include <string>

namespace db {
    // PostgressManager ONLY fetches data from PostgreSQL, NO JOIN operations
    class PostgressManager final : public actor_zeta::actor::actor_mixin<PostgressManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        PostgressManager(std::pmr::memory_resource* res, std::shared_ptr<pg::ConnectorManager> connector_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutine — ONLY fetches data, does NOT perform JOIN
        actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> execute(session_hash_t id,
                                                                                 ParsedQueryDataPtr data);

        using dispatch_traits = actor_zeta::dispatch_traits<&PostgressManager::execute>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        std::shared_ptr<pg::ConnectorManager> connector_manager_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "PostgressManager::mutex");
        actor_zeta::behavior_t current_behavior_;
    };
} // namespace db
