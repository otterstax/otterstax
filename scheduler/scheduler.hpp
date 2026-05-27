// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <components/log/log.hpp>

#include "otterbrix/parser/parser.hpp"
#include "scheduler/result.hpp"
#include "scheduler/worker.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"

#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <vector>

class Scheduler final : public actor_zeta::actor::actor_mixin<Scheduler> {
public:
    using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;
    using session_result = otterstax::result<session_payload>;

    Scheduler(std::pmr::memory_resource* res,
              actor_zeta::scheduler::sharing_scheduler* az_scheduler,
              std::size_t worker_count,
              std::function<std::unique_ptr<IParser>()> parser_factory,
              actor_zeta::address_t sql_connection_manager,
              actor_zeta::address_t pg_connection_manager,
              actor_zeta::address_t ch_connection_manager,
              actor_zeta::address_t otterbrix_manager,
              actor_zeta::address_t catalog_manager);

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    /// Non-coroutine passthrough to a worker selected by session hash. The
    /// returned future is fulfilled by the Worker via dispatch.hpp's
    /// future-of-future passthrough.
    unique_future<session_result> execute(session_hash_t id, std::string sql);
    unique_future<session_result> execute_statement(session_hash_t id);
    unique_future<session_result>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters);
    unique_future<session_result> prepare_schema(session_hash_t id, std::string sql);

    using dispatch_traits = actor_zeta::dispatch_traits<&Scheduler::execute,
                                                        &Scheduler::execute_statement,
                                                        &Scheduler::execute_prepared_statement,
                                                        &Scheduler::prepare_schema>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    /// Synchronous dispatch (actor_mixin pattern, identical to the backend
    /// managers): keeps the behavior() coroutine alive and pumps it to
    /// completion so dispatch's future-of-future forwarding actually fires.
    /// execute() itself delegates the work to a Worker subordinate.
    std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    Worker& pick(session_hash_t id) noexcept { return *workers_[id % workers_.size()]; }

    std::pmr::memory_resource* resource_;
    actor_zeta::scheduler::sharing_scheduler* sched_;
    std::vector<std::unique_ptr<Worker, actor_zeta::pmr::deleter_t>> workers_;
    log_t log_;
    std::mutex mutex_;
    actor_zeta::behavior_t current_behavior_;
};
