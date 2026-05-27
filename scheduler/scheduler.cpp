// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler.hpp"

#include "utility/logger.hpp"

#include <cassert>
#include <mutex>
#include <thread>
#include <utility>

Scheduler::Scheduler(std::pmr::memory_resource* res,
                     actor_zeta::scheduler::sharing_scheduler* az_scheduler,
                     std::size_t worker_count,
                     std::function<std::unique_ptr<IParser>()> parser_factory,
                     actor_zeta::address_t sql_connection_manager,
                     actor_zeta::address_t pg_connection_manager,
                     actor_zeta::address_t ch_connection_manager,
                     actor_zeta::address_t otterbrix_manager,
                     actor_zeta::address_t catalog_manager)
    : resource_(res)
    , sched_(az_scheduler)
    , log_(get_logger(logger_tag::SCHEDULER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(sched_ != nullptr);
    assert(worker_count > 0);
    assert(parser_factory);

    workers_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(actor_zeta::spawn<Worker>(res,
                                                        i,
                                                        worker_count,
                                                        parser_factory(),
                                                        sql_connection_manager,
                                                        pg_connection_manager,
                                                        ch_connection_manager,
                                                        otterbrix_manager,
                                                        catalog_manager));
    }
    log_->info("Scheduler initialized with {} workers", worker_count);
}

actor_zeta::behavior_t Scheduler::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute_statement>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute_prepared_statement>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute_prepared_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::prepare_schema>) {
        co_await actor_zeta::dispatch(this, &Scheduler::prepare_schema, msg);
    }
}

std::pair<bool, actor_zeta::detail::enqueue_result>
Scheduler::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
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

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute(session_hash_t id, std::string sql) {
    auto& w = pick(id);
    auto [ns, fut] = actor_zeta::send(&w, &Worker::execute, id, std::move(sql));
    if (ns) {
        sched_->enqueue(&w);
    }
    return std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute_statement(session_hash_t id) {
    auto& w = pick(id);
    auto [ns, fut] = actor_zeta::send(&w, &Worker::execute_statement, id);
    if (ns) {
        sched_->enqueue(&w);
    }
    return std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute_prepared_statement(session_hash_t id,
                                      std::pmr::vector<components::types::logical_value_t> parameters) {
    auto& w = pick(id);
    auto [ns, fut] = actor_zeta::send(&w, &Worker::execute_prepared_statement, id, std::move(parameters));
    if (ns) {
        sched_->enqueue(&w);
    }
    return std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::prepare_schema(session_hash_t id, std::string sql) {
    auto& w = pick(id);
    auto [ns, fut] = actor_zeta::send(&w, &Worker::prepare_schema, id, std::move(sql));
    if (ns) {
        sched_->enqueue(&w);
    }
    return std::move(fut);
}
