// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "scheduler/scheduler.hpp"

#include "otterbrix/parser/parser.hpp"
#include "utility/logger.hpp"

#include <actor-zeta.hpp>

#include <cassert>
#include <chrono>
#include <list>
#include <memory_resource>
#include <utility>

Scheduler::Scheduler(std::pmr::memory_resource* res,
                     actor_zeta::scheduler_raw scheduler,
                     std::size_t worker_count,
                     parser_factory_fn parser_factory,
                     actor_zeta::address_t sql_connection_manager,
                     actor_zeta::address_t pg_connection_manager,
                     actor_zeta::address_t ch_connection_manager,
                     actor_zeta::address_t otterbrix_manager,
                     actor_zeta::address_t catalog_manager,
                     actor_zeta::address_t s3_manager,
                     actor_zeta::address_t file_manager,
                     actor_zeta::address_t kafka_manager)
    : resource_(res)
    , scheduler_(scheduler)
    , log_(get_logger(logger_tag::SCHEDULER))
    , workers_(res)
    , worker_addresses_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(scheduler_ != nullptr);
    assert(worker_count > 0);
    assert(parser_factory != nullptr);

    // Each Worker owns its own parser instance — no shared objects between actors
    // (codex rule 10). The factory is a plain function pointer (rule 14: no
    // std::function); the built parser is moved into the Worker. s3/file manager
    // addresses are forwarded so each Worker can dispatch external-table
    // statements (CREATE EXTERNAL TABLE / COPY ... TO) without touching shared
    // state.
    workers_.reserve(worker_count);
    worker_addresses_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        auto worker = actor_zeta::spawn<Worker>(res,
                                                i,
                                                worker_count,
                                                parser_factory(res),
                                                sql_connection_manager,
                                                pg_connection_manager,
                                                ch_connection_manager,
                                                otterbrix_manager,
                                                catalog_manager,
                                                s3_manager,
                                                file_manager,
                                                kafka_manager);
        worker_addresses_.push_back(worker->address());
        workers_.push_back(std::move(worker));
    }

    // Event-loop-in-thread (otterbrix dispatcher model). enqueue_impl (any sender
    // thread) only pushes into inbox_ and wakes pump_cv_; this thread owns ALL
    // behaviour processing. The in-flight slot list is LOCAL to the loop, so no
    // lock guards the phase logic.
    loop_thread_ = std::thread([this] {
        std::pmr::list<in_flight_entry_t> in_flight(resource_);
        while (loop_running_.load(std::memory_order_acquire)) {
            // Drain the inbox into local slots, re-wrapping each raw pointer into a
            // message_ptr. The behavior created below holds a raw pointer into the
            // message, so pending_msg must outlive it.
            actor_zeta::mailbox::message* raw = nullptr;
            while (inbox_.pop(raw)) {
                in_flight.emplace_back();
                in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
            }

            bool progress = true;
            while (progress) {
                progress = false;

                // (a) Create the behavior for the first slot that needs one. The
                //     coroutine runs on this loop thread until its first co_await.
                {
                    in_flight_entry_t* slot = nullptr;
                    for (auto& e : in_flight) {
                        if (e.pending_msg && !e.behavior) {
                            slot = &e;
                            break;
                        }
                    }
                    if (slot) {
                        slot->behavior = behavior(slot->pending_msg.get());
                        progress = true;
                        continue;
                    }
                }

                // (b) Resume the first behavior whose awaited future is ready.
                {
                    actor_zeta::detail::coroutine_handle<> cont{};
                    for (auto& e : in_flight) {
                        if (e.behavior.is_awaited_ready()) {
                            cont = e.behavior.take_awaited_continuation();
                            if (cont) {
                                break;
                            }
                        }
                    }
                    if (cont) {
                        cont.resume();
                        progress = true;
                        continue;
                    }
                }

                // (c) Erase one finished slot, then restart the pass (erase
                //     invalidates the iteration). behavior + pending_msg destruct
                //     on this thread.
                for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                    if (it->behavior && it->behavior.done()) {
                        in_flight.erase(it);
                        progress = true;
                        break;
                    }
                }
            }

            // Idle sleep. The 100µs timeout re-polls is_awaited_ready() even if a
            // worker completes cross-thread without a new inbox push, guaranteeing
            // liveness (no lost-wakeup on this side).
            std::unique_lock<std::mutex> lk(mutex_);
            if (inbox_.empty()) {
                pump_cv_.wait_for(lk, std::chrono::microseconds(100));
            }
        }
        // Local in_flight destructs HERE on the loop thread: still-suspended
        // behaviors are destroyed safely.
    });

    log_->info("Scheduler initialized with {} workers", worker_count);
}

Scheduler::~Scheduler() {
    loop_running_.store(false, std::memory_order_release);
    pump_cv_.notify_one();
    if (loop_thread_.joinable()) {
        loop_thread_.join();
    }
    // Drain any leftover inbox_ raw pointers so their PMR memory is freed.
    actor_zeta::mailbox::message* raw = nullptr;
    while (inbox_.pop(raw)) {
        actor_zeta::mailbox::message_ptr drop{raw};
    }
}

std::pair<bool, actor_zeta::detail::enqueue_result>
Scheduler::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    // Delivery only — ALL processing happens on loop_thread_. The lock-free inbox
    // takes ownership of the raw message* (release()); the loop re-wraps it.
    inbox_.push(msg.release());
    pump_cv_.notify_one();
    return {false, actor_zeta::detail::enqueue_result::success};
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
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::release_session>) {
        co_await actor_zeta::dispatch(this, &Scheduler::release_session, msg);
    } else if (cmd == actor_zeta::msg_id<Scheduler, &Scheduler::execute_plan>) {
        co_await actor_zeta::dispatch(this, &Scheduler::execute_plan, msg);
    }
}

// ─── Handlers: route by session hash to a Worker, forward the Worker's future ──

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute(session_hash_t id, std::string sql) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] = actor_zeta::send(worker_addresses_[idx], &Worker::execute, id, std::move(sql));
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute_statement(session_hash_t id) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] = actor_zeta::send(worker_addresses_[idx], &Worker::execute_statement, id);
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute_prepared_statement(session_hash_t id,
                                      std::pmr::vector<components::types::logical_value_t> parameters) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] =
        actor_zeta::send(worker_addresses_[idx], &Worker::execute_prepared_statement, id, std::move(parameters));
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::prepare_schema(session_hash_t id, std::string sql) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] = actor_zeta::send(worker_addresses_[idx], &Worker::prepare_schema, id, std::move(sql));
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}

actor_zeta::unique_future<void>
Scheduler::release_session(session_hash_t id) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] = actor_zeta::send(worker_addresses_[idx], &Worker::release_session, id);
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}

actor_zeta::unique_future<Scheduler::session_result>
Scheduler::execute_plan(session_hash_t id, ParsedQueryDataPtr data) {
    const auto idx = id % workers_.size();
    auto [needs_sched, fut] = actor_zeta::send(worker_addresses_[idx], &Worker::execute_plan, id, std::move(data));
    if (needs_sched) {
        scheduler_->enqueue(workers_[idx].get());
    }
    co_return co_await std::move(fut);
}
