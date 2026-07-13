// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <components/log/log.hpp>

#include "scheduler/session_data.hpp"
#include "scheduler/worker.hpp"
#include "utility/session.hpp"

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <condition_variable>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>

// Scheduler — a thin session-affinity router over a pool of Worker actors,
// modelled on otterbrix's services::dispatcher::manager_dispatcher_t.
//
// Two structural pieces:
//   * the worker pool: N Worker actors spawned on the actor-zeta sharing_scheduler;
//     a query keyed by session_hash always routes to workers_[id % N] (sticky), so
//     prepared-statement state stays on one Worker and no state is shared (rules 10, 12).
//   * the event loop: enqueue_impl (any sender thread) only delivers into the
//     lock-free inbox_ and wakes loop_thread_; ALL behaviour processing — coroutine
//     creation, continuation resume — happens on loop_thread_. This decouples the
//     asio frontend threads from the work and removes the inline-pump yield-spin.
//
// Each handler is a coroutine that forwards to a Worker and co_returns the Worker's
// result (future-of-future passthrough); the frontend awaits the resulting future
// through frontend/common/asio_future_bridge.hpp.
class Scheduler final : public actor_zeta::actor::actor_mixin<Scheduler> {
public:
    using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;
    using session_result = core::result_wrapper_t<session_payload>;

    // Stateless parser factory: a plain function pointer (NOT std::function —
    // codex rule 14) so production passes &make_parser and tests pass
    // &make_mock_parser. Each Worker builds its own parser instance from it, so
    // no parser object is shared between actors (rule 10).
    using parser_factory_fn = parser_ptr (*)(std::pmr::memory_resource*);

    // s3_manager / file_manager addresses are forwarded as-is to every Worker so
    // CREATE EXTERNAL TABLE and COPY ... TO statements (parsed as
    // otterstax::external::external_node_t) can be routed to db::S3Manager or
    // conn::file::FileManager from inside the Worker pipeline.
    Scheduler(std::pmr::memory_resource* res,
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
              // Optional: production (component_manager) passes the KafkaManager;
              // tests that don't exercise Kafka omit it (Workers never send to an
              // empty address unless a kafka_node_t is parsed).
              actor_zeta::address_t kafka_manager = actor_zeta::address_t::empty_address());
    ~Scheduler();

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

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

    // Delivery-only: push into the lock-free inbox and wake loop_thread_. All
    // processing happens on loop_thread_ (otterbrix dispatcher event-loop model).
    std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    // One in-flight message in the event loop. The behavior coroutine holds a raw
    // pointer into pending_msg across suspension, so pending_msg must outlive it.
    struct in_flight_entry_t {
        actor_zeta::mailbox::message_ptr pending_msg{};
        actor_zeta::behavior_t behavior{};
    };

    std::pmr::memory_resource* resource_;
    actor_zeta::scheduler_raw scheduler_;
    log_t log_;

    std::pmr::vector<std::unique_ptr<Worker, actor_zeta::pmr::deleter_t>> workers_;
    std::pmr::vector<actor_zeta::address_t> worker_addresses_;

    // Event-loop machinery: enqueue_impl delivers into inbox_ + notifies pump_cv_;
    // loop_thread_ owns all processing. mutex_/pump_cv_ guard only the loop's idle
    // sleep (infra synchronisation, not a data lock — codex rule 12).
    std::thread loop_thread_;
    std::atomic<bool> loop_running_{true};
    boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
    std::mutex mutex_;
    std::condition_variable pump_cv_;
};
