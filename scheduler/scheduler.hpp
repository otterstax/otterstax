// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "scheduler/session_data.hpp"
#include "scheduler/worker.hpp"
#include "utility/session.hpp"
#include "utility/execution_context.hpp"

#include <memory>
#include <memory_resource>

namespace db {
    class MySQLManager;
    class PostgressManager;
}

// Scheduler — a thin session-affinity router over a pool of Worker actors,
// modelled on otterbrix's services::dispatcher::manager_dispatcher_t.
//
// Two structural pieces:
//   * the worker pool: N Worker actors, each with an interleaved actor mailbox;
//     a query keyed by request ID always routes to workers_[id % N] (sticky), so
//     prepared-statement state stays on one Worker and no state is shared (rules 10, 12).
//   * the event loop: enqueue_impl (any sender thread) only delivers into the
//     mailbox; all coroutine creation and resumption happens on the actor's
//     own thread. Pending handlers can advance independent requests.
//
// Each handler is a coroutine that forwards to a Worker and co_returns the Worker's
// result (future-of-future passthrough); the frontend awaits the resulting future
// through frontend/common/asio_future_bridge.hpp. Nothing in a handler throws:
// errors travel as core::error_t inside the Worker's result.
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
    using parser_factory_fn = parser_ptr (*)(std::pmr::memory_resource*, const otterstax::names::alias_registry_t&);

    // s3_manager / file_manager addresses are forwarded as-is to every Worker so
    // CREATE EXTERNAL TABLE and COPY ... TO statements (parsed as
    // otterstax::external::external_node_t) can be routed to db::S3Manager or
    // conn::file::FileManager from inside the Worker pipeline.
    Scheduler(std::pmr::memory_resource* res,
              std::size_t worker_count,
              parser_factory_fn parser_factory,
              const otterstax::names::alias_registry_t& aliases,
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

    unique_future<session_result> execute(session_hash_t id, std::string sql, execution_context context);
    unique_future<session_result> execute_statement(session_hash_t id, execution_context context);
    unique_future<session_result>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters, execution_context context);
    unique_future<session_result> prepare_schema(session_hash_t id, std::string sql, execution_context context);
    unique_future<session_result> close_statement(session_hash_t id);
    // Wait for this connection's outstanding work, then roll back and release
    // its engine session. A timed-out caller may drop its future; cleanup still
    // runs on the Scheduler after the request actually finishes.
    unique_future<session_result> close_session(execution_context context);
    unique_future<core::result_wrapper_t<actor_zeta::address_t>> enlist_transaction(execution_context context, backend_type_t backend, std::string alias);
    unique_future<session_result> finish_transaction(execution_context context, bool commit);
    // Pre-built plans (the Spark Connect frontend translates a DataFrame
    // relation straight into a logical plan): execute runs it like a parsed
    // statement, prepare describes its result schema without running it.
    unique_future<session_result> execute_plan(session_hash_t id, ParsedQueryDataPtr data);
    unique_future<session_result> prepare_plan(session_hash_t id, ParsedQueryDataPtr data);

    using dispatch_traits = actor_zeta::dispatch_traits<&Scheduler::execute,
                                                        &Scheduler::execute_statement,
                                                        &Scheduler::execute_prepared_statement,
                                                        &Scheduler::prepare_schema,
                                                        &Scheduler::close_statement,
                                                        &Scheduler::close_session,
                                                        &Scheduler::enlist_transaction,
                                                        &Scheduler::finish_transaction,
                                                        &Scheduler::execute_plan,
                                                        &Scheduler::prepare_plan>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    // Delivery only; the mailbox owns handler and continuation execution.
    std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

private:
    struct connection_work {
        size_t pending{0};
        bool closing{false};
        backend_type_t participant{backend_type_t::Unknown};
        std::string alias;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> mysql{nullptr, actor_zeta::pmr::deleter_t{std::pmr::new_delete_resource()}};
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> postgres{nullptr, actor_zeta::pmr::deleter_t{std::pmr::new_delete_resource()}};
        actor_zeta::address_t backend_address() const;
        std::optional<actor_zeta::promise<bool>> drained;
    };
    std::unordered_map<session_id, connection_work> connections_;
    // Requests using the same prepared ID must stay ordered even while a
    // Worker services other IDs during an asynchronous backend operation.
    std::unordered_map<session_hash_t, std::deque<actor_zeta::promise<bool>>> request_lanes_;
    bool start_request(const execution_context& context);
    void finish_request(const execution_context& context);
    unique_future<session_result> finish_transaction_impl(execution_context context, bool commit);
    // Keep messages for one prepared ID ordered, forward to its Worker, and
    // release the next message only after the current result is complete.
    template<typename Method, typename... Args>
    unique_future<session_result> route(Method method, session_hash_t id, Args... args);

    std::pmr::memory_resource* resource_;
    log_t log_;
    actor_zeta::address_t sql_connection_manager_;
    actor_zeta::address_t pg_connection_manager_;

    std::pmr::vector<std::unique_ptr<Worker, actor_zeta::pmr::deleter_t>> workers_;
    std::pmr::vector<actor_zeta::address_t> worker_addresses_;

    otterstax::actor_mailbox<Scheduler, otterstax::actor_execution::interleaved> mailbox_{*this};
};
