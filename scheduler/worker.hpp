// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>
#include <components/log/log.hpp>

#include "otterbrix/parser/parser.hpp"
#include "scheduler/session_data.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/session.hpp"

#include "scheduler/parking.hpp"

#include <core/result_wrapper.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <unordered_map>

// Forward declarations for the external-table dispatch path; the full types would pull Arrow's S3
// filesystem and the parser AST into every includer of this header.
namespace db {
    struct backend_registry_t;
    struct backend_set_t;
}
namespace conn {
    class transfer_pools_t;
    struct transfer_result_t;
}
namespace otterstax::external {
    class external_node_t;
}

// One execution lane of the Scheduler worker pool. A Worker is a cooperative
// actor driven by the actor-zeta sharing_scheduler; the Scheduler routes every
// session (by session_hash) to a single Worker, so each Worker owns its session
// metadata exclusively — no shared state, no locks (codex rules 10, 12). Each
// Worker carries its own parser instance (rule 10: no shared objects between
// actors).
//
// Prepared-statement lifecycle: prepare_schema stores the parsed statement under
// its session hash; the first execute_statement / execute_prepared_statement
// consumes it — the entry is erased on every exit path, success or error — and
// any later execute on that session answers invalid_parameter ("prepared
// statement must be re-prepared"). close_statement erases an entry the frontend
// will never execute; it is idempotent (an unknown or consumed session closes
// with success). execute_plan / prepare_plan take a pre-built plan (the Spark
// Connect frontend) in place of SQL text and follow execute / prepare_schema
// from the point where parsing ends. Errors travel as core::error_t; the only try/catch sits
// directly around IParser::parse.
class Worker final : public actor_zeta::basic_actor<Worker> {
public:
    template<typename T>
    using unique_future = actor_zeta::unique_future<T>;
    using session_result = core::result_wrapper_t<session_payload>;

    // `scheduler` is the sharing_scheduler the Worker runs on: a backend or transfer pool
    // answering one of its statements reschedules it there (wake).
    Worker(std::pmr::memory_resource* res,
           actor_zeta::scheduler_raw scheduler,
           std::size_t self_index,
           std::size_t worker_count,
           std::unique_ptr<IParser> parser,
           db::backend_registry_t& backends,
           actor_zeta::address_t otterbrix_manager,
           actor_zeta::address_t catalog_manager,
           actor_zeta::address_t kafka_manager);
    ~Worker();

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    unique_future<session_result> execute(session_hash_t id, std::string sql);
    unique_future<session_result> execute_statement(session_hash_t id);
    unique_future<session_result>
    execute_prepared_statement(session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters);
    unique_future<session_result> prepare_schema(session_hash_t id, std::string sql);
    unique_future<session_result> close_statement(session_hash_t id);
    unique_future<session_result> execute_plan(session_hash_t id, ParsedQueryDataPtr data);
    unique_future<session_result> prepare_plan(session_hash_t id, ParsedQueryDataPtr data);
    // Does nothing. A pool answer's waker sends it when the Worker is not parked: actor-zeta 1.2.0
    // leaves a cooperative actor blocked on its mailbox if, after resuming one awaited answer, its handler
    // suspends on the next with no message queued. At most one is queued (actor_parking_t::woken).
    unique_future<void> wake();

    // Hides cooperative_actor::resume (the scheduler calls it statically typed): a Worker
    // whose behavior is suspended on an unsettled backend or transfer answer parks instead
    // of being re-run until the answer arrives (scheduler/parking.hpp).
    actor_zeta::scheduler::resume_info resume(std::size_t max_throughput) noexcept;

    // How many times the scheduler has run any Worker, for tests. A parked Worker runs a few
    // times per statement; a spinning one thousands of times per wait.
    static std::uint64_t runs() noexcept { return runs_.load(std::memory_order_relaxed); }

    using dispatch_traits = actor_zeta::dispatch_traits<&Worker::execute,
                                                        &Worker::execute_statement,
                                                        &Worker::execute_prepared_statement,
                                                        &Worker::prepare_schema,
                                                        &Worker::close_statement,
                                                        &Worker::execute_plan,
                                                        &Worker::prepare_plan,
                                                        &Worker::wake>;

    actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

private:
    struct metadata_t {
        components::types::complex_logical_type schema;
        ParsedQueryDataPtr query_data_ptr;
        NodeTag tag;
        backend_type_t backend_type{backend_type_t::Unknown};
    };

    static inline std::atomic<std::uint64_t> runs_{0};

    std::pmr::memory_resource* resource_;
    // The conn::wait_owner_t of this Worker's backends and transfers; declared before them.
    actor_parking_t<Worker> parking_;
    std::size_t self_index_;
    std::size_t worker_count_;
    log_t log_;
    std::unique_ptr<IParser> parser_;
    // This Worker's own MySQL / PostgreSQL / ClickHouse backends, over the shared
    // connection pools: a statement's slots are sent to the pool and awaited here, so
    // the Worker suspends instead of blocking its scheduler thread.
    std::unique_ptr<db::backend_set_t> backends_;
    actor_zeta::address_t otterbrix_manager_;
    actor_zeta::address_t catalog_manager_;
    // S3 / local-file transfers of CREATE EXTERNAL TABLE and COPY ... TO; shared, owned
    // with the backends' registry.
    conn::transfer_pools_t* transfers_;
    actor_zeta::address_t kafka_manager_;
    std::pmr::unordered_map<session_hash_t, metadata_t> metadata_map_;

    // The parser boundary: the one place that may throw, converted to error_t
    // on the spot. Also rejects a parse that produced no plan.
    core::result_wrapper_t<ParsedQueryDataPtr> parse_sql(const std::string& sql);

    // What execute and execute_plan share once the statement is parsed: the
    // database-DDL guard, classification (catalog or engine), then run_pipeline.
    // The caller owns the entry's erase guard.
    unique_future<session_result> classify_and_run(session_hash_t id, ParsedQueryDataPtr data);

    // What prepare_schema and prepare_plan share once the statement is parsed:
    // the result schema from the plan and the backends' describe probes,
    // without executing the statement; the entry stays for execute or close.
    unique_future<session_result> describe_parsed(session_hash_t id, ParsedQueryDataPtr parsed_data);

    // Backend slices first (each inlines its rows as node_raw_data), then the
    // engine; the result is built from the session's stored schema/tag.
    unique_future<session_result>
    run_pipeline(session_hash_t id, ParsedQueryDataPtr data_ptr, backend_type_t backend);

    // CREATE DATABASE / DROP DATABASE are engine-local by grammar, but the
    // engine also hosts one database per connection uid (the mirrored remote
    // schema) and the kafka object database. The catalog owns those names and
    // refuses them (invalid_parameter) before the statement reaches the
    // engine; any other statement passes with no_error.
    unique_future<core::error_t> guard_database_ddl(const ParsedQueryData& data);

    // Runs a parsed external-table statement (CREATE EXTERNAL TABLE / COPY ... TO): a transfer pool
    // moves the bytes, the engine calls stay on this Worker. Returns an empty session_payload on success.
    unique_future<session_result>
    handle_external_statement(session_hash_t id, const otterstax::external::external_node_t& ext);
    unique_future<session_result>
    load_external_table(session_hash_t id, const otterstax::external::external_node_t& ext);
    unique_future<session_result> copy_to(session_hash_t id, const otterstax::external::external_node_t& ext);
    // Submits a transfer with a ticket and awaits its answer the way a backend awaits a
    // statement (db::backend_t::answer), so the Worker parks while it runs.
    template<typename Submit>
    unique_future<core::result_wrapper_t<conn::transfer_result_t>> run_transfer(Submit submit);

    void update_metadata(session_hash_t id,
                         ParsedQueryDataPtr metadata,
                         components::types::complex_logical_type schema = {});

    session_result finish_schema_value(session_hash_t id,
                                       components::cursor::cursor_t_ptr cursor,
                                       ParsedQueryDataPtr data);
    session_result take_payload(session_hash_t id, components::cursor::cursor_t& cursor);
    // Takes the rows of a statement a backend answered whole straight into the payload. Empty when the
    // engine still has work: other plan nodes, a remote DML (counted by OtterbrixManager::execute),
    // or a column-less result.
    std::optional<session_result> take_backend_result(session_hash_t id, ParsedQueryData& data);
};
