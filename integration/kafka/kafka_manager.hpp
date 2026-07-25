// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/table/column_definition.hpp>
#include <core/result_wrapper.hpp>

#include <boost/lockfree/queue.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace otterstax::kafka {
    namespace detail {
        class kafka_poller_t;
        class kafka_producer_t;
        class kafka_stream_t;
    } // namespace detail

    // Runtime owner of kafka SOURCE / STREAM objects. On a kafka_node_t (CREATE/DROP)
    // it creates the backing otterbrix table and records the binding:
    //   SOURCE -> CREATE TABLE (user-visible, materialized) + ingestion poller
    //   STREAM -> continuous-query worker (no user table)
    class KafkaManager final : public actor_zeta::actor::actor_mixin<KafkaManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // start_pollers: when true, CREATE SOURCE also launches a background
        // ingestion poller per source (needs a reachable broker). Off by default
        // so broker-free unit/system tests don't spawn consumer threads;
        // component_manager turns it on
        KafkaManager(std::pmr::memory_resource* resource,
                     actor_zeta::address_t engine_dispatcher_address,
                     bool start_pollers = false);
        ~KafkaManager();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Handle one parsed kafka DDL statement. Returns an (empty) success
        // cursor or an error cursor — the Scheduler completes the session off it
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> execute(session_hash_t id, kafka_node_ptr node);

        // Publish the rows of an `INSERT INTO kafka.<obj>` to the object's topic
        // `source` is the insert's source subplan, executed here to materialise the
        // rows (no table write); the object's poller re-ingests them. Returns an
        // (empty) success cursor or an error cursor — the Scheduler completes off it
        actor_zeta::unique_future<components::cursor::cursor_t_ptr>
        produce(session_hash_t id, std::string relname, components::logical_plan::node_ptr source);

        // Set up a continuous ksqlDB "INSERT INTO query": `INSERT INTO
        // kafka.<stream> SELECT ... FROM kafka.<src>` adds another persistent worker
        // that consumes <src>'s topic, applies the SELECT, and produces into the
        // EXISTING stream's output topic (fan-in / union of several sources into one
        // stream). `stream` is the INSERT target; `insert_sql` is the raw statement,
        // re-compiled here (and on restart recovery) to derive the source + operators
        // The target must be a STREAM (an INSERT ... SELECT into a SOURCE is rejected —
        // a one-shot snapshot would be a silent surprise). The query inherits the
        // stream's exactly-once guarantee. Returns an (empty) success or error cursor
        actor_zeta::unique_future<components::cursor::cursor_t_ptr>
        add_stream_insert(session_hash_t id, std::string stream, std::string insert_sql);

        // Relaunch persisted SOURCEs after an engine restart. Called
        // by component_manager AFTER full init (scheduler/executor pool started); NOT
        // from the ctor (which runs mid-spawn — a poller started that early races the
        // half-initialised engine). No-op unless start_pollers_. Reads kafka.__sources,
        // re-reads each backing table's columns from the catalog, rebuilds registry_,
        // relaunches pollers (Phase 2 table-seek then resumes from kafka.<src>__offsets)
        // Drives the dispatcher directly (no actor mailbox), safe to call inline
        void recover();

        using dispatch_traits = actor_zeta::
            dispatch_traits<&KafkaManager::execute, &KafkaManager::produce, &KafkaManager::add_stream_insert>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        // Kafka binding for one registered object — drives the poller/producer
        // (and continuous query for streams) in later sub-steps
        struct kafka_object_t {
            kafka_op op;
            std::vector<kafka_column_t> columns;
            std::unordered_map<std::string, std::string> options;
            std::string as_select; // streams only
        };

        // Async hand-off of a finished plan to the engine's manager_dispatcher_t
        // Non-coroutine (returns the send() future); the caller co_awaits
        actor_zeta::unique_future<components::cursor::cursor_t_ptr>
        send_plan(components::logical_plan::execution_plan_t plan);

        // execute() dispatches to one coroutine handler per kafka DDL op
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> handle_create_source(kafka_node_ptr node);
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> handle_create_stream(kafka_node_ptr node);
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> handle_drop_source(kafka_node_ptr node);
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> handle_drop_stream(kafka_node_ptr node);

        // CREATE DATABASE "kafka" IF NOT EXISTS — idempotent across restarts
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> ensure_database();

        // CREATE TABLE kafka.<name> (columns). Non-coroutine (returns the send()
        // future); the caller co_awaits
        actor_zeta::unique_future<components::cursor::cursor_t_ptr>
        create_table(const std::string& name, std::vector<components::table::column_definition_t> columns);

        // Lazily open (and cache) the topic producer for a kafka object; returns the
        // cached producer or an error if the broker connection could not be opened
        core::result_wrapper_t<detail::kafka_producer_t*>
        ensure_producer(const std::string& name, const std::string& bootstrap_servers, const std::string& topic);

        // SOURCE persistence + poller auto-restart on recovery
        // The kafka object registry is in-memory, so a restart would forget every
        // SOURCE and never relaunch its poller (the engine recovers the backing
        // tables but nothing restarts ingestion). We persist each object's options
        // to a kafka.__sources table; on startup recover() reads it back, re-reads
        // the columns from the recovered backing table (LIMIT-1 probe with an
        // unlimited fallback — b2-rc-2 LIMIT plans over empty tables return no metadata),
        // rebuilds registry_, and relaunches the pollers (Phase 2 table-seek then
        // resumes from kafka.<src>__offsets). Columns are NOT stored — the catalog
        // is their single source of truth

        // CREATE TABLE kafka.__sources IF NOT EXISTS (idempotent across restarts)
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> ensure_sources_table();
        // co_await ensure_sources_table() once per process (idempotent guard)
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> ensure_sources_table_once();
        // INSERT one object's options into kafka.__sources (called after register)
        // Returns the engine future — co_awaited inside execute (NOT spun, so it
        // composes with the handler coroutine)
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> persist_source_meta(const std::string& name,
                                                                                        const std::string& kind,
                                                                                        const std::string& topic,
                                                                                        const std::string& bootstrap,
                                                                                        const std::string& group_id,
                                                                                        const std::string& offset_reset,
                                                                                        bool transactional,
                                                                                        const std::string& as_select);
        // DELETE one object's row from kafka.__sources (called on DROP). co_awaited
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> delete_source_meta(const std::string& name);
        // Launch the ingestion poller for an already-registered source
        // (registry_[name] supplies columns + options). Shared by CREATE SOURCE and
        // recover(). Logs and leaves the poller absent on failure
        void launch_source_poller(const std::string& name, bool transactional);
        // Launch the continuous-query worker for an already-registered stream
        // (registry_[name].as_select + options; its source must already be in
        // registry_). Shared by CREATE STREAM and recover(). Logs + skips on failure
        void launch_stream(const std::string& name);

        std::pmr::memory_resource* resource_;
        actor_zeta::address_t dispatcher_address_;
        log_t log_;
        bool start_pollers_;
        bool database_ensured_{false};
        bool sources_table_ensured_{false}; // kafka.__sources created this process
        std::unordered_map<std::string, kafka_object_t> registry_;
        std::unordered_map<std::string, std::unique_ptr<detail::kafka_poller_t>> pollers_;
        std::unordered_map<std::string, std::unique_ptr<detail::kafka_producer_t>> producers_;
        std::unordered_map<std::string, std::unique_ptr<detail::kafka_stream_t>> streams_;
        // For each STREAM, the internal names of its continuous INSERT INTO
        // queries (each also lives in registry_ + streams_ under its own name). Used
        // to stop + forget every such query when the stream is dropped
        std::unordered_map<std::string, std::vector<std::string>> stream_insert_queries_;
        std::uint64_t insert_query_seq_{0}; // monotonic suffix for INSERT-query internal names

        struct in_flight_entry_t {
            actor_zeta::mailbox::message_ptr pending_msg{};
            actor_zeta::behavior_t behavior{};
        };
        std::thread loop_thread_;
        std::atomic<bool> loop_running_{true};
        boost::lockfree::queue<actor_zeta::mailbox::message*> inbox_{128};
        std::mutex mutex_;
        std::condition_variable pump_cv_;
    };
} // namespace otterstax::kafka
