// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/reachability.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <actor-zeta/mailbox/make_message.hpp>
#include <actor-zeta/mailbox/message.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>

#include <components/casts/cast_registry.hpp>
#include <components/logical_plan/detached_block.hpp>
#include <components/planner/host_hooks.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/storage_contract.hpp>
#include <services/loop_thread.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <vector>

namespace sql_gen {
    struct statement_needs_t;
} // namespace sql_gen

// The backend actor of one connection of config.yaml: the storage of every table of the connection
// (otterbrix's storage contract, services/disk/storage_contract.hpp; docs/embedding-host-api.md, "The
// backend actor"). otterbrix plans every statement and reaches a remote table's rows only through
// these eighteen messages, sent to the address the map of open_engine gives the table's connection
// (backend_address(): one entry per connection, one actor each). The actor turns each into SQL of the
// connection's dialect (otterbrix/query_generation/) and hands it to one of its agents
// (backend_agent.hpp: one physical connection each, pool_size of them), which run on the runtime's
// backends pool; it never blocks its loop.
//
// Shaped like otterbrix's manager_disk_t with its agent_disk_t: an actor_mixin with a loop thread of
// its own; enqueue pushes into an inbox under a mutex and wakes the loop, the loop starts each
// message's behavior and resumes the coroutines whose awaited reply is ready. A statement takes a
// free agent (the agent of its transaction, when it has one), is sent to it and the agent is
// scheduled on the pool; with no agent free the statement waits in the actor's queue (no thread is
// held) until one is released, up to the checkout timeout. The agent settles the answer and wakes the
// loop (actor-zeta has no waker on a future: AZ-waker). With nothing to do the loop sleeps.
//
// Names. otterbrix keeps every remote table in lower case (c.d.s.t, its columns); the backend's
// spelling of each is the actor's, keyed by the table's oid and the columns' attoids, kept in the
// host table federation.public.remote_names (metadata.hpp) that every DDL reply writes (host rows)
// and the startup hands back (load_tables).
//
// Transactions. A PostgreSQL / MySQL transaction holds one agent from its first request on
// (ctx.txn.transaction_id) to its COMMIT / ROLLBACK / unpin; BEGIN goes before the first statement
// of a block (a read too: a REPEATABLE READ snapshot holds only inside a transaction) or before an
// autocommit write, SAVEPOINT s<sub_id> before the first statement of a savepoint level,
// storage_prepare is COMMIT, abort_transaction(0) ROLLBACK and abort_transaction(k) ROLLBACK TO the
// oldest savepoint of a level >= k; a release without a prepare rolls back what is open and returns
// the agent. A statement of the backend transaction that failed breaks it (the agent: PostgreSQL
// until a ROLLBACK TO a savepoint set before it, a MySQL deadlock for good): its COMMIT is refused.
// ClickHouse has no transaction: its reads take any agent and a write inside a transaction block is
// refused.
//
// Registration. A CREATE TABLE / DROP TABLE / ALTER TABLE of a connection's table on the
// registration session (registration_session(), catalog/registration.hpp) only records the table:
// no SQL reaches the backend. On any other session the DDL is the backend's too. discover (not of
// the contract) answers a discovery of the backend, the session's registration of a table a
// statement missed.
namespace otterstax::federation {

    using components::catalog::oid_t;

    // The agents (backend_agent.hpp), complete where the actor sends to them.
    class backend_agent_t;
    using backend_agent_ptr = std::unique_ptr<backend_agent_t, actor_zeta::pmr::deleter_t>;
    enum class write_kind_t : std::uint8_t;
    enum class transaction_step_t : std::uint8_t;

    // A connection of config.yaml as the actor serves it.
    struct connection_t {
        std::string name; // as config.yaml spells it; otterbrix knows it folded (fold_identifier)
        backend_type_t kind{backend_type_t::PostgreSQL};
    };

    // One column of a registered table: otterbrix's attoid, name (lower case) and type, the
    // backend's spelling and the backend's own type where the dialect reads by it (ClickHouse's
    // system.columns type; empty: none). A column an ADD COLUMN / DROP COLUMN of a transaction that
    // has not ended yet changes names that transaction (added_by / dropped_by): as otterbrix's
    // snapshot of the table's columns, the transaction sees its ADD and no longer its DROP, every
    // other one the columns as they were; the change is applied when the transaction committed and
    // dropped when it did not.
    struct registered_column_t {
        oid_t attoid{components::catalog::INVALID_OID};
        std::string name;
        components::types::complex_logical_type type{components::types::logical_type::NA};
        std::string remote;
        std::string remote_type{};
        std::optional<uint64_t> added_by{};
        std::optional<uint64_t> dropped_by{};
    };


    // A table of a connection's database: its oid and full name in otterbrix (lower case), where it
    // is on the backend (the database of MySQL / ClickHouse or the schema of PostgreSQL, and the
    // table, as the backend spells them) and its columns in the declared order.
    struct registered_table_t {
        oid_t oid{components::catalog::INVALID_OID};
        qualified_name_t name;
        std::string container;
        std::string remote;
        std::vector<registered_column_t> columns;
    };

    // The columns of `table` transaction `transaction` sees, in their order: the positions a scan's
    // projected_cols name.
    std::vector<registered_column_t> visible_columns(const registered_table_t& table, uint64_t transaction);

    // The rows one write message carries at most (backend_address_t::max_rows_per_write).
    std::uint64_t max_rows_per_write(backend_type_t kind) noexcept;

    class backend_actor_t final : public actor_zeta::actor::actor_mixin<backend_actor_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;
        using execution_context_t = services::disk::execution_context_t;
        using session_id_t = services::disk::session_id_t;
        using pool_t = actor_zeta::scheduler::sharing_scheduler;

        // `resource` must be thread-safe: the pages and receipts the actor answers live on it and
        // otterbrix frees them on its threads. `agents`: the connection's agents (pool_size, at
        // least one), built over `resource` and not yet serving; the actor schedules them on `pool`
        // (the runtime's backends pool), which must stop before the actor is destroyed (the agents
        // are destroyed with it). A loop thread the OS refuses is construction_error().
        backend_actor_t(std::pmr::memory_resource* resource,
                        connection_t connection,
                        session_id_t registration_session,
                        std::vector<backend_agent_ptr> agents,
                        pool_t* pool);
        backend_actor_t(const backend_actor_t&) = delete;
        backend_actor_t& operator=(const backend_actor_t&) = delete;
        ~backend_actor_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char* { return "otterstax_backend"; }
        [[nodiscard]] const core::error_t& construction_error() const noexcept { return construction_error_; }
        // How many times the loop woke up (a test reads it: no work, no pass).
        [[nodiscard]] std::uint64_t loop_passes() const noexcept { return loop_passes_.load(std::memory_order_relaxed); }

        // ---- the host's side: thread-safe, not messages ----

        const connection_t& connection() const noexcept { return connection_; }
        // The entry of the map open_engine reads: the connection (folded), this actor, its
        // max_rows_per_write. The name lives in the actor.
        components::planner::backend_address_t backend_address() noexcept;
        // The session the host registers its tables on: DDL of it sends no SQL to a backend.
        const session_id_t& registration_session() const noexcept { return registration_session_; }
        // The oid of federation.public.remote_names, the table the DDL replies write.
        void set_names_table(oid_t oid);
        // The tables the catalog holds (startup): their spelling and columns.
        void load_tables(std::vector<registered_table_t> tables);
        // The backend's spelling of the table the next create_storage_disk of `table.name` on the
        // registration session creates (oids unset): what discovery found.
        void stage_table(registered_table_t table);
        // The backend's spelling (and type) of the column the next add_storage_column of `table`
        // named `name` on the registration session adds.
        void stage_column(oid_t table, std::string name, std::string remote, std::string remote_type = {});
        // Forgets what stage_table / stage_column kept for a DDL that never reached the actor (a
        // CREATE TABLE IF NOT EXISTS of a table the catalog had, a refused ALTER TABLE).
        void unstage_table(const qualified_name_t& name);
        void unstage_column(oid_t table, const std::string& name);
        // The spelling of the table `oid`, if the actor knows it.
        [[nodiscard]] std::optional<registered_table_t> table(oid_t oid) const;
        // The transactions holding an agent right now.
        [[nodiscard]] std::size_t pinned() const noexcept { return pinned_.load(std::memory_order_relaxed); }
        // The statements waiting in the queue for an agent right now.
        [[nodiscard]] std::size_t waiting() const noexcept { return waiting_.load(std::memory_order_relaxed); }
        // How long a statement waits for a free agent (60 s by default), then io_error.
        void set_checkout_timeout(std::chrono::milliseconds timeout) noexcept {
            checkout_timeout_ms_.store(timeout.count(), std::memory_order_relaxed);
        }
        // How long a failed connect marks the connection unreachable (10 s; a test shortens it).
        void set_unreachable_interval(std::chrono::milliseconds interval) noexcept {
            reachability_.interval_ms.store(interval.count(), std::memory_order_relaxed);
        }
        // Aborts the statements the agents run and refuses every later one (the shutdown, once the
        // engine is gone: then the pool stops, then the actor goes). Never blocks.
        void cancel_agents() noexcept;

        // ---- storage_contract, in its order ----

        unique_future<core::result_wrapper_t<services::disk::fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 oid_t table_oid,
                                 uint64_t cursor_id,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        unique_future<void> storage_close_cursor(session_id_t session, oid_t table_oid, uint64_t cursor_id);
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session, oid_t table_oid);
        unique_future<core::result_wrapper_t<services::disk::write_receipt_t>>
        storage_append(execution_context_t ctx,
                       oid_t table_oid,
                       std::pmr::vector<oid_t> attoids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        unique_future<core::result_wrapper_t<services::disk::update_receipt_t>>
        storage_update(execution_context_t ctx,
                       oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        unique_future<core::result_wrapper_t<services::disk::write_receipt_t>>
        storage_delete_rows(execution_context_t ctx,
                            oid_t table_oid,
                            components::table::row_id_batch_t row_ids,
                            bool returning);
        unique_future<core::result_wrapper_t<services::disk::ddl_reply_t>>
        create_storage_disk(execution_context_t ctx,
                            oid_t table_oid,
                            oid_t database_oid,
                            qualified_name_t name,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        unique_future<core::result_wrapper_t<services::disk::ddl_reply_t>>
        mark_storage_dropped_many(execution_context_t ctx,
                                  std::pmr::vector<oid_t> table_oids,
                                  std::pmr::vector<qualified_name_t> names);
        unique_future<core::result_wrapper_t<services::disk::ddl_reply_t>>
        add_storage_column(execution_context_t ctx, oid_t table_oid, components::table::column_definition_t column);
        unique_future<core::result_wrapper_t<services::disk::ddl_reply_t>>
        stamp_column_dropped(execution_context_t ctx, oid_t table_oid, oid_t attoid);
        unique_future<core::error_t> storage_prepare(execution_context_t ctx, std::pmr::set<oid_t> tables);
        unique_future<void> storage_release_prepared(execution_context_t ctx, std::pmr::set<oid_t> tables);
        unique_future<core::error_t>
        abort_transaction(execution_context_t ctx, uint64_t from_sub_id, std::pmr::set<oid_t> tables);
        unique_future<core::result_wrapper_t<std::pmr::vector<services::disk::block_take_t>>>
        storage_offer(execution_context_t ctx,
                      components::logical_plan::detached_block_t block,
                      components::vector::storage_parameters parameters);
        unique_future<core::result_wrapper_t<services::disk::fetch_batch_t>>
        storage_run_block(execution_context_t ctx,
                          std::pmr::string request,
                          uint64_t cursor_id,
                          components::vector::storage_parameters parameters);
        unique_future<void> storage_close_block(execution_context_t ctx, uint64_t cursor_id);
        unique_future<void> storage_release_block(execution_context_t ctx, std::pmr::string request);
        unique_future<core::result_wrapper_t<services::disk::write_receipt_t>>
        storage_run_write(execution_context_t ctx,
                          std::pmr::string request,
                          components::vector::storage_parameters parameters,
                          bool returning);

        // ---- the host's message (after the contract's: the message ids are positional) ----

        // The tables of `scope` on the backend (backend_agent_t::discover), run by a free agent:
        // the session's registration of a table a statement missed.
        unique_future<core::result_wrapper_t<discovered_tables_t>> discover(qualified_name_t scope);
        // The client's connection of engine session `session` is closed (query_session_t::close): its
        // statements waiting in the queue are answered connection_closed at once, and its transaction
        // ends on the backend here and now — ROLLBACK (or unpin) on the agent it holds, after the
        // statement that agent runs, then the compensations — not when otterbrix's own ROLLBACK of the
        // closed session reaches the actor. That ROLLBACK is a statement for one of otterbrix's
        // executors, each of which runs one statement at a time, and every executor may be running a
        // statement of another block waiting in this very queue for the agent the closed block holds.
        // The transaction is refused every later agent (connection_closed); otterbrix's abort or
        // release of it then only forgets it. A transaction whose COMMIT runs, or ran, is left to
        // otterbrix's release.
        unique_future<void> end_session(session_id_t session);

        // The contract, checked; the dispatch is the contract's eighteen in its order, then discover
        // and end_session.
        using contract_check = actor_zeta::implements<services::disk::storage_contract,
                                                      &backend_actor_t::storage_fetch_next_batch,
                                                      &backend_actor_t::storage_close_cursor,
                                                      &backend_actor_t::storage_total_rows,
                                                      &backend_actor_t::storage_append,
                                                      &backend_actor_t::storage_update,
                                                      &backend_actor_t::storage_delete_rows,
                                                      &backend_actor_t::create_storage_disk,
                                                      &backend_actor_t::mark_storage_dropped_many,
                                                      &backend_actor_t::add_storage_column,
                                                      &backend_actor_t::stamp_column_dropped,
                                                      &backend_actor_t::storage_prepare,
                                                      &backend_actor_t::storage_release_prepared,
                                                      &backend_actor_t::abort_transaction,
                                                      &backend_actor_t::storage_offer,
                                                      &backend_actor_t::storage_run_block,
                                                      &backend_actor_t::storage_close_block,
                                                      &backend_actor_t::storage_release_block,
                                                      &backend_actor_t::storage_run_write>;
        using dispatch_traits = actor_zeta::dispatch_traits<&backend_actor_t::storage_fetch_next_batch,
                                                            &backend_actor_t::storage_close_cursor,
                                                            &backend_actor_t::storage_total_rows,
                                                            &backend_actor_t::storage_append,
                                                            &backend_actor_t::storage_update,
                                                            &backend_actor_t::storage_delete_rows,
                                                            &backend_actor_t::create_storage_disk,
                                                            &backend_actor_t::mark_storage_dropped_many,
                                                            &backend_actor_t::add_storage_column,
                                                            &backend_actor_t::stamp_column_dropped,
                                                            &backend_actor_t::storage_prepare,
                                                            &backend_actor_t::storage_release_prepared,
                                                            &backend_actor_t::abort_transaction,
                                                            &backend_actor_t::storage_offer,
                                                            &backend_actor_t::storage_run_block,
                                                            &backend_actor_t::storage_close_block,
                                                            &backend_actor_t::storage_release_block,
                                                            &backend_actor_t::storage_run_write,
                                                            &backend_actor_t::discover,
                                                            &backend_actor_t::end_session>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
            requires(actor_zeta::type_traits::is_unique_future_v<ReturnType>)
        [[nodiscard]] ReturnType
        enqueue_impl(actor_zeta::actor::address_t sender, actor_zeta::mailbox::message_id cmd, Args&&... args) {
            using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;
            auto [msg, future] =
                actor_zeta::detail::make_message<R>(resource(), std::move(sender), cmd, std::forward<Args>(args)...);
            static_cast<void>(enqueue_impl(std::move(msg)));
            return std::move(future);
        }

    private:
        // One step of a backend transaction or one statement, run in order.
        struct step_t {
            std::optional<transaction_step_t> step; // a transaction step; else `sql`
            std::string savepoint;
            std::string sql;
            bool pinned{true}; // `sql` runs on the transaction's agent (else any)
        };

        // The connection inside one otterbrix transaction.
        struct participant_t {
            session_id_t session; // the engine session of its requests
            // Its client's connection closed (end_session): it ended on the backend, no agent serves it
            // any more, and otterbrix's abort or release only forgets it.
            bool closed{false};
            bool preparing{false}; // storage_prepare runs its COMMIT
            bool pinned{false};    // a request ran on the transaction's agent
            bool began{false};  // BEGIN was sent
            // The backend savepoints s<sub_id>, in the order they were set.
            std::vector<uint64_t> savepoints;
            // A DROP of a client's statement, run at prepare (inside the transaction on PostgreSQL).
            std::vector<std::string> at_prepare;
            // What undoes the DDL MySQL / ClickHouse ran at once (recorded once it ran), run at
            // abort, the last one first.
            std::vector<std::string> on_abort;
            // The tables the actor forgets once the transaction ended with a prepare.
            std::vector<oid_t> forget;
            // The tables it created, forgotten again when it did not end with a prepare.
            std::vector<oid_t> created;
            bool prepared{false};
        };

        // A taken block (storage_offer), until its release.
        struct take_t {
            // The block as offered (blocks_, kept until its last take is released) and the node taken.
            uint64_t block{0};
            components::logical_plan::node_ptr node;
            bool write{false};
            // A read of one table with a WHERE the actor took (the rest of the block stays
            // otterbrix's): the table, the columns read (positions; empty: every one) and the
            // conjuncts sent.
            bool table_read{false};
            oid_t table{components::catalog::INVALID_OID};
            std::vector<std::size_t> columns;
            std::vector<components::expressions::expression_ptr> where;
            // The columns of the answer, as otterbrix planned the taken node.
            std::vector<components::types::complex_logical_type> types;
        };

        // The rows of an open scan or taken read, handed out a page at a time.
        struct cursor_t {
            std::vector<components::vector::data_chunk_t> chunks;
            std::size_t chunk{0};
            std::uint64_t offset{0};
            std::vector<components::types::complex_logical_type> types;
        };

        struct in_flight_t {
            actor_zeta::mailbox::message_ptr msg;
            actor_zeta::behavior_t behavior;
        };

        // An agent and who holds it.
        struct agent_slot_t {
            backend_agent_ptr agent;
            bool busy{false};
            std::optional<uint64_t> transaction; // the transaction it is pinned to
        };

        // A statement waiting for an agent.
        struct waiter_t {
            std::optional<uint64_t> transaction;
            session_id_t session;
            bool ending{false}; // a step that ends its transaction: a close never takes it out
            actor_zeta::promise<core::result_wrapper_t<std::size_t>> promise;
            std::chrono::steady_clock::time_point deadline;
        };

        void loop_body();
        void wake() noexcept;
        static void wake_hook(void* self) noexcept;

        // Every helper below runs on the loop thread.
        [[nodiscard]] core::error_t error(core::error_code_t code, const std::string& what) const;
        bool owns(const qualified_name_t& name) const;
        std::optional<registered_table_t> table_locked(oid_t oid) const;
        qualified_name_t generator_target(const registered_table_t& table) const;
        participant_t& participant(uint64_t transaction, const session_id_t& session);
        // The steps a statement of `txn` on its transaction's agent needs before it: BEGIN (before
        // the first statement of a block, or a write), SAVEPOINT s<sub_id> before the first
        // statement of a savepoint level. None on ClickHouse.
        std::vector<step_t> before_statement(const components::table::transaction_data& txn,
                                             const session_id_t& session);
        // The end of `transaction` for the actor's own state: the columns its ADD / DROP COLUMN
        // changed and the tables it created or dropped, kept (`committed`) or undone.
        void end_tables(uint64_t transaction, bool committed);
        std::optional<uint64_t> pin_of(const components::table::transaction_data& txn, const session_id_t& session);
        // A write the connection cannot take: ClickHouse inside a block, RETURNING off PostgreSQL.
        [[nodiscard]] core::error_t refused_write(const execution_context_t& ctx, bool returning) const;

        // ---- the agents ----
        // An agent for a statement of `transaction` (nullopt: of none) of `session`: its own agent, else
        // a free one (then pinned to it); answers when one is free, io_error after the checkout
        // timeout; connection_closed for a closed transaction (end_session) unless `ending`, a step
        // that ends it.
        unique_future<core::result_wrapper_t<std::size_t>>
        acquire(std::optional<uint64_t> transaction, const session_id_t& session, bool ending = false);
        std::optional<std::size_t> free_for(const std::optional<uint64_t>& transaction) const;
        void take(std::size_t agent, const std::optional<uint64_t>& transaction);
        // The agent is free again (and, `unpin`, no transaction's): the next waiter it suits takes it.
        void release(std::size_t agent, bool unpin);
        void serve_waiters();
        void expire_waiters();
        // Sends `method(args...)` to the agent and schedules it on the pool.
        template<typename Method, typename... Args>
        auto send_to(std::size_t agent, Method method, Args&&... args);

        unique_future<core::error_t>
        run_steps(uint64_t transaction, session_id_t session, std::vector<step_t> steps);
        // The steps that end `state` without a COMMIT: ROLLBACK (or unpin), then the compensations of
        // the DDL MySQL / ClickHouse ran at once, the last first.
        static std::vector<step_t> rollback_steps(participant_t& state);
        // The rows of `sql`, held to `types`; `declared`: ClickHouse's types of the columns read.
        unique_future<core::result_wrapper_t<std::vector<components::vector::data_chunk_t>>>
        read(std::optional<uint64_t> transaction,
             session_id_t session,
             std::string sql,
             std::vector<std::optional<std::string>> declared,
             std::vector<components::types::complex_logical_type> types,
             core::date::timezone_offset_t timezone);
        // `strict_store`: the statement relies on a strict sql_mode (sql_gen::statement_needs_t).
        unique_future<core::result_wrapper_t<std::uint64_t>> write(std::optional<uint64_t> transaction,
                                                                   session_id_t session,
                                                                   std::string sql,
                                                                   write_kind_t kind,
                                                                   bool strict_store = false,
                                                                   qualified_name_t table = {});
        services::disk::fetch_batch_t open_cursor(std::vector<components::vector::data_chunk_t> chunks,
                                                  std::vector<components::types::complex_logical_type> types);
        core::result_wrapper_t<services::disk::fetch_batch_t> next_page(uint64_t cursor_id);
        std::pmr::vector<components::vector::data_chunk_t>
        returned_rows(std::vector<components::vector::data_chunk_t> chunks);
        services::disk::host_rows_t names_insert(const registered_table_t& table,
                                                 std::span<const registered_column_t> columns,
                                                 bool with_table) const;
        services::disk::host_rows_t names_delete(std::size_t key_column, std::span<const oid_t> keys) const;
        // storage_offer's probe (backend_actor_offer.cpp): what of `block` the dialect can say.
        core::result_wrapper_t<std::pmr::vector<services::disk::block_take_t>>
        offer_block(const execution_context_t& ctx,
                    std::unique_ptr<components::logical_plan::detached_block_t> block,
                    const components::vector::storage_parameters& parameters);
        // The statement of a take with `parameters` (placeholders: every parameter `?`); `needs`
        // (optional): what it asks of the session that runs it.
        core::result_wrapper_t<std::string> take_sql(const take_t& take,
                                                     const components::vector::storage_parameters* parameters,
                                                     bool placeholders,
                                                     bool returning,
                                                     sql_gen::statement_needs_t* needs = nullptr) const;

        std::pmr::memory_resource* resource_;
        connection_t connection_;
        std::string folded_name_;
        session_id_t registration_session_;
        components::casts::cast_registry_t casts_;
        pool_t* pool_;
        core::error_t construction_error_{core::error_t::no_error()};

        // The inbox and the wake flag: the only state enqueue touches, under mutex_.
        std::mutex mutex_;
        std::condition_variable cv_;
        std::deque<actor_zeta::mailbox::message*> inbox_;
        bool poked_{false};
        std::atomic<bool> running_{true};
        std::atomic<std::uint64_t> loop_passes_{0};
        std::atomic<std::size_t> pinned_{0};
        std::atomic<std::size_t> waiting_{0};
        std::atomic<std::chrono::milliseconds::rep> checkout_timeout_ms_{60000};
        // Read and written by the agents (declared before agents_, which goes first).
        reachability_t reachability_;

        // What the host's side writes too, under tables_mutex_.
        mutable std::mutex tables_mutex_;
        std::map<oid_t, registered_table_t> tables_;
        std::map<std::string, registered_table_t> staged_tables_;
        std::map<std::pair<oid_t, std::string>, std::pair<std::string, std::string>> staged_columns_;
        oid_t names_table_{components::catalog::INVALID_OID};

        // The loop thread's own.
        std::list<in_flight_t> in_flight_;
        std::map<uint64_t, cursor_t> cursors_;
        std::map<std::string, take_t> takes_;
        std::map<uint64_t, std::unique_ptr<components::logical_plan::detached_block_t>> blocks_;
        uint64_t next_block_id_{1};
        std::map<uint64_t, participant_t> transactions_;
        uint64_t next_cursor_id_{1};
        uint64_t next_take_id_{1};
        std::vector<agent_slot_t> agents_;
        std::map<uint64_t, std::size_t> pins_; // transaction -> its agent
        std::deque<waiter_t> waiters_;
        // The discoveries the actor ran, from 1: an agent reads its dialect metadata again when it
        // changed (backend_agent_t::read).
        std::uint64_t metadata_{1};

        services::loop_thread_t loop_thread_;
    };

    using backend_actor_ptr = std::unique_ptr<backend_actor_t, actor_zeta::pmr::deleter_t>;

} // namespace otterstax::federation
