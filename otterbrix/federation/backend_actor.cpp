// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backend_actor.hpp"

#include "otterbrix/federation/backend_agent.hpp"
#include "otterbrix/federation/conform.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/casts/default_casts.hpp>

#include <algorithm>
#include <chrono>
#include <utility>

namespace otterstax::federation {

    namespace disk = services::disk;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::data_chunk_t;

    namespace {

        // A backend wakes the loop when it settles a future (its completion hook); this only bounds
        // a wait on a future something else settles without the hook.
        constexpr auto safety_interval = std::chrono::seconds(1);

        // storage_total_rows: an estimate is an answer (it only orders the sides of a hash join), and
        // the discovery reads no statistics, so every remote table answers the same (OTB-total-rows).
        constexpr std::uint64_t estimated_rows = 1000;

        std::string savepoint_name(uint64_t sub_id) { return "s" + std::to_string(sub_id); }

        // otterbrix numbers the commands of a transaction block from 1; a statement of its own runs
        // with command 0.
        bool in_block(const components::table::transaction_data& txn) noexcept { return txn.command_id != 0; }

        std::string kind_name(backend_type_t kind) {
            switch (kind) {
                case backend_type_t::MySQL:
                    return "MySQL";
                case backend_type_t::PostgreSQL:
                    return "PostgreSQL";
                case backend_type_t::ClickHouse:
                    return "ClickHouse";
            }
            return "unknown";
        }

    } // namespace

    std::vector<registered_column_t> visible_columns(const registered_table_t& table, uint64_t transaction) {
        std::vector<registered_column_t> out;
        out.reserve(table.columns.size());
        for (const auto& column : table.columns) {
            const bool added_elsewhere = column.added_by && *column.added_by != transaction;
            const bool dropped_here = column.dropped_by && *column.dropped_by == transaction;
            if (!added_elsewhere && !dropped_here) {
                out.push_back(column);
            }
        }
        return out;
    }

    std::uint64_t max_rows_per_write(backend_type_t kind) noexcept {
        return kind == backend_type_t::ClickHouse ? 10000 : 1000;
    }

    backend_actor_t::backend_actor_t(std::pmr::memory_resource* resource,
                                     connection_t connection,
                                     session_id_t registration_session,
                                     std::vector<backend_agent_ptr> agents,
                                     pool_t* pool)
        : resource_(resource)
        , connection_(std::move(connection))
        , folded_name_(fold_identifier(connection_.name))
        , registration_session_(std::move(registration_session))
        , casts_(resource)
        , pool_(pool) {
        components::casts::register_default_casts(casts_);
        agents_.reserve(agents.size());
        for (auto& agent : agents) {
            agent->set_owner(&backend_actor_t::wake_hook, this);
            agent->set_reachability(&reachability_);
            agents_.push_back(agent_slot_t{std::move(agent), false, std::nullopt});
        }
        construction_error_ =
            loop_thread_.start<backend_actor_t, &backend_actor_t::loop_body>(resource_, "otterstax_backend", this);
    }

    void backend_actor_t::wake_hook(void* self) noexcept { static_cast<backend_actor_t*>(self)->wake(); }

    backend_actor_t::~backend_actor_t() {
        running_.store(false, std::memory_order_release);
        wake();
        loop_thread_.join();
        in_flight_.clear();
        waiters_.clear();
        std::lock_guard guard(mutex_);
        for (auto* raw : inbox_) {
            actor_zeta::mailbox::message_ptr drained{raw};
        }
        inbox_.clear();
        // The agents go with agents_: the pool they ran on stopped before (cancel_agents, then the
        // pool's stop), so no job of theirs exists any more.
    }

    components::planner::backend_address_t backend_actor_t::backend_address() noexcept {
        return components::planner::backend_address_t{folded_name_, address(), max_rows_per_write(connection_.kind)};
    }

    void backend_actor_t::cancel_agents() noexcept {
        OTX_ZONE_N("backend_actor::cancel_agents");
        for (auto& slot : agents_) {
            slot.agent->cancel();
        }
    }

    // ---- the loop (manager_disk_t::loop_body's shape) ----

    std::pair<bool, actor_zeta::detail::enqueue_result> backend_actor_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        {
            std::lock_guard guard(mutex_);
            inbox_.push_back(msg.release());
            poked_ = true;
        }
        cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    void backend_actor_t::wake() noexcept {
        {
            std::lock_guard guard(mutex_);
            poked_ = true;
        }
        cv_.notify_one();
    }

    void backend_actor_t::loop_body() {
        while (running_.load(std::memory_order_acquire)) {
            std::deque<actor_zeta::mailbox::message*> arrived;
            {
                std::unique_lock lock(mutex_);
                auto woken = [this] { return poked_ || !running_.load(std::memory_order_acquire); };
                if (in_flight_.empty()) {
                    cv_.wait(lock, woken);
                } else {
                    cv_.wait_for(lock, safety_interval, woken);
                }
                loop_passes_.fetch_add(1, std::memory_order_relaxed);
                poked_ = false;
                arrived.swap(inbox_);
            }
            if (!running_.load(std::memory_order_acquire)) {
                for (auto* raw : arrived) {
                    actor_zeta::mailbox::message_ptr drained{raw};
                }
                return;
            }
            expire_waiters();
            // The message stays in its slot: the coroutine holds a raw pointer to it across suspensions.
            for (auto* raw : arrived) {
                in_flight_.emplace_back();
                in_flight_.back().msg = actor_zeta::mailbox::message_ptr{raw};
                in_flight_.back().behavior = behavior(in_flight_.back().msg.get());
            }
            bool progress = true;
            while (progress) {
                progress = false;
                for (auto& entry : in_flight_) {
                    if (entry.behavior.is_awaited_ready()) {
                        if (auto continuation = entry.behavior.take_awaited_continuation()) {
                            continuation.resume();
                            progress = true;
                            break;
                        }
                    }
                }
                in_flight_.remove_if([](const in_flight_t& entry) { return entry.behavior.done(); });
            }
        }
    }

    actor_zeta::behavior_t backend_actor_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_fetch_next_batch>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_fetch_next_batch, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_close_cursor>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_close_cursor, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_total_rows>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_total_rows, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_append>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_append, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_update>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_update, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_delete_rows>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_delete_rows, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::create_storage_disk>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::create_storage_disk, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::mark_storage_dropped_many>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::mark_storage_dropped_many, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::add_storage_column>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::add_storage_column, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::stamp_column_dropped>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::stamp_column_dropped, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_prepare>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_prepare, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_release_prepared>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_release_prepared, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::abort_transaction>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::abort_transaction, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_offer>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_offer, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_run_block>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_run_block, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_close_block>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_close_block, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_release_block>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_release_block, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::storage_run_write>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::storage_run_write, msg);
                break;
            case actor_zeta::msg_id<backend_actor_t, &backend_actor_t::discover>:
                co_await actor_zeta::dispatch(this, &backend_actor_t::discover, msg);
                break;
            default:
                break;
        }
    }

    // ---- the host's side ----

    void backend_actor_t::set_names_table(oid_t oid) {
        std::lock_guard guard(tables_mutex_);
        names_table_ = oid;
    }

    void backend_actor_t::load_tables(std::vector<registered_table_t> tables) {
        std::lock_guard guard(tables_mutex_);
        for (auto& table : tables) {
            const auto oid = table.oid;
            tables_[oid] = std::move(table);
        }
    }

    void backend_actor_t::stage_table(registered_table_t table) {
        std::lock_guard guard(tables_mutex_);
        auto key = fold_identifier(table.name.to_string());
        staged_tables_[std::move(key)] = std::move(table);
    }

    void backend_actor_t::stage_column(oid_t table, std::string name, std::string remote, std::string remote_type) {
        std::lock_guard guard(tables_mutex_);
        staged_columns_[{table, std::move(name)}] = {std::move(remote), std::move(remote_type)};
    }

    void backend_actor_t::unstage_table(const qualified_name_t& name) {
        std::lock_guard guard(tables_mutex_);
        staged_tables_.erase(fold_identifier(name.to_string()));
    }

    void backend_actor_t::unstage_column(oid_t table, const std::string& name) {
        std::lock_guard guard(tables_mutex_);
        staged_columns_.erase({table, name});
    }

    std::optional<registered_table_t> backend_actor_t::table(oid_t oid) const {
        std::lock_guard guard(tables_mutex_);
        auto found = tables_.find(oid);
        if (found == tables_.end()) {
            return std::nullopt;
        }
        return found->second;
    }

    // ---- helpers (loop thread) ----

    core::error_t backend_actor_t::error(core::error_code_t code, const std::string& what) const {
        return core::error_t{code, std::pmr::string{what, resource_}};
    }

    bool backend_actor_t::owns(const qualified_name_t& name) const {
        return fold_identifier(name.connection.t) == folded_name_;
    }

    std::optional<registered_table_t> backend_actor_t::table_locked(oid_t oid) const { return table(oid); }

    qualified_name_t backend_actor_t::generator_target(const registered_table_t& table) const {
        return generator_name(connection_.kind, remote_table_name_t{table.container, table.container, table.remote});
    }

    backend_actor_t::participant_t& backend_actor_t::participant(uint64_t transaction) {
        return transactions_[transaction];
    }

    std::optional<uint64_t> backend_actor_t::pin_of(const components::table::transaction_data& txn) {
        if (connection_.kind == backend_type_t::ClickHouse) {
            return std::nullopt;
        }
        // A read of a statement of its own takes any agent; inside a block, or once the transaction
        // wrote, it reads on the transaction's agent (its own writes).
        auto found = transactions_.find(txn.transaction_id);
        const bool pinned = found != transactions_.end() && found->second.pinned;
        if (!in_block(txn) && !pinned) {
            return std::nullopt;
        }
        participant(txn.transaction_id).pinned = true;
        return txn.transaction_id;
    }

    std::vector<backend_actor_t::step_t>
    backend_actor_t::before_statement(const components::table::transaction_data& txn) {
        std::vector<step_t> steps;
        if (connection_.kind == backend_type_t::ClickHouse) {
            return steps;
        }
        // A block is one backend transaction from its first statement on, a read included: a MySQL
        // REPEATABLE READ snapshot (and a PostgreSQL one) holds only inside a transaction.
        auto& state = participant(txn.transaction_id);
        state.pinned = true;
        if (!state.began) {
            state.began = true;
            steps.push_back(step_t{transaction_step_t::begin, {}, {}, true});
        }
        // Before ANY statement of the level, a read too: a PostgreSQL statement that fails aborts the
        // backend transaction, and only a ROLLBACK TO a savepoint set before it recovers it.
        const auto level = txn.sub_id;
        if (level > 0 && std::find(state.savepoints.begin(), state.savepoints.end(), level) == state.savepoints.end()) {
            state.savepoints.push_back(level);
            steps.push_back(step_t{transaction_step_t::savepoint, savepoint_name(level), {}, true});
        }
        return steps;
    }

    void backend_actor_t::end_tables(uint64_t transaction, bool committed) {
        std::vector<oid_t> forget;
        std::vector<oid_t> created;
        if (auto found = transactions_.find(transaction); found != transactions_.end()) {
            forget = found->second.forget;
            created = found->second.created;
        }
        std::lock_guard guard(tables_mutex_);
        for (auto& [oid, table] : tables_) {
            auto& columns = table.columns;
            columns.erase(std::remove_if(columns.begin(),
                                         columns.end(),
                                         [&](const registered_column_t& c) {
                                             const auto& gone = committed ? c.dropped_by : c.added_by;
                                             return gone && *gone == transaction;
                                         }),
                          columns.end());
            for (auto& column : columns) {
                if (column.added_by == transaction) {
                    column.added_by.reset();
                }
                if (column.dropped_by == transaction) {
                    column.dropped_by.reset();
                }
            }
        }
        for (auto oid : committed ? forget : created) {
            tables_.erase(oid);
        }
    }

    // ---- the agents ----

    std::optional<std::size_t> backend_actor_t::free_for(const std::optional<uint64_t>& transaction) const {
        if (transaction) {
            if (auto pinned = pins_.find(*transaction); pinned != pins_.end()) {
                return agents_[pinned->second].busy ? std::nullopt : std::optional<std::size_t>{pinned->second};
            }
        }
        for (std::size_t i = 0; i < agents_.size(); ++i) {
            if (!agents_[i].busy && !agents_[i].transaction) {
                return i;
            }
        }
        return std::nullopt;
    }

    void backend_actor_t::take(std::size_t agent, const std::optional<uint64_t>& transaction) {
        agents_[agent].busy = true;
        if (transaction && !agents_[agent].transaction) {
            agents_[agent].transaction = transaction;
            pins_[*transaction] = agent;
            pinned_.store(pins_.size(), std::memory_order_relaxed);
        }
    }

    backend_actor_t::unique_future<core::result_wrapper_t<std::size_t>>
    backend_actor_t::acquire(std::optional<uint64_t> transaction) {
        actor_zeta::promise<core::result_wrapper_t<std::size_t>> promise(resource_);
        auto future = promise.get_future();
        if (agents_.empty()) {
            promise.set_value(error(core::error_code_t::io_error,
                                    "connection \"" + connection_.name + "\" has no connection open"));
            return future;
        }
        if (auto agent = free_for(transaction)) {
            take(*agent, transaction);
            promise.set_value(*agent);
            return future;
        }
        const auto timeout = std::chrono::milliseconds{checkout_timeout_ms_.load(std::memory_order_relaxed)};
        waiters_.push_back(waiter_t{transaction, std::move(promise), std::chrono::steady_clock::now() + timeout});
        return future;
    }

    void backend_actor_t::release(std::size_t agent, bool unpin) {
        auto& slot = agents_[agent];
        slot.busy = false;
        if (unpin && slot.transaction) {
            pins_.erase(*slot.transaction);
            slot.transaction.reset();
            pinned_.store(pins_.size(), std::memory_order_relaxed);
        }
        serve_waiters();
    }

    void backend_actor_t::serve_waiters() {
        for (auto it = waiters_.begin(); it != waiters_.end();) {
            if (auto agent = free_for(it->transaction)) {
                take(*agent, it->transaction);
                it->promise.set_value(*agent);
                it = waiters_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void backend_actor_t::expire_waiters() {
        const auto now = std::chrono::steady_clock::now();
        for (auto it = waiters_.begin(); it != waiters_.end();) {
            // A statement waiting for its own transaction's agent waits for the statement of the
            // transaction that holds it, however long: no other agent could take it, and an end of
            // the transaction (COMMIT / ROLLBACK) given up would leave the agent pinned.
            if (it->deadline > now || (it->transaction && pins_.contains(*it->transaction))) {
                ++it;
                continue;
            }
            it->promise.set_value(error(core::error_code_t::io_error,
                                        "no free connection of \"" + connection_.name + "\" within " +
                                            std::to_string(checkout_timeout_ms_.load(std::memory_order_relaxed)) +
                                            " ms (pool_size " + std::to_string(agents_.size()) + ")"));
            it = waiters_.erase(it);
        }
    }

    template<typename Method, typename... Args>
    auto backend_actor_t::send_to(std::size_t agent, Method method, Args&&... args) {
        auto* target = agents_[agent].agent.get();
        auto [needs_sched, future] = actor_zeta::send(target, method, std::forward<Args>(args)...);
        if (needs_sched) {
            pool_->enqueue(target);
        }
        return std::move(future);
    }

    backend_actor_t::unique_future<core::error_t> backend_actor_t::run_steps(uint64_t transaction,
                                                                              std::vector<step_t> steps) {
        for (auto& step : steps) {
            if (step.step) {
                const bool ends = *step.step == transaction_step_t::commit ||
                                  *step.step == transaction_step_t::rollback ||
                                  *step.step == transaction_step_t::unpin;
                // An end of a transaction that holds no agent has nothing to end.
                if (ends && !pins_.contains(transaction)) {
                    continue;
                }
                auto agent = co_await acquire(transaction);
                if (agent.has_error()) {
                    co_return agent.error();
                }
                auto refused = co_await send_to(agent.value(), &backend_agent_t::step, *step.step, std::move(step.savepoint));
                release(agent.value(), ends);
                if (refused.contains_error()) {
                    co_return refused;
                }
                continue;
            }
            auto written = co_await write(step.pinned ? std::optional<uint64_t>{transaction} : std::nullopt,
                                          std::move(step.sql),
                                          write_kind_t::insert);
            if (written.has_error()) {
                co_return written.error();
            }
        }
        co_return core::error_t::no_error();
    }

    backend_actor_t::unique_future<core::result_wrapper_t<std::vector<data_chunk_t>>>
    backend_actor_t::read(std::optional<uint64_t> transaction,
                          std::string sql,
                          std::vector<std::optional<std::string>> declared,
                          std::vector<complex_logical_type> types,
                          core::date::timezone_offset_t timezone) {
        auto agent = co_await acquire(transaction);
        if (agent.has_error()) {
            co_return agent.error();
        }
        auto rows = co_await send_to(agent.value(), &backend_agent_t::read, sql, std::move(declared), metadata_);
        release(agent.value(), false);
        if (rows.has_error()) {
            co_return rows.error();
        }
        for (auto& chunk : rows.value()) {
            if (auto refused = conform_to_result(resource_, casts_, sql, types, timezone, chunk);
                refused.contains_error()) {
                co_return refused;
            }
        }
        co_return std::move(rows.value());
    }

    backend_actor_t::unique_future<core::result_wrapper_t<std::uint64_t>>
    backend_actor_t::write(std::optional<uint64_t> transaction, std::string sql, write_kind_t kind) {
        auto agent = co_await acquire(transaction);
        if (agent.has_error()) {
            co_return agent.error();
        }
        auto written = co_await send_to(agent.value(), &backend_agent_t::write, std::move(sql), kind);
        release(agent.value(), false);
        co_return written;
    }

    disk::fetch_batch_t backend_actor_t::open_cursor(std::vector<data_chunk_t> chunks,
                                                     std::vector<complex_logical_type> types) {
        const auto id = next_cursor_id_++;
        cursors_.emplace(id, cursor_t{std::move(chunks), 0, 0, std::move(types)});
        // A cursor just opened is there.
        return std::move(next_page(id).value());
    }

    core::result_wrapper_t<disk::fetch_batch_t> backend_actor_t::next_page(uint64_t cursor_id) {
        auto found = cursors_.find(cursor_id);
        if (found == cursors_.end()) {
            return error(core::error_code_t::invalid_parameter,
                         "the backend of OtterStax has no cursor " + std::to_string(cursor_id));
        }
        auto& cursor = found->second;
        while (cursor.chunk < cursor.chunks.size() && cursor.offset >= cursor.chunks[cursor.chunk].size()) {
            ++cursor.chunk;
            cursor.offset = 0;
        }
        if (cursor.chunk == cursor.chunks.size()) {
            // The end: an empty page of the cursor's columns, and the cursor is gone.
            std::pmr::vector<complex_logical_type> types{cursor.types.begin(), cursor.types.end(), resource_};
            auto empty = std::make_unique<data_chunk_t>(resource_, types, 1);
            empty->set_cardinality(0);
            cursors_.erase(found);
            return disk::fetch_batch_t{std::move(empty), cursor_id};
        }
        const auto& source = cursor.chunks[cursor.chunk];
        const auto count =
            std::min<std::uint64_t>(source.size() - cursor.offset, components::vector::DEFAULT_VECTOR_CAPACITY);
        auto page = std::make_unique<data_chunk_t>(source.partial_copy(resource_, cursor.offset, count));
        cursor.offset += count;
        return disk::fetch_batch_t{std::move(page), cursor_id};
    }

    std::pmr::vector<data_chunk_t> backend_actor_t::returned_rows(std::vector<data_chunk_t> chunks) {
        std::pmr::vector<data_chunk_t> out{resource_};
        for (const auto& chunk : chunks) {
            for (std::uint64_t offset = 0; offset < chunk.size(); offset += components::vector::DEFAULT_VECTOR_CAPACITY) {
                const auto count =
                    std::min<std::uint64_t>(chunk.size() - offset, components::vector::DEFAULT_VECTOR_CAPACITY);
                out.push_back(chunk.partial_copy(resource_, offset, count));
            }
        }
        return out;
    }

    disk::host_rows_t backend_actor_t::names_insert(const registered_table_t& table,
                                                    std::span<const registered_column_t> columns,
                                                    bool with_table) const {
        disk::host_rows_t rows{};
        rows.insert = std::pmr::vector<data_chunk_t>{resource_};
        rows.delete_keys = std::pmr::vector<data_chunk_t>{resource_};
        {
            std::lock_guard guard(tables_mutex_);
            rows.table = names_table_;
        }
        if (rows.table == components::catalog::INVALID_OID) {
            return rows; // no host table (a stack without the registration): nothing to keep
        }
        std::pmr::vector<complex_logical_type> types{resource_};
        for (std::size_t i = 0; i < remote_names_layout::count; ++i) {
            complex_logical_type type{i < remote_names_layout::remote_name ? logical_type::BIGINT
                                                                           : logical_type::STRING_LITERAL};
            type.set_alias(std::string{remote_names_layout::names[i]});
            types.push_back(std::move(type));
        }
        const std::size_t total = columns.size() + (with_table ? 1 : 0);
        std::size_t written = 0;
        while (written < total) {
            const auto count = std::min<std::size_t>(total - written, components::vector::DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(resource_, types, count);
            for (std::size_t row = 0; row < count; ++row) {
                const auto at = written + row;
                const bool table_row = with_table && at == 0;
                const auto* column = table_row ? nullptr : &columns[at - (with_table ? 1 : 0)];
                chunk.set_value(remote_names_layout::table_oid, row, static_cast<int64_t>(table.oid));
                chunk.set_value(remote_names_layout::attoid,
                                row,
                                static_cast<int64_t>(table_row ? 0 : column->attoid));
                chunk.set_value(remote_names_layout::remote_name,
                                row,
                                std::string_view{table_row ? table.remote : column->remote});
                if (table_row) {
                    chunk.set_value(remote_names_layout::remote_container, row, std::string_view{table.container});
                } else {
                    chunk.data[remote_names_layout::remote_container].set_null(row, true);
                }
                if (table_row || column->remote_type.empty()) {
                    chunk.data[remote_names_layout::remote_type].set_null(row, true);
                } else {
                    chunk.set_value(remote_names_layout::remote_type, row, std::string_view{column->remote_type});
                }
            }
            chunk.set_cardinality(count);
            rows.insert.push_back(std::move(chunk));
            written += count;
        }
        return rows;
    }

    disk::host_rows_t backend_actor_t::names_delete(std::size_t key_column, std::span<const oid_t> keys) const {
        disk::host_rows_t rows{};
        rows.insert = std::pmr::vector<data_chunk_t>{resource_};
        rows.delete_keys = std::pmr::vector<data_chunk_t>{resource_};
        {
            std::lock_guard guard(tables_mutex_);
            rows.table = names_table_;
        }
        if (rows.table == components::catalog::INVALID_OID || keys.empty()) {
            return rows;
        }
        rows.delete_column = key_column;
        complex_logical_type type{logical_type::BIGINT};
        type.set_alias(std::string{remote_names_layout::names[key_column]});
        std::pmr::vector<complex_logical_type> types{resource_};
        types.push_back(type);
        for (std::size_t written = 0; written < keys.size(); written += components::vector::DEFAULT_VECTOR_CAPACITY) {
            const auto count = std::min<std::size_t>(keys.size() - written, components::vector::DEFAULT_VECTOR_CAPACITY);
            data_chunk_t chunk(resource_, types, count);
            for (std::size_t row = 0; row < count; ++row) {
                chunk.set_value(0, row, static_cast<int64_t>(keys[written + row]));
            }
            chunk.set_cardinality(count);
            rows.delete_keys.push_back(std::move(chunk));
        }
        return rows;
    }

    namespace {

        // engine column name -> the backend's spelling and the declared type.
        sql_gen::column_map_t column_map_of(const registered_table_t& table) {
            sql_gen::column_map_t map;
            for (const auto& column : table.columns) {
                map.emplace(column.name, sql_gen::remote_column_t{column.remote, column.type});
            }
            return map;
        }

        std::vector<std::string> remote_columns_of(const registered_table_t& table) {
            std::vector<std::string> out;
            out.reserve(table.columns.size());
            for (const auto& column : table.columns) {
                out.push_back(column.remote);
            }
            return out;
        }

        // The type ClickHouse's reads hold a column to (ch_to_chunk's declared types).
        std::optional<std::string> declared_type(const registered_column_t& column) {
            if (column.remote_type.empty()) {
                return std::nullopt;
            }
            return column.remote_type;
        }

        complex_logical_type named(const registered_column_t& column) {
            auto type = column.type;
            type.set_alias(column.name);
            return type;
        }

        // Where a table is on the backend when the client's CREATE names it: the database (MySQL /
        // ClickHouse) or schema (PostgreSQL) of a table already registered beside it, as spelled
        // there, else the name as written.
        std::string container_of(const std::map<oid_t, registered_table_t>& tables,
                                 backend_type_t kind,
                                 const qualified_name_t& name) {
            const bool by_schema = kind == backend_type_t::PostgreSQL;
            for (const auto& [oid, table] : tables) {
                if (table.name.connection.t == name.connection.t && table.name.database.t == name.database.t &&
                    (!by_schema || table.name.schema.t == name.schema.t)) {
                    return table.container;
                }
            }
            return by_schema ? name.schema.t : name.database.t;
        }

    } // namespace

    // ---- storage_contract ----

    backend_actor_t::unique_future<core::result_wrapper_t<disk::fetch_batch_t>>
    backend_actor_t::storage_fetch_next_batch(session_id_t,
                                              oid_t table_oid,
                                              uint64_t cursor_id,
                                              int64_t limit,
                                              std::vector<size_t> projected_cols,
                                              components::table::transaction_data txn) {
        std::optional<uint64_t> transaction;
        std::vector<step_t> steps;
        std::string sql;
        std::vector<std::optional<std::string>> declared;
        std::vector<complex_logical_type> types;
        {
            OTX_ZONE_N("backend_actor::storage_fetch_next_batch");
            if (cursor_id != 0) {
                co_return next_page(cursor_id);
            }
            auto table = table_locked(table_oid);
            if (!table) {
                co_return error(core::error_code_t::undefined_object,
                                "the backend of OtterStax has no table of oid " + std::to_string(table_oid));
            }
            // projected_cols are positions among the columns of the transaction's snapshot.
            table->columns = visible_columns(*table, txn.transaction_id);
            std::vector<std::size_t> positions{projected_cols.begin(), projected_cols.end()};
            if (positions.empty()) {
                for (std::size_t i = 0; i < table->columns.size(); ++i) {
                    positions.push_back(i);
                }
            }
            for (auto position : positions) {
                if (position >= table->columns.size()) {
                    co_return error(core::error_code_t::protocol_violation,
                                    "a scan of \"" + table->name.to_string() + "\" asked for column " +
                                        std::to_string(position) + " of " + std::to_string(table->columns.size()));
                }
                types.push_back(named(table->columns[position]));
                declared.push_back(declared_type(table->columns[position]));
            }
            const auto map = column_map_of(*table);
            const auto all = remote_columns_of(*table);
            const sql_gen::statement_target_t target{generator_target(*table), {}, &map, nullptr, all, {}};
            auto generated =
                sql_gen::generate_table_read(connection_.kind, target, positions, {}, nullptr, limit, resource_);
            if (generated.has_error()) {
                co_return generated.error();
            }
            sql = std::move(generated.value());
            transaction = pin_of(txn);
            if (transaction) {
                steps = before_statement(txn);
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused;
            }
        }
        // The contract hands a scan its session and transaction only, no execution context: no
        // session time zone reaches it, so a read of a table is conformed in UTC (the zone otterbrix's
        // own timestamps carry); a taken block (storage_run_block) has the session's.
        auto rows = co_await read(transaction, std::move(sql), std::move(declared), types, {});
        if (rows.has_error()) {
            co_return rows.error();
        }
        co_return open_cursor(std::move(rows.value()), std::move(types));
    }

    backend_actor_t::unique_future<void>
    backend_actor_t::storage_close_cursor(session_id_t, oid_t, uint64_t cursor_id) {
        OTX_ZONE_N("backend_actor::storage_close_cursor");
        cursors_.erase(cursor_id);
        co_return;
    }

    backend_actor_t::unique_future<core::result_wrapper_t<uint64_t>>
    backend_actor_t::storage_total_rows(session_id_t, oid_t) {
        OTX_ZONE_N("backend_actor::storage_total_rows");
        co_return estimated_rows;
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::write_receipt_t>>
    backend_actor_t::storage_append(execution_context_t ctx,
                                    oid_t table_oid,
                                    std::pmr::vector<oid_t> attoids,
                                    std::pmr::vector<data_chunk_t> data,
                                    bool returning) {
        std::optional<uint64_t> transaction;
        std::vector<step_t> steps;
        std::vector<std::string> statements;
        std::vector<complex_logical_type> types;
        {
            OTX_ZONE_N("backend_actor::storage_append");
            auto table = table_locked(table_oid);
            if (!table) {
                co_return error(core::error_code_t::undefined_object,
                                "the backend of OtterStax has no table of oid " + std::to_string(table_oid));
            }
            if (auto refused = refused_write(ctx, returning); refused.contains_error()) {
                co_return refused;
            }
            table->columns = visible_columns(*table, ctx.txn.transaction_id);
            std::vector<std::string> columns;
            std::vector<std::size_t> indexes;
            for (std::size_t i = 0; i < attoids.size(); ++i) {
                auto column = std::find_if(table->columns.begin(), table->columns.end(), [&](const auto& c) {
                    return c.attoid == attoids[i];
                });
                if (column == table->columns.end()) {
                    co_return error(core::error_code_t::undefined_column,
                                    "table \"" + table->name.to_string() + "\" has no column of attoid " +
                                        std::to_string(attoids[i]));
                }
                columns.push_back(column->remote);
                indexes.push_back(i);
            }
            const auto all = remote_columns_of(*table);
            for (const auto& column : table->columns) {
                types.push_back(named(column));
            }
            const auto name = generator_target(*table);
            for (const auto& chunk : data) {
                if (chunk.size() == 0) {
                    continue;
                }
                auto sql = sql_gen::generate_insert_rows(connection_.kind,
                                                         name,
                                                         columns,
                                                         indexes,
                                                         chunk,
                                                         resource_,
                                                         returning ? std::span<const std::string>{all}
                                                                   : std::span<const std::string>{});
                if (sql.has_error()) {
                    co_return sql.error();
                }
                statements.push_back(std::move(sql.value()));
            }
            if (connection_.kind != backend_type_t::ClickHouse && !statements.empty()) {
                steps = before_statement(ctx.txn);
                transaction = ctx.txn.transaction_id;
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused;
            }
        }
        disk::write_receipt_t receipt{0, 0, false, std::pmr::vector<data_chunk_t>{resource_}};
        std::vector<data_chunk_t> returned;
        for (auto& sql : statements) {
            if (returning) {
                auto rows = co_await read(transaction, std::move(sql), {}, types, ctx.session_tz);
                if (rows.has_error()) {
                    co_return rows.error();
                }
                for (auto& chunk : rows.value()) {
                    receipt.count += chunk.size();
                    returned.push_back(std::move(chunk));
                }
                continue;
            }
            auto written = co_await write(transaction, std::move(sql), write_kind_t::insert);
            if (written.has_error()) {
                co_return written.error();
            }
            receipt.count += written.value();
        }
        receipt.returned = returned_rows(std::move(returned));
        co_return std::move(receipt);
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::update_receipt_t>>
    backend_actor_t::storage_update(execution_context_t,
                                    oid_t table_oid,
                                    std::pmr::vector<components::vector::vector_t>,
                                    std::pmr::vector<data_chunk_t>,
                                    bool) {
        OTX_ZONE_N("backend_actor::storage_update");
        auto table = table_locked(table_oid);
        co_return error(core::error_code_t::unimplemented_yet,
                        "UPDATE of remote table \"" + (table ? table->name.to_string() : std::to_string(table_oid)) +
                            "\" runs only as one statement of its backend: OtterStax numbers no remote row");
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::write_receipt_t>>
    backend_actor_t::storage_delete_rows(execution_context_t, oid_t table_oid, components::table::row_id_batch_t, bool) {
        OTX_ZONE_N("backend_actor::storage_delete_rows");
        auto table = table_locked(table_oid);
        co_return error(core::error_code_t::unimplemented_yet,
                        "DELETE of remote table \"" + (table ? table->name.to_string() : std::to_string(table_oid)) +
                            "\" runs only as one statement of its backend: OtterStax numbers no remote row");
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
    backend_actor_t::create_storage_disk(execution_context_t ctx,
                                         oid_t table_oid,
                                         oid_t,
                                         qualified_name_t name,
                                         std::vector<components::table::column_definition_t> columns,
                                         bool) {
        std::vector<step_t> steps;
        disk::host_rows_t rows{};
        registered_table_t table;
        std::optional<std::string> undo; // what drops the table again on MySQL / ClickHouse
        {
            OTX_ZONE_N("backend_actor::create_storage_disk");
            if (!owns(name)) {
                co_return error(core::error_code_t::undefined_object,
                                "connection \"" + name.connection.t + "\" has no backend in OtterStax");
            }
            const bool registration = ctx.session == registration_session_;
            table.oid = table_oid;
            table.name = name;
            std::optional<registered_table_t> staged;
            {
                std::lock_guard guard(tables_mutex_);
                if (registration) {
                    auto it = staged_tables_.find(fold_identifier(name.to_string()));
                    if (it != staged_tables_.end()) {
                        staged = std::move(it->second);
                        staged_tables_.erase(it);
                    }
                }
                table.container = staged ? staged->container : container_of(tables_, connection_.kind, name);
                table.remote = staged ? staged->remote : name.collection.t;
            }
            for (const auto& column : columns) {
                registered_column_t registered{column.attoid(), column.name(), column.type(), column.name()};
                if (staged) {
                    for (const auto& spelled : staged->columns) {
                        if (spelled.name == column.name()) {
                            registered.remote = spelled.remote;
                            registered.remote_type = spelled.remote_type;
                        }
                    }
                }
                table.columns.push_back(std::move(registered));
            }
            rows = names_insert(table, table.columns, true);
            if (!registration) {
                std::vector<sql_gen::remote_column_t> remote_columns;
                for (const auto& column : table.columns) {
                    remote_columns.push_back(sql_gen::remote_column_t{column.remote, column.type});
                }
                auto sql =
                    sql_gen::generate_create_table(connection_.kind, generator_target(table), remote_columns, resource_);
                if (sql.has_error()) {
                    co_return sql.error();
                }
                if (connection_.kind == backend_type_t::PostgreSQL) {
                    // In the statement's backend transaction: COMMIT at prepare, ROLLBACK at abort.
                    steps = before_statement(ctx.txn);
                    steps.push_back(step_t{std::nullopt, {}, std::move(sql.value()), true});
                } else {
                    // MySQL / ClickHouse DDL commits at once: once it ran, an abort drops the table
                    // again (a CREATE the backend refused created nothing to drop).
                    steps.push_back(step_t{std::nullopt, {}, std::move(sql.value()), false});
                    auto drop = sql_gen::generate_drop_table(connection_.kind, generator_target(table), resource_);
                    if (drop.has_error()) {
                        co_return drop.error();
                    }
                    undo = std::move(drop.value());
                }
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused;
            }
        }
        {
            OTX_ZONE_N("backend_actor::create_storage_disk::created");
            auto& state = participant(ctx.txn.transaction_id);
            state.created.push_back(table_oid);
            if (undo) {
                state.on_abort.push_back(std::move(*undo));
            }
            std::lock_guard guard(tables_mutex_);
            tables_[table_oid] = std::move(table);
        }
        co_return disk::ddl_reply_t{false, std::move(rows)};
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
    backend_actor_t::mark_storage_dropped_many(execution_context_t ctx,
                                               std::pmr::vector<oid_t> table_oids,
                                               std::pmr::vector<qualified_name_t>) {
        OTX_ZONE_N("backend_actor::mark_storage_dropped_many");
        const bool registration = ctx.session == registration_session_;
        for (auto oid : table_oids) {
            auto table = table_locked(oid);
            if (!table) {
                continue; // a table whose spelling OtterStax lost: only the catalog's rows go
            }
            auto& state = participant(ctx.txn.transaction_id);
            state.forget.push_back(oid);
            if (registration) {
                continue;
            }
            // The DROP waits for the COMMIT: until then a ROLLBACK keeps the table on both sides.
            auto drop = sql_gen::generate_drop_table(connection_.kind, generator_target(*table), resource_);
            if (drop.has_error()) {
                co_return drop.error();
            }
            state.at_prepare.push_back(std::move(drop.value()));
        }
        co_return disk::ddl_reply_t{false, names_delete(remote_names_layout::table_oid, table_oids)};
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
    backend_actor_t::add_storage_column(execution_context_t ctx,
                                        oid_t table_oid,
                                        components::table::column_definition_t column) {
        std::vector<step_t> steps;
        disk::host_rows_t rows{};
        registered_column_t added{column.attoid(), column.name(), column.type(), column.name()};
        std::optional<std::string> undo; // what drops the column again on MySQL / ClickHouse
        {
            OTX_ZONE_N("backend_actor::add_storage_column");
            auto table = table_locked(table_oid);
            if (!table) {
                co_return error(core::error_code_t::undefined_object,
                                "the backend of OtterStax has no table of oid " + std::to_string(table_oid));
            }
            const bool registration = ctx.session == registration_session_;
            // The transaction's own until it ended (end_tables).
            added.added_by = ctx.txn.transaction_id;
            if (registration) {
                std::lock_guard guard(tables_mutex_);
                auto it = staged_columns_.find({table_oid, column.name()});
                if (it != staged_columns_.end()) {
                    added.remote = std::move(it->second.first);
                    added.remote_type = std::move(it->second.second);
                    staged_columns_.erase(it);
                }
            }
            rows = names_insert(*table, std::span<const registered_column_t>{&added, 1}, false);
            if (!registration) {
                const auto name = generator_target(*table);
                auto sql = sql_gen::generate_add_column(connection_.kind,
                                                        name,
                                                        sql_gen::remote_column_t{added.remote, added.type},
                                                        resource_);
                if (sql.has_error()) {
                    co_return sql.error();
                }
                if (connection_.kind == backend_type_t::PostgreSQL) {
                    steps = before_statement(ctx.txn);
                    steps.push_back(step_t{std::nullopt, {}, std::move(sql.value()), true});
                } else {
                    steps.push_back(step_t{std::nullopt, {}, std::move(sql.value()), false});
                    auto drop = sql_gen::generate_drop_column(connection_.kind, name, added.remote, resource_);
                    if (drop.has_error()) {
                        co_return drop.error();
                    }
                    undo = std::move(drop.value());
                }
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused; // an ADD the backend refused added nothing: nothing to undo
            }
        }
        {
            OTX_ZONE_N("backend_actor::add_storage_column::added");
            auto& state = participant(ctx.txn.transaction_id);
            if (undo) {
                state.on_abort.push_back(std::move(*undo));
            }
            std::lock_guard guard(tables_mutex_);
            if (auto found = tables_.find(table_oid); found != tables_.end()) {
                found->second.columns.push_back(std::move(added));
            }
        }
        co_return disk::ddl_reply_t{false, std::move(rows)};
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::ddl_reply_t>>
    backend_actor_t::stamp_column_dropped(execution_context_t ctx, oid_t table_oid, oid_t attoid) {
        OTX_ZONE_N("backend_actor::stamp_column_dropped");
        auto table = table_locked(table_oid);
        if (!table) {
            co_return error(core::error_code_t::undefined_object,
                            "the backend of OtterStax has no table of oid " + std::to_string(table_oid));
        }
        auto column = std::find_if(table->columns.begin(), table->columns.end(), [attoid](const auto& c) {
            return c.attoid == attoid;
        });
        if (column != table->columns.end()) {
            {
                // Kept until the transaction ended: every other transaction still sees it, and the
                // scans number their columns by what they see (end_tables).
                std::lock_guard guard(tables_mutex_);
                for (auto& kept : tables_[table_oid].columns) {
                    if (kept.attoid == attoid) {
                        kept.dropped_by = ctx.txn.transaction_id;
                    }
                }
            }
            if (ctx.session != registration_session_) {
                auto drop =
                    sql_gen::generate_drop_column(connection_.kind, generator_target(*table), column->remote, resource_);
                if (drop.has_error()) {
                    co_return drop.error();
                }
                participant(ctx.txn.transaction_id).at_prepare.push_back(std::move(drop.value()));
            } else {
                static_cast<void>(participant(ctx.txn.transaction_id));
            }
        }
        const oid_t keys[] = {attoid};
        co_return disk::ddl_reply_t{false, names_delete(remote_names_layout::attoid, keys)};
    }

    backend_actor_t::unique_future<core::error_t> backend_actor_t::storage_prepare(execution_context_t ctx,
                                                                                    std::pmr::set<oid_t>) {
        std::vector<step_t> steps;
        bool commits = false;
        {
            OTX_ZONE_N("backend_actor::storage_prepare");
            auto found = transactions_.find(ctx.txn.transaction_id);
            if (found == transactions_.end()) {
                co_return core::error_t::no_error();
            }
            auto& state = found->second;
            const bool transactional = connection_.kind != backend_type_t::ClickHouse;
            for (auto& sql : state.at_prepare) {
                if (transactional && !state.began) {
                    state.began = true;
                    state.pinned = true;
                    steps.push_back(step_t{transaction_step_t::begin, {}, {}, true});
                }
                steps.push_back(step_t{std::nullopt, {}, std::move(sql), transactional});
            }
            state.at_prepare.clear();
            commits = state.began;
            if (commits) {
                steps.push_back(step_t{transaction_step_t::commit, {}, {}, true});
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                // The transaction stays as it is: otterbrix answers a refused prepare with an abort,
                // which rolls the backend transaction back, frees its agent (an ended COMMIT freed it
                // already) and runs the compensations.
                co_return refused;
            }
        }
        {
            OTX_ZONE_N("backend_actor::storage_prepare::committed");
            if (auto found = transactions_.find(ctx.txn.transaction_id); found != transactions_.end()) {
                auto& state = found->second;
                state.began = false;
                // The COMMIT freed the agent; a transaction that held one without BEGIN keeps it until
                // its release (unpin).
                if (commits) {
                    state.pinned = false;
                }
                state.savepoints.clear();
                state.on_abort.clear();
                state.prepared = true;
            }
        }
        co_return core::error_t::no_error();
    }

    backend_actor_t::unique_future<void> backend_actor_t::storage_release_prepared(execution_context_t ctx,
                                                                                   std::pmr::set<oid_t>) {
        std::vector<step_t> steps;
        {
            OTX_ZONE_N("backend_actor::storage_release_prepared");
            auto found = transactions_.find(ctx.txn.transaction_id);
            if (found == transactions_.end()) {
                co_return;
            }
            auto& state = found->second;
            end_tables(ctx.txn.transaction_id, state.prepared);
            if (state.began) {
                // Never prepared: what it did goes (a block that only read ends its backend
                // transaction too).
                steps.push_back(step_t{transaction_step_t::rollback, {}, {}, true});
            } else if (state.pinned) {
                // Pinned without a backend transaction: its agent is free again.
                steps.push_back(step_t{transaction_step_t::unpin, {}, {}, true});
            }
            if (!state.prepared) {
                for (auto it = state.on_abort.rbegin(); it != state.on_abort.rend(); ++it) {
                    steps.push_back(step_t{std::nullopt, {}, std::move(*it), false});
                }
            }
            transactions_.erase(found);
        }
        // Nothing awaits a release: a refusal here only loses the agent's session state (an ending
        // step frees the agent whatever its answer, run_steps).
        static_cast<void>(co_await run_steps(ctx.txn.transaction_id, std::move(steps)));
        co_return;
    }

    backend_actor_t::unique_future<core::error_t>
    backend_actor_t::abort_transaction(execution_context_t ctx, uint64_t from_sub_id, std::pmr::set<oid_t>) {
        std::vector<step_t> steps;
        {
            OTX_ZONE_N("backend_actor::abort_transaction");
            auto found = transactions_.find(ctx.txn.transaction_id);
            if (found == transactions_.end()) {
                co_return core::error_t::no_error();
            }
            auto& state = found->second;
            if (from_sub_id == 0) {
                end_tables(ctx.txn.transaction_id, false);
                if (state.began) {
                    steps.push_back(step_t{transaction_step_t::rollback, {}, {}, true});
                } else if (state.pinned) {
                    steps.push_back(step_t{transaction_step_t::unpin, {}, {}, true});
                }
                // The DDL MySQL / ClickHouse committed at once, undone the last first (otterbrix runs
                // a connection's DDL in autocommit only, so a ROLLBACK TO never meets one).
                for (auto it = state.on_abort.rbegin(); it != state.on_abort.rend(); ++it) {
                    steps.push_back(step_t{std::nullopt, {}, std::move(*it), false});
                }
                transactions_.erase(found);
            } else {
                // ROLLBACK TO the oldest backend savepoint of a level >= from_sub_id: every statement
                // of those levels came after it (a level's savepoint is set before its first
                // statement, and a level below from_sub_id runs nothing while its savepoint stands).
                // It stays; the ones set after it go.
                auto oldest = std::find_if(state.savepoints.begin(), state.savepoints.end(), [from_sub_id](uint64_t s) {
                    return s >= from_sub_id;
                });
                if (state.began && oldest != state.savepoints.end()) {
                    steps.push_back(
                        step_t{transaction_step_t::rollback_to_savepoint, savepoint_name(*oldest), {}, true});
                    state.savepoints.erase(std::next(oldest), state.savepoints.end());
                }
            }
        }
        co_return co_await run_steps(ctx.txn.transaction_id, std::move(steps));
    }

    backend_actor_t::unique_future<core::result_wrapper_t<std::pmr::vector<disk::block_take_t>>>
    backend_actor_t::storage_offer(execution_context_t ctx,
                                   components::logical_plan::detached_block_t block,
                                   components::vector::storage_parameters parameters) {
        OTX_ZONE_N("backend_actor::storage_offer");
        co_return offer_block(ctx, std::make_unique<components::logical_plan::detached_block_t>(std::move(block)), parameters);
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::fetch_batch_t>>
    backend_actor_t::storage_run_block(execution_context_t ctx,
                                       std::pmr::string request,
                                       uint64_t cursor_id,
                                       components::vector::storage_parameters parameters) {
        std::optional<uint64_t> transaction;
        std::vector<step_t> steps;
        std::string sql;
        std::vector<std::optional<std::string>> declared;
        std::vector<complex_logical_type> types;
        {
            OTX_ZONE_N("backend_actor::storage_run_block");
            if (cursor_id != 0) {
                co_return next_page(cursor_id);
            }
            auto found = takes_.find(std::string{request});
            if (found == takes_.end()) {
                co_return error(core::error_code_t::internal_error,
                                "the backend of OtterStax has no take \"" + std::string{request} + "\"");
            }
            const auto& take = found->second;
            auto generated = take_sql(take, &parameters, false, false);
            if (generated.has_error()) {
                co_return generated.error();
            }
            sql = std::move(generated.value());
            types = take.types;
            if (take.table_read) {
                if (auto table = table_locked(take.table)) {
                    if (take.columns.empty()) {
                        for (const auto& column : table->columns) {
                            declared.push_back(declared_type(column));
                        }
                    } else {
                        const auto seen = visible_columns(*table, ctx.txn.transaction_id);
                        for (auto position : take.columns) {
                            if (position < seen.size()) {
                                declared.push_back(declared_type(seen[position]));
                            }
                        }
                    }
                }
            }
            transaction = pin_of(ctx.txn);
            if (transaction) {
                steps = before_statement(ctx.txn);
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused;
            }
        }
        auto rows = co_await read(transaction, std::move(sql), std::move(declared), types, ctx.session_tz);
        if (rows.has_error()) {
            co_return rows.error();
        }
        co_return open_cursor(std::move(rows.value()), std::move(types));
    }

    backend_actor_t::unique_future<void> backend_actor_t::storage_close_block(execution_context_t, uint64_t cursor_id) {
        OTX_ZONE_N("backend_actor::storage_close_block");
        cursors_.erase(cursor_id);
        co_return;
    }

    backend_actor_t::unique_future<void> backend_actor_t::storage_release_block(execution_context_t,
                                                                                std::pmr::string request) {
        OTX_ZONE_N("backend_actor::storage_release_block");
        auto found = takes_.find(std::string{request});
        if (found != takes_.end()) {
            const auto block = found->second.block;
            takes_.erase(found);
            if (std::none_of(takes_.begin(), takes_.end(), [block](const auto& take) { return take.second.block == block; })) {
                blocks_.erase(block);
            }
        }
        co_return;
    }

    backend_actor_t::unique_future<core::result_wrapper_t<disk::write_receipt_t>>
    backend_actor_t::storage_run_write(execution_context_t ctx,
                                       std::pmr::string request,
                                       components::vector::storage_parameters parameters,
                                       bool returning) {
        std::optional<uint64_t> transaction;
        std::vector<step_t> steps;
        std::string sql;
        write_kind_t kind = write_kind_t::update;
        std::vector<complex_logical_type> types;
        {
            OTX_ZONE_N("backend_actor::storage_run_write");
            auto found = takes_.find(std::string{request});
            if (found == takes_.end() || !found->second.write) {
                co_return error(core::error_code_t::internal_error,
                                "the backend of OtterStax has no write take \"" + std::string{request} + "\"");
            }
            const auto& take = found->second;
            if (auto refused = refused_write(ctx, returning); refused.contains_error()) {
                co_return refused;
            }
            kind = take.node->type() == components::logical_plan::node_type::delete_t ? write_kind_t::delete_
                                                                                       : write_kind_t::update;
            auto generated = take_sql(take, &parameters, false, returning);
            if (generated.has_error()) {
                co_return generated.error();
            }
            sql = std::move(generated.value());
            if (returning) {
                if (auto table = table_locked(take.node->table_oid())) {
                    for (const auto& column : visible_columns(*table, ctx.txn.transaction_id)) {
                        types.push_back(named(column));
                    }
                }
            }
            if (connection_.kind != backend_type_t::ClickHouse) {
                steps = before_statement(ctx.txn);
                transaction = ctx.txn.transaction_id;
            }
        }
        if (!steps.empty()) {
            if (auto refused = co_await run_steps(ctx.txn.transaction_id, std::move(steps)); refused.contains_error()) {
                co_return refused;
            }
        }
        disk::write_receipt_t receipt{0, 0, false, std::pmr::vector<data_chunk_t>{resource_}};
        if (returning) {
            auto rows = co_await read(transaction, std::move(sql), {}, types, ctx.session_tz);
            if (rows.has_error()) {
                co_return rows.error();
            }
            for (const auto& chunk : rows.value()) {
                receipt.count += chunk.size();
            }
            receipt.returned = returned_rows(std::move(rows.value()));
            co_return std::move(receipt);
        }
        auto written = co_await write(transaction, std::move(sql), kind);
        if (written.has_error()) {
            co_return written.error();
        }
        receipt.count = written.value();
        co_return std::move(receipt);
    }

    // ---- the host's message ----

    backend_actor_t::unique_future<core::result_wrapper_t<discovered_tables_t>>
    backend_actor_t::discover(qualified_name_t scope) {
        // Nothing to do before the first co_await: the zone covers the work after the answer (the
        // agent's discovery has its own, backend_agent::discover).
        auto agent = co_await acquire(std::nullopt);
        if (agent.has_error()) {
            co_return agent.error();
        }
        auto reply = co_await send_to(agent.value(), &backend_agent_t::discover, std::move(scope));
        {
            OTX_ZONE_N("backend_actor::discover");
            release(agent.value(), false);
            // What a table's reads hold to may have changed (a PostgreSQL ENUM type of a new table).
            ++metadata_;
        }
        co_return reply;
    }

    core::error_t backend_actor_t::refused_write(const execution_context_t& ctx, bool returning) const {
        if (connection_.kind == backend_type_t::ClickHouse && in_block(ctx.txn)) {
            return error(core::error_code_t::unimplemented_yet,
                         "connection \"" + connection_.name +
                             "\" is ClickHouse, which has no transactions: a write inside a transaction block is not "
                             "supported");
        }
        if (returning && connection_.kind != backend_type_t::PostgreSQL) {
            return error(core::error_code_t::unimplemented_yet,
                         "RETURNING of a write to connection \"" + connection_.name + "\" (" +
                             kind_name(connection_.kind) + ") is not supported: only PostgreSQL answers its rows");
        }
        return core::error_t::no_error();
    }

} // namespace otterstax::federation
