// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace conn {

    // How long a statement waits for a free connection of its uid before it is
    // refused. Generous on purpose: with pool_size 1 a statement queues behind the
    // whole of the statement in front of it, and every caller blocks on the
    // result anyway. The bound turns a backend that stopped answering — every
    // connection of the uid held by a statement that never completes — into an
    // error instead of a hang.
    inline constexpr std::chrono::milliseconds default_checkout_timeout{std::chrono::seconds(60)};

    // A fixed set of connections to one backend uid, checked out one at a time:
    // a connection serves exactly one statement at a time, which is what keeps
    // statements of one uid from overlapping on one connection.
    //
    // Generic over the connection type so the checkout / return logic is
    // testable with a fake (tests/unit/connectors/). The pool never talks to the
    // backend: it only hands out exclusive ownership. The caller decides whether a
    // checked-out connection is usable (ping, reconnect).
    //
    // Thread safety: every member function may be called from any thread. A
    // handle_t is owned by one thread at a time; returning it (destruction or
    // release()) may happen on another thread than the checkout, e.g. on the
    // io_context thread that finished the statement.
    //
    // Lifetime: the shared state outlives the pool object while handles are out,
    // so a handle returned after the pool is gone is safe — the pool is closed by
    // then, and a connection returned into a closed pool is destroyed instead of
    // being kept. Idle connections are destroyed when the pool is destroyed.
    template<typename Connection>
    class connection_pool_t {
        struct state_t {
            mutable std::mutex mutex;
            std::condition_variable available;
            std::vector<std::unique_ptr<Connection>> slots;
            std::vector<std::size_t> idle; // LIFO: the most recently used connection is reused first
            std::size_t checked_out{0};
            bool closed{false};

            void give_back(std::size_t index) {
                {
                    std::lock_guard lock(mutex);
                    --checked_out;
                    if (closed) {
                        slots[index].reset();
                        return;
                    }
                    idle.push_back(index);
                }
                available.notify_one();
            }
        };

    public:
        // Exclusive ownership of one connection until destroyed or release()d.
        class handle_t {
        public:
            handle_t() noexcept = default;
            handle_t(const handle_t&) = delete;
            handle_t& operator=(const handle_t&) = delete;
            handle_t(handle_t&& other) noexcept
                : state_(std::move(other.state_))
                , index_(other.index_) {}
            handle_t& operator=(handle_t&& other) noexcept {
                if (this != &other) {
                    release();
                    state_ = std::move(other.state_);
                    index_ = other.index_;
                }
                return *this;
            }
            ~handle_t() { release(); }

            explicit operator bool() const noexcept { return state_ != nullptr; }
            Connection& operator*() const noexcept { return *get(); }
            Connection* operator->() const noexcept { return get(); }
            Connection* get() const noexcept { return state_ ? state_->slots[index_].get() : nullptr; }

            // Returns the connection to the pool now.
            void release() noexcept {
                if (state_) {
                    auto state = std::move(state_);
                    state_ = nullptr;
                    state->give_back(index_);
                }
            }

        private:
            friend class connection_pool_t;
            handle_t(std::shared_ptr<state_t> state, std::size_t index) noexcept
                : state_(std::move(state))
                , index_(index) {}

            std::shared_ptr<state_t> state_;
            std::size_t index_{0};
        };

        explicit connection_pool_t(std::vector<std::unique_ptr<Connection>> connections)
            : state_(std::make_shared<state_t>()) {
            state_->slots.reserve(connections.size());
            state_->idle.reserve(connections.size());
            for (auto& connection : connections) {
                state_->slots.push_back(std::move(connection));
            }
            // Reverse order so the first checkout gets slot 0.
            for (std::size_t i = state_->slots.size(); i > 0; --i) {
                state_->idle.push_back(i - 1);
            }
        }

        connection_pool_t(const connection_pool_t&) = delete;
        connection_pool_t& operator=(const connection_pool_t&) = delete;

        ~connection_pool_t() { close(); }

        // Refuses every later checkout, wakes the waiters (they get nullopt) and
        // destroys the idle connections; a connection still checked out is
        // destroyed when its handle returns it.
        void close() {
            std::vector<std::unique_ptr<Connection>> doomed;
            {
                std::lock_guard lock(state_->mutex);
                if (state_->closed) {
                    return;
                }
                state_->closed = true;
                for (auto index : state_->idle) {
                    doomed.push_back(std::move(state_->slots[index]));
                }
                state_->idle.clear();
            }
            state_->available.notify_all();
            // `doomed` is destroyed here, outside the lock: closing a connection
            // may talk to the backend.
        }

        // Waits up to `timeout` for a free connection (a zero timeout never
        // blocks); nullopt on timeout or when the pool is (or gets) closed.
        std::optional<handle_t> checkout_for(std::chrono::milliseconds timeout) {
            std::unique_lock lock(state_->mutex);
            if (!state_->available.wait_for(lock, timeout, [this] { return state_->closed || !state_->idle.empty(); })) {
                return std::nullopt;
            }
            return take_idle_locked();
        }

        // Applies `f(Connection&)` to every connection that is not checked out.
        // Holds the pool lock: `f` must not check out from this pool.
        template<typename F>
        void for_each_idle(F&& f) {
            std::lock_guard lock(state_->mutex);
            for (auto index : state_->idle) {
                f(*state_->slots[index]);
            }
        }

        // Applies `f(Connection&)` to every connection checked out right now. Holds the pool lock: `f`
        // must neither block nor touch this pool — a connector's cancel(), which aborts the statement
        // the connection runs from this (any) thread.
        template<typename F>
        void for_each_in_use(F&& f) {
            std::lock_guard lock(state_->mutex);
            for (std::size_t index = 0; index < state_->slots.size(); ++index) {
                const bool idle = std::find(state_->idle.begin(), state_->idle.end(), index) != state_->idle.end();
                if (!idle && state_->slots[index]) {
                    f(*state_->slots[index]);
                }
            }
        }

        std::size_t size() const noexcept { return state_->slots.size(); }
        std::size_t idle() const {
            std::lock_guard lock(state_->mutex);
            return state_->idle.size();
        }
        std::size_t in_use() const {
            std::lock_guard lock(state_->mutex);
            return state_->checked_out;
        }

    private:
        std::optional<handle_t> take_idle_locked() {
            if (state_->closed || state_->idle.empty()) {
                return std::nullopt;
            }
            const std::size_t index = state_->idle.back();
            state_->idle.pop_back();
            ++state_->checked_out;
            return handle_t{state_, index};
        }

        std::shared_ptr<state_t> state_;
    };

} // namespace conn
