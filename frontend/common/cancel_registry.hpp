// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>

#include <openssl/crypto.h>

// A client's cancel of a statement another connection is waiting for: PostgreSQL's CancelRequest (a connection of
// its own, naming the waiting one by its BackendKeyData) and MySQL's KILL QUERY / KILL CONNECTION <id> (a statement
// of another connection). The engine has no cancel: a cancelled statement runs on to its end in its session, and
// the cancel only stops the waiting connection's await — which answers the client "cancelled" and fails an open
// block behind the statement, exactly as a timeout does (asio_future_bridge.hpp, await_status_t::cancelled).
namespace frontend {
    class frontend_connection;

    // A connection's own: which of its awaits is running, and which one a cancel named. The await numbers itself
    // (begin, on the connection's strand), so a cancel that arrives when the statement it was meant for has ended
    // reaches no later one.
    class statement_cancel_t {
    public:
        // The connection's strand: an await starts / ends.
        std::uint64_t begin() noexcept {
            const auto statement = ++next_;
            running_.store(statement, std::memory_order_release);
            return statement;
        }
        void end() noexcept { running_.store(0, std::memory_order_release); }

        // Any thread: cancel the await running now; false when none runs (nothing to cancel, as PostgreSQL
        // ignores a cancel between statements).
        bool request() noexcept {
            const auto statement = running_.load(std::memory_order_acquire);
            if (statement == 0) {
                return false;
            }
            cancelled_.store(statement, std::memory_order_release);
            return true;
        }

        // The connection's strand: the await `statement` was cancelled.
        [[nodiscard]] bool cancelled(std::uint64_t statement) const noexcept {
            return cancelled_.load(std::memory_order_acquire) == statement;
        }

    private:
        std::uint64_t next_{0};
        std::atomic<std::uint64_t> running_{0};
        std::atomic<std::uint64_t> cancelled_{0};
    };

    // A server's connections that can be cancelled, by connection id (MySQL's connection id, PostgreSQL's
    // BackendKeyData process id). Owned by the server (frontend_server, reached through connection_close_sink) and
    // outliving every connection: a connection adds itself once it can be named, and removes itself in its
    // finish_impl, before it is destroyed — under the registry's lock, so no cancel ever reaches a connection gone.
    class cancel_registry_t {
    public:
        void add(std::uint32_t id,
                 statement_cancel_t* cancel,
                 frontend_connection* connection,
                 std::vector<std::uint8_t> secret = {}) {
            std::lock_guard lock(mutex_);
            entries_[id] = entry_t{cancel, connection, std::move(secret)};
        }

        void remove(std::uint32_t id) {
            std::lock_guard lock(mutex_);
            entries_.erase(id);
        }

        enum class outcome_t : std::uint8_t
        {
            no_such_connection, // no connection of that id (or the secret did not match)
            nothing_running,    // the connection waits for no statement
            cancelled,
        };

        // MySQL KILL QUERY <id>.
        outcome_t cancel(std::uint32_t id) {
            std::lock_guard lock(mutex_);
            const auto found = entries_.find(id);
            if (found == entries_.end()) {
                return outcome_t::no_such_connection;
            }
            return found->second.cancel->request() ? outcome_t::cancelled : outcome_t::nothing_running;
        }

        // PostgreSQL CancelRequest: the process id and its secret key, compared in constant time; a wrong key is
        // no connection (PostgreSQL answers neither).
        outcome_t cancel(std::uint32_t id, std::span<const std::uint8_t> secret) {
            std::lock_guard lock(mutex_);
            const auto found = entries_.find(id);
            if (found == entries_.end() || found->second.secret.size() != secret.size() || secret.empty() ||
                CRYPTO_memcmp(found->second.secret.data(), secret.data(), secret.size()) != 0) {
                return outcome_t::no_such_connection;
            }
            return found->second.cancel->request() ? outcome_t::cancelled : outcome_t::nothing_running;
        }

        // MySQL KILL [CONNECTION] <id>: the statement it waits for cancelled and the connection closed
        // (frontend_connection::finish, callable from any thread). False for no connection of that id.
        bool close(std::uint32_t id);

    private:
        struct entry_t {
            statement_cancel_t* cancel;
            frontend_connection* connection;
            std::vector<std::uint8_t> secret;
        };
        std::mutex mutex_;
        std::unordered_map<std::uint32_t, entry_t> entries_;
    };
} // namespace frontend
