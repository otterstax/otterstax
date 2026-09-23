// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "frontend_connection.hpp"
#include "protocol_config.hpp"
#include "utility/logger.hpp"
#include "utility/thread_pool_manager.hpp"
#include "utility/tracy_profiler.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <components/log/log.hpp>

#include <atomic>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

namespace frontend {
    struct frontend_server_config {
        std::pmr::memory_resource* resource;
        uint16_t port;
        actor_zeta::address_t scheduler;
        // How long a connection waits for the next client bytes before it is
        // closed as idle. Production passes CONNECTION_TIMEOUT_SEC; there is no
        // default — a zero timeout is a configuration error (asserted).
        std::chrono::milliseconds read_timeout;
        // How long the accept chain pauses after building a connection threw.
        // Production passes ACCEPT_RETRY_DELAY_MS; no default — a zero delay is a
        // configuration error (asserted).
        std::chrono::milliseconds accept_retry_delay;
        size_t pool_size = std::thread::hardware_concurrency();
    };

    template<typename DerivedConnection>
    class frontend_server : public connection_close_sink {
    public:
        explicit frontend_server(const frontend_server_config& config)
            : resource_(config.resource)
            , thread_pool_manager_(config.pool_size)
            , acceptor_(thread_pool_manager_.ctx(), {boost::asio::ip::tcp::v4(), config.port})
            , reject_packet_(DerivedConnection::build_too_many_connections_error())
            , accept_retry_timer_(thread_pool_manager_.ctx())
            , next_connection_id_(1)
            , scheduler_(config.scheduler)
            , read_timeout_(config.read_timeout)
            , accept_retry_delay_(config.accept_retry_delay)
            , log_(get_logger(logger_tag::FRONTEND_SERVER)) {
            assert(log_.is_valid());
            assert(resource_ != nullptr && "memory resource must not be null");
            assert(static_cast<bool>(scheduler_) && "scheduler address must not be null");
            assert(read_timeout_.count() > 0 && "read timeout must be positive");
            assert(accept_retry_delay_.count() > 0 && "accept retry delay must be positive");
            connection_pool_.reserve(MAX_CONNECTIONS);
        }

        ~frontend_server() { stop(); }

        thread_pool_status status() const noexcept { return thread_pool_manager_.status(); }

        // The port the acceptor is bound to — the actual one when the config
        // asked for port 0.
        uint16_t local_port() const { return acceptor_.local_endpoint().port(); }

        void start() {
            accept_connections();
            thread_pool_manager_.start();
        }

        // A pooled connection always holds a socket that was already accepted:
        // the socket of an accept in flight belongs to asio until admit() is
        // handed it, so nothing here can close it under the accept.
        void stop() {
            bool expected = false;
            if (!stopped_.compare_exchange_strong(expected, true)) {
                return;
            }

            {
                std::lock_guard lock(pool_mutex_);
                // The accept chain arms under the same lock, so the acceptor is
                // never closed in the middle of an arm on a pool thread.
                boost::system::error_code ec;
                acceptor_.cancel(ec);
                acceptor_.close(ec);
                // A pending retry completes with operation_aborted and does not
                // re-arm, so the join below does not wait out its pause; the
                // socket it holds for a client closes with it.
                accept_retry_timer_.cancel();
                for (auto& conn : connection_pool_) {
                    if (conn) {
                        conn->finish();
                    }
                }
            }

            thread_pool_manager_.stop();
        }

    private:
        static constexpr size_t MAX_CONNECTIONS = 1000;

        // The accept chain: one accept in flight at a time, re-armed only from
        // its own completions (admit(), an accept error, the retry pause).
        void accept_connections() {
            try {
                // stop() sets stopped_ and closes the acceptor under this lock, so
                // an accept is armed on an open acceptor or not at all. Re-arming
                // on a closed one would complete at once with an error and spin the
                // pool threads forever.
                std::lock_guard lock(pool_mutex_);
                if (stopped_.load(std::memory_order_acquire)) {
                    return;
                }
                acceptor_.async_accept(boost::asio::make_strand(thread_pool_manager_.ctx()),
                                       [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
                                           if (ec) {
                                               accept_connections();
                                               return;
                                           }
                                           admit(std::move(socket));
                                       });
            } catch (const std::exception& e) {
                log_->error("Fatal connection error: {}", e.what());
                std::lock_guard lock(pool_mutex_);
                pause_chain([this] { accept_connections(); });
            }
        }

        void admit(boost::asio::ip::tcp::socket socket) {
            std::optional<size_t> slot;
            try {
                // stop() finishes pooled connections under this lock, so one pooled after it would never be finished.
                std::lock_guard lock(pool_mutex_);
                if (stopped_.load(std::memory_order_acquire)) {
                    return;
                }
                slot = acquire_connection_slot();
                if (!slot) {
                    reject(std::move(socket));
                } else {
                    connection_pool_[*slot] = std::make_unique<DerivedConnection>(resource_,
                                                                                  std::move(socket),
                                                                                  next_connection_id_.fetch_add(1),
                                                                                  scheduler_,
                                                                                  *this,
                                                                                  *slot,
                                                                                  read_timeout_);
                    log_->debug("Connection accepted (slot {})", *slot);
                    connection_pool_[*slot]->start();
                }
            } catch (const std::exception& e) {
                log_->error("Fatal connection error: {}", e.what());
                std::lock_guard lock(pool_mutex_);
                // A slot whose connection was never built is empty but taken:
                // left so, MAX_CONNECTIONS failures fill the pool and every later
                // client is refused. A connection that was built stays pooled
                // until stop() finishes it.
                if (slot && !connection_pool_[*slot]) {
                    available_slots_.push(*slot);
                }
                pause_chain([this, socket = std::move(socket)]() mutable {
                    if (socket.is_open()) {
                        admit(std::move(socket));
                    } else {
                        accept_connections();
                    }
                });
                return;
            }
            accept_connections();
        }

        // The caller MUST hold pool_mutex_
        template<typename Resume>
        void pause_chain(Resume resume) {
            if (stopped_.load(std::memory_order_acquire)) {
                return;
            }
            accept_retry_timer_.expires_after(accept_retry_delay_);
            accept_retry_timer_.async_wait([resume = std::move(resume)](boost::system::error_code ec) mutable {
                if (ec != boost::asio::error::operation_aborted) {
                    resume();
                }
            });
        }

        void reject(boost::asio::ip::tcp::socket socket) {
            auto owned = std::make_unique<boost::asio::ip::tcp::socket>(std::move(socket));
            auto& target = *owned;
            boost::asio::async_write(target,
                                     boost::asio::buffer(reject_packet_),
                                     [this, owned = std::move(owned)](boost::system::error_code ec, std::size_t) {
                                         if (ec) {
                                             log_->error("Failed to send rejection packet: {}", ec.message());
                                         }
                                     });
        }

        // The caller MUST hold pool_mutex_
        std::optional<size_t> acquire_connection_slot() {
            if (!available_slots_.empty()) {
                size_t slot = available_slots_.front();
                available_slots_.pop();
                return slot;
            }

            if (connection_pool_.size() < MAX_CONNECTIONS) {
                connection_pool_.emplace_back(nullptr);
                return connection_pool_.size() - 1;
            }

            return std::nullopt;
        }

        // connection_close_sink: called by a connection from its own executor
        // when it closes; destroys the connection and frees the slot.
        void release_connection_slot(size_t slot) override {
            log_->debug("Connection closed (slot {})", slot);
            std::lock_guard lock(pool_mutex_);

            if (slot < connection_pool_.size()) {
                connection_pool_[slot].reset();
                available_slots_.push(slot);
            }
        }

        std::pmr::memory_resource* resource_;
        thread_pool_manager thread_pool_manager_;
        boost::asio::ip::tcp::acceptor acceptor_;
        const std::vector<uint8_t> reject_packet_;
        // The pause of the accept chain after an exception (one chain, re-armed
        // only from its own completions, so one timer). Armed and cancelled under
        // pool_mutex_; declared after thread_pool_manager_, whose io_context it
        // runs on, so it is destroyed first.
        boost::asio::steady_timer accept_retry_timer_;

        actor_zeta::address_t scheduler_;
        std::atomic<uint32_t> next_connection_id_;
        std::chrono::milliseconds read_timeout_;
        std::chrono::milliseconds accept_retry_delay_;
        std::vector<std::unique_ptr<DerivedConnection>> connection_pool_;
        std::queue<size_t> available_slots_;
        log_t log_;

        mutable OTX_LOCKABLE_N(std::mutex, pool_mutex_, "frontend::pool_lock");
        std::atomic<bool> stopped_{false};
    };

} // namespace frontend
