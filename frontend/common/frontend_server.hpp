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
            , rejector_socket_(thread_pool_manager_.ctx())
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

        // Once stopped_ is set, stop() owns every pooled connection: it finishes
        // each of them itself and the accept handler no longer touches the pool
        // (see accept_connections). finish() closes the socket on the
        // connection's strand and the last aborted completion releases the
        // slot; the pool is joined only after all of those have run.
        void stop() {
            bool expected = false;
            if (!stopped_.compare_exchange_strong(expected, true)) {
                return;
            }

            boost::system::error_code ec;
            acceptor_.cancel(ec);
            acceptor_.close(ec);
            rejector_socket_.close(ec);

            {
                std::lock_guard lock(pool_mutex_);
                // A pending accept retry completes with operation_aborted and
                // does not re-arm, so the join below does not wait out its pause.
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

        void accept_connections() {
            // The acceptor is closed: an accept re-armed on it completes at once
            // with an error, and re-arming from that completion would spin the
            // pool threads forever.
            if (stopped_.load(std::memory_order_acquire)) {
                return;
            }
            // The slot this iteration takes; the catch hands it back when the
            // connection for it was never built.
            std::optional<size_t> slot_opt;
            try {
                slot_opt = acquire_connection_slot();
                if (!slot_opt) {
                    acceptor_.async_accept(rejector_socket_, [this](boost::system::error_code ec) {
                        if (!ec) {
                            reject_connection();
                        }
                        accept_connections();
                    });
                    return;
                }

                const size_t index = *slot_opt;
                // The slot is written and the accept armed under the pool lock:
                // stop() finishes pooled connections under the same lock, so the
                // fresh connection cannot be finished (and its slot released)
                // between being pooled and having its socket handed to the
                // acceptor.
                std::lock_guard lock(pool_mutex_);
                connection_pool_[index] = std::make_unique<DerivedConnection>(resource_,
                                                                              thread_pool_manager_.ctx(),
                                                                              next_connection_id_.fetch_add(1),
                                                                              scheduler_,
                                                                              *this,
                                                                              index,
                                                                              read_timeout_);

                acceptor_.async_accept(connection_pool_[index]->socket(), [this, index](boost::system::error_code ec) {
                    {
                        std::lock_guard lock(pool_mutex_);
                        // stop() sets stopped_ before it takes this lock to finish
                        // every pooled connection, so a handler that sees it set
                        // leaves the slot to stop(): releasing it here would destroy
                        // a connection whose finish is already posted.
                        if (stopped_.load(std::memory_order_acquire)) {
                            return;
                        }
                        if (!ec) {
                            log_->debug("Connection accepted (slot {})", index);
                            connection_pool_[index]->start();
                        } else {
                            // The connection closes itself through finish(): the
                            // completion of its last pending operation releases the
                            // slot, the only path that destroys a pooled connection.
                            connection_pool_[index]->finish();
                        }
                    }
                    accept_connections();
                });
            } catch (const std::exception& e) {
                log_->error("Fatal connection error: {}", e.what());
                // The pause is armed under the pool lock and only while stopped_
                // is clear: stop() cancels the timer under the same lock after
                // setting stopped_, so a retry is either cancelled by it or never
                // armed. The chain is the only user of the timer and re-arms it
                // only from its own completions, so no wait is pending here.
                std::lock_guard lock(pool_mutex_);
                // A slot whose connection was never built is empty but taken:
                // left so, MAX_CONNECTIONS failures fill the pool and every later
                // client is refused. A connection that was built (async_accept
                // itself threw) stays pooled until stop() finishes it.
                if (slot_opt && !connection_pool_[*slot_opt]) {
                    available_slots_.push(*slot_opt);
                }
                if (stopped_.load(std::memory_order_acquire)) {
                    return;
                }
                accept_retry_timer_.expires_after(accept_retry_delay_);
                accept_retry_timer_.async_wait([this](boost::system::error_code ec) {
                    // Cancelled by stop(); accept_connections() itself returns
                    // once stopped_ is set.
                    if (ec == boost::asio::error::operation_aborted) {
                        return;
                    }
                    accept_connections();
                });
            }
        }

        // One rejection is in flight at a time (the rejector socket is re-armed
        // only from this completion), so the packet lives in reject_packet_.
        void reject_connection() {
            boost::asio::async_write(rejector_socket_,
                                     boost::asio::buffer(reject_packet_),
                                     [this](boost::system::error_code ec, std::size_t) {
                                         boost::system::error_code close_ec;
                                         rejector_socket_.close(close_ec);

                                         if (ec) {
                                             log_->error("Failed to send rejection packet: {}", ec.message());
                                         }
                                         if (close_ec) {
                                             log_->error("Failed to close rejector socket: {}", close_ec.message());
                                         }
                                         accept_connections();
                                     });
        }

        std::optional<size_t> acquire_connection_slot() {
            std::lock_guard lock(pool_mutex_);

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
        boost::asio::ip::tcp::socket rejector_socket_;
        std::vector<uint8_t> reject_packet_;
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
