// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "frontend_connection.hpp"
#include "protocol_config.hpp"
#include "utility/logger.hpp"
#include "utility/thread_pool_manager.hpp"

#include <actor-zeta.hpp>
#include <atomic>
#include <boost/asio.hpp>
#include <components/log/log.hpp>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <vector>

namespace frontend {
    struct frontend_server_config {
        std::pmr::memory_resource* resource;
        uint16_t port;
        actor_zeta::address_t scheduler;
        size_t pool_size = std::thread::hardware_concurrency();
    };

    template<typename DerivedConnection>
    class frontend_server {
    public:
        explicit frontend_server(const frontend_server_config& config)
            : resource_(config.resource)
            , thread_pool_manager_(config.pool_size)
            , acceptor_(thread_pool_manager_.ctx(),
                        boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), config.port))
            , rejector_socket_(thread_pool_manager_.ctx())
            , next_connection_id_(1)
            , active_connections_(0)
            , scheduler_(config.scheduler)
            , log_(get_logger(logger_tag::FRONTEND_SERVER)) {
            assert(log_.is_valid());
            assert(resource_ != nullptr && "memory resource must not be null");
            assert(static_cast<bool>(scheduler_) && "scheduler address must not be null");
            connection_pool_.reserve(MAX_CONNECTIONS);
        }

        thread_pool_status status() const noexcept { return thread_pool_manager_.status(); }

        void start() {
            accept_connections();
            thread_pool_manager_.start();
        }

        void stop() { thread_pool_manager_.stop(); }

        virtual ~frontend_server() { shutdown(); }

        void release_connection_slot(size_t slot) {
            std::lock_guard<std::mutex> lock(pool_mutex_);

            if (slot < connection_pool_.size()) {
                available_slots_.push(slot);
                if (active_connections_.fetch_sub(1) == 0) { // underflow
                    active_connections_.store(0);
                }
            }
        }

    protected:
        void shutdown() {
            shutting_down.store(true);

            acceptor_.cancel();
            acceptor_.close();
            {
                std::lock_guard<std::mutex> lock(pool_mutex_);
                for (auto& conn : connection_pool_) {
                    conn.finish();
                }
            }

            thread_pool_manager_.stop();
            rejector_socket_.close();
        }

    private:
        void accept_connections() {
            if (shutting_down.load()) {
                return;
            }

            try {
                if (auto slt = acquire_connection_slot(); slt.has_value()) {
                    acceptor_.async_accept(connection_pool_[slt.value()].socket(),
                                           [this, slot = slt.value()](boost::system::error_code ec) {
                                               if (!ec) {
                                                   log_->debug("Connection accepted (slot {})", slot);
                                                   connection_pool_[slot].start();
                                                   active_connections_.fetch_add(1);
                                               } else {
                                                   release_connection_slot(slot);
                                               }
                                               accept_connections();
                                           });
                } else {
                    acceptor_.async_accept(rejector_socket_, [this](boost::system::error_code ec) {
                        if (!ec) {
                            log_->debug("Connection pool exhausted: rejecting connection");
                            reject_connection();
                        } else {
                            accept_connections();
                        }
                    });
                }
            } catch (const std::exception& e) {
                log_->error("Fatal connection error: {}", e.what());
                auto timer = std::make_shared<boost::asio::steady_timer>(thread_pool_manager_.ctx(),
                                                                         CONNECTION_EXCEPTION_TIMEOUT);
                timer->async_wait([this, timer](boost::system::error_code) { accept_connections(); });
            }
        }

        void reject_connection() {
            boost::asio::async_write(rejector_socket_,
                                     boost::asio::buffer(DerivedConnection::build_too_many_connections_error()),
                                     [this](boost::system::error_code ec, std::size_t) {
                                         boost::system::error_code close_ec;
                                         rejector_socket_.close(close_ec);

                                         if (ec) {
                                             log_->error("Failed to send rejection packet: {}", ec.message());
                                         }
                                         accept_connections();
                                     });
        }

        std::optional<size_t> acquire_connection_slot() {
            static auto callback_factory = [this](size_t slot) {
                return [this, slot]() {
                    log_->debug("Connection closed (slot {})", slot);
                    release_connection_slot(slot);
                };
            };

            std::lock_guard<std::mutex> lock(pool_mutex_);
            if (!available_slots_.empty()) {
                size_t slot = available_slots_.front();
                available_slots_.pop();

                connection_pool_[slot].finish();
                connection_pool_[slot] = DerivedConnection(resource_,
                                                           thread_pool_manager_.ctx(),
                                                           next_connection_id_.fetch_add(1),
                                                           scheduler_,
                                                           callback_factory(slot));
                return slot;
            }

            if (connection_pool_.size() < MAX_CONNECTIONS) {
                connection_pool_.emplace_back(resource_,
                                              thread_pool_manager_.ctx(),
                                              next_connection_id_.fetch_add(1),
                                              scheduler_,
                                              callback_factory(connection_pool_.size()));
                return connection_pool_.size() - 1;
            }

            return {};
        }

        static constexpr size_t MAX_CONNECTIONS = 1000;
        static constexpr std::chrono::milliseconds CONNECTION_EXCEPTION_TIMEOUT = std::chrono::milliseconds(100);

        std::pmr::memory_resource* resource_;
        thread_pool_manager thread_pool_manager_;
        boost::asio::ip::tcp::acceptor acceptor_;
        boost::asio::ip::tcp::socket rejector_socket_;
        std::atomic<uint32_t> next_connection_id_;
        std::atomic<size_t> active_connections_;
        std::atomic<bool> shutting_down = false;

        actor_zeta::address_t scheduler_;
        std::vector<DerivedConnection> connection_pool_;
        std::queue<size_t> available_slots_;
        log_t log_;

        mutable std::mutex pool_mutex_;
    };
} // namespace frontend
