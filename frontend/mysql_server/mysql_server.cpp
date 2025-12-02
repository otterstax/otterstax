// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_server.hpp"
#include <csignal>

using boost::asio::ip::tcp;

namespace mysql_front {
    constexpr std::chrono::milliseconds CONNECTION_EXCEPTION_TIMEOUT(100);

    mysql_server::mysql_server(const mysql_server_config& config)
        : resource_(config.resource)
        , thread_pool_manager_(config.pool_size)
        , acceptor_(thread_pool_manager_.ctx(), tcp::endpoint(tcp::v4(), config.port))
        , rejector_socket_(thread_pool_manager_.ctx())
        , next_connection_id_(1)
        , active_connections_(0)
        , scheduler_(config.scheduler) {
        connection_pool_.reserve(MAX_CONNECTIONS);
    }

    thread_pool_status mysql_server::status() const noexcept { return thread_pool_manager_.status(); }

    void mysql_server::start() {
        accept_connections();
        thread_pool_manager_.start();
    }

    void mysql_server::stop() { thread_pool_manager_.stop(); }

    mysql_server::~mysql_server() {
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

    void mysql_server::accept_connections() {
        if (shutting_down.load()) {
            return;
        }

        try {
            if (auto slt = acquire_connection_slot(); slt.has_value()) {
                acceptor_.async_accept(connection_pool_[slt.value()].socket(),
                                       [this, slot = slt.value()](boost::system::error_code ec) {
                                           if (!ec) {
                                               std::cout << "Connection accepted (slot " << slot << ")" << std::endl;
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
                        std::cout << "Connection pool exhausted: rejecting connection" << std::endl;
                        reject_connection();
                    } else {
                        accept_connections();
                    }
                });
            }
        } catch (const std::exception& e) {
            std::cerr << "Fatal connection error: " << e.what() << std::endl;
            auto timer =
                std::make_shared<boost::asio::steady_timer>(thread_pool_manager_.ctx(), CONNECTION_EXCEPTION_TIMEOUT);
            timer->async_wait([this, timer](boost::system::error_code) { accept_connections(); });
        }
    }

    void mysql_server::reject_connection() {
        packet_writer writer;
        auto error_packet = build_error(writer, 0, mysql_error::ER_CON_COUNT_ERROR, "Too many connections");

        boost::asio::async_write(rejector_socket_,
                                 boost::asio::buffer(error_packet),
                                 [this](boost::system::error_code ec, std::size_t) {
                                     boost::system::error_code close_ec;
                                     rejector_socket_.close(close_ec);

                                     if (ec) {
                                         std::cerr << "Failed to send rejection packet: " << ec.message() << std::endl;
                                     }
                                     accept_connections();
                                 });
    }

    std::optional<size_t> mysql_server::acquire_connection_slot() {
        static auto callback_factory = [this](size_t slot) {
            return [this, slot]() {
                std::cout << "Connection closed (slot " << slot << ")" << std::endl;
                release_connection_slot(slot);
            };
        };

        std::lock_guard<std::mutex> lock(pool_mutex_);
        if (!available_slots_.empty()) {
            size_t slot = available_slots_.front();
            available_slots_.pop();

            connection_pool_[slot].finish();
            connection_pool_[slot] = mysql_connection(resource_,
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

    void mysql_server::release_connection_slot(size_t slot) {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        if (slot < connection_pool_.size()) {
            available_slots_.push(slot);
            if (active_connections_.fetch_sub(1) == 0) { // underflow
                active_connections_.store(0);
            }
        }
    }
} // namespace mysql_front