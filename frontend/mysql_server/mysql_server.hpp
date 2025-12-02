// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../../utility/thread_pool_manager.hpp"
#include "connection/mysql_connection.hpp"
#include "protocol_const.hpp"

#include <actor-zeta.hpp>
#include <atomic>
#include <boost/asio.hpp>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <vector>

namespace mysql_front {

    struct mysql_server_config {
        std::pmr::memory_resource* resource;
        uint16_t port;
        actor_zeta::address_t scheduler;
        size_t pool_size = std::thread::hardware_concurrency();
    };

    class mysql_server {
    public:
        mysql_server(const mysql_server_config& config);
        thread_pool_status status() const noexcept;
        void start();
        void stop();

        ~mysql_server();

        void release_connection_slot(size_t slot);

    private:
        void accept_connections();
        void reject_connection();

        std::optional<size_t> acquire_connection_slot();

        static constexpr size_t MAX_CONNECTIONS = 1000;

        std::pmr::memory_resource* resource_;
        thread_pool_manager thread_pool_manager_;
        boost::asio::ip::tcp::acceptor acceptor_;
        boost::asio::ip::tcp::socket rejector_socket_;
        std::atomic<uint32_t> next_connection_id_;
        std::atomic<size_t> active_connections_;
        std::atomic<bool> shutting_down = false;

        actor_zeta::address_t scheduler_;
        std::vector<mysql_connection> connection_pool_;
        std::queue<size_t> available_slots_;

        mutable std::mutex pool_mutex_;
    };
} // namespace mysql_front