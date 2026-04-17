// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>
#include <iostream>
#include <memory>
#include <thread>

#include "../clickhouse/manager.hpp"
#include "../mysql/manager.hpp"
#include "../postgresql/manager.hpp"
#include "connectors/s3/manager.hpp"
#include <actor-zeta.hpp>

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace conn::api_server {
    class Session : public std::enable_shared_from_this<Session> {
        tcp::socket socket_;
        beast::flat_buffer buffer_{8192};
        http::request<http::string_body> request_;
        http::response<http::string_body> response_;
        std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pg::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<ch::ConnectorManager> ch_conn_manager_;
        actor_zeta::address_t s3_manager_;

    public:
        explicit Session(tcp::socket socket,
                         std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager,
                         std::shared_ptr<pg::ConnectorManager> pg_conn_manager,
                         std::shared_ptr<ch::ConnectorManager> ch_conn_manager,
                         actor_zeta::address_t s3_manager);
        void start();

    private:
        void read_request();
        void handle_request();
        void write_response();
    };

    class Server {
        asio::io_context& ioc_;
        tcp::acceptor acceptor_;

    public:
        Server(asio::io_context& ioc, unsigned short port,
               std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager,
               std::shared_ptr<pg::ConnectorManager> pg_conn_manager,
               std::shared_ptr<ch::ConnectorManager> ch_conn_manager,
               actor_zeta::address_t s3_manager);

    private:
        void accept();
        std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pg::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<ch::ConnectorManager> ch_conn_manager_;
        actor_zeta::address_t s3_manager_;
    };
} // namespace conn::api_server
