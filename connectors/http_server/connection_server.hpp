// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>
#include <iostream>
#include <memory>
#include <thread>

#include "../mysql/manager.hpp"
#include "../postgresql/manager.hpp"
#include "../clickhouse/manager.hpp"
#include "../file/manager.hpp"
#include "../s3/manager.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

namespace http_server {
    class Session : public std::enable_shared_from_this<Session> {
        tcp::socket socket_;
        beast::flat_buffer buffer_{8192};
        http::request<http::string_body> request_;
        http::response<http::string_body> response_;
        std::shared_ptr<mysqlc::ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pgc::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<chc::ConnectorManager> ch_conn_manager_;
        std::shared_ptr<filec::ConnectorManager> file_conn_manager_;
        std::shared_ptr<s3c::ConnectorManager> s3_conn_manager_;

    public:
        explicit Session(tcp::socket socket,
                         std::shared_ptr<mysqlc::ConnectorManager> mysql_conn_manager,
                         std::shared_ptr<pgc::ConnectorManager> pg_conn_manager,
                         std::shared_ptr<chc::ConnectorManager> ch_conn_manager,
                         std::shared_ptr<filec::ConnectorManager> file_conn_manager,
                         std::shared_ptr<s3c::ConnectorManager> s3_conn_manager);
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
               std::shared_ptr<mysqlc::ConnectorManager> mysql_conn_manager,
               std::shared_ptr<pgc::ConnectorManager> pg_conn_manager,
               std::shared_ptr<chc::ConnectorManager> ch_conn_manager,
               std::shared_ptr<filec::ConnectorManager> file_conn_manager,
               std::shared_ptr<s3c::ConnectorManager> s3_conn_manager);

    private:
        void accept();
        std::shared_ptr<mysqlc::ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pgc::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<chc::ConnectorManager> ch_conn_manager_;
        std::shared_ptr<filec::ConnectorManager> file_conn_manager_;
        std::shared_ptr<s3c::ConnectorManager> s3_conn_manager_;
    };
} // namespace http_server
