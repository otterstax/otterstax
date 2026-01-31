// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_server.hpp"
#include <optional>

#include "connection_config.hpp"

namespace {
    std::string get_current_timestamp() {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);

        std::stringstream ss;
        ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
        return ss.str();
    }

    std::optional<std::string> check_json_body(const boost::json::object& json_body) {
        const static std::vector<std::string> required_keys =
            {"alias", "host", "port", "username", "password", "database", "table"};

        for (const auto& key : required_keys) {
            if (!json_body.contains(key)) {
                std::stringstream ss;
                ss << "Missing key: " << key;
                return ss.str();
            }
            if (!json_body.at(key).is_string()) {
                std::stringstream ss;
                ss << "Key is not a string: " << key;
                return ss.str();
            }
        }
        return std::nullopt;
    }
} // namespace

namespace http_server {
    Session::Session(tcp::socket socket, std::shared_ptr<mysqlc::ConnectorManager> conn_manager)
        : socket_(std::move(socket))
        , conn_manager_(std::move(conn_manager)) {}

    void Session::start() { read_request(); }

    void Session::read_request() {
        auto self = shared_from_this();
        http::async_read(socket_, buffer_, request_, [self](beast::error_code ec, std::size_t) {
            if (!ec)
                self->handle_request();
        });
    }

    void Session::handle_request() {
        response_.clear();
        response_.version(request_.version());
        response_.keep_alive(request_.keep_alive());

        // Health Check Endpoint
        if (request_.method() == http::verb::get && request_.target() == "/health") {
            response_.result(http::status::ok);
            response_.set(http::field::content_type, "application/json");
            response_.body() = R"({"status": "healthy", "timestamp": ")" + get_current_timestamp() + "\"}";
            response_.content_length(response_.body().size());
            // Health check OK
        } else if (request_.method() == http::verb::post && request_.target() == "/add_connection") {
            try {
                auto json_body = boost::json::parse(request_.body());
                if (auto err = check_json_body(boost::json::parse(request_.body()).as_object()); err.has_value()) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Invalid JSON: ") + err.value();
                    write_response();
                    return;
                }
                ConnectionParams params{
                    .alias = json_body.at("alias").as_string().c_str(),
                    .host = json_body.at("host").as_string().c_str(),
                    .port = json_body.at("port").as_string().c_str(),
                    .username = json_body.at("username").as_string().c_str(),
                    .password = json_body.at("password").as_string().c_str(),
                    .database = json_body.at("database").as_string().c_str(),
                    .table = json_body.at("table").as_string().c_str(),
                };

                conn_manager_->addConnection(params);

                response_.result(http::status::ok);
                response_.set(http::field::content_type, "application/json");
                response_.body() = std::string("Connection added");
                response_.content_length(response_.body().size());

            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::get && request_.target() == "/check_connection") {
            try {
                auto json_body = boost::json::parse(request_.body());
                if (!boost::json::parse(request_.body()).as_object().contains("alias")) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Missing alias");

                    write_response();
                    return;
                }
                std::string alias = json_body.at("alias").as_string().c_str();

                const bool conn_exist = conn_manager_->hasConnection(alias);

                if (conn_exist) {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("Connection [" + alias + "] exists");
                    response_.content_length(response_.body().size());
                } else {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("Connection [" + alias + "] not exist exists");
                    response_.content_length(response_.body().size());
                }
            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else {
            response_.result(http::status::not_found);
            response_.body() = "Resource not found";
        }
        response_.prepare_payload();
        write_response();
    }

    void Session::write_response() {
        auto self = shared_from_this();
        http::async_write(socket_, response_, [self](beast::error_code ec, std::size_t) {
            self->socket_.shutdown(tcp::socket::shutdown_send, ec);
        });
    }

    Server::Server(asio::io_context& ioc, unsigned short port, std::shared_ptr<mysqlc::ConnectorManager> conn_manager)
        : ioc_(ioc)
        , acceptor_(ioc, tcp::endpoint(tcp::v4(), port))
        , conn_manager_(std::move(conn_manager)) {
        accept();
    }

    void Server::accept() {
        acceptor_.async_accept([this](beast::error_code ec, tcp::socket socket) {
            if (!ec)
                std::make_shared<Session>(std::move(socket), conn_manager_)->start();
            accept();
        });
    }
} // namespace http_server
