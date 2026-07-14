// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_server.hpp"
#include <optional>

#include "../api_connections/ch_connection_config.hpp"
#include "../api_connections/connection_config.hpp"
#include "../api_connections/pg_connection_config.hpp"
#include "utility/tracy_profiler.hpp"

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

namespace conn::api_server {
    Session::Session(tcp::socket socket,
                     std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager,
                     std::shared_ptr<pg::ConnectorManager> pg_conn_manager,
                     std::shared_ptr<ch::ConnectorManager> ch_conn_manager,
                     actor_zeta::address_t s3_manager)
        : socket_(std::move(socket))
        , mysql_conn_manager_(std::move(mysql_conn_manager))
        , pg_conn_manager_(std::move(pg_conn_manager))
        , ch_conn_manager_(std::move(ch_conn_manager))
        , s3_manager_(std::move(s3_manager)) {}

    void Session::start() { read_request(); }

    void Session::read_request() {
        auto self = shared_from_this();
        http::async_read(socket_, buffer_, request_, [self](beast::error_code ec, std::size_t) {
            if (!ec)
                self->handle_request();
        });
    }

    void Session::handle_request() {
        OTX_ZONE_N("http::handle_request");

        response_.clear();
        response_.version(request_.version());
        response_.keep_alive(request_.keep_alive());

        // Health Check Endpoint
        if (request_.method() == http::verb::get && request_.target() == "/health") {
            response_.result(http::status::ok);
            response_.set(http::field::content_type, "application/json");
            response_.body() = R"({"status": "healthy", "timestamp": ")" + get_current_timestamp() + "\"}";
            response_.content_length(response_.body().size());
        } else if (request_.method() == http::verb::post && request_.target() == "/add_connection") {
            OTX_ZONE_N("http::add_connection");
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

                mysql_conn_manager_->addConnection(params);

                response_.result(http::status::ok);
                response_.set(http::field::content_type, "application/json");
                response_.body() = std::string("Connection added");
                response_.content_length(response_.body().size());

            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::post && request_.target() == "/add_pg_connection") {
            OTX_ZONE_N("http::add_pg_connection");
            try {
                auto json_body = boost::json::parse(request_.body());
                if (auto err = check_json_body(boost::json::parse(request_.body()).as_object()); err.has_value()) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Invalid JSON: ") + err.value();
                    write_response();
                    return;
                }
                PgConnectionParams params{
                    .alias = json_body.at("alias").as_string().c_str(),
                    .host = json_body.at("host").as_string().c_str(),
                    .port = json_body.at("port").as_string().c_str(),
                    .username = json_body.at("username").as_string().c_str(),
                    .password = json_body.at("password").as_string().c_str(),
                    .database = json_body.at("database").as_string().c_str(),
                    .schema = json_body.as_object().contains("schema") ? json_body.at("schema").as_string().c_str()
                                                                       : "public",
                    .table = json_body.at("table").as_string().c_str(),
                };

                pg_conn_manager_->addConnection(params);

                response_.result(http::status::ok);
                response_.set(http::field::content_type, "application/json");
                response_.body() = std::string("PostgreSQL connection added");
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

                const bool conn_exist = mysql_conn_manager_->hasConnection(alias);

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
        } else if (request_.method() == http::verb::get && request_.target() == "/check_pg_connection") {
            try {
                auto json_body = boost::json::parse(request_.body());
                if (!boost::json::parse(request_.body()).as_object().contains("alias")) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Missing alias");

                    write_response();
                    return;
                }
                std::string alias = json_body.at("alias").as_string().c_str();

                const bool conn_exist = pg_conn_manager_->hasConnection(alias);

                if (conn_exist) {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("PostgreSQL connection [" + alias + "] exists");
                    response_.content_length(response_.body().size());
                } else {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("PostgreSQL connection [" + alias + "] not exist");
                    response_.content_length(response_.body().size());
                }
            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::post && request_.target() == "/add_ch_connection") {
            OTX_ZONE_N("http::add_ch_connection");
            try {
                auto json_body = boost::json::parse(request_.body());
                const static std::vector<std::string> ch_required_keys =
                    {"alias", "host", "port", "username", "password", "database", "table"};
                for (const auto& key : ch_required_keys) {
                    if (!json_body.as_object().contains(key)) {
                        response_.result(http::status::bad_request);
                        response_.body() = std::string("Missing key: ") + key;
                        write_response();
                        return;
                    }
                    if (!json_body.at(key).is_string()) {
                        response_.result(http::status::bad_request);
                        response_.body() = std::string("Key is not a string: ") + key;
                        write_response();
                        return;
                    }
                }
                ChConnectionParams params{
                    .alias = json_body.at("alias").as_string().c_str(),
                    .host = json_body.at("host").as_string().c_str(),
                    .port = json_body.at("port").as_string().c_str(),
                    .username = json_body.at("username").as_string().c_str(),
                    .password = json_body.at("password").as_string().c_str(),
                    .database = json_body.at("database").as_string().c_str(),
                    .table = json_body.at("table").as_string().c_str(),
                };

                ch_conn_manager_->addConnection(params);

                response_.result(http::status::ok);
                response_.set(http::field::content_type, "application/json");
                response_.body() = std::string("ClickHouse connection added");
                response_.content_length(response_.body().size());

            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::get && request_.target() == "/check_ch_connection") {
            try {
                auto json_body = boost::json::parse(request_.body());
                if (!json_body.as_object().contains("alias")) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Missing alias");
                    write_response();
                    return;
                }
                std::string alias = json_body.at("alias").as_string().c_str();

                const bool conn_exist = ch_conn_manager_->hasConnection(alias);

                if (conn_exist) {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("ClickHouse connection [" + alias + "] exists");
                    response_.content_length(response_.body().size());
                } else {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("ClickHouse connection [" + alias + "] not exist");
                    response_.content_length(response_.body().size());
                }
            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::get && request_.target() == "/s3/add_credentials") {
            try {
                auto json_body = boost::json::parse(request_.body()).as_object();
                for (const auto& key : {"alias", "access_key", "secret_key"}) {
                    if (!json_body.contains(key)) {
                        response_.result(http::status::bad_request);
                        response_.body() = std::string("Missing key: ") + key;
                        write_response();
                        return;
                    }
                }
                s3::connect_params params{
                    .region        = json_body.contains("region")
                                         ? std::string(json_body.at("region").as_string()) : "",
                    .access_key    = std::string(json_body.at("access_key").as_string()),
                    .secret_key    = std::string(json_body.at("secret_key").as_string()),
                    .session_token = json_body.contains("session_token")
                                         ? std::string(json_body.at("session_token").as_string()) : "",
                    .endpoint      = json_body.contains("endpoint")
                                         ? std::string(json_body.at("endpoint").as_string()) : "",
                    .alias         = std::string(json_body.at("alias").as_string()),
                };
                auto fut = actor_zeta::send(s3_manager_, &s3::ConnectorManager::add_credentials, session_id().hash(), std::move(params));
                auto result = std::move(fut.second).take_ready();
                if (!result.has_error()) {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("S3 credentials added");
                } else {
                    response_.result(http::status::internal_server_error);
                    response_.body() = std::string(result.error().what.c_str());
                }
                response_.content_length(response_.body().size());
            } catch (const std::exception& e) {
                response_.result(http::status::bad_request);
                response_.body() = std::string("ERROR: ") + e.what();
            }
        } else if (request_.method() == http::verb::post && request_.target() == "/s3/remove_credentials") {
            try {
                auto json_body = boost::json::parse(request_.body()).as_object();
                if (!json_body.contains("alias")) {
                    response_.result(http::status::bad_request);
                    response_.body() = std::string("Missing key: alias");
                    write_response();
                    return;
                }
                std::string alias = std::string(json_body.at("alias").as_string());
                auto fut = actor_zeta::send(s3_manager_, &s3::ConnectorManager::remove_credentials, session_id().hash(), std::move(alias));
                auto result = std::move(fut.second).take_ready();
                if (!result.has_error()) {
                    response_.result(http::status::ok);
                    response_.set(http::field::content_type, "application/json");
                    response_.body() = std::string("S3 credentials removed");
                } else {
                    response_.result(http::status::internal_server_error);
                    response_.body() = std::string(result.error().what.c_str());
                }
                response_.content_length(response_.body().size());
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
        OTX_ZONE_N("http::write_response");
        auto self = shared_from_this();
        http::async_write(socket_, response_, [self](beast::error_code ec, std::size_t) {
            self->socket_.shutdown(tcp::socket::shutdown_send, ec);
        });
    }

    Server::Server(asio::io_context& ioc,
                   unsigned short port,
                   std::shared_ptr<mysql::ConnectorManager> mysql_conn_manager,
                   std::shared_ptr<pg::ConnectorManager> pg_conn_manager,
                   std::shared_ptr<ch::ConnectorManager> ch_conn_manager,
                   actor_zeta::address_t s3_manager)
        : ioc_(ioc)
        , acceptor_(ioc, tcp::endpoint(tcp::v4(), port))
        , mysql_conn_manager_(std::move(mysql_conn_manager))
        , pg_conn_manager_(std::move(pg_conn_manager))
        , ch_conn_manager_(std::move(ch_conn_manager))
        , s3_manager_(std::move(s3_manager)) {
        accept();
    }

    void Server::accept() {
        acceptor_.async_accept([this](beast::error_code ec, tcp::socket socket) {
            OTX_ZONE_N("http::accept");
            if (!ec)
                std::make_shared<Session>(std::move(socket),
                                          mysql_conn_manager_,
                                          pg_conn_manager_,
                                          ch_conn_manager_,
                                          s3_manager_)
                    ->start();
            accept();
        });
    }
} // namespace conn::api_server
