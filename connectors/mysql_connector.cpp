// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_connector.hpp"

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <iostream>

namespace mysql = boost::mysql;
namespace asio = boost::asio;

namespace mysqlc {
    Connector::Connector(asio::io_context& io_ctx, mysql::connect_params params, std::string alias)
        : conn_(io_ctx)
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {}

    mysql::connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        std::cout << "[Connector] Alias: " << alias_ << " close connection\n";
        if (status_ != Status::Connected) {
            return;
        }
        conn_.close();
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    void Connector::connect() {
        conn_.set_meta_mode(mysql::metadata_mode::full);
        boost::system::error_code ec;
        boost::mysql::diagnostics diag;
        conn_.connect(params_, ec, diag);
        if (ec) {
            std::cout << "[Connector] Alias: " << alias_ << " connect failed\n";
            tryReconnect();
        }
        status_ = Status::Connected;
    }

    bool Connector::isConnected() {
        if (status_ != Status::Connected)
            return false;
        boost::system::error_code ec;
        boost::mysql::diagnostics diag;
        conn_.ping(ec, diag);
        if (ec) {
            status_ = Status::Disconnected;
            std::cout << "[Connector] Alias: " << alias_ << " Ping failed: " << ec.message() << std::endl;
            return false;
        }
        return true;
    }

    void Connector::tryReconnect() {
        if (status_ == Status::Connected) {
            return;
        }
        status_ = Status::Disconnected;
        size_t attempts = 0;
        std::cout << "[Connector] Alias: " << alias_ << " Try to reconnect\n";

        boost::system::error_code ec;
        boost::mysql::diagnostics diag;
        do {
            std::cout << "[Connector] Alias: " << alias_ << " Attempt: " << attempts << std::endl;
            conn_.connect(params_, ec, diag);
            if (!ec) {
                std::cout << "[Connector] Alias: " << alias_ << " Reconnect success\n";
                status_ = Status::Connected;
                return;
            }
            std::cout << "[Connector] Alias: " << alias_ << " Reconnect attempt: " << attempts
                      << " failed: " << ec.message() << std::endl
                      << diag.server_message() << std::endl;
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (attempts < 3);
        std::string error = "[Connector] Alias: " + alias_ + " connect failed " + ec.message();
        std::cout << error;
        throw std::runtime_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }
} // namespace mysqlc