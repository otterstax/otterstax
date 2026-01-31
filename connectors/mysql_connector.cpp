// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_connector.hpp"

#include "utility/logger.hpp"
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace mysql = boost::mysql;
namespace asio = boost::asio;

namespace mysqlc {
    Connector::Connector(asio::io_context& io_ctx, mysql::connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , conn_(io_ctx)
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {
        assert(log_.is_valid());
    }

    mysql::connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        log_->debug("Alias: {} close connection", alias_);
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
            log_->debug("Alias: {} connect failed", alias_);
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
            log_->debug("Alias: {} Ping failed: {}", alias_, ec.message());
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
        log_->debug("Alias: {} Try to reconnect", alias_);

        boost::system::error_code ec;
        boost::mysql::diagnostics diag;
        do {
            log_->debug("Alias: {} Attempt: {}", alias_, attempts);
            conn_.connect(params_, ec, diag);
            if (!ec) {
                log_->debug("Alias: {} Reconnect success", alias_);
                status_ = Status::Connected;
                return;
            }
            log_->debug("Alias: {} Reconnect attempt: {} failed: {} - {}",
                        alias_,
                        attempts,
                        ec.message(),
                        diag.server_message());
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (attempts < 3);
        std::string error = "[Connector] Alias: " + alias_ + " connect failed " + ec.message();
        log_->error(error);
        throw std::runtime_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }
} // namespace mysqlc