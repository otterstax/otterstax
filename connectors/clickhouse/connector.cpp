// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"

#include "utility/logger.hpp"
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace ch {

    Connector::Connector(connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , client_(nullptr)
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {
        assert(log_.is_valid());
    }

    connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        log_->debug("Alias: {} close connection", alias_);
        if (status_ != Status::Connected) {
            return;
        }
        client_.reset();
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    void Connector::connect() {
        log_->debug("Alias: {} connecting with: host={} port={} database={}",
                    alias_, params_.host, params_.port, params_.database);

        try {
            clickhouse::ClientOptions opts;
            opts.SetHost(params_.host)
                .SetPort(params_.port)
                .SetUser(params_.username)
                .SetPassword(params_.password)
                .SetDefaultDatabase(params_.database)
                .SetPingBeforeQuery(true)
                .SetSendRetries(3)
                .SetRetryTimeout(std::chrono::seconds(5));

            client_ = std::make_unique<clickhouse::Client>(opts);
            client_->Ping();
            status_ = Status::Connected;
            log_->debug("Alias: {} connected successfully", alias_);
        } catch (const std::exception& e) {
            log_->debug("Alias: {} connect failed: {}", alias_, e.what());
            tryReconnect();
        }
    }

    bool Connector::isConnected() {
        if (status_ != Status::Connected)
            return false;

        if (!client_) {
            status_ = Status::Disconnected;
            log_->debug("Alias: {} connection check failed", alias_);
            return false;
        }

        try {
            client_->Ping();
            return true;
        } catch (...) {
            status_ = Status::Disconnected;
            log_->debug("Alias: {} Ping failed", alias_);
            return false;
        }
    }

    void Connector::tryReconnect() {
        if (status_ == Status::Connected) {
            return;
        }
        status_ = Status::Disconnected;
        size_t attempts = 0;
        log_->debug("Alias: {} Try to reconnect (max_attempts={}, delay={}ms)",
                    alias_, params_.max_reconnect_attempts, params_.reconnect_delay_ms);

        do {
            log_->debug("Alias: {} Attempt: {}", alias_, attempts);
            try {
                clickhouse::ClientOptions opts;
                opts.SetHost(params_.host)
                    .SetPort(params_.port)
                    .SetUser(params_.username)
                    .SetPassword(params_.password)
                    .SetDefaultDatabase(params_.database)
                    .SetPingBeforeQuery(true);

                client_ = std::make_unique<clickhouse::Client>(opts);
                client_->Ping();
                log_->debug("Alias: {} Reconnect success", alias_);
                status_ = Status::Connected;
                return;
            } catch (const std::exception& e) {
                log_->debug("Alias: {} Reconnect attempt: {} failed: {}",
                            alias_, attempts, e.what());
            }
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(params_.reconnect_delay_ms));
        } while (attempts < params_.max_reconnect_attempts);

        std::string error = "[Connector] Alias: " + alias_ + " connect failed";
        log_->error(error);
        throw std::runtime_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }

} // namespace ch
