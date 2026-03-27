// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"

#include "utility/logger.hpp"
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace pgc {

    Connector::Connector(connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , conn_(nullptr)
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
        conn_.reset();
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    void Connector::connect() {
        std::string conn_str = params_.connection_string();
        log_->debug("Alias: {} connecting with: host={} port={} dbname={}",
                    alias_, params_.host, params_.port, params_.database);

        conn_.reset(PQconnectdb(conn_str.c_str()));

        if (PQstatus(conn_.get()) != CONNECTION_OK) {
            std::string err = PQerrorMessage(conn_.get());
            log_->debug("Alias: {} connect failed: {}", alias_, err);
            tryReconnect();
            return;
        }
        status_ = Status::Connected;
        log_->debug("Alias: {} connected successfully", alias_);
    }

    bool Connector::isConnected() {
        if (status_ != Status::Connected)
            return false;

        if (!conn_ || PQstatus(conn_.get()) != CONNECTION_OK) {
            status_ = Status::Disconnected;
            log_->debug("Alias: {} connection check failed", alias_);
            return false;
        }

        // Ping the server to check connection is alive
        PGResultPtr result(PQexec(conn_.get(), "SELECT 1"));
        if (!result || PQresultStatus(result.get()) != PGRES_TUPLES_OK) {
            status_ = Status::Disconnected;
            log_->debug("Alias: {} Ping failed", alias_);
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
        log_->debug("Alias: {} Try to reconnect (max_attempts={}, delay={}ms)",alias_, params_.max_reconnect_attempts, params_.reconnect_delay_ms);

        std::string conn_str = params_.connection_string();

        do {
            log_->debug("Alias: {} Attempt: {}", alias_, attempts);
            conn_.reset(PQconnectdb(conn_str.c_str()));

            if (PQstatus(conn_.get()) == CONNECTION_OK) {
                log_->debug("Alias: {} Reconnect success", alias_);
                status_ = Status::Connected;
                return;
            }

            std::string err = PQerrorMessage(conn_.get());
            log_->debug("Alias: {} Reconnect attempt: {} failed: {}",
                        alias_, attempts, err);
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(params_.reconnect_delay_ms));
        } while (attempts < params_.max_reconnect_attempts);

        std::string error = "[Connector] Alias: " + alias_ + " connect failed " +
                            std::string(PQerrorMessage(conn_.get()));
        log_->error(error);
        throw std::runtime_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }

} // namespace pgc
