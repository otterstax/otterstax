// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"

#include "errors.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace pg {

    Connector::Connector(std::pmr::memory_resource* resource, connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        , conn_(nullptr)
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
    }

    connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        OTX_ZONE_N("pg::Connector::close");
        log_->debug("Alias: {} close connection", alias_);
        if (status_ != Status::Connected) {
            return;
        }
        conn_.reset();
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    core::error_t Connector::query_error(const PGresult* result, std::string_view query) const {
        const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        const char* message = PQresultErrorMessage(result);
        const std::string_view state = sqlstate ? sqlstate : "";
        std::string what = "[Run query] Alias: " + alias_ + " query [" + std::string(query) + "] failed";
        if (!state.empty()) {
            what += " (SQLSTATE " + std::string(state) + ")";
        }
        what += ": ";
        what += message ? message : "no server message";
        log_->error(what);
        return core::error_t(classify_sqlstate(state), std::pmr::string{what.c_str(), resource_});
    }

    core::error_t Connector::connection_error(std::string_view what) const {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource_});
    }

    core::error_t Connector::connect() {
        OTX_ZONE_N("pg::Connector::connect");
        std::string conn_str = params_.connection_string();
        log_->debug("Alias: {} connecting with: host={} port={} dbname={}",
                    alias_, params_.host, params_.port, params_.database);

        conn_.reset(PQconnectdb(conn_str.c_str()));

        if (PQstatus(conn_.get()) != CONNECTION_OK) {
            std::string err = PQerrorMessage(conn_.get());
            log_->debug("Alias: {} connect failed: {}", alias_, err);
            return tryReconnect();
        }
        status_ = Status::Connected;
        log_->debug("Alias: {} connected successfully", alias_);
        return core::error_t::no_error();
    }

    bool Connector::isConnected() {
        OTX_ZONE_N("pg::Connector::isConnected");
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

    core::error_t Connector::tryReconnect() {
        OTX_ZONE_N("pg::Connector::tryReconnect");
        if (status_ == Status::Connected) {
            return core::error_t::no_error();
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
                return core::error_t::no_error();
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
        return connection_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }

} // namespace pg
