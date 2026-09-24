// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connector.hpp"

#include "errors.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include <clickhouse/exceptions.h>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace ch {

    namespace {

        // The driver bounds only the TCP connect on its own (5 s); every read
        // and write on the socket is a plain blocking recv()/send() with
        // SO_RCVTIMEO / SO_SNDTIMEO left at 0 unless the options set them — the
        // ServerHello the Client constructor waits for in its handshake, the
        // Pong Ping() waits for, every packet of a query. A backend that
        // accepted the connection and never answered therefore froze the
        // calling thread for good; at server start that is the startup thread
        // inside ComponentManager::register_connections, and the wire-protocol
        // ports never came up. One bound for connect, recv and send: the 10 s
        // the MySQL connector gives a connect attempt. A running query does not
        // idle that long — the server sends a Progress packet every
        // interactive_delay (100 ms by default) while it works — so the recv
        // bound is on a stalled backend, not on a slow query. The driver
        // reports the expiry as std::system_error (EAGAIN out of recv), the
        // same path as a refused connect, and connect()/tryReconnect() turn it
        // into their io_error value.
        constexpr std::chrono::seconds io_timeout{10};

        // send_retries is the number of back-to-back connect attempts the
        // Client constructor makes, with no delay between them: one, so that
        // connect()/tryReconnect() own the retry ladder and a silent backend
        // costs io_timeout per attempt of that ladder, not a multiple of it.
        constexpr unsigned int driver_connect_attempts{1};

        // The one set of options both connect() and tryReconnect() open the
        // driver with. No ping_before_query: with it, Execute pings first and
        // on a socket failure enters the driver's RetryGuard — sleep
        // retry_timeout, reconnect, ping again — which it leaves only when the
        // ping succeeds or the reconnect fails on exactly its send_retries-th
        // turn, so a backend that completed every handshake but never answered
        // a Ping kept the io thread reconnecting for good. The liveness check
        // is the manager's: ConnectorManager::executeQuery runs isConnected()
        // (one direct Ping, bounded by io_timeout) and tryReconnect() in front
        // of every query, so the driver's own ping was a second copy of it.
        // Without it RetryGuard — the only user of retry_timeout, which is
        // therefore left alone — is never entered, and a query is SendQuery
        // plus reads, each bounded.
        clickhouse::ClientOptions client_options(const connect_params& params) {
            clickhouse::ClientOptions opts;
            opts.SetHost(params.host)
                .SetPort(params.port)
                .SetUser(params.username)
                .SetPassword(params.password)
                .SetDefaultDatabase(params.database)
                .SetPingBeforeQuery(false)
                .SetSendRetries(driver_connect_attempts)
                .SetConnectionConnectTimeout(io_timeout)
                .SetConnectionRecvTimeout(io_timeout)
                .SetConnectionSendTimeout(io_timeout);
            return opts;
        }

    } // namespace

    Connector::Connector(std::pmr::memory_resource* resource, connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        , client_(nullptr)
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
    }

    connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        OTX_ZONE_N("ch::Connector::close");
        log_->debug("Alias: {} close connection", alias_);
        if (status_ != Status::Connected) {
            return;
        }
        client_.reset();
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    core::error_t Connector::connection_error(std::string_view what) const {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource_});
    }

    clickhouse::Query make_statement(std::string_view sql, select_result_t& out) {
        OTX_ZONE_N("ch::make_statement");
        clickhouse::Query statement{std::string(sql)};
        statement.OnData([&out](const clickhouse::Block& block) { out.blocks.push_back(block); })
            .OnProgress([&out](const clickhouse::Progress& progress) { out.written_rows += progress.written_rows; });
        return statement;
    }

    std::optional<core::error_t> Connector::execute_statement(std::string_view query, select_result_t& out) {
        OTX_ZONE_N("ch::Connector::execute_statement");
        std::optional<core::error_t> failure;
        try {
            client_->Execute(make_statement(query, out));
        } catch (const clickhouse::ServerException& e) {
            std::string what = "[Run query] Alias: " + alias_ + " query [" + std::string(query) + "] failed: (" +
                               std::to_string(e.GetCode()) + ") " + e.what();
            log_->error(what);
            failure.emplace(classify_server_code(e.GetCode()), std::pmr::string{what.c_str(), resource_});
        } catch (const std::exception& e) {
            std::string what =
                "[Run query] Alias: " + alias_ + " query [" + std::string(query) + "] failed: " + e.what();
            log_->error(what);
            failure.emplace(core::error_code_t::io_error, std::pmr::string{what.c_str(), resource_});
        }
        return failure;
    }

    core::error_t Connector::connect() {
        OTX_ZONE_N("ch::Connector::connect");
        log_->debug("Alias: {} connecting with: host={} port={} database={}",
                    alias_, params_.host, params_.port, params_.database);

        // The constructor connects and completes the handshake; the Ping is the
        // first round trip over the session. Both are bounded by io_timeout.
        try {
            client_ = std::make_unique<clickhouse::Client>(client_options(params_));
            client_->Ping();
            status_ = Status::Connected;
            log_->debug("Alias: {} connected successfully", alias_);
        } catch (const std::exception& e) {
            log_->debug("Alias: {} connect failed: {}", alias_, e.what());
        }
        if (status_ == Status::Connected) {
            return core::error_t::no_error();
        }
        return tryReconnect();
    }

    bool Connector::isConnected() {
        OTX_ZONE_N("ch::Connector::isConnected");
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

    core::error_t Connector::tryReconnect() {
        OTX_ZONE_N("ch::Connector::tryReconnect");
        if (status_ == Status::Connected) {
            return core::error_t::no_error();
        }
        status_ = Status::Disconnected;
        size_t attempts = 0;
        log_->debug("Alias: {} Try to reconnect (max_attempts={}, delay={}ms)",
                    alias_, params_.max_reconnect_attempts, params_.reconnect_delay_ms);

        std::string last_failure;
        do {
            log_->debug("Alias: {} Attempt: {}", alias_, attempts);
            try {
                client_ = std::make_unique<clickhouse::Client>(client_options(params_));
                client_->Ping();
                log_->debug("Alias: {} Reconnect success", alias_);
                status_ = Status::Connected;
            } catch (const std::exception& e) {
                last_failure = e.what();
                log_->debug("Alias: {} Reconnect attempt: {} failed: {}",
                            alias_, attempts, e.what());
            }
            if (status_ == Status::Connected) {
                return core::error_t::no_error();
            }
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(params_.reconnect_delay_ms));
        } while (attempts < params_.max_reconnect_attempts);

        std::string error = "[Connector] Alias: " + alias_ + " connect failed: " + last_failure;
        log_->error(error);
        return connection_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }

} // namespace ch
