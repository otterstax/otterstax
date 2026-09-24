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

namespace bm = boost::mysql;
namespace asio = boost::asio;

namespace mysql {
    namespace {
        std::string to_std_string(bm::string_view text) { return std::string{text.data(), text.size()}; }
    } // namespace

    Connector::Connector(std::pmr::memory_resource* resource,
                         asio::io_context& io_ctx,
                         bm::connect_params params,
                         std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        // A strand over the pool, not the bare io_context: it serializes the
        // two completion handlers of the deadline-bounded connect (see
        // connectWithTimeout). Queries are unaffected — their coroutines are
        // spawned on the io_context and carry its executor.
        , conn_(asio::make_strand(io_ctx))
        , params_{std::move(params)}
        , status_{Status::Created}
        , alias_{std::move(alias)} {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
    }

    bm::connect_params Connector::params() const noexcept { return params_; }

    Status Connector::status() const noexcept { return status_; }

    void Connector::close() {
        OTX_ZONE_N("mysql::Connector::close");
        log_->debug("Alias: {} close connection", alias_);
        if (status_ != Status::Connected) {
            return;
        }
        // The non-throwing overload: close() also runs from ~Connector(), and
        // the COM_QUIT it writes fails (broken_pipe) when the server has
        // already dropped the connection. The transport is closed either way,
        // so the connector is Closed whatever the quit's verdict.
        boost::system::error_code ec;
        bm::diagnostics diag;
        conn_.close(ec, diag);
        if (ec) {
            log_->warn("Alias: {} close failed: {}", alias_, ec.message());
        }
        status_ = Status::Closed;
    }

    Connector::~Connector() { close(); }

    asio::awaitable<core::error_t> Connector::asyncConnect_(bm::diagnostics& diag) {
        constexpr std::chrono::seconds connect_timeout{10};
        boost::system::error_code ec;
        co_await conn_.async_connect(
            params_,
            diag,
            asio::cancel_after(connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            co_return connection_error(ec.message());
        }
        co_return core::error_t::no_error();
    }

    core::error_t Connector::connectWithTimeout(bm::diagnostics& diag) {
        OTX_ZONE_N("mysql::Connector::connectWithTimeout");
        return otterstax::spawn_marshaled<otterstax::asio_error_t>(conn_.get_executor(), asyncConnect_(diag), resource_)
            .get();
    }

    core::error_t Connector::query_error(const boost::system::error_code& ec,
                                         const bm::diagnostics& diag,
                                         std::string_view query) const {
        std::string what = "[Run query] Alias: " + alias_ + " query [" + std::string(query) + "] failed: (" +
                           std::to_string(ec.value()) + ") " + ec.message();
        if (!diag.server_message().empty()) {
            what += ": " + to_std_string(diag.server_message());
        }
        log_->error(what);
        return core::error_t(classify_error(ec), std::pmr::string{what.c_str(), resource_});
    }

    core::error_t Connector::connection_error(std::string_view what) const {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource_});
    }

    core::error_t Connector::connect() {
        OTX_ZONE_N("mysql::Connector::connect");
        conn_.set_meta_mode(bm::metadata_mode::full);
        boost::mysql::diagnostics diag;
        if (auto err = connectWithTimeout(diag); err.contains_error()) {
            log_->debug("Alias: {} connect failed: {}", alias_, err.what);
            return tryReconnect();
        }
        status_ = Status::Connected;
        return core::error_t::no_error();
    }

    bool Connector::isConnected() {
        OTX_ZONE_N("mysql::Connector::isConnected");
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

    core::error_t Connector::tryReconnect() {
        OTX_ZONE_N("mysql::Connector::tryReconnect");
        if (status_ == Status::Connected) {
            return core::error_t::no_error();
        }
        status_ = Status::Disconnected;
        size_t attempts = 0;
        log_->debug("Alias: {} Try to reconnect", alias_);

        std::string last_failure;
        boost::mysql::diagnostics diag;
        do {
            log_->debug("Alias: {} Attempt: {}", alias_, attempts);
            core::error_t err = connectWithTimeout(diag);
            if (!err.contains_error()) {
                log_->debug("Alias: {} Reconnect success", alias_);
                status_ = Status::Connected;
                return core::error_t::no_error();
            }
            last_failure.assign(err.what.data(), err.what.size());
            log_->debug("Alias: {} Reconnect attempt: {} failed: {} - {}",
                        alias_,
                        attempts,
                        last_failure,
                        diag.server_message());
            ++attempts;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        } while (attempts < 3);
        std::string error = "[Connector] Alias: " + alias_ + " connect failed: " + last_failure;
        if (!diag.server_message().empty()) {
            error += ": " + to_std_string(diag.server_message());
        }
        log_->error(error);
        return connection_error(error);
    }

    bool Connector::isClosed() const noexcept { return status_ == Status::Closed; }
    std::string Connector::alias() const noexcept { return alias_; }
} // namespace mysql
