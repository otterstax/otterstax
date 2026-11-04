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
        // A strand over the io_context, not the bare io_context: it serializes the
        // two completion handlers of the deadline-bounded connect (see
        // connectWithTimeout), and a statement runs on it too (runQuery_), so
        // cancel() can emit its signal there.
        , strand_(asio::make_strand(io_ctx))
        , conn_(strand_)
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

    core::error_t Connector::cancelled_error() const {
        return connection_error("[Run query] Connector with alias: " + alias_ + " was cancelled");
    }

    void Connector::cancel() noexcept {
        OTX_ZONE_N("mysql::Connector::cancel");
        cancel_->requested.store(true, std::memory_order_release);
        try {
            // The signal belongs to the strand: the emission is posted there, after the store above,
            // so a statement either sees `requested` before its next operation or is in one whose
            // slot the emission reaches.
            asio::post(strand_,
                       [state = cancel_] { state->signal.emit(asio::cancellation_type::terminal); });
        } catch (...) {
            // Posting allocates; without the emission the statement still stops at its next check.
        }
    }

    asio::awaitable<core::error_t> Connector::asyncConnect_(bm::diagnostics& diag) {
        constexpr std::chrono::seconds connect_timeout{10};
        boost::system::error_code ec;
        if (cancel_->requested.load(std::memory_order_acquire)) {
            co_return cancelled_error();
        }
        // The deadline, and cancel() too: the connect binds the cancel signal's slot like a
        // statement (cancel_after forwards the slot of the handler it wraps), so a reconnect of a
        // server that never answers ends at the shutdown, not at its deadline.
        co_await conn_.async_connect(
            params_,
            diag,
            asio::cancel_after(connect_timeout,
                               asio::bind_cancellation_slot(cancel_->signal.slot(),
                                                            asio::redirect_error(asio::use_awaitable, ec))));
        if (cancel_->requested.load(std::memory_order_acquire)) {
            co_return cancelled_error();
        }
        if (ec) {
            co_return connection_error(ec.message());
        }
        co_return core::error_t::no_error();
    }

    core::error_t Connector::connectWithTimeout(bm::diagnostics& diag) {
        OTX_ZONE_N("mysql::Connector::connectWithTimeout");
        // strand_, never conn_.get_executor(): that executor is the stream's, and the connect replaces
        // the stream (variant_stream's connect assigns a fresh socket) on whichever thread runs the
        // spawned coroutine — on an io_context more than one thread runs, another thread, while this
        // one would still be reading it. strand_ is never written after the constructor.
        auto& io = strand_.get_inner_executor().context();
        auto outcome = otterstax::spawn_marshaled<otterstax::asio_error_t>(strand_, asyncConnect_(diag), resource_);
        // The calling thread runs the connection's io_context until the connect completed: the
        // backend agent that owns the connector (and its io_context) runs it on its own thread for
        // every statement, and nobody else does. An io_context other threads run as well (a test's
        // pool) is driven by them too; run_one_for only bounds this thread's wait for its next handler.
        while (outcome.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
            if (io.stopped()) {
                io.restart();
            }
            io.run_one_for(std::chrono::milliseconds(10));
        }
        return outcome.get();
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
            if (cancel_->requested.load(std::memory_order_acquire)) {
                return cancelled_error();
            }
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
            if (cancel_->requested.load(std::memory_order_acquire)) {
                return cancelled_error();
            }
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
    std::unique_ptr<IConnector>
    make_mysql_connector(std::pmr::memory_resource* resource, asio::io_context& io_ctx, bm::connect_params params, std::string alias) {
        return std::make_unique<Connector>(resource, io_ctx, std::move(params), std::move(alias));
    }
} // namespace mysql
