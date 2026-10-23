// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "driver.hpp"

#include "errors.hpp"

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/field_view.hpp>
#include <boost/mysql/pipeline.hpp>
#include <boost/mysql/statement.hpp>

#include <algorithm>
#include <exception>
#include <utility>
#include <vector>

namespace mysql {

    namespace {

        std::string to_std_string(bm::string_view text) { return std::string{text.data(), text.size()}; }

        // Runs `body` to completion on `io`, which nothing else runs: every handler of the
        // operations it awaits, cancel_after's deadline timers included, runs on this thread,
        // one after the other. Returns what the body let escape — nothing should, every
        // operation uses redirect_error.
        std::exception_ptr run_on(asio::io_context& io, asio::awaitable<void> body) {
            std::exception_ptr escaped;
            asio::co_spawn(io, std::move(body), [&escaped](std::exception_ptr e) { escaped = std::move(e); });
            io.run();
            io.restart();
            return escaped;
        }

        // What is left of `deadline`, never zero: an expired deadline still lets the
        // operation start, and it is cancelled at once.
        std::chrono::milliseconds remaining(std::chrono::steady_clock::time_point deadline) {
            const auto left =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
            return std::max(left, std::chrono::milliseconds{1});
        }

    } // namespace

    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const bm::results& rows, const conn::statement_t& statement) {
        OTX_ZONE_N("mysql::decode");
        conn::statement_result_t result;
        switch (statement.kind) {
            case conn::result_kind::affected_rows:
                result.affected_rows = rows.affected_rows();
                break;
            case conn::result_kind::schema:
                // A SELECT's column definitions precede its rows (metadata_mode::full); a bare
                // OK packet has none, and then `schema` stays empty.
                if (!rows.meta().empty()) {
                    auto schema = tsl::mysql_to_struct(resource, rows);
                    if (schema.has_error()) {
                        return schema.convert_error<conn::statement_result_t>();
                    }
                    result.schema.emplace(std::move(schema.value()));
                }
                break;
            case conn::result_kind::rows: {
                auto chunk = tsl::mysql_to_chunk(resource, rows);
                if (chunk.has_error()) {
                    return chunk.convert_error<conn::statement_result_t>();
                }
                result.rows.emplace(std::move(chunk.value()));
                break;
            }
        }
        return std::move(result);
    }

    driver_t::driver_t(std::pmr::memory_resource* resource, connect_params params, std::string alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        , params_(std::move(params))
        , alias_(std::move(alias))
        , conn_(io_.get_executor()) {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
        conn_.set_meta_mode(bm::metadata_mode::full);
    }

    driver_t::~driver_t() { close(); }

    core::error_t driver_t::connection_error(std::string_view what) const {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource_});
    }

    core::error_t driver_t::query_error(const boost::system::error_code& ec,
                                        const bm::diagnostics& diag,
                                        std::string_view sql) const {
        std::string what = "[Run query] Alias: " + alias_ + " query [" + std::string(sql) + "] failed: (" +
                           std::to_string(ec.value()) + ") " + ec.message();
        if (!diag.server_message().empty()) {
            what += ": " + to_std_string(diag.server_message());
        }
        log_->error(what);
        return core::error_t(classify_error(ec), std::pmr::string{what.c_str(), resource_});
    }

    driver_t::failure_t
    driver_t::classify(const boost::system::error_code& ec, const bm::diagnostics& diag, std::string_view sql) const {
        if (ec == asio::error::operation_aborted) {
            // Only our deadline cancels an operation here.
            const std::string what = "[Run query] Alias: " + alias_ + " query [" + std::string(sql) +
                                     "] failed: the statement did not finish within its deadline";
            log_->error(what);
            return {connection_error(what), failure_kind::broken};
        }
        // classify_error (errors.hpp) answers io_error exactly for a client or transport
        // error, and the engine's code for everything the server raised.
        const auto kind =
            classify_error(ec) == core::error_code_t::io_error ? failure_kind::stale : failure_kind::server;
        return {query_error(ec, diag, sql), kind};
    }

    void driver_t::reset_connection() {
        conn_ = bm::any_connection(io_.get_executor());
        conn_.set_meta_mode(bm::metadata_mode::full);
        connected_ = false;
        session_id_.reset();
    }

    asio::awaitable<void> driver_t::connect_(bm::diagnostics& diag, std::optional<core::error_t>& out) {
        boost::system::error_code ec;
        co_await conn_.async_connect(
            params_.server,
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            std::string what = "[Connector] Alias: " + alias_ + " connect failed: " + ec.message();
            if (!diag.server_message().empty()) {
                what += ": " + to_std_string(diag.server_message());
            }
            out.emplace(connection_error(what));
            co_return;
        }
        // The session's id, which KILL QUERY names when a statement passes its deadline
        // (cancel_on_server). Boost.MySQL reads it from the server greeting but does not
        // expose it. A server refusing the query leaves the connection usable, only without
        // the cancel; a transport failure fails the connect.
        bm::results id;
        co_await conn_.async_execute(
            "SELECT CONNECTION_ID()",
            id,
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            const std::string what = "[Connector] Alias: " + alias_ + " cannot read the session id: " + ec.message();
            if (ec == asio::error::operation_aborted || classify_error(ec) == core::error_code_t::io_error) {
                out.emplace(connection_error(what));
                co_return;
            }
            log_->warn(what);
        } else if (!id.rows().empty() && id.rows().at(0).size() == 1) {
            const auto field = id.rows().at(0).at(0);
            if (field.is_uint64()) {
                session_id_ = field.get_uint64();
            } else if (field.is_int64() && field.get_int64() > 0) {
                session_id_ = static_cast<uint64_t>(field.get_int64());
            }
        }
        out.emplace(core::error_t::no_error());
    }

    asio::awaitable<void> driver_t::kill_(uint64_t session_id, boost::system::error_code& ec, bm::diagnostics& diag) {
        bm::any_connection side(io_.get_executor());
        co_await side.async_connect(
            params_.server,
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            co_return;
        }
        const std::string kill = "KILL QUERY " + std::to_string(session_id);
        bm::results ignored;
        co_await side.async_execute(
            kill,
            ignored,
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
        boost::system::error_code closed;
        bm::diagnostics close_diag;
        co_await side.async_close(
            close_diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, closed)));
    }

    // A statement past its deadline keeps running on the server after the socket closes —
    // until the server next writes to it — so the driver kills it over a connection of its
    // own: the alias's parameters, connect_timeout for the connect and for the KILL.
    // Best-effort: a failure (1094, the statement finished meanwhile, included) is logged and
    // the statement's own deadline error stands.
    void driver_t::cancel_on_server(uint64_t session_id) {
        OTX_ZONE_N("mysql::driver::cancel_on_server");
        boost::system::error_code ec;
        bm::diagnostics diag;
        const bool escaped = static_cast<bool>(run_on(io_, kill_(session_id, ec, diag)));
        if (escaped || ec) {
            std::string what = escaped ? std::string{"unexpected exception"} : ec.message();
            if (!diag.server_message().empty()) {
                what += ": " + to_std_string(diag.server_message());
            }
            log_->warn("Alias: {} cannot kill the statement of session {}: {}", alias_, session_id, what);
            return;
        }
        log_->debug("Alias: {} killed the statement of session {} after its deadline", alias_, session_id);
    }

    core::error_t driver_t::connect() {
        OTX_ZONE_N("mysql::driver::connect");
        reset_connection();
        bm::diagnostics diag;
        std::optional<core::error_t> out;
        if (auto escaped = run_on(io_, connect_(diag, out)); escaped || !out) {
            reset_connection();
            return connection_error("[Connector] Alias: " + alias_ + " connect failed: unexpected exception");
        }
        if (out->contains_error()) {
            log_->warn("{}", out->what.c_str());
            reset_connection();
            return std::move(*out);
        }
        connected_ = true;
        return core::error_t::no_error();
    }

    asio::awaitable<void> driver_t::ping_(boost::system::error_code& ec, bm::diagnostics& diag) {
        co_await conn_.async_ping(
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
    }

    core::error_t driver_t::ping() {
        OTX_ZONE_N("mysql::driver::ping");
        if (!connected_) {
            return connection_error("[Connector] Alias: " + alias_ + " is not connected");
        }
        boost::system::error_code ec;
        bm::diagnostics diag;
        const bool escaped = static_cast<bool>(run_on(io_, ping_(ec, diag)));
        if (escaped || ec) {
            reset_connection(); // the next execute() reconnects first
            log_->debug("Alias: {} ping failed: {}", alias_, ec.message());
            return connection_error("[Connector] Alias: " + alias_ + " ping failed: " + ec.message());
        }
        return core::error_t::no_error();
    }

    asio::awaitable<void> driver_t::close_(boost::system::error_code& ec, bm::diagnostics& diag) {
        co_await conn_.async_close(
            diag,
            asio::cancel_after(params_.connect_timeout, asio::redirect_error(asio::use_awaitable, ec)));
    }

    void driver_t::close() noexcept {
        OTX_ZONE_N("mysql::driver::close");
        if (!connected_) {
            return;
        }
        // COM_QUIT, bounded: it fails (broken_pipe) when the server has already dropped the
        // connection, and the transport is closed either way.
        boost::system::error_code ec;
        bm::diagnostics diag;
        try {
            run_on(io_, close_(ec, diag));
            if (ec) {
                log_->warn("Alias: {} close failed: {}", alias_, ec.message());
            }
            reset_connection();
        } catch (const std::exception& e) {
            log_->warn("Alias: {} close failed: {}", alias_, e.what());
            connected_ = false;
        }
    }

    std::unique_ptr<conn::IDriver> driver_t::clone_disconnected() const {
        return std::make_unique<driver_t>(resource_, params_, alias_);
    }

    asio::awaitable<void> driver_t::send_(std::string_view sql, clock::time_point deadline, sent_t& sent) {
        bm::diagnostics diag;
        boost::system::error_code ec;
        bm::statement prepared = co_await conn_.async_prepare_statement(
            sql,
            diag,
            asio::cancel_after(remaining(deadline), asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            sent.failure = classify(ec, diag, sql);
            co_return;
        }
        bm::pipeline_request pipeline;
        pipeline.add_execute(prepared).add_close_statement(prepared);
        std::vector<bm::stage_response> stages;
        co_await conn_.async_run_pipeline(
            pipeline,
            stages,
            diag,
            asio::cancel_after(remaining(deadline), asio::redirect_error(asio::use_awaitable, ec)));
        if (ec) {
            const bool stage_failed = !stages.empty() && stages.front().error();
            sent.failure = classify(ec, stage_failed ? stages.front().diag() : diag, sql);
            co_return;
        }
        sent.rows = std::move(stages.front().as_results());
    }

    driver_t::sent_t driver_t::send(const conn::statement_t& statement) {
        OTX_ZONE_N("mysql::driver::send");
        sent_t sent;
        const auto limit = statement.timeout.count() > 0 ? statement.timeout : params_.statement_timeout;
        if (auto escaped = run_on(io_, send_(statement.sql, clock::now() + limit, sent)); escaped) {
            sent.failure = failure_t{connection_error("[Run query] Alias: " + alias_ + " query [" + statement.sql +
                                                      "] failed: unexpected exception"),
                                     failure_kind::broken};
        }
        if (sent.failure && sent.failure->kind != failure_kind::server) {
            const auto session_id = sent.failure->kind == failure_kind::broken ? session_id_ : std::nullopt;
            reset_connection();
            if (session_id) {
                cancel_on_server(*session_id);
            }
        }
        return sent;
    }

    conn::statement_outcome_t driver_t::execute(const conn::statement_t& statement) {
        OTX_ZONE_N("mysql::driver::execute"); // the coroutines never leave this thread
        // An earlier failure reset the connection: reconnect first. This replaces both pings
        // the old connector sent in front of every query.
        if (!connected_) {
            if (auto err = connect(); err.contains_error()) {
                return std::move(err);
            }
        }
        log_->debug("Alias: {} query: {}", alias_, statement.sql);
        auto sent = send(statement);
        // A stale connection (server restarted, wait_timeout closed it): reconnect and repeat
        // a read once. Not after the deadline, and never a DML or DDL (statement.repeatable).
        if (sent.failure && sent.failure->kind == failure_kind::stale && statement.repeatable &&
            !connect().contains_error()) {
            sent = send(statement);
        }
        if (sent.failure) {
            return std::move(sent.failure->error);
        }
        return decode(resource_, sent.rows, statement);
    }

    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string alias) {
        return std::make_unique<driver_t>(resource, std::move(params), std::move(alias));
    }

} // namespace mysql
