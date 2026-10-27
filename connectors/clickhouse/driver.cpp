// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "driver.hpp"

#include "errors.hpp"

#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <clickhouse/exceptions.h>

#include <cerrno>
#include <chrono>
#include <string>
#include <system_error>
#include <utility>
#include <variant>

namespace ch {

    namespace {

        // One 10 s deadline for connect, recv and send: clickhouse-cpp bounds only the TCP
        // connect, and every other socket read/write blocks forever on a silent backend.
        // A running query sends Progress every interactive_delay, so 10 s means a stall.
        constexpr std::chrono::seconds io_timeout{10};

        // send_retries is the number of back-to-back connect attempts the Client constructor
        // makes, with no delay between them: one, so a silent backend costs io_timeout per
        // connect() call; the startup retry ladder (service.connection_retry) is the caller's.
        constexpr unsigned int driver_connect_attempts{1};

        // No ping_before_query: Execute would ping first and on a socket failure enter the
        // driver's RetryGuard (sleep, reconnect, ping), which loops forever against a backend
        // that handshakes but never answers. Liveness is the pool's idle ping and execute()'s reconnect.
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

        bool is_timeout(const std::system_error& e) {
            return e.code().value() == EAGAIN || e.code().value() == EWOULDBLOCK ||
                   e.code() == std::make_error_condition(std::errc::timed_out);
        }

    } // namespace

    clickhouse::Query make_statement(std::string_view sql, select_result_t& out) {
        OTX_ZONE_N("ch::make_statement");
        clickhouse::Query statement{std::string(sql)}; // clickhouse-cpp owns the text as a std::string
        statement.OnData([&out](const clickhouse::Block& block) { out.blocks.push_back(block); })
            .OnProgress([&out](const clickhouse::Progress& progress) { out.written_rows += progress.written_rows; });
        return statement;
    }

    conn::statement_outcome_t
    decode(std::pmr::memory_resource* resource, const select_result_t& raw, const conn::statement_t& statement) {
        OTX_ZONE_N("ch::decode");
        // No allocation: an empty map holds no node and no bucket array.
        static const conn::named_types_t no_overrides{std::pmr::null_memory_resource()};
        const auto* hint = std::get_if<conn::named_types_ptr>(&statement.hints);
        const conn::named_types_t& overrides = (hint != nullptr && *hint) ? **hint : no_overrides;

        conn::statement_result_t result;
        switch (statement.kind) {
            case conn::result_kind::affected_rows:
                // No block carries a count: the only one ClickHouse reports is the
                // written-row progress. What it means per statement is the caller's call.
                result.affected_rows = raw.written_rows;
                break;
            case conn::result_kind::schema:
                // The header is the first block with columns; the column-less block that ends
                // the stream describes nothing. No such block leaves `schema` empty.
                for (const auto& block : raw.blocks) {
                    if (block.GetColumnCount() > 0) {
                        result.schema.emplace(tsl::ch_to_struct(resource, block, overrides));
                        break;
                    }
                }
                break;
            case conn::result_kind::rows: {
                auto chunk = tsl::ch_to_chunk(resource, raw.blocks, overrides);
                if (chunk.has_error()) {
                    return chunk.convert_error<conn::statement_result_t>();
                }
                result.rows.emplace(std::move(chunk.value()));
                break;
            }
        }
        return std::move(result);
    }

    driver_t::driver_t(std::pmr::memory_resource* resource, connect_params params, std::string_view alias)
        : log_(get_logger(logger_tag::CONNECTOR))
        , resource_(resource)
        , params_(std::move(params))
        , alias_(alias, resource) {
        assert(log_.is_valid());
        assert(resource_ != nullptr);
    }

    driver_t::~driver_t() { close(); }

    core::error_t driver_t::connect() {
        OTX_ZONE_N("ch::driver::connect");
        log_->debug("Alias: {} connecting to {}:{} database={}", alias_, params_.host, params_.port, params_.database);
        // The constructor connects and completes the handshake; the Ping is the first round
        // trip over the session. Both are bounded by io_timeout.
        try {
            client_ = std::make_unique<clickhouse::Client>(client_options(params_));
            client_->Ping();
            return core::error_t::no_error();
        } catch (const std::exception& e) {
            client_.reset();
            auto what = otterstax::concat(resource_, {"[Connector] Alias: ", alias_, " connect failed: ", e.what()});
            log_->warn(std::string_view(what));
            return core::error_t(core::error_code_t::io_error, std::move(what));
        }
    }

    core::error_t driver_t::ping() {
        OTX_ZONE_N("ch::driver::ping");
        if (!client_) {
            return core::error_t(core::error_code_t::io_error,
                                 otterstax::concat(resource_, {"[Connector] Alias: ", alias_, " is not connected"}));
        }
        try {
            client_->Ping();
            return core::error_t::no_error();
        } catch (const std::exception& e) {
            client_.reset(); // the next execute() reconnects first
            log_->debug("Alias: {} ping failed: {}", alias_, e.what());
            return core::error_t(core::error_code_t::io_error,
                                 otterstax::concat(resource_, {"[Connector] Alias: ", alias_, " ping failed: ", e.what()}));
        }
    }

    void driver_t::close() noexcept { client_.reset(); }

    std::unique_ptr<conn::IDriver> driver_t::clone_disconnected() const {
        return std::make_unique<driver_t>(resource_, params_, alias_);
    }

    std::optional<driver_t::send_failure_t> driver_t::send(std::string_view sql, select_result_t& out) {
        OTX_ZONE_N("ch::driver::send");
        // The message, logged, then moved into the error.
        const auto failed = [&](std::string_view reason, std::string_view code = {}) {
            auto what =
                otterstax::concat(resource_, {"[Run query] Alias: ", alias_, " query [", sql, "] failed: ", code, reason});
            log_->error(std::string_view(what));
            return what;
        };
        try {
            client_->Execute(make_statement(sql, out));
            return std::nullopt;
        } catch (const clickhouse::ServerException& e) {
            // The server's verdict: the connection is fine and keeps serving.
            std::pmr::string code(resource_);
            code += '(';
            otterstax::append_number(code, e.GetCode());
            code += ") ";
            return send_failure_t{core::error_t(classify_server_code(e.GetCode()), failed(e.what(), code)), false};
        } catch (const clickhouse::ValidationError& e) {
            // A stream the client could not read: not a dead socket, but the connection is out
            // of step with the server, so it is dropped and nothing is repeated.
            client_.reset();
            return send_failure_t{core::error_t(core::error_code_t::io_error, failed(e.what())), false};
        } catch (const clickhouse::UnimplementedError& e) {
            client_.reset();
            return send_failure_t{core::error_t(core::error_code_t::io_error, failed(e.what())), false};
        } catch (const std::system_error& e) {
            // EAGAIN out of recv() is io_timeout expiring on a stalled backend: no repeat.
            // Anything else at the socket level is a dead connection: a read may be repeated.
            client_.reset();
            return send_failure_t{core::error_t(core::error_code_t::io_error, failed(e.what())), !is_timeout(e)};
        } catch (const std::exception& e) {
            client_.reset();
            return send_failure_t{core::error_t(core::error_code_t::io_error, failed(e.what())), true};
        }
    }

    conn::statement_outcome_t driver_t::execute(const conn::statement_t& statement) {
        OTX_ZONE_N("ch::driver::execute");
        // An earlier failure dropped the client: reconnect first. This replaces the Ping the
        // old connector manager sent in front of every query.
        if (!client_) {
            if (auto err = connect(); err.contains_error()) {
                return std::move(err);
            }
        }
        log_->debug("Alias: {} query: {}", alias_, statement.sql);
        select_result_t raw(resource_);
        auto failure = send(statement.sql, raw);
        // A stale socket (server restarted, idle connection closed by the server or a
        // balancer): reconnect and repeat a read once. Not after a timeout, and never a DML or
        // DDL (statement.repeatable).
        if (failure && failure->repeatable && statement.repeatable && !connect().contains_error()) {
            raw = select_result_t(resource_);
            failure = send(statement.sql, raw);
        }
        if (failure) {
            return std::move(failure->error);
        }
        return decode(resource_, raw, statement);
    }

    std::unique_ptr<conn::IDriver>
    make_driver(std::pmr::memory_resource* resource, connect_params params, std::string_view alias) {
        return std::make_unique<driver_t>(resource, std::move(params), alias);
    }

} // namespace ch
