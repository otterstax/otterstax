// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backend_agent.hpp"

#include "catalog/discovery.hpp"
#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <clickhouse/columns/string.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <exception>
#include <type_traits>
#include <utility>

namespace otterstax::federation {
    using components::vector::data_chunk_t;

    namespace {
        bool ends_transaction(transaction_step_t step) noexcept {
            return step == transaction_step_t::commit || step == transaction_step_t::rollback;
        }

        std::string kind_name(backend_type_t kind) {
            switch (kind) {
                case backend_type_t::MySQL:
                    return "MySQL";
                case backend_type_t::PostgreSQL:
                    return "PostgreSQL";
                case backend_type_t::ClickHouse:
                    return "ClickHouse";
            }
            return "unknown";
        }
    } // namespace

    std::optional<std::uint64_t> mysql_update_rows_matched(std::string_view info) {
        OTX_ZONE_N("mysql_update_rows_matched");
        // The numbers of the message, in order. ER_UPDATE_INFO is a printf format with three %ld and
        // no positional arguments, so every translation keeps them in that order; no translation
        // spells a digit in its words.
        std::uint64_t numbers[3] = {0, 0, 0};
        std::size_t count = 0;
        const char* at = info.data();
        const char* last = info.data() + info.size();
        while (at != last) {
            if (*at < '0' || *at > '9') {
                ++at;
                continue;
            }
            std::uint64_t value = 0;
            auto [end, ec] = std::from_chars(at, last, value);
            if (ec != std::errc{} || count == 3) {
                return std::nullopt;
            }
            numbers[count++] = value;
            at = end;
        }
        if (count != 3) {
            return std::nullopt;
        }
        return numbers[0];
    }

    std::optional<std::string>
    transaction_statement(backend_type_t backend, transaction_step_t step, std::string_view savepoint) {
        if (step == transaction_step_t::unpin) {
            return std::string{};
        }
        if (backend == backend_type_t::ClickHouse) {
            return std::nullopt;
        }
        const bool names_savepoint = step == transaction_step_t::savepoint ||
                                     step == transaction_step_t::rollback_to_savepoint ||
                                     step == transaction_step_t::release_savepoint;
        if (names_savepoint && savepoint.empty()) {
            return std::nullopt;
        }
        const char quote = backend == backend_type_t::PostgreSQL ? '"' : '`';
        std::string name(1, quote);
        for (char c : savepoint) {
            if (c == quote) {
                name += quote;
            }
            name += c;
        }
        name += quote;
        switch (step) {
            case transaction_step_t::begin:
                return std::string{backend == backend_type_t::PostgreSQL ? "BEGIN" : "START TRANSACTION"};
            case transaction_step_t::commit:
                return std::string{"COMMIT"};
            case transaction_step_t::rollback:
                return std::string{"ROLLBACK"};
            case transaction_step_t::savepoint:
                return "SAVEPOINT " + name;
            case transaction_step_t::rollback_to_savepoint:
                return "ROLLBACK TO SAVEPOINT " + name;
            case transaction_step_t::release_savepoint:
                return "RELEASE SAVEPOINT " + name;
            case transaction_step_t::unpin:
                break;
        }
        return std::nullopt;
    }

    bool pg_commit_rolled_back(std::string_view command_status) noexcept { return command_status == "ROLLBACK"; }

    std::vector<std::string> ch_failed_mutations(std::string_view message) {
        OTX_ZONE_N("ch_failed_mutations");
        constexpr std::string_view lead = "Exception happened during execution of mutation";
        constexpr std::string_view tail = "' with part '";
        std::vector<std::string> ids;
        auto at = message.find(lead);
        if (at == std::string_view::npos) {
            return ids;
        }
        at += lead.size();
        if (at < message.size() && message[at] == 's') {
            ++at;
        }
        if (message.substr(at, 2) != " '") {
            return ids;
        }
        at += 2;
        const auto end = message.find(tail, at);
        if (end == std::string_view::npos) {
            return ids;
        }
        // The ids joined by ", " (a MergeTree's mutation_<n>.txt, a replicated table's 0000000005).
        auto list = message.substr(at, end - at);
        while (!list.empty()) {
            const auto comma = list.find(", ");
            ids.emplace_back(list.substr(0, comma));
            if (comma == std::string_view::npos) {
                break;
            }
            list.remove_prefix(comma + 2);
        }
        if (std::any_of(ids.begin(), ids.end(), [](const std::string& id) { return id.empty(); })) {
            ids.clear();
        }
        return ids;
    }

    backend_agent_t::backend_agent_t(std::pmr::memory_resource* resource, std::string connection)
        : actor_zeta::basic_actor<backend_agent_t>(resource)
        , resource_(resource)
        , connection_(std::move(connection))
        , log_(get_logger(logger_tag::BACKENDS)) {}

    backend_agent_t::~backend_agent_t() = default;

    void backend_agent_t::attach(std::unique_ptr<mysql::IConnector> connector) {
        kind_ = backend_type_t::MySQL;
        connector_ = std::move(connector);
    }

    void backend_agent_t::attach(std::unique_ptr<pg::IConnector> connector) {
        kind_ = backend_type_t::PostgreSQL;
        connector_ = std::move(connector);
    }

    void backend_agent_t::attach(std::unique_ptr<ch::IConnector> connector) {
        kind_ = backend_type_t::ClickHouse;
        connector_ = std::move(connector);
    }

    void backend_agent_t::set_owner(void (*wake)(void*) noexcept, void* owner) noexcept {
        wake_ = wake;
        owner_ = owner;
    }

    core::error_t backend_agent_t::connect() {
        OTX_ZONE_N("backend_agent::connect");
        auto failed = std::visit(
            [this](auto& connector) -> core::error_t {
                if constexpr (std::is_same_v<std::decay_t<decltype(connector)>, std::monostate>) {
                    return error(core::error_code_t::internal_error, "no connector attached");
                } else {
                    return connector->connect();
                }
            },
            connector_);
        if (failed.contains_error()) {
            return failed;
        }
        return check_session();
    }

    void backend_agent_t::cancel() noexcept {
        OTX_ZONE_N("backend_agent::cancel");
        cancelled_.store(true, std::memory_order_release);
        std::visit(
            [](auto& connector) {
                if constexpr (!std::is_same_v<std::decay_t<decltype(connector)>, std::monostate>) {
                    connector->cancel();
                }
            },
            connector_);
    }

    core::error_t backend_agent_t::error(core::error_code_t code, const std::string& what) const {
        return tsl::make_error(resource_, code, kind_name(kind_) + " connection '" + connection_ + "': " + what);
    }

    core::error_t backend_agent_t::ready() {
        if (cancelled_.load(std::memory_order_acquire)) {
            return error(core::error_code_t::io_error, "the server is shutting down");
        }
        return std::visit(
            [this](auto& connector) -> core::error_t {
                if constexpr (std::is_same_v<std::decay_t<decltype(connector)>, std::monostate>) {
                    return error(core::error_code_t::internal_error, "no connector attached");
                } else {
                    if (connector->isClosed() && !reopen_) {
                        return error(core::error_code_t::io_error, "the connection is closed");
                    }
                    if (reopen_ || !connector->isConnected()) {
                        return reconnect(*connector);
                    }
                    return core::error_t::no_error();
                }
            },
            connector_);
    }

    template<typename Connector>
    core::error_t backend_agent_t::reconnect(Connector& connector) {
        OTX_ZONE_N("backend_agent::reconnect");
        const auto now = [] {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        };
        if (reachability_ != nullptr) {
            if (now() < reachability_->unreachable_until_ms.load(std::memory_order_acquire)) {
                return error(core::error_code_t::io_error,
                             "connection \"" + connection_ + "\" is unreachable: a connect failed less than " +
                                 std::to_string(reachability_->interval_ms.load(std::memory_order_relaxed) / 1000) +
                                 " s ago, the next one is tried after that");
            }
        }
        auto failed = connector.tryReconnect();
        if (reachability_ != nullptr) {
            const auto until =
                failed.contains_error() ? now() + reachability_->interval_ms.load(std::memory_order_relaxed) : 0;
            reachability_->unreachable_until_ms.store(until, std::memory_order_release);
        }
        if (failed.contains_error()) {
            return error(core::error_code_t::io_error,
                         "connection \"" + connection_ +
                             "\" is unreachable: " + std::string{failed.what.data(), failed.what.size()});
        }
        return check_session();
    }

    template<typename R, typename Arg, typename Connector, typename Handler>
    auto backend_agent_t::run(Connector& connector, std::string_view sql, Handler handler) {
        using outcome_t = otterstax::query_result_t<R>;
        try {
            auto outcome = otterstax::spawn_marshaled<R>(
                io_,
                otterstax::run_with_owned_handler<R, Arg>(connector, sql, std::move(handler)),
                resource_);
            // The connector's io_context is the agent's alone: it runs here until the statement (and
            // every handler it derived, a deadline included) completed.
            io_.restart();
            io_.run();
            return outcome.get();
        } catch (const std::exception& e) {
            return outcome_t{error(core::error_code_t::io_error, std::string{"the statement failed: "} + e.what())};
        }
    }

    core::error_t backend_agent_t::execute(std::string_view sql) {
        if (log_.is_valid()) {
            log_->debug("{}: \"{}\"", connection_, sql);
        }
        return std::visit(
            [this, sql](auto& connector) -> core::error_t {
                using T = std::decay_t<decltype(connector)>;
                auto ok = [](auto&&) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
                if constexpr (std::is_same_v<T, std::unique_ptr<mysql::IConnector>>) {
                    return run<otterstax::asio_error_t, const boost::mysql::results&>(*connector, sql, ok);
                } else if constexpr (std::is_same_v<T, std::unique_ptr<pg::IConnector>>) {
                    return run<otterstax::asio_error_t, PGresult*>(*connector, sql, ok);
                } else if constexpr (std::is_same_v<T, std::unique_ptr<ch::IConnector>>) {
                    return run<otterstax::asio_error_t, const ch::select_result_t&>(*connector, sql, ok);
                } else {
                    return error(core::error_code_t::internal_error, "no connector attached");
                }
            },
            connector_);
    }

    core::error_t backend_agent_t::check_session() {
        OTX_ZONE_N("backend_agent::check_session");
        reopen_ = false;
        session_refused_ = false;
        auto* my = std::get_if<std::unique_ptr<mysql::IConnector>>(&connector_);
        if (my == nullptr) {
            return core::error_t::no_error();
        }
        // The generated literals double a backslash, which a session under NO_BACKSLASH_ESCAPES
        // would read as two characters: such a session is refused rather than written to with the
        // wrong text. The mode is the session's (the server's global sql_mode, or a role's init
        // command), so every session is asked.
        // Whether the mode is strict decides which UPDATE may leave its range check to the store into
        // the column (sql_gen::statement_needs_t, write).
        bool no_backslash_escapes = false;
        strict_session_ = false;
        auto mode_handler = [this,
                             &no_backslash_escapes](const boost::mysql::results& result) -> otterstax::asio_error_t {
            for (auto row : result.rows()) {
                if (row.size() != 0 && row.at(0).is_string()) {
                    const auto mode = row.at(0).as_string();
                    no_backslash_escapes = mode.find("NO_BACKSLASH_ESCAPES") != std::string_view::npos;
                    strict_session_ = mode.find("STRICT_TRANS_TABLES") != std::string_view::npos ||
                                      mode.find("STRICT_ALL_TABLES") != std::string_view::npos;
                }
            }
            return otterstax::asio_error_t{};
        };
        auto failed =
            run<otterstax::asio_error_t, const boost::mysql::results&>(**my, "SELECT @@SESSION.sql_mode", mode_handler);
        if (!failed.contains_error() && !no_backslash_escapes) {
            return core::error_t::no_error();
        }
        // A session that is not known to be writable is not kept.
        (*my)->close();
        reopen_ = true;
        if (failed.contains_error()) {
            return error(failed.type,
                         "the session's sql_mode could not be read: " +
                             std::string{failed.what.data(), failed.what.size()});
        }
        session_refused_ = true;
        auto refused = error(core::error_code_t::unimplemented_yet,
                             "the server runs with sql_mode NO_BACKSLASH_ESCAPES: OtterStax escapes a backslash in "
                             "the string literals it sends");
        if (log_.is_valid()) {
            log_->error("{}", refused.what.c_str());
        }
        return refused;
    }

    core::error_t backend_agent_t::commit() {
        if (log_.is_valid()) {
            log_->debug("{}: \"COMMIT\"", connection_);
        }
        if (auto* pg = std::get_if<std::unique_ptr<pg::IConnector>>(&connector_)) {
            auto handler = [this](PGresult* result) -> otterstax::asio_error_t {
                const char* status = result == nullptr ? nullptr : PQcmdStatus(result);
                if (status != nullptr && pg_commit_rolled_back(status)) {
                    return error(core::error_code_t::transaction_finalized,
                                 "COMMIT was answered ROLLBACK: the server had aborted the transaction, nothing of it "
                                 "was kept");
                }
                return otterstax::asio_error_t{};
            };
            return run<otterstax::asio_error_t, PGresult*>(**pg, "COMMIT", handler);
        }
        return execute("COMMIT");
    }

    void backend_agent_t::end_transaction() noexcept {
        in_transaction_ = false;
        savepoints_.clear();
        broken_.reset();
    }

    void backend_agent_t::note_failure(const core::error_t& failed) {
        if (!in_transaction_ || broken_ || !failed.contains_error()) {
            return;
        }
        const std::string what{failed.what.data(), failed.what.size()};
        if (kind_ == backend_type_t::PostgreSQL) {
            broken_ = broken_t{failed.type, what, savepoints_.size(), false};
            return;
        }
        if (kind_ != backend_type_t::MySQL) {
            return;
        }
        if (failed.type == core::error_code_t::write_conflict) {
            broken_ = broken_t{failed.type, what, 0, true};
            savepoints_.clear();
            // A lock wait timeout without innodb_rollback_on_timeout left the transaction open: it
            // ends now too, so what follows can never run outside it.
            if (execute("ROLLBACK").contains_error()) {
                reset_session();
            }
        } else if (failed.type == core::error_code_t::io_error) {
            broken_ = broken_t{failed.type, what, 0, true};
            savepoints_.clear();
        }
    }

    core::error_t backend_agent_t::broken_error(std::string_view what) const {
        return error(core::error_code_t::transaction_finalized,
                     std::string{what} + " refused: a statement of the backend transaction failed (" + broken_->what +
                         "); only its ROLLBACK" +
                         (broken_->whole ? std::string{} : std::string{" or a ROLLBACK TO a savepoint set before it"}) +
                         " runs");
    }

    void backend_agent_t::reset_session() {
        OTX_ZONE_N("backend_agent::reset_session");
        if (log_.is_valid()) {
            log_->warn("{}: a transaction ended in an unknown state: the session is replaced", connection_);
        }
        std::visit(
            [this](auto& connector) {
                if constexpr (!std::is_same_v<std::decay_t<decltype(connector)>, std::monostate>) {
                    connector->close();
                    if (cancelled_.load(std::memory_order_acquire)) {
                        return;
                    }
                    // A failed reconnect leaves the connection disconnected: the next statement's
                    // check reconnects it.
                    if (auto failed = reconnect(*connector); failed.contains_error() && log_.is_valid()) {
                        log_->error("{}: reconnect failed: {}", connection_, failed.what.c_str());
                    }
                }
            },
            connector_);
    }

    // ---- messages ----

    actor_zeta::behavior_t backend_agent_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<backend_agent_t, &backend_agent_t::read>:
                co_await actor_zeta::dispatch(this, &backend_agent_t::read, msg);
                break;
            case actor_zeta::msg_id<backend_agent_t, &backend_agent_t::write>:
                co_await actor_zeta::dispatch(this, &backend_agent_t::write, msg);
                break;
            case actor_zeta::msg_id<backend_agent_t, &backend_agent_t::step>:
                co_await actor_zeta::dispatch(this, &backend_agent_t::step, msg);
                break;
            case actor_zeta::msg_id<backend_agent_t, &backend_agent_t::discover>:
                co_await actor_zeta::dispatch(this, &backend_agent_t::discover, msg);
                break;
            default:
                break;
        }
        // The answer is settled: the actor's loop, which awaits it, wakes (actor-zeta's futures wake
        // nobody).
        if (wake_ != nullptr) {
            wake_(owner_);
        }
    }

    backend_agent_t::unique_future<backend_agent_t::rows_t>
    backend_agent_t::read(std::string sql, std::vector<std::optional<std::string>> declared, std::uint64_t metadata) {
        OTX_ZONE_N("backend_agent::read");
        if (!in_transaction_) {
            if (auto refused = ready(); refused.contains_error()) {
                co_return refused;
            }
        }
        if (broken_) {
            co_return broken_error("a read");
        }
        if (log_.is_valid()) {
            log_->debug("{} read: \"{}\"", connection_, sql);
        }
        rows_t rows = error(core::error_code_t::internal_error, "no connector attached");
        if (auto* my = std::get_if<std::unique_ptr<mysql::IConnector>>(&connector_)) {
            rows = read_mysql(**my, sql);
        } else if (auto* pg = std::get_if<std::unique_ptr<pg::IConnector>>(&connector_)) {
            rows = read_pg(**pg, sql, metadata);
        } else if (auto* ch = std::get_if<std::unique_ptr<ch::IConnector>>(&connector_)) {
            rows = read_ch(**ch, sql, declared);
        }
        if (rows.has_error()) {
            note_failure(rows.error());
        }
        co_return rows;
    }

    backend_agent_t::unique_future<core::result_wrapper_t<std::uint64_t>>
    backend_agent_t::write(std::string sql, write_kind_t kind, bool strict_store, qualified_name_t table) {
        OTX_ZONE_N("backend_agent::write");
        if (!in_transaction_) {
            if (auto refused = ready(); refused.contains_error()) {
                co_return refused;
            }
        }
        if (broken_) {
            co_return broken_error("a write");
        }
        if (strict_store && kind_ == backend_type_t::MySQL && !strict_session_) {
            // Outside a strict sql_mode MySQL clamps a value out of the column's range with a warning
            // where the engine's assignment cast refuses it.
            co_return error(core::error_code_t::unimplemented_yet,
                            "the UPDATE narrows a value to its column's type, which MySQL range-checks only under a "
                            "strict sql_mode (STRICT_TRANS_TABLES or STRICT_ALL_TABLES); the session's sql_mode is "
                            "not strict");
        }
        if (log_.is_valid()) {
            log_->debug("{} write: \"{}\"", connection_, sql);
        }
        core::result_wrapper_t<std::uint64_t> written =
            error(core::error_code_t::internal_error, "no connector attached");
        if (auto* my = std::get_if<std::unique_ptr<mysql::IConnector>>(&connector_)) {
            written = write_mysql(**my, sql, kind);
        } else if (auto* pg = std::get_if<std::unique_ptr<pg::IConnector>>(&connector_)) {
            written = write_pg(**pg, sql);
        } else if (auto* ch = std::get_if<std::unique_ptr<ch::IConnector>>(&connector_)) {
            written = write_ch(**ch, sql, kind, table);
        }
        if (written.has_error()) {
            note_failure(written.error());
        }
        co_return written;
    }

    backend_agent_t::unique_future<core::error_t> backend_agent_t::step(transaction_step_t step,
                                                                        std::string savepoint) {
        OTX_ZONE_N("backend_agent::step");
        if (kind_ == backend_type_t::ClickHouse) {
            if (step == transaction_step_t::unpin) {
                co_return core::error_t::no_error();
            }
            co_return error(core::error_code_t::unimplemented_yet,
                            "ClickHouse has no transactions: a write inside a transaction block is not supported");
        }
        auto statement = transaction_statement(kind_, step, savepoint);
        if (!statement) {
            co_return error(core::error_code_t::invalid_parameter, "a savepoint step without a savepoint name");
        }
        if (step == transaction_step_t::unpin) {
            if (!in_transaction_) {
                co_return core::error_t::no_error();
            }
            // The caller's mistake: an open transaction is never left behind on the connection.
            end_transaction();
            if (execute("ROLLBACK").contains_error()) {
                reset_session();
            }
            co_return error(core::error_code_t::invalid_parameter,
                            "a transaction was unpinned after a transaction statement ran on it: it was rolled back");
        }
        if (step == transaction_step_t::commit && broken_) {
            // Nothing of it can commit: it is rolled back, and the COMMIT refused.
            auto refused = broken_error("COMMIT");
            end_transaction();
            if (execute("ROLLBACK").contains_error()) {
                reset_session();
            }
            co_return refused;
        }
        if (ends_transaction(step)) {
            end_transaction();
            auto failed = step == transaction_step_t::commit ? commit() : execute(*statement);
            if (failed.contains_error()) {
                // The backend's state of the transaction is unknown: no later statement may land in it.
                reset_session();
            }
            co_return failed;
        }
        if (broken_ && (step != transaction_step_t::rollback_to_savepoint || broken_->whole)) {
            co_return broken_error(*statement);
        }
        if (!in_transaction_) {
            if (auto refused = ready(); refused.contains_error()) {
                co_return refused;
            }
        }
        if (step == transaction_step_t::begin) {
            end_transaction();
        }
        in_transaction_ = true;
        auto failed = execute(*statement);
        if (failed.contains_error()) {
            note_failure(failed);
            co_return failed;
        }
        auto held = std::find(savepoints_.begin(), savepoints_.end(), savepoint);
        switch (step) {
            case transaction_step_t::savepoint:
                savepoints_.push_back(std::move(savepoint));
                break;
            case transaction_step_t::rollback_to_savepoint:
                if (held != savepoints_.end()) {
                    // The savepoint stays, the later ones go; a failure after it is undone.
                    const auto position = static_cast<std::size_t>(held - savepoints_.begin());
                    savepoints_.erase(std::next(held), savepoints_.end());
                    if (broken_ && position < broken_->savepoints) {
                        broken_.reset();
                    }
                }
                break;
            case transaction_step_t::release_savepoint:
                savepoints_.erase(held, savepoints_.end());
                break;
            default:
                break;
        }
        co_return core::error_t::no_error();
    }

    backend_agent_t::unique_future<core::result_wrapper_t<discovered_tables_t>>
    backend_agent_t::discover(qualified_name_t scope) {
        OTX_ZONE_N("backend_agent::discover");
        co_return discover_now(scope);
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_now(const qualified_name_t& scope) {
        OTX_ZONE_N("backend_agent::discover_now");
        if (!in_transaction_) {
            if (auto refused = ready(); refused.contains_error()) {
                return refused;
            }
        }
        if (auto* my = std::get_if<std::unique_ptr<mysql::IConnector>>(&connector_)) {
            return discover_mysql(**my, scope);
        }
        if (auto* pg = std::get_if<std::unique_ptr<pg::IConnector>>(&connector_)) {
            return discover_pg(**pg, scope);
        }
        if (auto* ch = std::get_if<std::unique_ptr<ch::IConnector>>(&connector_)) {
            return discover_ch(**ch, scope);
        }
        return error(core::error_code_t::internal_error, "no connector attached");
    }

    // ---- MySQL ----

    backend_agent_t::rows_t backend_agent_t::read_mysql(mysql::IConnector& connector, const std::string& sql) {
        std::optional<core::error_t> conversion;
        auto handler = [this, &conversion](const boost::mysql::results& result) -> std::unique_ptr<data_chunk_t> {
            auto converted = tsl::mysql_to_chunk(resource_, result);
            if (converted.has_error()) {
                conversion.emplace(core::error_on(resource_, converted.error()));
                return nullptr;
            }
            return std::make_unique<data_chunk_t>(std::move(converted.value()));
        };
        auto outcome = run<std::unique_ptr<data_chunk_t>, const boost::mysql::results&>(connector, sql, handler);
        if (outcome.has_error()) {
            return core::error_on(resource_, outcome.error());
        }
        if (!outcome.value()) {
            return conversion ? std::move(*conversion)
                              : error(core::error_code_t::internal_error, "the backend returned no result chunk");
        }
        std::vector<data_chunk_t> rows;
        rows.push_back(std::move(*outcome.value()));
        return rows;
    }

    core::result_wrapper_t<std::uint64_t>
    backend_agent_t::write_mysql(mysql::IConnector& connector, const std::string& sql, write_kind_t kind) {
        // An UPDATE answers the rows it matched (mysql_update_rows_matched), an INSERT / DELETE its
        // affected rows.
        const bool update = kind == write_kind_t::update;
        bool info_missing = false;
        auto handler = [update, &info_missing](const boost::mysql::results& result) -> int64_t {
            if (!update) {
                return static_cast<int64_t>(result.affected_rows());
            }
            const auto matched = mysql_update_rows_matched(result.info());
            if (!matched) {
                info_missing = true;
                return 0;
            }
            return static_cast<int64_t>(*matched);
        };
        auto outcome = run<int64_t, const boost::mysql::results&>(connector, sql, handler);
        if (outcome.has_error()) {
            return core::error_on(resource_, outcome.error());
        }
        if (info_missing) {
            return error(core::error_code_t::protocol_violation, "an UPDATE answered without its matched rows");
        }
        return static_cast<std::uint64_t>(outcome.value());
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_mysql(mysql::IConnector& connector,
                                                                                const qualified_name_t& scope) {
        if (!scope.database.t.empty() || !scope.collection.t.empty()) {
            return discover_mysql_in(connector, scope);
        }
        // The whole server: each of its user databases.
        std::vector<std::string> databases;
        auto listed = [&databases](const boost::mysql::results& result) -> otterstax::asio_error_t {
            for (auto row : result.rows()) {
                auto view = row.at(0).as_string();
                databases.emplace_back(view.data(), view.size());
            }
            return otterstax::asio_error_t{};
        };
        auto query = otterstax::catalog::make_list_databases_query(resource_, backend_type_t::MySQL);
        if (auto failed = run<otterstax::asio_error_t, const boost::mysql::results&>(connector, query, listed);
            failed.contains_error()) {
            return failed;
        }
        discovered_tables_t every(resource_);
        for (const auto& database : databases) {
            auto in_one = scope;
            in_one.database = core::dbname_t{database};
            auto found = discover_mysql_in(connector, in_one);
            if (found.has_error()) {
                return found;
            }
            for (auto& table : found.value()) {
                every.push_back(std::move(table));
            }
        }
        return every;
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_mysql_in(mysql::IConnector& connector,
                                                                                   const qualified_name_t& scope) {
        discovered_tables_t out(resource_);
        // The session's sql_mode was checked when it was opened (check_session).
        auto probe = [this, &out, &connector](const qualified_name_t& table) -> core::error_t {
            auto schema_handler = [this, table, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
                // Read by the very table a read types its rows with, so a recorded column's type is
                // the read column's type by construction.
                auto schema = tsl::mysql_to_struct(resource_, result);
                if (schema.has_error()) {
                    return tsl::make_error(resource_, schema.error().type, schema.error().what.c_str());
                }
                out.push_back(discovered_table_t{table, std::move(schema.value())});
                return otterstax::asio_error_t{};
            };
            auto query = otterstax::catalog::make_schema_probe_query(resource_, table, backend_type_t::MySQL);
            if (query.has_error()) {
                return core::error_on(resource_, query.error());
            }
            return run<otterstax::asio_error_t, const boost::mysql::results&>(connector, query.value(), schema_handler);
        };

        if (!scope.collection.t.empty()) {
            if (auto failed = probe(scope); failed.contains_error()) {
                return failed;
            }
            return out;
        }
        auto list = otterstax::catalog::make_list_tables_query(resource_, backend_type_t::MySQL, scope.database.t);
        std::vector<std::string> names;
        auto list_handler = [&names](const boost::mysql::results& result) -> otterstax::asio_error_t {
            for (auto row : result.rows()) {
                auto view = row.at(0).as_string();
                names.emplace_back(view.data(), view.size());
            }
            return otterstax::asio_error_t{};
        };
        if (auto failed = run<otterstax::asio_error_t, const boost::mysql::results&>(connector, list, list_handler);
            failed.contains_error()) {
            return failed;
        }
        std::pmr::vector<std::pmr::string> failed_tables(resource_);
        for (const auto& name : names) {
            qualified_name_t table(scope.connection, scope.database, core::schema_t{}, core::relname_t{name});
            if (auto failed = probe(table); failed.contains_error()) {
                if (log_.is_valid()) {
                    log_->error("discover: {}.{}: {}", scope.database.t, name, failed.what.c_str());
                }
                failed_tables.emplace_back((scope.database.t + "." + name).c_str());
            }
        }
        if (!failed_tables.empty()) {
            return otterstax::catalog::make_discovery_error(resource_, failed_tables);
        }
        return out;
    }

    // ---- PostgreSQL ----

    core::error_t backend_agent_t::fetch_enums(pg::IConnector& connector) {
        OTX_ZONE_N("backend_agent::fetch_enums");
        const std::string query = "SELECT t.oid, t.typname, e.enumlabel FROM pg_type t JOIN pg_enum e ON e.enumtypid = "
                                  "t.oid WHERE t.typtype = 'e' ORDER BY t.oid, e.enumsortorder;";
        tsl::pg_enum_oid_map map;
        auto handler = [this, &map](PGresult* result) -> otterstax::asio_error_t {
            if (result == nullptr) {
                return error(core::error_code_t::io_error, "the pg_enum query produced no result");
            }
            const int rows = PQntuples(result);
            for (int i = 0; i < rows; ++i) {
                const char* oid_text = PQgetvalue(result, i, 0);
                const char* typname = PQgetvalue(result, i, 1);
                const char* label = PQgetvalue(result, i, 2);
                if (oid_text == nullptr || typname == nullptr || label == nullptr) {
                    return error(core::error_code_t::protocol_violation,
                                 "pg_enum row " + std::to_string(i) + " has a NULL oid, typname or label");
                }
                unsigned int oid = 0;
                const std::string_view text{oid_text};
                auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), oid);
                if (ec != std::errc{} || end != text.data() + text.size()) {
                    return error(core::error_code_t::protocol_violation,
                                 "pg_enum row " + std::to_string(i) + " has a non-numeric oid '" + oid_text + "'");
                }
                auto& descriptor = map[oid];
                if (descriptor.typname.empty()) {
                    descriptor.typname = typname;
                }
                descriptor.values.emplace_back(label);
            }
            return otterstax::asio_error_t{};
        };
        if (auto failed = run<otterstax::asio_error_t, PGresult*>(connector, query, handler); failed.contains_error()) {
            return failed;
        }
        enums_ = std::move(map);
        return core::error_t::no_error();
    }

    backend_agent_t::rows_t
    backend_agent_t::read_pg(pg::IConnector& connector, const std::string& sql, std::uint64_t metadata) {
        if (metadata != enums_metadata_) {
            if (auto failed = fetch_enums(connector); failed.contains_error()) {
                return failed;
            }
            enums_metadata_ = metadata;
        }
        std::optional<core::error_t> conversion;
        auto handler = [this, &conversion](PGresult* result) -> std::unique_ptr<data_chunk_t> {
            auto converted = tsl::pg_to_chunk(resource_, result, enums_);
            if (converted.has_error()) {
                conversion.emplace(core::error_on(resource_, converted.error()));
                return nullptr;
            }
            return std::make_unique<data_chunk_t>(std::move(converted.value()));
        };
        auto outcome = run<std::unique_ptr<data_chunk_t>, PGresult*>(connector, sql, handler);
        if (outcome.has_error()) {
            return core::error_on(resource_, outcome.error());
        }
        if (!outcome.value()) {
            return conversion ? std::move(*conversion)
                              : error(core::error_code_t::internal_error, "the backend returned no result chunk");
        }
        std::vector<data_chunk_t> rows;
        rows.push_back(std::move(*outcome.value()));
        return rows;
    }

    core::result_wrapper_t<std::uint64_t> backend_agent_t::write_pg(pg::IConnector& connector, const std::string& sql) {
        // The command tag's count (INSERT 0 n / UPDATE n / DELETE n).
        auto handler = [](PGresult* result) -> int64_t {
            const char* tuples = result == nullptr ? nullptr : PQcmdTuples(result);
            if (tuples == nullptr || *tuples == '\0') {
                return 0;
            }
            int64_t count = 0;
            const std::string_view text{tuples};
            std::from_chars(text.data(), text.data() + text.size(), count);
            return count;
        };
        auto outcome = run<int64_t, PGresult*>(connector, sql, handler);
        if (outcome.has_error()) {
            return core::error_on(resource_, outcome.error());
        }
        return static_cast<std::uint64_t>(outcome.value());
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_pg(pg::IConnector& connector,
                                                                             const qualified_name_t& scope) {
        // The ENUM labels first: the probes decode an enum column with them, so a failed metadata
        // query fails the discovery rather than record a table with degraded column types.
        if (auto failed = fetch_enums(connector); failed.contains_error()) {
            return failed;
        }
        discovered_tables_t out(resource_);
        auto probe = [this, &out, &connector](const qualified_name_t& table) -> core::error_t {
            auto schema_handler = [this, table, &out](PGresult* result) -> otterstax::asio_error_t {
                out.push_back(discovered_table_t{table, tsl::pg_to_struct(resource_, result, enums_)});
                return otterstax::asio_error_t{};
            };
            auto query = otterstax::catalog::make_schema_probe_query(resource_, table, backend_type_t::PostgreSQL);
            if (query.has_error()) {
                return core::error_on(resource_, query.error());
            }
            return run<otterstax::asio_error_t, PGresult*>(connector, query.value(), schema_handler);
        };
        if (!scope.collection.t.empty() && !scope.schema.t.empty()) {
            if (auto failed = probe(scope); failed.contains_error()) {
                return failed;
            }
            return out;
        }
        // The tables of the schema, or of every user schema of the database: (table_schema,
        // table_name) rows; a configured table without a schema is looked for in each.
        auto list = otterstax::catalog::make_list_tables_query(resource_, backend_type_t::PostgreSQL, scope.schema.t);
        std::vector<std::pair<std::string, std::string>> names;
        auto list_handler = [this, &names, &scope](PGresult* result) -> otterstax::asio_error_t {
            if (result == nullptr || (PQntuples(result) > 0 && PQnfields(result) != 2)) {
                return error(core::error_code_t::protocol_violation,
                             "the information_schema listing did not answer (table_schema, table_name)");
            }
            for (int i = 0; i < PQntuples(result); ++i) {
                std::string table{PQgetvalue(result, i, 1)};
                if (!scope.collection.t.empty() && table != scope.collection.t) {
                    continue;
                }
                names.emplace_back(std::string{PQgetvalue(result, i, 0)}, std::move(table));
            }
            return otterstax::asio_error_t{};
        };
        if (auto failed = run<otterstax::asio_error_t, PGresult*>(connector, list, list_handler);
            failed.contains_error()) {
            return failed;
        }
        std::pmr::vector<std::pmr::string> failed_tables(resource_);
        for (const auto& [schema, name] : names) {
            qualified_name_t table = scope;
            table.schema = schema;
            table.collection = name;
            if (auto failed = probe(table); failed.contains_error()) {
                if (log_.is_valid()) {
                    log_->error("discover: {}.{}: {}", schema, name, failed.what.c_str());
                }
                failed_tables.emplace_back((schema + "." + name).c_str());
            }
        }
        if (!failed_tables.empty()) {
            return otterstax::catalog::make_discovery_error(resource_, failed_tables);
        }
        return out;
    }

    // ---- ClickHouse ----

    backend_agent_t::rows_t backend_agent_t::read_ch(ch::IConnector& connector,
                                                     const std::string& sql,
                                                     const std::vector<std::optional<std::string>>& declared) {
        std::optional<core::error_t> conversion;
        auto handler =
            [this, &conversion, &declared](const ch::select_result_t& result) -> std::unique_ptr<data_chunk_t> {
            auto converted = tsl::ch_to_chunk(resource_, result.blocks, declared);
            if (converted.has_error()) {
                conversion.emplace(core::error_on(resource_, converted.error()));
                return nullptr;
            }
            return std::make_unique<data_chunk_t>(std::move(converted.value()));
        };
        auto outcome = run<std::unique_ptr<data_chunk_t>, const ch::select_result_t&>(connector, sql, handler);
        if (outcome.has_error()) {
            return core::error_on(resource_, outcome.error());
        }
        if (!outcome.value()) {
            return conversion ? std::move(*conversion)
                              : error(core::error_code_t::internal_error, "the backend returned no result chunk");
        }
        std::vector<data_chunk_t> rows;
        rows.push_back(std::move(*outcome.value()));
        return rows;
    }

    core::result_wrapper_t<std::uint64_t> backend_agent_t::write_ch(ch::IConnector& connector,
                                                                    const std::string& sql,
                                                                    write_kind_t kind,
                                                                    const qualified_name_t& table) {
        // No ClickHouse block carries an affected-row count: written_rows (the sum of the Progress
        // packets) is an INSERT's count; a lightweight DELETE / ALTER TABLE ... UPDATE is a mutation
        // that reports nothing — 0, whatever the server wrote.
        const bool insert = kind == write_kind_t::insert;
        auto handler = [insert](const ch::select_result_t& result) -> int64_t {
            return insert ? static_cast<int64_t>(result.written_rows) : int64_t{0};
        };
        auto outcome = run<int64_t, const ch::select_result_t&>(connector, sql, handler);
        if (outcome.has_error()) {
            auto failed = core::error_on(resource_, outcome.error());
            if (insert) {
                return failed;
            }
            return kill_failed_mutation(connector, sql, table, std::move(failed));
        }
        return static_cast<std::uint64_t>(outcome.value());
    }

    core::error_t backend_agent_t::kill_failed_mutation(ch::IConnector& connector,
                                                        const std::string& sql,
                                                        const qualified_name_t& table,
                                                        core::error_t failed) {
        OTX_ZONE_N("backend_agent::kill_failed_mutation");
        // A mutation that failed on a part stays in system.mutations (is_done = 0): ClickHouse
        // retries it forever, and every later mutation of the table, executed together with it,
        // fails with its error. The server's text follows the statement's in the connector's error
        // (the statement's literals are not searched).
        const std::string_view what{failed.what.data(), failed.what.size()};
        const auto statement = what.find(sql);
        const auto ids =
            ch_failed_mutations(statement == std::string_view::npos ? what : what.substr(statement + sql.size()));
        if (ids.empty() || table.database.t.empty() || table.collection.t.empty()) {
            return failed;
        }
        const auto with = [this, &failed](const std::string& note) {
            return core::error_t{
                failed.type,
                std::pmr::string{(std::string{failed.what.data(), failed.what.size()} + note).c_str(), resource_}};
        };
        if (ids.size() > 1) {
            // Which of them is this statement's ClickHouse does not say; the others are not ours.
            std::string listed;
            for (const auto& id : ids) {
                listed += (listed.empty() ? "" : ", ") + id;
            }
            return with("; ClickHouse names the mutations " + listed +
                        " as failed together and does not say which "
                        "one this statement's is: none was killed (KILL MUTATION by hand)");
        }
        std::string kill = "KILL MUTATION WHERE database = ";
        kill += otterstax::catalog::escape_sql_literal(resource_, table.database.t, backend_type_t::ClickHouse);
        kill += " AND table = ";
        kill += otterstax::catalog::escape_sql_literal(resource_, table.collection.t, backend_type_t::ClickHouse);
        kill += " AND mutation_id = ";
        kill += otterstax::catalog::escape_sql_literal(resource_, ids.front(), backend_type_t::ClickHouse);
        if (log_.is_valid()) {
            log_->warn("{}: the mutation {} of {}.{} failed, killing it: \"{}\"",
                       connection_,
                       ids.front(),
                       table.database.t,
                       table.collection.t,
                       kill);
        }
        auto ok = [](const ch::select_result_t&) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
        if (auto refused = run<otterstax::asio_error_t, const ch::select_result_t&>(connector, kill, ok);
            refused.contains_error()) {
            return with("; the failed mutation " + ids.front() + " was not killed, ClickHouse retries it: " +
                        std::string{refused.what.data(), refused.what.size()});
        }
        return failed;
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_ch(ch::IConnector& connector,
                                                                             const qualified_name_t& scope) {
        if (!scope.database.t.empty()) {
            return discover_ch_in(connector, scope);
        }
        // The whole server: each of its user databases.
        std::vector<std::string> databases;
        auto listed = [this, &databases](const ch::select_result_t& result) -> otterstax::asio_error_t {
            for (const auto& block : result.blocks) {
                if (block.GetRowCount() == 0) {
                    continue;
                }
                auto column = block[0]->As<clickhouse::ColumnString>();
                if (!column) {
                    return error(core::error_code_t::protocol_violation, "the database listing is not a string column");
                }
                for (size_t i = 0; i < block.GetRowCount(); ++i) {
                    auto view = column->At(i);
                    databases.emplace_back(view.data(), view.size());
                }
            }
            return otterstax::asio_error_t{};
        };
        auto query = otterstax::catalog::make_list_databases_query(resource_, backend_type_t::ClickHouse);
        if (auto failed = run<otterstax::asio_error_t, const ch::select_result_t&>(connector, query, listed);
            failed.contains_error()) {
            return failed;
        }
        discovered_tables_t every(resource_);
        for (const auto& database : databases) {
            auto in_one = scope;
            in_one.database = core::dbname_t{database};
            auto found = discover_ch_in(connector, in_one);
            if (found.has_error()) {
                return found;
            }
            for (auto& table : found.value()) {
                every.push_back(std::move(table));
            }
        }
        return every;
    }

    core::result_wrapper_t<discovered_tables_t> backend_agent_t::discover_ch_in(ch::IConnector& connector,
                                                                                const qualified_name_t& scope) {
        const std::string& database = scope.database.t;
        discovered_tables_t out(resource_);
        // The system.columns types first (a named Tuple, a Bool, a LowCardinality column is read as
        // its declared type only through them), then the `WHERE 1 = 0` probe, which answers the
        // header block; no block or one without columns means the probe did not describe the table.
        auto probe = [this, &out, &connector, &scope, &database](const std::string& name) -> core::error_t {
            std::unordered_map<std::string, std::string> types;
            auto types_handler = [this, &types](const ch::select_result_t& columns) -> otterstax::asio_error_t {
                for (const auto& block : columns.blocks) {
                    if (block.GetRowCount() == 0) {
                        continue;
                    }
                    if (block.GetColumnCount() < 2) {
                        return error(core::error_code_t::protocol_violation,
                                     "system.columns answered with " + std::to_string(block.GetColumnCount()) +
                                         " column(s) instead of (name, type)");
                    }
                    auto names = block[0]->As<clickhouse::ColumnString>();
                    auto declared = block[1]->As<clickhouse::ColumnString>();
                    if (!names || !declared) {
                        return error(core::error_code_t::protocol_violation,
                                     "system.columns answered with non-string name / type columns");
                    }
                    for (size_t i = 0; i < block.GetRowCount(); ++i) {
                        types.insert_or_assign(std::string(names->At(i)), std::string(declared->At(i)));
                    }
                }
                return otterstax::asio_error_t{};
            };
            auto types_query = otterstax::catalog::make_named_types_query(resource_, database, name);
            if (auto failed =
                    run<otterstax::asio_error_t, const ch::select_result_t&>(connector, types_query, types_handler);
                failed.contains_error()) {
                return failed;
            }
            qualified_name_t table(scope.connection, core::dbname_t{database}, core::schema_t{}, core::relname_t{name});
            auto schema_handler =
                [this, &table, &types, &out](const ch::select_result_t& result) -> otterstax::asio_error_t {
                if (result.blocks.empty() || result.blocks[0].GetColumnCount() == 0) {
                    return error(core::error_code_t::protocol_violation,
                                 "the schema probe returned no columns for " + table.database.t + "." +
                                     table.collection.t);
                }
                out.push_back(discovered_table_t{table, tsl::ch_to_struct(resource_, result.blocks[0], types), types});
                return otterstax::asio_error_t{};
            };
            auto query = otterstax::catalog::make_schema_probe_query(resource_, table, backend_type_t::ClickHouse);
            if (query.has_error()) {
                return core::error_on(resource_, query.error());
            }
            return run<otterstax::asio_error_t, const ch::select_result_t&>(connector, query.value(), schema_handler);
        };
        if (!scope.collection.t.empty()) {
            if (auto failed = probe(scope.collection.t); failed.contains_error()) {
                return failed;
            }
            return out;
        }
        auto list = otterstax::catalog::make_list_tables_query(resource_, backend_type_t::ClickHouse, database);
        std::vector<std::string> names;
        auto list_handler = [this, &names](const ch::select_result_t& listed) -> otterstax::asio_error_t {
            for (const auto& block : listed.blocks) {
                if (block.GetRowCount() == 0) {
                    continue;
                }
                auto column = block[0]->As<clickhouse::ColumnString>();
                if (!column) {
                    return error(core::error_code_t::protocol_violation, "the table listing is not a string column");
                }
                for (size_t i = 0; i < block.GetRowCount(); ++i) {
                    auto view = column->At(i);
                    names.emplace_back(view.data(), view.size());
                }
            }
            return otterstax::asio_error_t{};
        };
        if (auto failed = run<otterstax::asio_error_t, const ch::select_result_t&>(connector, list, list_handler);
            failed.contains_error()) {
            return failed;
        }
        std::pmr::vector<std::pmr::string> failed_tables(resource_);
        for (const auto& name : names) {
            if (auto failed = probe(name); failed.contains_error()) {
                if (log_.is_valid()) {
                    log_->error("discover: {}.{}: {}", database, name, failed.what.c_str());
                }
                failed_tables.emplace_back((database + "." + name).c_str());
            }
        }
        if (!failed_tables.empty()) {
            return otterstax::catalog::make_discovery_error(resource_, failed_tables);
        }
        return out;
    }
} // namespace otterstax::federation
