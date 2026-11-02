// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// A fake backend server and the fake connectors of its connections (an IConnector of each dialect):
// canned tables, a log of every statement and transaction step (with the connection it ran on), a
// read answering the columns the SELECT list names. What the backend agents
// (otterbrix/federation/backend_agent.hpp) run in test_federation and test_unit_connectors.

#include "otterbrix/federation/backend_agent.hpp"
#include "tests/mock/sql_db_connector.hpp"

#include <actor-zeta/spawn.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace fakebackend {

    namespace fed = otterstax::federation;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    struct null_t {};
    using value_t = std::variant<std::int64_t, std::string, double, null_t>;
    using row_t = std::vector<value_t>;

    inline complex_logical_type column(const std::string& name, logical_type type) {
        complex_logical_type out{type};
        out.set_alias(name);
        return out;
    }

    // One table of a fake backend: where it is (backend spelling), its columns (named in the
    // backend's spelling) and rows.
    struct canned_table_t {
        std::string database;
        std::string schema;
        std::string table;
        std::vector<complex_logical_type> columns;
        std::vector<row_t> rows;
    };

    inline components::vector::data_chunk_t chunk_of(std::pmr::memory_resource* resource,
                                                      const std::vector<complex_logical_type>& columns,
                                                      const std::vector<row_t>& rows) {
        std::pmr::vector<complex_logical_type> types{columns.begin(), columns.end(), resource};
        components::vector::data_chunk_t chunk(resource, types, std::max<std::size_t>(rows.size(), 1));
        for (std::size_t r = 0; r < rows.size(); ++r) {
            for (std::size_t c = 0; c < columns.size(); ++c) {
                const auto& value = rows[r][c];
                if (std::holds_alternative<null_t>(value)) {
                    chunk.data[c].set_null(r, true);
                } else if (std::holds_alternative<std::int64_t>(value)) {
                    chunk.set_value(c, r, std::get<std::int64_t>(value));
                } else if (std::holds_alternative<double>(value)) {
                    chunk.set_value(c, r, std::get<double>(value));
                } else {
                    chunk.set_value(c, r, std::string_view{std::get<std::string>(value)});
                }
            }
        }
        chunk.set_cardinality(rows.size());
        return chunk;
    }

    // The fake server of one connection, shared by the fake connectors of its agents. Thread-safe:
    // the agents run on the pool's threads.
    class fake_backend_t final {
    public:
        explicit fake_backend_t(backend_type_t kind)
            : kind_(kind) {}

        backend_type_t kind() const noexcept { return kind_; }

        void add_table(canned_table_t table) {
            std::lock_guard guard(mutex_);
            tables_.push_back(std::move(table));
        }
        void drop_table(const std::string& name) {
            std::lock_guard guard(mutex_);
            tables_.erase(std::remove_if(tables_.begin(), tables_.end(), [&](const auto& t) { return t.table == name; }),
                          tables_.end());
        }
        canned_table_t* find(const std::string& name) {
            for (auto& table : tables_) {
                if (table.table == name) {
                    return &table;
                }
            }
            return nullptr;
        }

        // Every statement and step, in order: "scan <sql>", "write <sql>", "begin", "commit",
        // "rollback", "savepoint s1", "rollback_to_savepoint s1"; a statement that ran on a
        // connection inside an open backend transaction carries " @txn" at the end, and every line
        // the connection it ran on in `connections()`.
        std::vector<std::string> log() const {
            std::lock_guard guard(mutex_);
            return log_;
        }
        // The connection (a fake connector's number) of each line of log().
        std::vector<int> connections() const {
            std::lock_guard guard(mutex_);
            return on_;
        }
        // The statements and steps only, without the reads.
        std::vector<std::string> changes() const {
            std::lock_guard guard(mutex_);
            std::vector<std::string> out;
            for (const auto& line : log_) {
                if (line.rfind("scan ", 0) != 0) {
                    out.push_back(line);
                }
            }
            return out;
        }
        void clear() {
            std::lock_guard guard(mutex_);
            log_.clear();
            on_.clear();
            discoveries_ = 0;
        }
        int discoveries() const {
            std::lock_guard guard(mutex_);
            return discoveries_;
        }

        // A read the hook answers instead of the canned tables.
        using scan_hook_t =
            std::function<std::optional<components::vector::data_chunk_t>(const std::string& sql, std::pmr::memory_resource*)>;
        void on_scan(scan_hook_t hook) {
            std::lock_guard guard(mutex_);
            hook_ = std::move(hook);
        }
        // A refusal of every write.
        void fail_writes(std::optional<core::error_code_t> code) {
            std::lock_guard guard(mutex_);
            fail_writes_ = code;
        }
        // A refusal of every COMMIT.
        void fail_commits(bool fail) {
            std::lock_guard guard(mutex_);
            fail_commits_ = fail;
        }
        // A refusal (`code`) of every read and write whose text contains `needle`: a table that
        // already exists, a column that does, a deadlock.
        void fail_statements(std::string needle, core::error_code_t code) {
            std::lock_guard guard(mutex_);
            fail_statements_.emplace_back(std::move(needle), code);
        }
        // The server goes down (every connection to it is lost, a connect fails after `connect_delay`,
        // as a host that does not answer) or comes back.
        void set_down(bool down, std::chrono::milliseconds connect_delay = std::chrono::milliseconds{0}) {
            std::lock_guard guard(mutex_);
            down_ = down;
            connect_delay_ = connect_delay;
        }
        bool down() const {
            std::lock_guard guard(mutex_);
            return down_;
        }
        // The connects tried since the start.
        int connects() const { return connects_.load(); }
        core::error_t try_connect() {
            ++connects_;
            std::chrono::milliseconds delay{0};
            bool down = false;
            {
                std::lock_guard guard(mutex_);
                delay = connect_delay_;
                down = down_;
            }
            if (!down) {
                return core::error_t::no_error();
            }
            std::this_thread::sleep_for(delay);
            return core::error_t{core::error_code_t::io_error, std::pmr::string{"fake backend: connection refused", &resource_}};
        }
        // A line of log() a connector writes for itself: a statement refused inside an aborted
        // transaction ("aborted <sql>").
        void note_line(int connection, std::string line) {
            std::lock_guard guard(mutex_);
            note(connection, std::move(line));
        }
        // Every read hangs until its connection is cancelled (cancel()), then fails with io_error,
        // as a backend that never answers.
        void block_reads(bool block) {
            std::lock_guard guard(mutex_);
            block_reads_ = block;
        }
        bool reads_blocked() const {
            std::lock_guard guard(mutex_);
            return block_reads_;
        }
        // The reads that started hanging.
        int hanging() const { return hanging_.load(); }
        void hang() { ++hanging_; }
        core::error_t cancelled_error() {
            std::lock_guard guard(mutex_);
            return core::error_t{core::error_code_t::io_error, std::pmr::string{"fake backend: cancelled", &resource_}};
        }
        // Every read takes `delay` on the agent's thread, as a slow backend does.
        void answer_later(std::chrono::milliseconds delay) {
            std::lock_guard guard(mutex_);
            delay_ = delay;
        }

        // ---- what the fake connectors run ----

        core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>> read(int connection,
                                                                                       bool open,
                                                                                       std::string_view query) {
            std::chrono::milliseconds delay{0};
            {
                std::lock_guard guard(mutex_);
                delay = delay_;
            }
            if (delay.count() > 0) {
                std::this_thread::sleep_for(delay);
            }
            std::lock_guard guard(mutex_);
            const std::string sql{query};
            note(connection, "scan " + sql + (open ? " @txn" : ""));
            if (auto refused = refusal(sql)) {
                return std::move(*refused);
            }
            if (hook_) {
                if (auto answer = hook_(sql, &resource_)) {
                    return std::make_unique<components::vector::data_chunk_t>(std::move(*answer));
                }
            }
            // A read of a canned table: the columns the SELECT list names, in its order.
            for (const auto& table : tables_) {
                if (sql.find(quoted(table.table)) == std::string::npos) {
                    continue;
                }
                const auto list = sql.substr(0, sql.find(" FROM "));
                if (list.find("COUNT(*)") != std::string::npos) {
                    return std::make_unique<components::vector::data_chunk_t>(
                        chunk_of(&resource_,
                                 {column("count", logical_type::BIGINT)},
                                 {{static_cast<std::int64_t>(table.rows.size())}}));
                }
                std::vector<std::pair<std::size_t, std::size_t>> picked; // (position in text, column)
                for (std::size_t c = 0; c < table.columns.size(); ++c) {
                    const auto at = list.find(quoted(table.columns[c].alias()));
                    if (at != std::string::npos) {
                        picked.emplace_back(at, c);
                    }
                }
                std::sort(picked.begin(), picked.end());
                std::vector<complex_logical_type> columns;
                for (auto [at, c] : picked) {
                    columns.push_back(table.columns[c]);
                }
                std::vector<row_t> out;
                for (const auto& row : table.rows) {
                    row_t kept;
                    for (auto [at, c] : picked) {
                        kept.push_back(row[c]);
                    }
                    out.push_back(std::move(kept));
                }
                return std::make_unique<components::vector::data_chunk_t>(chunk_of(&resource_, columns, out));
            }
            return core::error_t{core::error_code_t::undefined_object,
                                 std::pmr::string{("fake backend: no table in " + sql).c_str(), &resource_}};
        }

        core::result_wrapper_t<int64_t> write(int connection, bool open, std::string_view query) {
            std::lock_guard guard(mutex_);
            const std::string sql{query};
            note(connection, "write " + sql + (open ? " @txn" : ""));
            if (fail_writes_) {
                return core::error_t{*fail_writes_, std::pmr::string{"fake backend: refused", &resource_}};
            }
            if (auto refused = refusal(sql)) {
                return std::move(*refused);
            }
            int64_t count = 1;
            if (sql.rfind("INSERT", 0) == 0) {
                std::size_t at = 0;
                while ((at = sql.find("), (", at)) != std::string::npos) {
                    ++count;
                    at += 4;
                }
            }
            return count;
        }

        // A transaction step, logged as its step; anything else as "exec <sql>". A refused COMMIT
        // (fail_commits) is io_error. A COMMIT of an aborted transaction (`aborted`: a PostgreSQL
        // statement of it failed) is "commit-rolled-back": PostgreSQL answers it without an error,
        // with the tag ROLLBACK, and keeps nothing.
        core::error_t step(int connection, std::string_view query, bool aborted = false) {
            std::lock_guard guard(mutex_);
            const std::string sql{query};
            auto name = [&sql] {
                auto at = sql.rfind(' ');
                std::string out = sql.substr(at + 1);
                out.erase(std::remove_if(out.begin(), out.end(), [](char c) { return c == '"' || c == '`'; }), out.end());
                return out;
            };
            std::string line;
            if (sql == "BEGIN" || sql == "START TRANSACTION") {
                line = "begin";
            } else if (sql == "COMMIT") {
                line = aborted ? "commit-rolled-back" : "commit";
            } else if (sql == "ROLLBACK") {
                line = "rollback";
            } else if (sql.rfind("ROLLBACK TO SAVEPOINT ", 0) == 0) {
                line = "rollback_to_savepoint " + name();
            } else if (sql.rfind("RELEASE SAVEPOINT ", 0) == 0) {
                line = "release_savepoint " + name();
            } else if (sql.rfind("SAVEPOINT ", 0) == 0) {
                line = "savepoint " + name();
            } else {
                line = "exec " + sql;
            }
            note(connection, line);
            if (line == "commit" && fail_commits_) {
                return core::error_t{core::error_code_t::io_error, std::pmr::string{"fake backend: COMMIT lost", &resource_}};
            }
            return core::error_t::no_error();
        }

        // The startup discovery the fixture hands the registration (what an agent's discovery
        // answers, built off the canned tables).
        fed::discovered_tables_t discover(const qualified_name_t& scope) {
            std::lock_guard guard(mutex_);
            ++discoveries_;
            fed::discovered_tables_t out{&resource_};
            for (const auto& table : tables_) {
                const bool same_place = kind_ == backend_type_t::PostgreSQL
                                            ? (scope.schema.t.empty() || scope.schema.t == table.schema)
                                            : scope.database.t == table.database;
                if (!same_place || (!scope.collection.t.empty() && scope.collection.t != table.table)) {
                    continue;
                }
                std::pmr::vector<complex_logical_type> fields{table.columns.begin(), table.columns.end(), &resource_};
                out.push_back(fed::discovered_table_t{
                    qualified_name_t{core::connection_name_t{scope.connection.t},
                                     core::dbname_t{kind_ == backend_type_t::PostgreSQL ? scope.database.t : table.database},
                                     core::schema_t{kind_ == backend_type_t::PostgreSQL ? table.schema : std::string{}},
                                     core::relname_t{table.table}},
                    complex_logical_type::create_struct("row", fields)});
            }
            return out;
        }

        // A MySQL discovery query, answered as a MySQL server would (the agent's discovery reads
        // these result sets): the sql_mode, the information_schema listing, a table's probe.
        std::optional<boost::mysql::results> mysql_metadata(std::string_view query) {
            std::lock_guard guard(mutex_);
            using boost::mysql::column_type;
            if (query.find("sql_mode") != std::string_view::npos) {
                ++discoveries_;
                return mysql::mock_results({{"@@SESSION.sql_mode", column_type::varchar}}, {{std::string{""}}});
            }
            if (query.find("information_schema") != std::string_view::npos) {
                std::vector<mysql::mock_row_t> rows;
                for (const auto& table : tables_) {
                    rows.push_back({table.table});
                }
                return mysql::mock_results({{"TABLE_NAME", column_type::varchar}}, rows);
            }
            if (query.find("1 = 0") != std::string_view::npos) {
                for (const auto& table : tables_) {
                    if (query.find(quoted(table.table)) == std::string_view::npos) {
                        continue;
                    }
                    std::vector<mysql::mock_column_t> columns;
                    for (const auto& c : table.columns) {
                        const auto type = c.type() == logical_type::BIGINT   ? column_type::bigint
                                          : c.type() == logical_type::DOUBLE ? column_type::double_
                                                                             : column_type::varchar;
                        columns.push_back({c.alias(), type});
                    }
                    return mysql::mock_results(columns);
                }
            }
            return std::nullopt;
        }

        // A PostgreSQL discovery query, answered as a PostgreSQL server would (the agent's discovery
        // reads these results): the information_schema listing of (table_schema, table_name) — the
        // schema the query names, or every schema — and a table's `WHERE 1 = 0` probe, the header of
        // its columns (BIGINT int8, DOUBLE float8, anything else text).
        using pg_result_ptr = std::unique_ptr<PGresult, void (*)(PGresult*)>;
        std::optional<pg_result_ptr> pg_metadata(std::string_view query) {
            std::lock_guard guard(mutex_);
            pg_result_ptr result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
            if (query.find("information_schema") != std::string_view::npos) {
                ++discoveries_;
                PGresAttDesc names[2] = {{const_cast<char*>("table_schema"), 0, 0, 0, 1043, -1, -1},
                                         {const_cast<char*>("table_name"), 0, 0, 0, 1043, -1, -1}};
                PQsetResultAttrs(result.get(), 2, names);
                const bool every_schema = query.find("table_schema = ") == std::string_view::npos;
                int row = 0;
                for (const auto& table : tables_) {
                    if (!every_schema && query.find("'" + table.schema + "'") == std::string_view::npos) {
                        continue;
                    }
                    PQsetvalue(result.get(), row, 0, const_cast<char*>(table.schema.c_str()), static_cast<int>(table.schema.size()));
                    PQsetvalue(result.get(), row, 1, const_cast<char*>(table.table.c_str()), static_cast<int>(table.table.size()));
                    ++row;
                }
                return result;
            }
            if (query.find("1 = 0") != std::string_view::npos) {
                for (const auto& table : tables_) {
                    if (query.find(quoted(table.schema) + "." + quoted(table.table)) == std::string_view::npos) {
                        continue;
                    }
                    std::vector<PGresAttDesc> columns;
                    for (const auto& c : table.columns) {
                        const unsigned oid = c.type() == logical_type::BIGINT   ? 20
                                             : c.type() == logical_type::DOUBLE ? 701
                                                                                : 25;
                        const int length = c.type() == logical_type::BIGINT || c.type() == logical_type::DOUBLE ? 8 : -1;
                        columns.push_back({const_cast<char*>(c.alias().c_str()), 0, 0, 0, oid, length, -1});
                    }
                    PQsetResultAttrs(result.get(), static_cast<int>(columns.size()), columns.data());
                    return result;
                }
            }
            return std::nullopt;
        }

    private:
        void note(int connection, std::string line) {
            log_.push_back(std::move(line));
            on_.push_back(connection);
        }
        std::optional<core::error_t> refusal(const std::string& sql) {
            for (const auto& [needle, code] : fail_statements_) {
                if (sql.find(needle) != std::string::npos) {
                    return core::error_t{code, std::pmr::string{("fake backend: refused " + sql).c_str(), &resource_}};
                }
            }
            return std::nullopt;
        }
        std::string quoted(const std::string& name) const {
            return kind_ == backend_type_t::PostgreSQL ? "\"" + name + "\"" : "`" + name + "`";
        }

        backend_type_t kind_;
        std::pmr::synchronized_pool_resource resource_;
        mutable std::mutex mutex_;
        std::vector<canned_table_t> tables_;
        std::vector<std::string> log_;
        std::vector<int> on_;
        scan_hook_t hook_;
        std::optional<core::error_code_t> fail_writes_;
        bool fail_commits_{false};
        std::vector<std::pair<std::string, core::error_code_t>> fail_statements_;
        bool down_{false};
        std::chrono::milliseconds connect_delay_{0};
        std::atomic<int> connects_{0};
        bool block_reads_{false};
        std::atomic<int> hanging_{0};
        int discoveries_{0};
        std::chrono::milliseconds delay_{0};
    };

    // One physical connection to a fake server: an IConnector of the dialect (Interface, its
    // status, params and handler argument). It knows whether a backend transaction is open on it
    // (a BEGIN it ran, no COMMIT / ROLLBACK since): what " @txn" in the log says. It keeps a
    // transaction the way its server would after a failed statement: PostgreSQL aborts it — every
    // later statement refused ("aborted <sql>" in the log) until a ROLLBACK, or a ROLLBACK TO a
    // savepoint it holds; its COMMIT is "commit-rolled-back" —, and a MySQL deadlock
    // (write_conflict) rolls the whole transaction back: its savepoints are gone, what follows runs
    // outside any transaction.
    template<typename Interface, typename Status, typename Params, typename Arg>
    class fake_connector_t final : public Interface {
        static constexpr bool postgresql = std::is_same_v<Arg, PGresult*>;
        static constexpr bool mysql_kind = std::is_same_v<Arg, const boost::mysql::results&>;

    public:
        fake_connector_t(fake_backend_t& server, int number)
            : server_(server)
            , number_(number) {}

        Status status() const noexcept override { return Status::Connected; }
        Params params() const noexcept override { return Params{}; }
        void close() override {
            open_ = false;
            aborted_ = false;
            savepoints_.clear();
            ++closes;
        }
        core::error_t connect() override { return core::error_t::no_error(); }
        // Connected until its server went down (set_down), then until a reconnect reached it.
        bool isConnected() override {
            if (server_.down()) {
                connected_ = false;
            }
            return connected_;
        }
        core::error_t tryReconnect() override {
            ++reconnects;
            auto failed = server_.try_connect();
            connected_ = !failed.contains_error();
            return failed;
        }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return "fake"; }
        void cancel() noexcept override { cancelled.store(true); }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(Arg)>) override {
            if (server_.reads_blocked()) {
                server_.hang();
                while (!cancelled.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                co_return server_.cancelled_error();
            }
            if (aborted_) {
                co_return refused_aborted(query);
            }
            auto rows = server_.read(number_, open_, query);
            if (rows.has_error()) {
                failed(rows.error().type);
            }
            co_return rows;
        }
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(Arg)>) override {
            if (aborted_) {
                co_return refused_aborted(query);
            }
            auto written = server_.write(number_, open_, query);
            if (written.has_error()) {
                failed(written.error().type);
            }
            co_return written;
        }
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query, otterstax::function_ref_t<otterstax::asio_error_t(Arg)> handler) override {
            if constexpr (mysql_kind) {
                if (auto metadata = server_.mysql_metadata(query)) {
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(*metadata));
                }
            } else if constexpr (postgresql) {
                if (query.find("pg_enum") != std::string_view::npos) {
                    std::unique_ptr<PGresult, decltype(&PQclear)> empty(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                        &PQclear);
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
                }
                if (auto metadata = server_.pg_metadata(query)) {
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(metadata->get()));
                }
            }
            const std::string sql{query};
            const auto savepoint = [&sql] {
                auto out = sql.substr(sql.rfind(' ') + 1);
                out.erase(std::remove_if(out.begin(), out.end(), [](char c) { return c == '"' || c == '`'; }), out.end());
                return out;
            };
            const bool ends = sql == "COMMIT" || sql == "ROLLBACK";
            const bool rollback_to = sql.rfind("ROLLBACK TO SAVEPOINT ", 0) == 0;
            if (aborted_ && !ends && !rollback_to) {
                co_return refused_aborted(query);
            }
            auto failed_step = server_.step(number_, query, aborted_ && sql == "COMMIT");
            if (sql == "BEGIN" || sql == "START TRANSACTION") {
                open_ = true;
                savepoints_.clear();
            } else if (ends) {
                open_ = false;
                aborted_ = false;
                savepoints_.clear();
            } else if (rollback_to) {
                auto held = std::find(savepoints_.begin(), savepoints_.end(), savepoint());
                if (held == savepoints_.end()) {
                    co_return core::error_t{core::error_code_t::missing_savepoint,
                                            std::pmr::string{"fake backend: SAVEPOINT does not exist", &resource_}};
                }
                savepoints_.erase(std::next(held), savepoints_.end());
                aborted_ = false;
            } else if (sql.rfind("SAVEPOINT ", 0) == 0) {
                savepoints_.push_back(savepoint());
            }
            co_return failed_step;
        }

        std::atomic<bool> cancelled{false};
        std::atomic<int> reconnects{0};
        std::atomic<int> closes{0};

    private:
        // A statement of the open transaction failed.
        void failed(core::error_code_t code) {
            if (!open_) {
                return;
            }
            if constexpr (postgresql) {
                aborted_ = true;
            } else if constexpr (mysql_kind) {
                if (code == core::error_code_t::write_conflict) {
                    open_ = false;
                    savepoints_.clear();
                }
            }
        }
        core::error_t refused_aborted(std::string_view query) {
            server_.note_line(number_, "aborted " + std::string{query});
            return core::error_t{core::error_code_t::transaction_finalized,
                                 std::pmr::string{"fake backend: current transaction is aborted", &resource_}};
        }

        fake_backend_t& server_;
        int number_;
        bool open_{false};
        bool connected_{true};
        bool aborted_{false};
        std::vector<std::string> savepoints_;
        std::pmr::synchronized_pool_resource resource_;
    };

    using fake_mysql_t = fake_connector_t<mysql::IConnector, mysql::Status, boost::mysql::connect_params, const boost::mysql::results&>;
    using fake_pg_t = fake_connector_t<pg::IConnector, pg::Status, pg::connect_params, PGresult*>;
    using fake_ch_t = fake_connector_t<ch::IConnector, ch::Status, ch::connect_params, const ch::select_result_t&>;

    // An agent over a fake connector of `kind` (its number `number`).
    inline fed::backend_agent_ptr
    fake_agent(std::pmr::memory_resource* resource, const std::string& name, backend_type_t kind, fake_backend_t& server, int number) {
        auto agent = actor_zeta::spawn<fed::backend_agent_t>(resource, name);
        switch (kind) {
            case backend_type_t::MySQL:
                agent->attach(std::make_unique<fake_mysql_t>(server, number));
                break;
            case backend_type_t::PostgreSQL:
                agent->attach(std::make_unique<fake_pg_t>(server, number));
                break;
            case backend_type_t::ClickHouse:
                agent->attach(std::make_unique<fake_ch_t>(server, number));
                break;
        }
        return agent;
    }

    // Waits on the test thread for a future an actor settles.
    template<typename T>
    T wait_ready(actor_zeta::unique_future<T> future) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!future.is_ready()) {
            REQUIRE(std::chrono::steady_clock::now() < deadline);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return std::move(future).take_ready();
    }

} // namespace fakebackend
