// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The registration of the remote tables over restarts and backend changes
// (catalog::register_connections, backends::register_remote_tables). Every start
// discovers the configured connections again and brings otterbrix's catalog in
// line, on the backend actor's registration session (no SQL to a backend), so a
// restart on the SAME data dir — the way a server restart is — sees the backend
// as it is now: a table keeps answering, a table whose backend schema changed is
// read with its new columns, a table the backend dropped is gone. A connection
// taken out of config.yaml keeps its databases (a warning; its statements answer
// that the connection does not exist). While the server runs, a table created on
// the backend is registered by the statement that misses it, which is retried
// once (the query session). Over a PostgreSQL mock connector whose table catalog
// the test switches between (and during) runs.

#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"

#include <catch2/catch_all.hpp>
#include <libpq-fe.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;

namespace {

    constexpr const char* kUid = "shop";
    constexpr const char* kSchema = "public";

    constexpr Oid kInt4Oid = 23;
    constexpr Oid kFloat8Oid = 701;
    constexpr Oid kVarcharOid = 1043;

    // One column of a backend table: name, PostgreSQL type oid and the logical
    // type pg_to_struct / pg_to_chunk map it to.
    struct backend_column {
        std::string name;
        Oid typid;
        components::types::logical_type type;
    };

    struct backend_table {
        std::string name;
        std::vector<backend_column> columns;
    };

    backend_column int4(std::string name) {
        return {std::move(name), kInt4Oid, components::types::logical_type::INTEGER};
    }
    backend_column varchar(std::string name) {
        return {std::move(name), kVarcharOid, components::types::logical_type::STRING_LITERAL};
    }
    backend_column float8(std::string name) {
        return {std::move(name), kFloat8Oid, components::types::logical_type::DOUBLE};
    }

    // connector_factory is a plain function pointer, so the backend's table
    // catalog the connector answers from is shared through this; the test
    // switches it between runs, while no stack is alive.
    std::mutex g_tables_mutex;
    std::vector<backend_table> g_tables;

    void set_backend_tables(std::vector<backend_table> tables) {
        std::lock_guard guard(g_tables_mutex);
        g_tables = std::move(tables);
    }

    std::vector<backend_table> backend_tables() {
        std::lock_guard guard(g_tables_mutex);
        return g_tables;
    }

    // The aliases whose backend refuses every connection (a server that is
    // down), switched between runs like the table catalog.
    std::vector<std::string> g_unreachable;

    void set_unreachable(std::vector<std::string> aliases) {
        std::lock_guard guard(g_tables_mutex);
        g_unreachable = std::move(aliases);
    }

    bool unreachable(const std::string& alias) {
        std::lock_guard guard(g_tables_mutex);
        return std::find(g_unreachable.begin(), g_unreachable.end(), alias) != g_unreachable.end();
    }

    // The table a generated statement names: the PostgreSQL reference is
    // `schema.table`, quoted per identifier when needed.
    const backend_table* referenced_table(const std::vector<backend_table>& tables, std::string_view query) {
        std::string unquoted;
        unquoted.reserve(query.size());
        for (char c : query) {
            if (c != '"') {
                unquoted.push_back(c);
            }
        }
        for (const auto& table : tables) {
            const std::string reference = std::string{kSchema} + "." + table.name;
            const auto at = unquoted.find(reference);
            if (at == std::string::npos) {
                continue;
            }
            const auto end = at + reference.size();
            if (end == unquoted.size() || !(std::isalnum(static_cast<unsigned char>(unquoted[end])) ||
                                            unquoted[end] == '_')) {
                return &table;
            }
        }
        return nullptr;
    }

    using pg_result_ptr = std::unique_ptr<PGresult, decltype(&PQclear)>;

    pg_result_ptr empty_tuples() { return pg_result_ptr(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear); }

    // The LIMIT-0 schema probe's answer: the table's attributes, no tuples.
    pg_result_ptr probe_result(const backend_table& table) {
        auto result = empty_tuples();
        std::vector<PGresAttDesc> attrs(table.columns.size());
        for (std::size_t c = 0; c < table.columns.size(); ++c) {
            attrs[c].name = const_cast<char*>(table.columns[c].name.c_str());
            attrs[c].tableid = 0;
            attrs[c].columnid = 0;
            attrs[c].format = 0;
            attrs[c].typid = table.columns[c].typid;
            attrs[c].typlen = -1;
            attrs[c].atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        return result;
    }

    // The information_schema listing's answer: one (table_schema, table_name) row per table.
    pg_result_ptr listing_result(const std::vector<backend_table>& tables) {
        auto result = empty_tuples();
        PGresAttDesc attrs[2]{};
        attrs[0].name = const_cast<char*>("table_schema");
        attrs[1].name = const_cast<char*>("table_name");
        for (auto& attr : attrs) {
            attr.typid = kVarcharOid;
            attr.typlen = -1;
            attr.atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), 2, attrs);
        for (std::size_t r = 0; r < tables.size(); ++r) {
            PQsetvalue(result.get(), static_cast<int>(r), 0, const_cast<char*>(kSchema), static_cast<int>(std::string_view{kSchema}.size()));
            PQsetvalue(result.get(),
                       static_cast<int>(r),
                       1,
                       const_cast<char*>(tables[r].name.c_str()),
                       static_cast<int>(tables[r].name.size()));
        }
        return result;
    }

    // The writes the backend received, in order.
    std::vector<std::string> g_writes;

    std::vector<std::string> take_writes() {
        std::lock_guard guard(g_tables_mutex);
        return std::exchange(g_writes, {});
    }

    constexpr std::size_t kBackendRows = 2;

    class catalog_pg_connector final : public pg::IConnector {
    public:
        catalog_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        pg::Status status() const noexcept override { return pg::Status::Connected; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override {
            if (unreachable(alias_)) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"mock backend: connection refused", resource_});
            }
            return core::error_t::no_error();
        }
        // A backend that is down answers no ping and refuses a reconnect, as a real one does.
        bool isConnected() override { return !unreachable(alias_); }
        core::error_t tryReconnect() override { return connect(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // Data path: kBackendRows rows of every column of the referenced table
        // (the tests only SELECT *).
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            using components::types::complex_logical_type;
            const auto tables = backend_tables();
            const auto* table = referenced_table(tables, query);
            if (table == nullptr) {
                co_return core::error_t(core::error_code_t::table_not_exists,
                                        std::pmr::string{"mock backend: unknown table in query", resource_});
            }
            std::pmr::vector<complex_logical_type> fields(resource_);
            for (const auto& column : table->columns) {
                fields.emplace_back(column.type, column.name);
            }
            auto chunk = std::make_unique<data_chunk_t>(resource_, fields, kBackendRows);
            for (std::size_t c = 0; c < table->columns.size(); ++c) {
                for (std::size_t row = 0; row < kBackendRows; ++row) {
                    set_cell(*chunk, c, table->columns[c].type, row);
                }
            }
            chunk->set_cardinality(kBackendRows);
            co_return std::move(chunk);
        }

        // A write: recorded, one row changed.
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            {
                std::lock_guard guard(g_tables_mutex);
                g_writes.emplace_back(query);
            }
            co_return int64_t{1};
        }

        // Discovery: no enums, the current table listing, the
        // referenced table's attributes for a probe.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                auto result = empty_tuples();
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            const auto tables = backend_tables();
            if (query.find("information_schema.tables") != std::string_view::npos) {
                auto result = listing_result(tables);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            const auto* table = referenced_table(tables, query);
            if (table == nullptr) {
                co_return core::error_t(core::error_code_t::table_not_exists,
                                        std::pmr::string{"mock backend: probe of an unknown table", resource_});
            }
            auto result = probe_result(*table);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        static void
        set_cell(data_chunk_t& chunk, std::size_t column, components::types::logical_type type, std::size_t row) {
            switch (type) {
                case components::types::logical_type::INTEGER:
                    chunk.set_value(column, row, static_cast<int32_t>(row + 1));
                    break;
                case components::types::logical_type::DOUBLE:
                    chunk.set_value(column, row, 1.5 * static_cast<double>(row + 1));
                    break;
                default:
                    chunk.set_value(column, row, std::string_view{row == 0 ? "first" : "second"});
                    break;
            }
        }

        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    catalog_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<catalog_pg_connector>(resource, std::move(params), std::move(alias));
    }

    // One server run: a real engine over `data_dir` — which is NOT cleared, a
    // restart sees what the previous run persisted — real parser, the
    // PostgreSQL connection over the mock connector, opened and discovered at
    // startup (whole schema). Destroying the run checkpoints the engine, as a
    // server shutdown does.
    class server_run {
    public:
        // `configured` false: the run's config.yaml has no connection at all
        // (the PostgreSQL one was taken out of it).
        explicit server_run(const std::string& data_dir, bool configured = true)
            : owner_(data_dir, options(configured)) {}

        // The run's config.yaml names these PostgreSQL connections, each over
        // database `pgdb` of the same mock backend.
        server_run(const std::string& data_dir, const std::vector<std::string>& aliases)
            : owner_(data_dir, options(aliases)) {}

        server_run(const server_run&) = delete;
        server_run& operator=(const server_run&) = delete;

        session_stack stack() const { return owner_.stack(); }
        otterstax::test::test_engine_t& engine() { return owner_.engine(); }

        // The connection's tables otterbrix's catalog keeps ("<connection>.<database>.<schema>.<table>"),
        // sorted; one connection when `connection` is set.
        std::vector<std::string> listed_tables(const std::string& connection = {}) {
            auto cursor = owner_.engine().sql(
                "SELECT d.datconnection, d.datname, n.nspname, c.relname FROM pg_catalog.pg_database d JOIN "
                "pg_catalog.pg_namespace n ON n.nspdatabase = d.oid JOIN pg_catalog.pg_class c ON c.relnamespace = "
                "n.oid WHERE d.datconnection <> '' AND c.relkind = 'r';");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> names;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    const std::string owner{chunk.data[0].get_value<std::string_view>(row)};
                    if (!connection.empty() && owner != connection) {
                        continue;
                    }
                    names.push_back(owner + "." + std::string{chunk.data[1].get_value<std::string_view>(row)} + "." +
                                    std::string{chunk.data[2].get_value<std::string_view>(row)} + "." +
                                    std::string{chunk.data[3].get_value<std::string_view>(row)});
                }
            }
            std::sort(names.begin(), names.end());
            return names;
        }

        // The live column names of a table of the connection, in attnum order.
        std::vector<std::string> listed_columns(const std::string& table) {
            auto cursor = owner_.engine().sql(
                "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                "WHERE c.relname = '" +
                table + "' AND a.attisdropped = false ORDER BY a.attnum;");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> columns;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    columns.emplace_back(chunk.data[0].get_value<std::string_view>(row));
                }
            }
            return columns;
        }

    private:
        // A restart sees what the previous run persisted: the directory is not
        // wiped (data_dir_guard below owns it).
        static otterstax::test::stack_options_t options(bool configured) {
            otterstax::test::stack_options_t options;
            options.connectors.postgres = &catalog_pg_factory;
            options.fresh = false;
            if (!configured) {
                return options;
            }
            options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = kUid,
                                                                              .host = "localhost",
                                                                              .port = "5432",
                                                                              .username = "user",
                                                                              .password = "pass",
                                                                              .database = "pgdb",
                                                                              .schema = kSchema,
                                                                              .table = ""});
            return options;
        }

        static otterstax::test::stack_options_t options(const std::vector<std::string>& aliases) {
            otterstax::test::stack_options_t options;
            options.connectors.postgres = &catalog_pg_factory;
            options.fresh = false;
            for (const auto& alias : aliases) {
                options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = alias,
                                                                                  .host = "localhost",
                                                                                  .port = "5432",
                                                                                  .username = "user",
                                                                                  .password = "pass",
                                                                                  .database = "pgdb",
                                                                                  .schema = kSchema,
                                                                                  .table = ""});
            }
            return options;
        }

        otterstax::test::session_stack_owner owner_;
    };

    // The data dir of one test: cleared before the first run and after the last.
    struct data_dir_guard {
        explicit data_dir_guard(std::string path)
            : path(std::move(path)) {
            std::filesystem::remove_all(this->path);
        }
        ~data_dir_guard() { std::filesystem::remove_all(path); }
        std::string path;
    };

    std::vector<backend_table> orders_and_customers() {
        return {backend_table{"orders", {int4("id"), varchar("name")}},
                backend_table{"customers", {int4("id"), varchar("email")}}};
    }

    // SELECT * over the external table through a session: the mock's rows
    // under the discovered columns.
    void require_select_star(const session_stack& s,
                             session_hash_t id,
                             const std::string& table,
                             std::size_t column_count) {
        auto payload = run_session_sql_payload(
            s, id, "SELECT * FROM " + std::string{kUid} + ".pgdb." + kSchema + "." + table + ";");
        INFO("SELECT: " << (payload.has_error() ? payload.error().what.c_str() : "ok"));
        REQUIRE_FALSE(payload.has_error());
        REQUIRE(payload.value().size() == kBackendRows);
        REQUIRE(payload.value().column_count() == column_count);
    }

} // namespace

TEST_CASE("restart reconciliation: after a restart the remote tables are registered once and keep answering") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_reuse");
    set_backend_tables(orders_and_customers());
    take_writes();
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables() == std::vector<std::string>{"shop.pgdb.public.customers", "shop.pgdb.public.orders"});
        require_select_star(first.stack(), 9490, "orders", 2);
    }

    server_run restarted(dir.path);
    // Registered once, not twice; no DDL reached the backend.
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"shop.pgdb.public.customers", "shop.pgdb.public.orders"});
    REQUIRE(restarted.listed_columns("orders") == std::vector<std::string>{"id", "name"});
    require_select_star(restarted.stack(), 9500, "orders", 2);
    require_select_star(restarted.stack(), 9501, "customers", 2);
    CHECK(take_writes().empty());
}

TEST_CASE("restart reconciliation: a table whose backend schema changed is read with the new columns") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_recreate");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_columns("orders").size() == 2);
    }

    // The backend gained a column on orders; customers is unchanged.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name"), float8("price")}},
                        backend_table{"customers", {int4("id"), varchar("email")}}});

    take_writes();
    server_run restarted(dir.path);
    REQUIRE(restarted.listed_columns("orders") == std::vector<std::string>{"id", "name", "price"});
    require_select_star(restarted.stack(), 9520, "orders", 3);
    require_select_star(restarted.stack(), 9521, "customers", 2);
    CHECK(take_writes().empty());
}

TEST_CASE("restart reconciliation: a table the backend no longer has is gone after a restart") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_drop");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables().size() == 2);
    }

    // customers was dropped on the backend between the runs.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name")}}});

    take_writes();
    server_run restarted(dir.path);
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"shop.pgdb.public.orders"});
    require_select_star(restarted.stack(), 9540, "orders", 2);
    CHECK(take_writes().empty());

    // A statement naming it misses: a registration finds nothing, the statement's own error answers.
    auto missing = run_session_sql_payload(restarted.stack(), 9541, "SELECT * FROM shop.pgdb.public.customers;");
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::table_not_exists);
}

TEST_CASE("restart reconciliation: a connection taken out of the configuration keeps its databases") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_unconfigured");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables().size() == 2);
    }

    // The next start's config.yaml no longer names the connection: its catalog rows stay (a warning),
    // and a statement over them answers that the connection does not exist.
    server_run restarted(dir.path, /*configured=*/false);
    CHECK(restarted.listed_tables().size() == 2);
    auto read = run_session_sql_payload(restarted.stack(), 9545, "SELECT * FROM shop.pgdb.public.orders;");
    REQUIRE(read.has_error());
    CHECK(read.error().type == core::error_code_t::undefined_object);
}

// Two connections, then a restart whose config.yaml keeps one: the kept one keeps answering, the other
// one's statements answer that its connection does not exist.
TEST_CASE("restart reconciliation: two connections then one keeps serving the configured one") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_two_to_one");
    set_backend_tables(orders_and_customers());
    set_unreachable({});
    {
        server_run first(dir.path, std::vector<std::string>{kUid, "outlet"});
        REQUIRE(first.listed_tables("outlet").size() == 2);
        REQUIRE(first.listed_tables(kUid).size() == 2);
    }

    server_run restarted(dir.path, std::vector<std::string>{kUid});
    CHECK(restarted.listed_tables(kUid) == std::vector<std::string>{"shop.pgdb.public.customers", "shop.pgdb.public.orders"});
    require_select_star(restarted.stack(), 9550, "orders", 2);
    auto gone = run_session_sql_payload(restarted.stack(), 9551, "SELECT * FROM outlet.pgdb.public.orders;");
    REQUIRE(gone.has_error());
    CHECK(gone.error().type == core::error_code_t::undefined_object);
}

// A connection still in config.yaml whose backend is down at startup is not discovered (its agents
// connect on their first statement, which fails at the connection while the backend is down), but
// its catalog rows stay as they were:
// only a discovery that answered changes them, and the next start that reaches the backend does.
TEST_CASE("restart reconciliation: a configured connection that is unreachable keeps its tables") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_unreachable");
    set_backend_tables(orders_and_customers());
    set_unreachable({});
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables().size() == 2);
    }

    set_unreachable({kUid});
    {
        server_run down(dir.path);
        CHECK(down.listed_tables().size() == 2);
        CHECK(down.listed_columns("orders").size() == 2);
        // The name resolves; the read fails at the connection, which was never opened — not as a
        // missing table.
        auto read = run_session_sql_payload(down.stack(), 9560, "SELECT * FROM shop.pgdb.public.orders;");
        REQUIRE(read.has_error());
        INFO(read.error().what.c_str());
        CHECK(read.error().type != core::error_code_t::table_not_exists);
    }

    set_unreachable({});
    server_run back(dir.path);
    require_select_star(back.stack(), 9561, "orders", 2);
}

TEST_CASE("registration by miss: a table created on the backend while the server runs is found by one retry") {
    data_dir_guard dir("/tmp/otterstax_rediscover_new_table");
    set_backend_tables(orders_and_customers());
    server_run run(dir.path);
    REQUIRE(run.listed_tables().size() == 2);

    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name")}},
                        backend_table{"customers", {int4("id"), varchar("email")}},
                        backend_table{"fresh", {int4("id"), float8("score")}}});
    take_writes();
    require_select_star(run.stack(), 9580, "fresh", 2);
    REQUIRE(run.listed_tables().size() == 3);
    CHECK(take_writes().empty());
}
