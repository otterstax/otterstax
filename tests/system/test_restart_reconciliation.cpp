// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The federation metadata over restarts and backend changes. Every start
// discovers the configured connections again and replaces their rows in
// federation.remote_tables / remote_columns (backends::publish_federation_metadata),
// so a restart on the SAME data dir — the way a server restart is — sees the
// backend as it is now: a table keeps answering, a table whose backend schema
// changed is read with its new columns, a table the backend dropped is gone. A
// mirror database an older OtterStax left under a connection's uid is dropped
// at startup. While the server runs, a table created on the backend, or one
// whose columns changed, is rediscovered by the statement that misses it and
// that statement is retried once. Over a PostgreSQL mock connector whose table
// catalog the test switches between (and during) runs.

#include "catalog/introspection.hpp"
#include "integration/postgresql/connection_manager.hpp"
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
        // The primary key's columns in key order (pg_index.indkey), empty for none.
        std::vector<std::string> primary_key{};
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

    // The information_schema listing's answer: one `table_name` row per table.
    pg_result_ptr listing_result(const std::vector<backend_table>& tables) {
        auto result = empty_tuples();
        PGresAttDesc attr{};
        attr.name = const_cast<char*>("table_name");
        attr.format = 0;
        attr.typid = kVarcharOid;
        attr.typlen = -1;
        attr.atttypmod = -1;
        PQsetResultAttrs(result.get(), 1, &attr);
        for (std::size_t r = 0; r < tables.size(); ++r) {
            PQsetvalue(result.get(),
                       static_cast<int>(r),
                       0,
                       const_cast<char*>(tables[r].name.c_str()),
                       static_cast<int>(tables[r].name.size()));
        }
        return result;
    }

    // The rows of make_primary_keys_query: (relname, attname) per key column, in key order per
    // table — what the query's ORDER BY relname, array_position(indkey, attnum) answers.
    pg_result_ptr primary_keys_result(const std::vector<backend_table>& tables) {
        auto result = empty_tuples();
        PGresAttDesc attrs[2]{};
        attrs[0].name = const_cast<char*>("relname");
        attrs[1].name = const_cast<char*>("attname");
        for (auto& attr : attrs) {
            attr.typid = kVarcharOid;
            attr.typlen = -1;
            attr.atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), 2, attrs);
        int row = 0;
        for (const auto& table : tables) {
            for (const auto& column : table.primary_key) {
                PQsetvalue(result.get(), row, 0, const_cast<char*>(table.name.c_str()), static_cast<int>(table.name.size()));
                PQsetvalue(result.get(), row, 1, const_cast<char*>(column.c_str()), static_cast<int>(column.size()));
                ++row;
            }
        }
        return result;
    }

    // The writes the backend received (the federation's row-change statements), in order.
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
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // Data path: kBackendRows rows of every column of the referenced table
        // (the tests only SELECT *).
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            using components::types::complex_logical_type;
            using components::types::logical_value_t;
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
                    chunk->set_value(c, row, cell(table->columns[c].type, row));
                }
            }
            chunk->set_cardinality(kBackendRows);
            co_return std::move(chunk);
        }

        // A write names one row (the federation's row-change statement): recorded, one row changed.
        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            {
                std::lock_guard guard(g_tables_mutex);
                g_writes.emplace_back(query);
            }
            co_return int64_t{1};
        }

        // Discovery: no enums, the tables' primary keys, the current table listing, the
        // referenced table's attributes for a probe.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                auto result = empty_tuples();
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            if (query.find("indisprimary") != std::string_view::npos) {
                auto result = primary_keys_result(backend_tables());
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
        components::types::logical_value_t cell(components::types::logical_type type, std::size_t row) const {
            using components::types::logical_value_t;
            switch (type) {
                case components::types::logical_type::INTEGER:
                    return logical_value_t(resource_, static_cast<int32_t>(row + 1));
                case components::types::logical_type::DOUBLE:
                    return logical_value_t(resource_, 1.5 * static_cast<double>(row + 1));
                default:
                    return logical_value_t(resource_, std::string_view{row == 0 ? "first" : "second"});
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

        // The uids of one federation metadata table.
        std::vector<std::string> listed_uids(const std::string& table) {
            auto cursor = owner_.engine().sql("SELECT uid FROM federation." + table + ";");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> uids;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    uids.emplace_back(chunk.value(0, row).value<std::string_view>());
                }
            }
            return uids;
        }

        // What the Flight SQL GetTables listing answers (catalog::list_tables
        // over its five reads), as "database.schema.table".
        std::vector<std::string> get_tables() {
            auto read = [this](std::string_view sql) {
                auto cursor = owner_.engine().sql(std::string{sql});
                REQUIRE(cursor);
                INFO(sql << ": " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
                REQUIRE_FALSE(cursor->is_error());
                return cursor;
            };
            auto namespaces = read(otterstax::catalog::namespaces_sql);
            auto classes = read(otterstax::catalog::classes_sql);
            auto remote_tables = read(otterstax::catalog::remote_tables_sql);
            otterstax::catalog::get_tables_command_t command;
            auto listed = otterstax::catalog::list_tables(owner_.engine().resource(),
                                                          command,
                                                          namespaces->chunks(),
                                                          classes->chunks(),
                                                          nullptr,
                                                          remote_tables->chunks(),
                                                          nullptr);
            REQUIRE_FALSE(listed.has_error());
            std::vector<std::string> names;
            for (const auto& table : listed.value()) {
                names.push_back(std::string{table.name.database.t.c_str()} + "." + table.name.schema.t.c_str() + "." +
                                table.name.collection.t.c_str());
            }
            return names;
        }

        // Sorted table names the federation metadata holds for the uid.
        std::vector<std::string> listed_tables() {
            auto cursor = owner_.engine().sql("SELECT table_name FROM federation.remote_tables;");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> names;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    names.emplace_back(chunk.value(0, row).value<std::string_view>());
                }
            }
            std::sort(names.begin(), names.end());
            return names;
        }

        // The (column_name, column_type) rows of a table, in ordinal order.
        std::vector<std::pair<std::string, std::string>> listed_columns(const std::string& table) {
            auto cursor = owner_.engine().sql("SELECT column_name, column_type FROM federation.remote_columns WHERE "
                                              "table_name = '" +
                                              table + "' ORDER BY ordinal;");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::pair<std::string, std::string>> columns;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    columns.emplace_back(chunk.value(0, row).value<std::string_view>(),
                                         chunk.value(1, row).value<std::string_view>());
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

TEST_CASE("restart reconciliation: after a restart the remote tables are discovered again and keep answering") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_reuse");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables() == std::vector<std::string>{"customers", "orders"});
        require_select_star(first.stack(), 9490, "orders", 2);
    }

    server_run restarted(dir.path);
    // The rows are replaced, not doubled.
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"customers", "orders"});
    REQUIRE(restarted.listed_columns("orders") ==
            std::vector<std::pair<std::string, std::string>>{{"id", "int4"}, {"name", "text"}});
    require_select_star(restarted.stack(), 9500, "orders", 2);
    require_select_star(restarted.stack(), 9501, "customers", 2);
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

    server_run restarted(dir.path);
    REQUIRE(restarted.listed_columns("orders") ==
            std::vector<std::pair<std::string, std::string>>{{"id", "int4"}, {"name", "text"}, {"price", "float8"}});
    require_select_star(restarted.stack(), 9520, "orders", 3);
    require_select_star(restarted.stack(), 9521, "customers", 2);
}

TEST_CASE("restart reconciliation: a table the backend no longer has is gone after a restart") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_drop");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables() == std::vector<std::string>{"customers", "orders"});
    }

    // customers was dropped on the backend between the runs.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name")}}});

    server_run restarted(dir.path);
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"orders"});
    REQUIRE(restarted.listed_columns("customers").empty());
    require_select_star(restarted.stack(), 9540, "orders", 2);

    // A statement naming it is a missing relation: rediscovered once, still
    // not there, the statement's own error answered.
    auto missing = run_session_sql_payload(restarted.stack(), 9541, "SELECT * FROM shop.pgdb.public.customers;");
    REQUIRE(missing.has_error());
    INFO(missing.error().what.c_str());
    CHECK(std::string{missing.error().what.c_str()}.find("does not exist") != std::string::npos);
}

TEST_CASE("restart reconciliation: a connection taken out of the configuration leaves no metadata behind") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_unconfigured");
    set_backend_tables(orders_and_customers());
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables() == std::vector<std::string>{"customers", "orders"});
        REQUIRE(first.get_tables() == std::vector<std::string>{"pgdb.public.customers", "pgdb.public.orders"});
    }

    // The next start's config.yaml no longer names the connection: its tables
    // and their columns go, and no listing shows them.
    server_run restarted(dir.path, /*configured=*/false);
    CHECK(restarted.listed_uids("remote_tables").empty());
    CHECK(restarted.listed_uids("remote_columns").empty());
    CHECK(restarted.get_tables().empty());
}

// Two connections, then a restart whose config.yaml keeps one: the dropped
// connection's rows go, the kept one's stay and it keeps answering.
TEST_CASE("restart reconciliation: two connections then one keeps only the configured one") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_two_to_one");
    set_backend_tables(orders_and_customers());
    set_unreachable({});
    {
        server_run first(dir.path, std::vector<std::string>{kUid, "outlet"});
        auto uids = first.listed_uids("remote_tables");
        REQUIRE(uids.size() == 4);
        std::sort(uids.begin(), uids.end());
        uids.erase(std::unique(uids.begin(), uids.end()), uids.end());
        REQUIRE(uids == std::vector<std::string>{"outlet", kUid});
    }

    server_run restarted(dir.path, std::vector<std::string>{kUid});
    for (const auto& uid : restarted.listed_uids("remote_tables")) {
        CHECK(uid == kUid);
    }
    for (const auto& uid : restarted.listed_uids("remote_columns")) {
        CHECK(uid == kUid);
    }
    CHECK(restarted.listed_uids("remote_tables").size() == 2);
    CHECK(restarted.get_tables() == std::vector<std::string>{"pgdb.public.customers", "pgdb.public.orders"});
    require_select_star(restarted.stack(), 9550, "orders", 2);
    auto gone = run_session_sql_payload(restarted.stack(), 9551, "SELECT * FROM outlet.pgdb.public.orders;");
    CHECK(gone.has_error());
}

// A connection still in config.yaml whose backend is down at startup is left
// out of the run (no connection opened; its statements fail at the
// connection), but it is still configured: publish_federation_metadata keeps
// the rows of its last discovery — the metadata is replaced
// only by a discovery that answered — so the listings show its tables and the
// next start that reaches the backend replaces them.
TEST_CASE("restart reconciliation: a configured connection that is unreachable keeps its rows") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_unreachable");
    set_backend_tables(orders_and_customers());
    set_unreachable({});
    {
        server_run first(dir.path);
        REQUIRE(first.listed_tables() == std::vector<std::string>{"customers", "orders"});
    }

    set_unreachable({kUid});
    {
        server_run down(dir.path);
        CHECK(down.listed_uids("remote_tables") == std::vector<std::string>{kUid, kUid});
        CHECK(down.listed_tables() == std::vector<std::string>{"customers", "orders"});
        CHECK(down.listed_columns("orders").size() == 2);
        CHECK(down.get_tables() == std::vector<std::string>{"pgdb.public.customers", "pgdb.public.orders"});
        // The name resolves from the kept rows; the read fails at the
        // connection, which was never opened — not as a missing table.
        auto read = run_session_sql_payload(down.stack(), 9560, "SELECT * FROM shop.pgdb.public.orders;");
        REQUIRE(read.has_error());
        INFO(read.error().what.c_str());
        CHECK(read.error().type != core::error_code_t::table_not_exists);
    }

    set_unreachable({});
    server_run back(dir.path);
    require_select_star(back.stack(), 9561, "orders", 2);
}

TEST_CASE("restart reconciliation: a mirror database an older OtterStax left under the uid is dropped at startup") {
    data_dir_guard dir("/tmp/otterstax_restart_legacy_mirror");
    set_backend_tables(orders_and_customers());
    {
        // What an older OtterStax left behind: the uid's database with its
        // manifest and a mirror collection under the encoded name.
        otterstax::test::test_engine_t old_run(dir.path, /*fresh=*/false);
        for (const char* sql : {"CREATE DATABASE shop;",
                                "CREATE TABLE shop.__otterstax_tables (collection string);",
                                "CREATE TABLE shop.\"pgdb:public:orders\" (id int, name string);",
                                "INSERT INTO shop.__otterstax_tables (collection) VALUES ('pgdb:public:orders');"}) {
            auto cursor = old_run.sql(sql);
            REQUIRE(cursor);
            INFO(sql << ": " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
        }
    }

    server_run upgraded(dir.path);
    auto namespaces = upgraded.engine().sql("SELECT nspname FROM pg_catalog.pg_namespace;");
    REQUIRE(namespaces);
    REQUIRE_FALSE(namespaces->is_error());
    for (const auto& chunk : namespaces->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            CHECK(chunk.value(0, row).value<std::string_view>() != "shop");
        }
    }
    REQUIRE(upgraded.listed_tables() == std::vector<std::string>{"customers", "orders"});
    require_select_star(upgraded.stack(), 9560, "orders", 2);
}

TEST_CASE("rediscovery: a table created on the backend while the server runs is found by one retry") {
    data_dir_guard dir("/tmp/otterstax_rediscover_new_table");
    set_backend_tables(orders_and_customers());
    server_run run(dir.path);
    REQUIRE(run.listed_tables() == std::vector<std::string>{"customers", "orders"});

    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name")}},
                        backend_table{"customers", {int4("id"), varchar("email")}},
                        backend_table{"fresh", {int4("id"), float8("score")}}});
    require_select_star(run.stack(), 9580, "fresh", 2);
    REQUIRE(run.listed_tables() == std::vector<std::string>{"customers", "fresh", "orders"});
}

TEST_CASE("rediscovery: a table whose columns changed while the server runs is read with the new ones") {
    data_dir_guard dir("/tmp/otterstax_rediscover_changed_table");
    set_backend_tables(orders_and_customers());
    server_run run(dir.path);
    require_select_star(run.stack(), 9600, "orders", 2);

    // The scan answers three columns where two are declared: stale, rediscovered,
    // the statement retried once.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name"), float8("price")}},
                        backend_table{"customers", {int4("id"), varchar("email")}}});
    require_select_star(run.stack(), 9601, "orders", 3);
    REQUIRE(run.listed_columns("orders").size() == 3);
}

// A table the backend spells in mixed case is rediscovered under that spelling
// (federation.remote_tables keeps it), not under the lower-case name the
// statement wrote: a backend whose names are case-sensitive would not find the
// lower-cased one.
TEST_CASE("rediscovery: a stale table is rediscovered under the backend's own spelling") {
    data_dir_guard dir("/tmp/otterstax_rediscover_spelling");
    set_backend_tables({backend_table{"Mixed", {int4("id"), varchar("name")}}});
    server_run run(dir.path);
    REQUIRE(run.listed_tables() == std::vector<std::string>{"mixed"});
    require_select_star(run.stack(), 9620, "mixed", 2);

    // The mock answers a probe of `public.Mixed` only.
    set_backend_tables({backend_table{"Mixed", {int4("id"), varchar("name"), float8("price")}}});
    require_select_star(run.stack(), 9621, "mixed", 3);
    REQUIRE(run.listed_columns("mixed").size() == 3);
}

TEST_CASE("primary key: a composite PostgreSQL key is recorded in key order and names the row a change writes") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_primary_key");
    // PRIMARY KEY (b, a): the key order is not the column order.
    set_backend_tables({backend_table{"orders", {int4("a"), int4("b"), varchar("name")}, {"b", "a"}},
                        backend_table{"notes", {int4("id"), varchar("text")}}});
    take_writes();
    server_run run(dir.path);

    auto keys = run.engine().sql("SELECT table_name, column_name, key_position FROM federation.remote_columns "
                                 "ORDER BY table_name, ordinal;");
    REQUIRE(keys);
    INFO((keys->is_error() ? keys->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(keys->is_error());
    std::vector<std::tuple<std::string, std::string, std::int64_t>> recorded;
    for (const auto& chunk : keys->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            recorded.emplace_back(chunk.value(0, row).value<std::string_view>(),
                                  chunk.value(1, row).value<std::string_view>(),
                                  chunk.value(2, row).value<std::int64_t>());
        }
    }
    CHECK(recorded == std::vector<std::tuple<std::string, std::string, std::int64_t>>{{"notes", "id", 0},
                                                                                      {"notes", "text", 0},
                                                                                      {"orders", "a", 2},
                                                                                      {"orders", "b", 1},
                                                                                      {"orders", "name", 0}});

    // A row the engine picks (the IN over a local table keeps the statement in the engine) is
    // changed on the backend by its key, the key's columns in key order.
    REQUIRE_FALSE(run.engine().sql("CREATE DATABASE loc;")->is_error());
    REQUIRE_FALSE(run.engine().sql("CREATE TABLE loc.ids (id INT);")->is_error());
    REQUIRE_FALSE(run.engine().sql("INSERT INTO loc.ids (id) VALUES (2);")->is_error());
    auto updated = run.engine().sql(std::string{"UPDATE "} + kUid + "." + kSchema +
                                    ".orders SET name = 'z' WHERE a IN (SELECT id FROM loc.ids);");
    REQUIRE(updated);
    INFO((updated->is_error() ? updated->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(updated->is_error());
    CHECK(updated->affected_rows() == 1);
    CHECK(take_writes() ==
          std::vector<std::string>{R"(UPDATE "public"."orders" SET "name" = 'z' WHERE "b" = 2 AND "a" = 2)"});

    // A table without a key has no row a change could name: refused before anything is sent.
    auto refused = run.engine().sql(std::string{"UPDATE "} + kUid + "." + kSchema +
                                    ".notes SET text = 'z' WHERE id IN (SELECT id FROM loc.ids);");
    REQUIRE(refused);
    REQUIRE(refused->is_error());
    CHECK(std::string{refused->get_error().what.c_str()}.find("has no primary key") != std::string::npos);
    CHECK(take_writes().empty());
}
