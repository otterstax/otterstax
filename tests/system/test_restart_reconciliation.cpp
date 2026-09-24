// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Registration of a connection on a data dir a previous run persisted. The
// engine restores its catalog from disk, so the uid's mirror database and its
// collections already exist when the catalog registers the connection again;
// registration must reconcile against them instead of refusing: the database
// is reused, a table whose columns still match keeps its engine OID, a table
// whose backend schema changed is recreated, a table the backend no longer
// has is dropped. Every run below is a fresh engine over the SAME data dir,
// the way a server restart is, over a PostgreSQL mock connector whose table
// catalog the test switches between runs.

#include "catalog/catalog_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "scheduler_stack.hpp"
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
#include <utility>
#include <vector>

using otterstax::test::init_test_otterbrix;
using otterstax::test::make_az_scheduler;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::worker_pool_size;

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
        core::error_t connect() override { return core::error_t::no_error(); }
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

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery: no enums, the current table listing, the referenced table's
        // attributes for a probe.
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

    using otterstax::test::wait_until_ready;

    // One server run: a real engine over `data_dir` — which is NOT cleared, a
    // restart sees what the previous run persisted — real parser and catalog,
    // the PostgreSQL connection over the mock connector. Destroying the run
    // checkpoints the engine, as a server shutdown does.
    class server_run {
    public:
        explicit server_run(const std::string& data_dir)
            : otterbrix_(init_test_otterbrix(data_dir))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(resource_,
                                                              catalog_->address(),
                                                              &catalog_pg_factory,
                                                              /*pool_size*/ 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(resource_, pg_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        server_run(const server_run&) = delete;
        server_run& operator=(const server_run&) = delete;

        ~server_run() {
            scheduler_.reset();
            pg_mgr_.reset();
            pg_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
        }

        // Registers the connection the way startup does: whole-schema discovery
        // over the mock's current table catalog.
        core::result_wrapper_t<std::string> register_connection() {
            conn::api_server::PgConnectionParams params;
            params.alias = kUid;
            params.host = "localhost";
            params.port = "5432";
            params.username = "user";
            params.password = "pass";
            params.database = "pgdb";
            params.schema = kSchema;
            params.table = "";
            return pg_conn_->addConnection(params);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

        // The engine OID the catalog stamps on a SELECT over `table` — what the
        // store holds for it.
        components::catalog::oid_t stamped_oid(const std::string& table) {
            auto parser = make_parser(resource_);
            auto parsed = parser->parse("SELECT * FROM " + std::string{kUid} + ".pgdb." + kSchema + "." + table + ";");
            REQUIRE_FALSE(parsed.has_error());
            auto [needs_sched, future] = actor_zeta::send(catalog_->address(),
                                                          &mysql::CatalogManager::update_backend_type,
                                                          session_hash_t{7},
                                                          std::move(parsed.value()));
            wait_until_ready(future);
            auto classified = std::move(future).take_ready();
            INFO("update_backend_type: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
            REQUIRE_FALSE(classified.has_error());
            const auto& slots = classified.value()->otterbrix_params->external_nodes;
            REQUIRE(slots.size() == 1);
            REQUIRE(slots.front().size() == 1);
            return slots.front().front().target.oid;
        }

        // Sorted table names the catalog lists for the uid's database.
        std::vector<std::string> listed_tables() {
            arrow::flight::sql::GetTables command;
            command.include_schema = false;
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_tables, command);
            wait_until_ready(future);
            auto listed = std::move(future).take_ready();
            REQUIRE_FALSE(listed.has_error());
            std::vector<std::string> names;
            for (const auto& table : listed.value()) {
                names.push_back(table.name.collection);
            }
            std::sort(names.begin(), names.end());
            return names;
        }

        // An engine collection of the uid's database as the engine holds it:
        // its pg_class oid and (name, type) columns. A collection the engine
        // does not hold is a test failure here — absence is asserted through
        // engine_verdict below.
        struct engine_table {
            components::catalog::oid_t oid;
            std::vector<std::pair<std::string, components::types::logical_type>> columns;
        };

        engine_table engine_collection(const std::string& collection) {
            components::catalog::oid_t oid = components::catalog::INVALID_OID;
            auto cursor = make_otterbrix_manager(otterbrix_)->describe_collection(kUid, collection, oid);
            REQUIRE(cursor);
            INFO("describe_collection: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            REQUIRE(cursor->type_data().size() == 1);
            engine_table table{oid, {}};
            for (const auto& column : cursor->type_data().front().child_types()) {
                table.columns.emplace_back(column.alias(), column.type());
            }
            return table;
        }

        // The engine's error code for describing a collection of the uid's
        // database (table_not_exists for one that is gone).
        core::error_code_t engine_verdict(const std::string& collection) {
            components::catalog::oid_t oid = components::catalog::INVALID_OID;
            auto cursor = make_otterbrix_manager(otterbrix_)->describe_collection(kUid, collection, oid);
            REQUIRE(cursor);
            REQUIRE(cursor->is_error());
            return cursor->get_error().type;
        }

        // Sorted encoded collection names the uid's mirror manifest lists.
        std::vector<std::string> manifest() {
            auto cursor = make_otterbrix_manager(otterbrix_)->execute_sql(
                sql_gen::select_column_statement(kUid, db::external_manifest_collection, db::external_manifest_column));
            REQUIRE(cursor);
            INFO("manifest: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> rows;
            while (cursor->has_next()) {
                cursor->advance();
                rows.emplace_back(cursor->value(0).value<std::string_view>());
            }
            std::sort(rows.begin(), rows.end());
            return rows;
        }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_scheduler() {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           pg_mgr_->address(),
                                           actor_zeta::address_t::empty_address());
            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                actor_zeta::address_t::empty_address(), // sql
                                                pg_mgr_->address(),
                                                actor_zeta::address_t::empty_address(), // ch
                                                otb_mgr_->address(),
                                                catalog_->address(),
                                                actor_zeta::address_t::empty_address(), // s3
                                                actor_zeta::address_t::empty_address()); // file
        }

        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<pg::ConnectorManager> pg_conn_;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
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

    std::string encoded(const std::string& table) { return "pgdb:" + std::string{kSchema} + ":" + table; }

    std::vector<backend_table> orders_and_customers() {
        return {backend_table{"orders", {int4("id"), varchar("name")}},
                backend_table{"customers", {int4("id"), varchar("email")}}};
    }

    void require_registered(const core::result_wrapper_t<std::string>& added) {
        INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
        REQUIRE_FALSE(added.has_error());
    }

    // SELECT * over the external table through the Scheduler: the mock's rows
    // under the discovered columns.
    void require_select_star(const scheduler_stack& s,
                             session_hash_t id,
                             const std::string& table,
                             std::size_t column_count) {
        auto payload = run_scheduler_sql_payload(
            s, id, "SELECT * FROM " + std::string{kUid} + ".pgdb." + kSchema + "." + table + ";");
        INFO("SELECT: " << (payload.has_error() ? payload.error().what.c_str() : "ok"));
        REQUIRE_FALSE(payload.has_error());
        REQUIRE(payload.value().size() == kBackendRows);
        REQUIRE(payload.value().column_count() == column_count);
    }

} // namespace

TEST_CASE("restart reconciliation: a persisted mirror database is reused and its tables keep their OIDs") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_reuse");
    set_backend_tables(orders_and_customers());

    components::catalog::oid_t orders_oid = components::catalog::INVALID_OID;
    components::catalog::oid_t customers_oid = components::catalog::INVALID_OID;
    {
        server_run first(dir.path);
        require_registered(first.register_connection());
        orders_oid = first.stamped_oid("orders");
        customers_oid = first.stamped_oid("customers");
        REQUIRE(orders_oid != components::catalog::INVALID_OID);
        REQUIRE(customers_oid != components::catalog::INVALID_OID);
        REQUIRE(orders_oid != customers_oid);
        REQUIRE(first.engine_collection(encoded("orders")).oid == orders_oid);
        REQUIRE(first.engine_collection(encoded("customers")).oid == customers_oid);
        REQUIRE(first.manifest() == std::vector<std::string>{encoded("customers"), encoded("orders")});
    }

    server_run restarted(dir.path);
    require_registered(restarted.register_connection());

    // The store resolves both tables to the OIDs the engine restored.
    REQUIRE(restarted.stamped_oid("orders") == orders_oid);
    REQUIRE(restarted.stamped_oid("customers") == customers_oid);
    REQUIRE(restarted.engine_collection(encoded("orders")).oid == orders_oid);
    REQUIRE(restarted.engine_collection(encoded("customers")).oid == customers_oid);
    REQUIRE(restarted.manifest() == std::vector<std::string>{encoded("customers"), encoded("orders")});
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"customers", "orders"});

    require_select_star(restarted.stack(), 9500, "orders", 2);
    require_select_star(restarted.stack(), 9501, "customers", 2);
}

TEST_CASE("restart reconciliation: a table whose backend schema changed is recreated with the new columns") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_recreate");
    set_backend_tables(orders_and_customers());

    components::catalog::oid_t orders_oid = components::catalog::INVALID_OID;
    components::catalog::oid_t customers_oid = components::catalog::INVALID_OID;
    {
        server_run first(dir.path);
        require_registered(first.register_connection());
        orders_oid = first.stamped_oid("orders");
        customers_oid = first.stamped_oid("customers");
    }

    // The backend gained a column on orders; customers is unchanged.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name"), float8("price")}},
                        backend_table{"customers", {int4("id"), varchar("email")}}});

    server_run restarted(dir.path);
    require_registered(restarted.register_connection());

    const auto new_orders_oid = restarted.stamped_oid("orders");
    REQUIRE(new_orders_oid != components::catalog::INVALID_OID);
    REQUIRE(new_orders_oid != orders_oid);
    REQUIRE(restarted.stamped_oid("customers") == customers_oid);

    using components::types::logical_type;
    const std::vector<std::pair<std::string, logical_type>> expected{{"id", logical_type::INTEGER},
                                                                     {"name", logical_type::STRING_LITERAL},
                                                                     {"price", logical_type::DOUBLE}};
    const auto orders = restarted.engine_collection(encoded("orders"));
    REQUIRE(orders.oid == new_orders_oid);
    REQUIRE(orders.columns == expected);
    REQUIRE(restarted.engine_collection(encoded("customers")).oid == customers_oid);
    REQUIRE(restarted.manifest() == std::vector<std::string>{encoded("customers"), encoded("orders")});

    require_select_star(restarted.stack(), 9520, "orders", 3);
}

TEST_CASE("restart reconciliation: a table the backend no longer has is dropped from the engine") {
    data_dir_guard dir("/tmp/otterstax_restart_reconcile_drop");
    set_backend_tables(orders_and_customers());

    components::catalog::oid_t orders_oid = components::catalog::INVALID_OID;
    {
        server_run first(dir.path);
        require_registered(first.register_connection());
        orders_oid = first.stamped_oid("orders");
        REQUIRE(first.manifest() == std::vector<std::string>{encoded("customers"), encoded("orders")});
    }

    // customers was dropped on the backend between the runs.
    set_backend_tables({backend_table{"orders", {int4("id"), varchar("name")}}});

    server_run restarted(dir.path);
    require_registered(restarted.register_connection());

    REQUIRE(restarted.stamped_oid("orders") == orders_oid);
    REQUIRE(restarted.engine_collection(encoded("orders")).oid == orders_oid);
    REQUIRE(restarted.listed_tables() == std::vector<std::string>{"orders"});

    // The dropped mirror is gone from the engine and from the manifest, not
    // only from the store.
    REQUIRE(restarted.engine_verdict(encoded("customers")) == core::error_code_t::table_not_exists);
    REQUIRE(restarted.manifest() == std::vector<std::string>{encoded("orders")});

    require_select_star(restarted.stack(), 9540, "orders", 2);
}
