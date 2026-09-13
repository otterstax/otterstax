// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Schema discovery runs on the backend actor that owns the connector manager
// (the catalog only sends `discover`). What is pinned here: the metadata
// queries that enrich a table's schema — PostgreSQL ENUM labels, ClickHouse
// named types — are part of the discovery, so their failure refuses the
// registration instead of degrading it silently; the ClickHouse named-types
// query quotes the names it embeds; and a rediscovered table carries the
// backend's current types, not the ones seen first.

#include "catalog/catalog_manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "../mock/pg_db_connector.hpp"

#include <actor-zeta.hpp>
#include <catch2/catch_all.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <core/result_wrapper.hpp>
#include <libpq-fe.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

    using otterstax::test::wait_until_ready;

    // ── PostgreSQL: the ENUM query fails, the rest of the discovery would not ──

    class pg_enum_failing_connector final : public pg::IConnector {
    public:
        pg_enum_failing_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            co_return std::unique_ptr<data_chunk_t>{};
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated pg_enum failure", resource_});
            }
            std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                 &PQclear);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    pg_enum_failing_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<pg_enum_failing_connector>(resource, std::move(params), std::move(alias));
    }

    // ── ClickHouse: a connector that records its metadata queries ──────────────

    // connector_factory is a plain function pointer, so the recording and the
    // type the connector reports are shared through these; every access is
    // ordered by the future the io-thread query settles.
    std::mutex g_ch_queries_mutex;
    std::vector<std::string> g_ch_queries;
    // The named type the connector reports for column "t"; switched between
    // discoveries.
    std::atomic<int> g_ch_type_variant{0};
    // Rows of the system.columns answer: 0 rows describes no column.
    std::atomic<bool> g_ch_named_types_malformed{false};

    std::vector<std::string> take_recorded_queries() {
        std::lock_guard guard(g_ch_queries_mutex);
        return std::exchange(g_ch_queries, {});
    }

    clickhouse::Block ch_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        auto types = std::make_shared<clickhouse::ColumnString>();
        names->Append("t");
        types->Append(g_ch_type_variant.load() == 0 ? "String" : "Tuple(a Int32)");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        block.AppendColumn("type", types);
        return block;
    }

    // One column, one row: not the (name, type) pair system.columns answers with.
    clickhouse::Block ch_malformed_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        names->Append("t");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        return block;
    }

    // A 0-row header block describing the column system.columns reports —
    // (t String), or (t Tuple(Int32)) for the named tuple: what the
    // `WHERE 1 = 0` schema probe streams back.
    clickhouse::Block ch_probe_block() {
        clickhouse::Block block;
        if (g_ch_type_variant.load() == 0) {
            block.AppendColumn("t", std::make_shared<clickhouse::ColumnString>());
        } else {
            block.AppendColumn("t",
                               std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
                                   std::make_shared<clickhouse::ColumnInt32>()}));
        }
        return block;
    }

    class ch_recording_connector final : public ch::IConnector {
    public:
        ch_recording_connector(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            ch::select_result_t outcome;
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            {
                std::lock_guard guard(g_ch_queries_mutex);
                g_ch_queries.emplace_back(query);
            }
            ch::select_result_t answer;
            if (query.find("system.columns") != std::string_view::npos) {
                answer.blocks.push_back(g_ch_named_types_malformed.load() ? ch_malformed_named_types_block()
                                                                          : ch_named_types_block());
            } else {
                answer.blocks.push_back(ch_probe_block());
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    ch_recording_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_recording_connector>(std::move(params), std::move(alias));
    }

    // Catalog + a ClickHouse connector manager over the recording connector +
    // the ClickHouse actor that drives it.
    struct ch_stack {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::unique_ptr<ch::ConnectorManager> connector_manager;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> manager;

        explicit ch_stack(std::pmr::memory_resource* res)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address()))
            , connector_manager(
                  std::make_unique<ch::ConnectorManager>(res, catalog_manager->address(), &ch_recording_factory, 2))
            , manager(actor_zeta::spawn<db::ClickhouseManager>(res, connector_manager.get())) {
            catalog_manager->set_backend_managers(actor_zeta::address_t::empty_address(),
                                                  actor_zeta::address_t::empty_address(),
                                                  manager->address());
            g_ch_type_variant.store(0);
            g_ch_named_types_malformed.store(false);
            take_recorded_queries();
        }

        core::result_wrapper_t<std::string>
        add(const std::string& alias, const std::string& database, const std::string& table) {
            ch::connect_params params;
            params.host = "localhost";
            params.database = database;
            params.table = table;
            return connector_manager->addConnection(std::move(params), alias);
        }

        core::result_wrapper_t<catalog_ext::discovered_tables_t> discover(qualified_name_t scope) {
            auto [needs_sched, future] =
                actor_zeta::send(manager->address(), &db::ClickhouseManager::discover, std::move(scope));
            wait_until_ready(future);
            return std::move(future).take_ready();
        }
    };

    // The type discovered for column "t" of the single discovered table.
    components::types::logical_type column_t_type(const catalog_ext::discovered_tables_t& tables) {
        REQUIRE(tables.size() == 1);
        const auto& fields = tables.front().schema.child_types();
        REQUIRE(fields.size() == 1);
        REQUIRE(fields.front().alias() == "t");
        return fields.front().type();
    }

} // namespace

TEST_CASE("PostgressManager::discover: a failed ENUM query refuses the registration") {
    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address());
    auto connector_manager =
        std::make_unique<pg::ConnectorManager>(resource, catalog_manager->address(), &pg_enum_failing_factory, 2);
    auto manager = actor_zeta::spawn<db::PostgressManager>(resource, connector_manager.get());
    catalog_manager->set_backend_managers(actor_zeta::address_t::empty_address(),
                                          manager->address(),
                                          actor_zeta::address_t::empty_address());

    pg::connect_params params;
    params.host = "localhost";
    params.database = "pgdb";
    params.schema = "public";
    params.table = "orders";
    auto added = connector_manager->addConnection(params, "shop");

    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{added.error().what.c_str()} == "simulated pg_enum failure");
    REQUIRE_FALSE(connector_manager->hasConnection("shop"));

    auto [needs_sched, future] =
        actor_zeta::send(catalog_manager->address(), &mysql::CatalogManager::get_tables, arrow::flight::sql::GetTables{});
    wait_until_ready(future);
    auto listed = std::move(future).take_ready();
    REQUIRE_FALSE(listed.has_error());
    REQUIRE(listed.value().empty());
}

TEST_CASE("ClickhouseManager::discover: the named-types query quotes the database and table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto added = stack.add("ev_alias", "d\\b", "ev'il");
    INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
    REQUIRE_FALSE(added.has_error());

    const auto queries = take_recorded_queries();
    const std::string expected = "SELECT name, type FROM system.columns WHERE database = 'd\\\\b' AND table = 'ev''il'";
    REQUIRE(std::find(queries.begin(), queries.end(), expected) != queries.end());
}

TEST_CASE("ClickhouseManager::discover: a malformed system.columns answer refuses the registration") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);
    g_ch_named_types_malformed.store(true);

    auto added = stack.add("ev_alias", "ev", "events");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(stack.connector_manager->hasConnection("ev_alias"));
}

TEST_CASE("ClickhouseManager::discover: a rediscovered table carries the backend's current named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto added = stack.add("1", "ev", "events");
    INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
    REQUIRE_FALSE(added.has_error());

    // First discovery: system.columns says String, the probe block is a String
    // column — a plain string column either way.
    auto first = stack.discover(qualified_name_t{"1", "ev", "", "events"});
    REQUIRE_FALSE(first.has_error());
    REQUIRE(column_t_type(first.value()) == components::types::logical_type::STRING_LITERAL);

    // The backend now reports a named tuple for the same column: the override
    // must shape the discovered schema, and the entry seen first must not win.
    g_ch_type_variant.store(1);
    auto second = stack.discover(qualified_name_t{"1", "ev", "", "events"});
    REQUIRE_FALSE(second.has_error());
    REQUIRE(column_t_type(second.value()) == components::types::logical_type::STRUCT);

    // The refreshed overrides are what execute converts with; the mock parser
    // stamps uid "1" and the connector answers a data query with no blocks.
    SimpleMockParser parser(mock_config{.resource = &arena});
    auto parsed = parser.parse("SELECT 1");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());
    auto& slot = data->otterbrix_params->external_nodes.front().front();
    *slot.node = schema_utils::make_node_schema_raw(&arena, slot.target.name, "SELECT 1", {});
    auto [needs_sched, future] =
        actor_zeta::send(stack.manager->address(), &db::ClickhouseManager::execute, session_hash_t{1}, std::move(data));
    wait_until_ready(future);
    auto executed = std::move(future).take_ready();
    INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(executed.has_error());
}
