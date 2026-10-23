// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Schema discovery runs through the catalog's own backends, on their connection
// pools (`backend->discover(scope)`). What is pinned here: the metadata
// queries that enrich a table's schema — PostgreSQL ENUM labels, ClickHouse
// named types — are part of the discovery, so their failure refuses the
// registration instead of degrading it silently; the ClickHouse named-types
// query quotes the names it embeds; and a rediscovered table carries the
// backend's current types, not the ones seen first.

#include "catalog/catalog_manager.hpp"
#include "connectors/clickhouse/driver.hpp"
#include "connectors/postgresql/driver.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "test_helpers.hpp"

#include "../mock/aliases.hpp"
#include "../mock/drive.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/mock_driver.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"

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

    using otterstax::test::drive;
    using otterstax::test::mock_backends;
    using otterstax::test::scripted_driver_t;
    using otterstax::test::wait_until_ready;

    // ── PostgreSQL: the ENUM query fails, the rest of the discovery would not ──

    std::unique_ptr<conn::IDriver> pg_enum_failing_driver(std::pmr::memory_resource* resource) {
        return std::make_unique<scripted_driver_t>(
            [](const conn::statement_t&) -> conn::statement_outcome_t { return conn::statement_result_t{}; },
            [resource](const conn::statement_t& statement) -> conn::statement_outcome_t {
                if (statement.sql.find("pg_enum") != std::string::npos) {
                    return core::error_t(core::error_code_t::io_error,
                                         std::pmr::string{"simulated pg_enum failure", resource});
                }
                std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                     &PQclear);
                return pg::decode(resource, result.get(), statement);
            });
    }

    // ── ClickHouse: a driver that records its discovery statements ────────────

    // The recording and the type the driver reports are shared through these;
    // every access is ordered by the future the pool thread settles.
    std::mutex g_ch_queries_mutex;
    std::vector<std::string> g_ch_queries;
    // The named type the driver reports for column "t"; switched between
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

    // Records every discovery statement and answers it — system.columns with the
    // named types, the schema probe with its header block — decoded by the
    // ClickHouse driver's own decode; a slot statement answers no blocks.
    std::unique_ptr<conn::IDriver> ch_recording_driver(std::pmr::memory_resource* resource) {
        return std::make_unique<scripted_driver_t>(
            [resource](const conn::statement_t& statement) {
                return ch::decode(resource, ch::select_result_t{}, statement);
            },
            [resource](const conn::statement_t& statement) {
                {
                    std::lock_guard guard(g_ch_queries_mutex);
                    g_ch_queries.emplace_back(statement.sql);
                }
                ch::select_result_t answer;
                if (statement.sql.find("system.columns") != std::string::npos) {
                    answer.blocks.push_back(g_ch_named_types_malformed.load() ? ch_malformed_named_types_block()
                                                                              : ch_named_types_block());
                } else {
                    answer.blocks.push_back(ch_probe_block());
                }
                return ch::decode(resource, answer, statement);
            });
    }

    // Catalog + the connection pools over the recording driver, the catalog's own
    // backends wired in.
    struct ch_stack {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        std::unique_ptr<mock_backends> backends;

        explicit ch_stack(std::pmr::memory_resource* res)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address(), no_aliases()))
            , backends(std::make_unique<mock_backends>(res, *catalog_manager)) {
            g_ch_type_variant.store(0);
            g_ch_named_types_malformed.store(false);
            take_recorded_queries();
        }

        core::error_t add(const std::string& alias, const std::string& database, const std::string& table) {
            return backends->add(backend_type_t::ClickHouse,
                                 alias,
                                 ch_recording_driver(resource),
                                 qualified_name_t(alias, database, "", table));
        }

        bool has(const std::string& alias) const { return backends->registry().clickhouse.contains(alias); }

        core::result_wrapper_t<catalog_ext::discovered_tables_t> discover(qualified_name_t scope) {
            return drive(resource, backends->backends().clickhouse.discover(std::move(scope)));
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(ParsedQueryDataPtr data) {
            return drive(resource, backends->backends().clickhouse.execute(session_hash_t{1}, std::move(data)));
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

TEST_CASE("PostgreSQL discover: a failed ENUM query refuses the registration") {
    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);

    auto added = backends.add(backend_type_t::PostgreSQL,
                              "shop",
                              pg_enum_failing_driver(resource),
                              qualified_name_t("shop", "pgdb", "public", "orders"));

    REQUIRE(added.contains_error());
    REQUIRE(added.type == core::error_code_t::io_error);
    REQUIRE(std::string{added.what.c_str()} == "simulated pg_enum failure");
    REQUIRE_FALSE(backends.registry().postgresql.contains("shop"));

    auto [needs_sched, future] =
        actor_zeta::send(catalog_manager->address(), &mysql::CatalogManager::get_tables, catalog_ext::get_tables_command_t{});
    wait_until_ready(future);
    auto listed = std::move(future).take_ready();
    REQUIRE_FALSE(listed.has_error());
    REQUIRE(listed.value().empty());
}

TEST_CASE("ClickHouse discover: the named-types query quotes the database and table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto added = stack.add("ev_alias", "d\\b", "ev'il");
    INFO("register: " << (added.contains_error() ? added.what.c_str() : "ok"));
    REQUIRE_FALSE(added.contains_error());

    const auto queries = take_recorded_queries();
    const std::string expected = "SELECT name, type FROM system.columns WHERE database = 'd\\\\b' AND table = 'ev''il'";
    REQUIRE(std::find(queries.begin(), queries.end(), expected) != queries.end());
}

TEST_CASE("ClickHouse discover: a malformed system.columns answer refuses the registration") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);
    g_ch_named_types_malformed.store(true);

    auto added = stack.add("ev_alias", "ev", "events");
    REQUIRE(added.contains_error());
    REQUIRE(added.type == core::error_code_t::schema_error);
    REQUIRE_FALSE(stack.has("ev_alias"));
}

TEST_CASE("ClickHouse discover: a rediscovered table carries the backend's current named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto added = stack.add("1", "ev", "events");
    INFO("register: " << (added.contains_error() ? added.what.c_str() : "ok"));
    REQUIRE_FALSE(added.contains_error());

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
    // stamps uid "1" and the driver answers a slot statement with no blocks.
    SimpleMockParser parser(mock_config{.resource = &arena});
    auto parsed = parser.parse("SELECT 1");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());
    auto& slot = data->otterbrix_params->external_nodes.front().front();
    *slot.node = schema_utils::make_node_schema_raw(&arena, slot.target.name, "SELECT 1", {});
    auto executed = stack.execute(std::move(data));
    INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(executed.has_error());
}
