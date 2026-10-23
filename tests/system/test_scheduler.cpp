// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/scheduler.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>
#include <atomic>
#include <chrono>
#include <thread>

namespace {
    // Worker pool, session-future bridge and pool sizing are the ones every
    // system test shares (scheduler_stack.hpp); the default-config engine and
    // the backends over mock drivers come from test_helpers.hpp. The table the
    // mock parser references ("1") is registered lazily on the first query.
    using otterstax::test::await_session;
    using otterstax::test::init_default_test_otterbrix;
    using otterstax::test::make_az_scheduler;
    using otterstax::test::mock_backends;
    using otterstax::test::worker_pool_size;
} // namespace

TEST_CASE("base test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    // Objects built here escape into the actor graph and are freed on other
    // threads: name the global allocator rather than arena-scoping it.
    auto resource = std::pmr::new_delete_resource(); // no otterbrix processing, plain malloc/free for the mock
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1").contains_error());
    REQUIRE_FALSE(backends.add_mysql("2").contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_id id; // Use session_id type for consistency
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id.hash(), sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.size() == 2);

    az_scheduler->stop();
}

TEST_CASE("Error in connector test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1", {.can_throw = true}).contains_error());
    REQUIRE_FALSE(backends.add_mysql("2", {.can_throw = true}).contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::io_error);
    REQUIRE(r.error().what == "mock driver: statement failed");

    az_scheduler->stop();
}

// The engine still runs the plan after the backend answered (the slot sits below a
// local aggregate), and its failure is the statement's. A statement the backend answers
// whole never reaches the engine (Worker::take_backend_result).
TEST_CASE("Error in otterbrix test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1").contains_error());
    REQUIRE_FALSE(backends.add_mysql("2").contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser_slot_below_root,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "Otterbrix execution failed: SimpleMockOtterbrixManager: exception in execute_plan");

    az_scheduler->stop();
}

TEST_CASE("Error in scheduler test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    // The catalog is not given its backends on purpose: it has nothing to run
    // the discovery through, so schema registration is rejected and the
    // connection is dropped. The statement never needs it — the parser throws
    // first.
    mock_backends backends(resource, *catalog_manager, /*wire_catalog=*/false);
    REQUIRE(backends.add_mysql("1").contains_error());
    REQUIRE(backends.add_mysql("2").contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_throwing_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().what == "SimpleMockParser: exception in parse");

    az_scheduler->stop();
}

TEST_CASE("Error in otterbrix + sql connector test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .can_throw = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1", {.can_throw = true}).contains_error());
    REQUIRE_FALSE(backends.add_mysql("2", {.can_throw = true}).contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << "check data" << std::endl;
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::io_error);
    REQUIRE(r.error().what == "mock driver: statement failed");

    az_scheduler->stop();
}

// Mock parser that creates cross-backend query (MySQL + PostgreSQL)
class CrossBackendMockParser : public IParser {
public:
    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override {
        std::cout << "CrossBackendMockParser: parsing SQL: " << sql << std::endl;

        auto resource = std::pmr::new_delete_resource();

        auto binder = sql::transform::transform_result(
            resource,
            logical_plan::execution_plan_t(resource,
                                           logical_plan::make_node_aggregate(resource,
                                                                             core::uid_t{"result"},
                                                                             core::dbname_t{"db"},
                                                                             core::relname_t{"result_table"}),
                                           logical_plan::make_parameter_node(resource)),
            sql::transform::transform_result::parameter_map_t{resource},
            sql::transform::transform_result::insert_map_t{resource},
            sql::transform::transform_result::insert_rows_t(resource));

        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 2), // 2 external nodes
            std::move(binder),
            NodeTag::T_SelectStmt);

        // Add external nodes - use the parsed node structure
        // Create two references to simulate MySQL and PostgreSQL backends
        // Note: In real usage, these would be separate nodes from different backends
        // For this mock test, we use the same node twice to verify backend detection works
        // uids/names must match the connections registered in the test
        // ("campaigns" MySQL with an empty database, "products" PostgreSQL
        // with pgdb/public/products) so that the catalog maps each node to
        // its backend and finds the registered schemas.
        parsed->otterbrix_params->external_nodes.emplace_back();
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{&parsed->otterbrix_params->node,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"campaigns", "", "", "campaigns"},
                                                                 {}}});
        parsed->otterbrix_params->external_nodes.emplace_back();
        parsed->otterbrix_params->external_nodes.back().push_back(
            external_entry_t{&parsed->otterbrix_params->node,
                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                 qualified_name_t{"products", "pgdb", "public", "products"},
                                                                 {}}});
        parsed->otterbrix_params->external_nodes_count = 2;

        std::cout << "CrossBackendMockParser: created query with 2 external nodes (simulating MySQL + PostgreSQL)"
                  << std::endl;
        return parsed;
    }
};

// Stateless factory for the cross-backend test: each Worker builds its own
// CrossBackendMockParser (a plain function pointer, the Scheduler's
// parser_factory_fn shape).
inline parser_ptr make_cross_backend_mock_parser(std::pmr::memory_resource*,
                                                 const otterstax::names::alias_registry_t&) {
    return std::make_unique<CrossBackendMockParser>();
}

// Mock Otterbrix Manager for cross-backend test
class CrossBackendMockOtterbrixManager : public SimpleMockOtterbrixManager {
public:
    explicit CrossBackendMockOtterbrixManager(std::pmr::memory_resource* resource)
        : SimpleMockOtterbrixManager(mock_config{.resource = resource}) {}

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override {
        std::cout << "CrossBackendMockOtterbrixManager: executing cross-backend plan" << std::endl;
        // Simulate successful execution - return a mock cursor
        std::pmr::memory_resource* resource = std::pmr::new_delete_resource();
        return components::cursor::make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
    }
};

TEST_CASE("Cross-backend JOIN detection test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = std::pmr::new_delete_resource(); // plain malloc/free for the mock
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    // Use mock Otterbrix manager that doesn't require real execution
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<CrossBackendMockOtterbrixManager>(resource));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("campaigns").contains_error());
    REQUIRE_FALSE(backends.add_postgresql("products", "pgdb", "public", "products").contains_error());

    // The Scheduler builds its parser from the injected factory (&make_mock_parser);
    // each Worker gets its own instance, so the mock plan drives backend detection.
    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_cross_backend_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);

    std::string sql = "SELECT * FROM products.pgdb.public.products p JOIN campaigns.db1.campaigns c ON "
                      "p.campaign_id = c.campaign_id";
    session_hash_t id = 1;

    std::cout << "[Main thread] " << std::this_thread::get_id() << " sending cross-backend query" << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);

    // Wait for completion
    auto r = await_session(std::move(fut), 10000ms, resource);

    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;

    // The test verifies that:
    // 1. get_catalog_schema() was called (detected Mixed backend)
    // 2. Both MySQL and PostgreSQL connections were recognized
    // 3. Query didn't fail with "database does not exist" due to backend detection failure

    std::cout << "Test completed with error: " << r.has_error() << std::endl;
    if (r.has_error()) {
        std::cout << "Error message: " << r.error().what << std::endl;
    }

    // Both uids must have been attributed to a backend: a backend_unknown
    // failure is the one outcome ruled out here (success, or any other typed
    // error, is acceptable).
    REQUIRE_FALSE((r.has_error() && r.error().what.starts_with("backend_unknown")));

    az_scheduler->stop();
}

// The shape a JOIN of two backend tables parses into: a local root over two slots in ONE
// batch — "campaigns" (MySQL) and "products" (PostgreSQL) — the way siblings of a JOIN
// share a batch. (CrossBackendMockParser puts one node in two batches, so its second slot
// is already data when its turn comes.)
class TwoSlotJoinMockParser : public IParser {
public:
    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string&) override {
        auto resource = std::pmr::new_delete_resource();
        auto root = logical_plan::make_node_aggregate(resource,
                                                      core::uid_t{""},
                                                      core::dbname_t{"db"},
                                                      core::relname_t{"joined"});
        root->append_child(logical_plan::make_node_aggregate(resource,
                                                             core::uid_t{"campaigns"},
                                                             core::dbname_t{""},
                                                             core::relname_t{"campaigns"}));
        root->append_child(logical_plan::make_node_aggregate(resource,
                                                             core::uid_t{"products"},
                                                             core::dbname_t{"pgdb"},
                                                             core::relname_t{"products"}));
        auto binder = sql::transform::transform_result(
            resource,
            logical_plan::execution_plan_t(resource, root, logical_plan::make_parameter_node(resource)),
            sql::transform::transform_result::parameter_map_t{resource},
            sql::transform::transform_result::insert_map_t{resource},
            sql::transform::transform_result::insert_rows_t(resource));
        auto parsed = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 2),
            std::move(binder),
            NodeTag::T_SelectStmt);
        auto& children = parsed->otterbrix_params->node->children();
        auto& batch = parsed->otterbrix_params->external_nodes.emplace_back();
        batch.push_back(external_entry_t{&children[0],
                                         otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                             qualified_name_t{"campaigns", "", "", "campaigns"},
                                                                             {}}});
        batch.push_back(external_entry_t{&children[1],
                                         otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                             qualified_name_t{"products", "pgdb", "public", "products"},
                                                                             {}}});
        parsed->otterbrix_params->external_nodes_count = 2;
        return parsed;
    }
};

inline parser_ptr make_two_slot_join_mock_parser(std::pmr::memory_resource*, const otterstax::names::alias_registry_t&) {
    return std::make_unique<TwoSlotJoinMockParser>();
}

// A Mixed statement's backends run side by side (backend_set_t::execute_mixed): each
// backend's slots are sent before any answer is awaited, so a MySQL ⋈ PostgreSQL JOIN
// whose two slices take 400 ms each costs about 400 ms, not the 800 ms of one backend
// after the other.
TEST_CASE("Cross-backend JOIN runs its backends side by side") {
    using namespace std::chrono_literals;
    constexpr auto slice_time = 400ms;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = std::pmr::new_delete_resource();

    auto az_scheduler = make_az_scheduler();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<CrossBackendMockOtterbrixManager>(resource));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);

    // Every data query sleeps slice_time and answers the mock driver's two (id, name) rows.
    std::atomic<int> data_queries{0};
    auto slow_slice = [resource, &data_queries, slice_time](const conn::statement_t&) -> conn::statement_outcome_t {
        std::this_thread::sleep_for(slice_time);
        ++data_queries;
        namespace ct = components::types;
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::INTEGER, "id");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "name");
        components::vector::data_chunk_t chunk(resource, types, 2);
        chunk.set_cardinality(2);
        conn::statement_result_t out;
        out.rows.emplace(std::move(chunk));
        return out;
    };
    REQUIRE_FALSE(backends
                      .add(backend_type_t::MySQL,
                           "campaigns",
                           std::make_unique<otterstax::test::scripted_driver_t>(slow_slice),
                           qualified_name_t("campaigns", "db", "", ""))
                      .contains_error());
    REQUIRE_FALSE(backends
                      .add(backend_type_t::PostgreSQL,
                           "products",
                           std::make_unique<otterstax::test::scripted_driver_t>(slow_slice),
                           qualified_name_t("products", "pgdb", "public", "products"))
                      .contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_two_slot_join_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());

    std::string sql = "SELECT * FROM products.pgdb.public.products p JOIN campaigns.db1.campaigns c ON "
                      "p.campaign_id = c.campaign_id";
    const auto started = std::chrono::steady_clock::now();
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, session_hash_t{1}, sql);
    auto r = await_session(std::move(fut), 10000ms, resource);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    az_scheduler->stop();

    INFO("error: " << (r.has_error() ? r.error().what.c_str() : "none"));
    INFO("elapsed ms: " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
    // Both slices were fetched, and the statement took less than the two of them in a row.
    CHECK(data_queries.load() == 2);
    CHECK(elapsed < 2 * slice_time - 100ms);
}

TEST_CASE("return empty test case") {
    using namespace std::chrono_literals;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource, .return_empty = true}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    // Use return_empty connector so MySQL returns empty results
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1", {.return_empty = true}).contains_error());
    REQUIRE_FALSE(backends.add_mysql("2", {.return_empty = true}).contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.empty() == true);

    az_scheduler->stop();
}

// Regression for the latent >1024-row truncation: a result reaches the client as a
// batch of <=1024-row chunks (never one oversized chunk). The backend answers N=2500
// rows in one chunk; db::backend_t splits it into ceil(2500/1024)=3 chunks, and since
// the backend answered the statement whole, the Worker hands them to the payload
// without the engine (Worker::take_backend_result). The whole vector must ride
// through the scheduler into the session_payload without losing rows.
TEST_CASE("multi-chunk result carries all rows through the scheduler") {
    using namespace std::chrono_literals;

    constexpr size_t N = 2500;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    assert(resource);

    auto az_scheduler = make_az_scheduler();

    // The engine mock answers nothing of its own: every row below came from the backend.
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    // (id INTEGER, name STRING), row r = (multi_chunk_id(r), multi_chunk_name(r)), in one chunk.
    auto n_rows = [resource](const conn::statement_t&) -> conn::statement_outcome_t {
        namespace ct = components::types;
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::INTEGER, "id");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "name");
        components::vector::data_chunk_t chunk(resource, types, N);
        for (size_t r = 0; r < N; ++r) {
            chunk.set_value(0, r, ct::logical_value_t(resource, SimpleMockOtterbrixManager::multi_chunk_id(r)));
            chunk.set_value(1, r, ct::logical_value_t(resource, SimpleMockOtterbrixManager::multi_chunk_name(r)));
        }
        chunk.set_cardinality(N);
        conn::statement_result_t out;
        out.rows.emplace(std::move(chunk));
        return out;
    };
    for (const char* alias : {"1", "2"}) {
        REQUIRE_FALSE(backends
                          .add(backend_type_t::MySQL,
                               alias,
                               std::make_unique<otterstax::test::scripted_driver_t>(n_rows),
                               qualified_name_t(alias, "db", "", ""))
                          .contains_error());
    }

    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());
    assert(scheduler);
    std::string sql = "SELECT 1 AS test";
    session_hash_t id = 1;
    std::cout << "[Main thread] " << std::this_thread::get_id() << std::endl;
    auto [ns, fut] = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), 5000ms, resource);
    std::cout << "[Main thread] " << std::this_thread::get_id() << " check data" << std::endl;
    REQUIRE(!r.has_error());
    auto sp = std::move(r.value());
    // No row loss across the chunk boundary, and the result really did arrive as
    // multiple chunks (would be 1 under a naive chunks().front()-only payload).
    REQUIRE(sp.size() == N);
    REQUIRE(sp.chunks.size() >= 3);
    // Every row carries its own values, in order, across the chunk boundaries —
    // the right total alone would not catch a chunk delivered empty or repeated.
    size_t row = 0;
    for (const auto& chunk : sp.chunks) {
        REQUIRE(chunk.column_count() == 2);
        for (size_t r_idx = 0; r_idx < chunk.size(); ++r_idx, ++row) {
            REQUIRE(chunk.value(0, r_idx).value<int32_t>() == SimpleMockOtterbrixManager::multi_chunk_id(row));
            REQUIRE(chunk.value(1, r_idx).value<std::string_view>() ==
                    SimpleMockOtterbrixManager::multi_chunk_name(row));
        }
    }
    REQUIRE(row == N);

    az_scheduler->stop();
}
// ---------------------------------------------------------------------------
// Sequence-root unwrap during schema resolution.
//
// A sequence-rooted statement wraps its data-producing node behind
// catalog_resolve_* siblings, the consumer LAST. CatalogManager::get_catalog_schema
// and OtterbrixManager::get_schema must unwrap that root instead of silently
// returning an empty schema.
//
// The parser no longer builds that shape — catalog lookups moved out of the plan
// tree into execution_plan_t::catalog_resolves, so a statement arrives as a bare
// consumer. The unwrap stayed, because a root that does arrive wrapped must not be
// read as an empty plan, and these cases are what pins it: each one builds the
// wrapped root itself with wrap_root_in_resolve_sequence.
// ---------------------------------------------------------------------------

#include "otterbrix/config.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_sequence.hpp>

#include <filesystem>

namespace {

    using otterstax::test::wait_until_ready;

    // The connection catalog_schema_fixture registers, as config.yaml declares it.
    const otterstax::names::alias_registry_t& products_aliases() {
        static const auto aliases = make_aliases({{"products", backend_type_t::PostgreSQL, "pgdb", "public"}});
        return aliases;
    }

    // Catalog actor graph with mocked connectors and a registered PostgreSQL
    // connection "products" (pgdb/public/products) — shared by the
    // sequence-unwrap catalog test cases below.
    struct catalog_schema_fixture {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_manager;
        // The connection pools the catalog discovers through; declared after the
        // catalog, so they are destroyed first.
        std::unique_ptr<mock_backends> backends;

        explicit catalog_schema_fixture(std::pmr::memory_resource* res)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_manager(
                  actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address(), products_aliases()))
            , backends(std::make_unique<mock_backends>(res, *catalog_manager)) {
            REQUIRE_FALSE(backends->add_postgresql("products", "pgdb", "public", "products").contains_error());
        }
    };

    // Rebuilds the sequence-rooted shape the unwrap paths defend against: a
    // node_sequence_t holding a catalog_resolve_t sibling first and the parsed
    // statement's consumer LAST. The parser hands over the bare consumer, so the
    // wrapped root exists only where a test makes one.
    //
    // An external slot is a pointer INTO the plan, so a slot that named the old root
    // is repointed at the consumer's new home inside the sequence; left alone it
    // would name a node the plan no longer roots. The repointing must happen BEFORE
    // the root slot is overwritten: a slot naming the root IS that slot, so once the
    // sequence is stored there the slot no longer reads as the consumer.
    void wrap_root_in_resolve_sequence(const ParsedQueryDataPtr& data, std::pmr::memory_resource* res) {
        auto consumer = data->otterbrix_params->node;
        REQUIRE(consumer);
        auto sequence = logical_plan::node_ptr(new logical_plan::node_sequence_t(res));
        sequence->append_child(logical_plan::make_node_catalog_resolve(res, logical_plan::resolve_kind::table));
        sequence->append_child(consumer);
        for (auto& batch : data->otterbrix_params->external_nodes) {
            for (auto& entry : batch) {
                if (*entry.node == consumer) {
                    entry.node = &sequence->children().back();
                }
            }
        }
        data->otterbrix_params->node = sequence;
    }

} // namespace

TEST_CASE("otterbrix get_schema: sequence-rooted SELECT resolves columns") {
    const char* disk_path = "/tmp/otterstax_seq_get_schema";
    std::filesystem::remove_all(disk_path);
    auto cfg = make_create_config(disk_path);
    auto inst = db::make_otterbrix_engine(cfg);
    auto* resource = inst->dispatcher()->resource();

    {
        // Local engine table the get_schema dependency probe resolves against.
        auto setup = make_otterbrix_manager(inst);
        auto db_cursor = setup->execute_sql("CREATE DATABASE db1;");
        REQUIRE(db_cursor);
        REQUIRE_FALSE(db_cursor->is_error());
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("campaign_id", types::complex_logical_type(types::logical_type::INTEGER));
        cols.emplace_back("campaign_name", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("budget", types::complex_logical_type(types::logical_type::DOUBLE));
        components::catalog::oid_t oid = components::catalog::INVALID_OID;
        auto create_cursor = setup->create_collection("db1", "campaigns", std::move(cols), oid);
        REQUIRE(create_cursor);
        REQUIRE_FALSE(create_cursor->is_error());
    }

    GreenplumParser parser(resource, products_aliases());
    auto parsed = parser.parse("SELECT campaign_name, budget FROM db1.campaigns;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    // The parser hands over the bare consumer; the wrapped root this case is about
    // is built here.
    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::aggregate_t);
    wrap_root_in_resolve_sequence(data, resource);

    // Wrapped shape: sequence root, the aggregate consumer is LAST.
    const auto& root = data->otterbrix_params->node;
    REQUIRE(root->type() == logical_plan::node_type::sequence_t);
    REQUIRE_FALSE(root->children().empty());
    REQUIRE(root->children().back()->type() == logical_plan::node_type::aggregate_t);

    std::pmr::map<qualified_name_t, size_t> dependencies(resource);
    dependencies.emplace(
        schema_utils::agg_key(static_cast<const logical_plan::node_aggregate_t&>(*root->children().back())),
        0);

    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(inst));
    auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                  &db::OtterbrixManager::get_schema,
                                                  session_hash_t{1},
                                                  std::move(dependencies),
                                                  std::move(data));
    wait_until_ready(future);
    auto result = std::move(future).take_ready();
    REQUIRE_FALSE(result.has_error());
    auto [cursor, returned] = std::move(result.value());
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    // A sequence root is not an empty plan: exactly one schema, for the
    // aggregate that ends the sequence.
    REQUIRE(cursor->size() == 1);
    const auto& schema = cursor->type_data()[0];
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    // The schema lists the SELECT-list columns by name, in statement order.
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "campaign_name");
    REQUIRE(schema.child_types()[1].alias() == "budget");
    // The probe resolves the column types, not only their names.
    REQUIRE(schema.child_types()[0].type() == types::logical_type::STRING_LITERAL);
    REQUIRE(schema.child_types()[1].type() == types::logical_type::DOUBLE);
}

TEST_CASE("otterbrix get_schema: non-aggregate plans keep the empty-schema contract") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    GreenplumParser parser(resource, products_aliases());

    SECTION("non-sequence non-aggregate root -> empty schema, no error") {
        auto parsed = parser.parse("SELECT campaign_name FROM db1.campaigns;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node =
            logical_plan::make_node_raw_data(resource, components::vector::data_chunk_t(resource, {}));

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto [cursor, returned] = std::move(result.value());
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
    }

    SECTION("sequence whose last child is not an aggregate (CREATE) -> empty schema, no error") {
        auto parsed = parser.parse("CREATE TABLE db1.newtable (id INT);");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::create_collection_t);
        wrap_root_in_resolve_sequence(data, resource);
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
        REQUIRE(data->otterbrix_params->node->children().back()->type() != logical_plan::node_type::aggregate_t);

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto [cursor, returned] = std::move(result.value());
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
    }

    SECTION("sequence with no children -> error, not UB") {
        auto parsed = parser.parse("SELECT campaign_name FROM db1.campaigns;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node = logical_plan::node_ptr(new logical_plan::node_sequence_t(resource));

        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        auto [needs_sched, future] = actor_zeta::send(otterbrix_manager->address(),
                                                      &db::OtterbrixManager::get_schema,
                                                      session_hash_t{1},
                                                      std::move(dependencies),
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE(result.has_error());
    }
}

TEST_CASE("catalog get_catalog_schema: sequence-rooted SELECT rewrites the external node") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);

    GreenplumParser parser(resource, products_aliases());
    auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::aggregate_t);
    wrap_root_in_resolve_sequence(data, resource);

    // Wrapped shape: sequence root, the external aggregate is LAST.
    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
    REQUIRE(data->otterbrix_params->external_nodes.size() == 1);
    REQUIRE(data->otterbrix_params->external_nodes.front().size() == 1);
    REQUIRE((*data->otterbrix_params->external_nodes.front().front().node)->type() ==
            logical_plan::node_type::aggregate_t);

    auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                  &mysql::CatalogManager::get_catalog_schema,
                                                  session_hash_t{1},
                                                  std::move(data));
    wait_until_ready(future);
    auto result = std::move(future).take_ready();
    REQUIRE_FALSE(result.has_error());
    auto updated = std::move(result.value());
    REQUIRE(updated->backend_type == backend_type_t::PostgreSQL);

    // The external aggregate under the sequence root must be rewritten into its
    // schema node; a root mistaken for an empty plan leaves it untouched.
    auto& entry = updated->otterbrix_params->external_nodes.front().front();
    REQUIRE((*entry.node)->type() == logical_plan::node_type::unused);
    const auto& schema = static_cast<schema_utils::schema_node_t&>(**entry.node).schema();
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "id");
    REQUIRE(schema.child_types()[1].alias() == "name");
}

TEST_CASE("catalog get_catalog_schema: non-aggregate plans keep the empty-schema contract") {
    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    catalog_schema_fixture fx(resource);
    GreenplumParser parser(resource, products_aliases());

    SECTION("sequence whose last child is not an aggregate (CREATE) -> empty schema, no error") {
        auto parsed = parser.parse("CREATE TABLE products.pgdb.public.newtable (id INT);");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::create_collection_t);
        wrap_root_in_resolve_sequence(data, resource);
        REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::sequence_t);
        REQUIRE(data->otterbrix_params->node->children().back()->type() ==
                logical_plan::node_type::create_collection_t);

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{2},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto updated = std::move(result.value());
        REQUIRE(updated->backend_type == backend_type_t::PostgreSQL);
        REQUIRE(updated->otterbrix_params->node->children().back()->type() ==
                logical_plan::node_type::create_collection_t);
    }

    SECTION("non-sequence non-aggregate root -> empty schema, no error") {
        auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node =
            logical_plan::make_node_raw_data(resource, components::vector::data_chunk_t(resource, {}));
        data->otterbrix_params->external_nodes.front().front().node = &data->otterbrix_params->node;

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{3},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE_FALSE(result.has_error());
        auto updated = std::move(result.value());
        REQUIRE(updated->otterbrix_params->node->type() == logical_plan::node_type::data_t);
    }

    SECTION("sequence with no children -> error, not UB") {
        auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        data->otterbrix_params->node = logical_plan::node_ptr(new logical_plan::node_sequence_t(resource));
        data->otterbrix_params->external_nodes.front().front().node = &data->otterbrix_params->node;

        auto [needs_sched, future] = actor_zeta::send(fx.catalog_manager->address(),
                                                      &mysql::CatalogManager::get_catalog_schema,
                                                      session_hash_t{4},
                                                      std::move(data));
        wait_until_ready(future);
        auto result = std::move(future).take_ready();
        REQUIRE(result.has_error());
    }
}
