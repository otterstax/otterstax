// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog/catalog_manager.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "connectors/clickhouse/driver.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "scheduler/session_data.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/drive.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/parser.hpp"

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <components/logical_plan/node_data.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>
#include <sys/resource.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    // Worker pool, session-future bridge and pool sizing are the ones every
    // system test shares (scheduler_stack.hpp); the default-config engine, the
    // backends over mock drivers and the future polling come from test_helpers.hpp.
    using otterstax::test::await_session;
    using otterstax::test::drive;
    using otterstax::test::init_default_test_otterbrix;
    using otterstax::test::make_az_scheduler;
    using otterstax::test::mock_backends;
    using otterstax::test::poll_until_ready;
    using otterstax::test::worker_pool_size;

    // How long one slow mock statement takes.
    constexpr auto SLOW_WAIT = std::chrono::milliseconds(300);

    // How long a test thread polls a cross-actor future; poll_until_ready
    // asserts nothing, so it is safe off the main thread.
    constexpr auto THREAD_POLL_TIMEOUT = std::chrono::seconds(30);

    // What a test thread records about one message it sent: Catch2 assertions
    // are not thread-safe, so the verdicts are checked after the join.
    struct thread_verdict {
        bool ok{false};
        std::string what;
        size_t rows{0};
    };

    // The single external slot of the mock parser's statement, rewritten as a
    // schema node carrying raw backend SQL — what the catalog produces for a
    // SELECT — so a backend actor's execute runs it against uid "1".
    ParsedQueryDataPtr raw_select_statement(std::pmr::memory_resource* resource) {
        SimpleMockParser parser(mock_config{.resource = resource});
        auto parsed = parser.parse("SELECT 1");
        if (parsed.has_error()) {
            return nullptr;
        }
        auto data = std::move(parsed.value());
        auto& slot = data->otterbrix_params->external_nodes.front().front();
        *slot.node = schema_utils::make_node_schema_raw(resource, slot.target.name, "SELECT 1", {});
        return data;
    }

    const data_chunk_t* fetched_slot(const ParsedQueryDataPtr& data) {
        const auto& slot = data->otterbrix_params->external_nodes.front().front();
        if ((*slot.node)->type() != components::logical_plan::node_type::data_t) {
            return nullptr;
        }
        return &static_cast<const components::logical_plan::node_data_t&>(**slot.node).data_chunk();
    }

    // ClickHouse driver for the concurrency case: every discovery statement (named
    // types, schema probe) is answered with a 0-row header block, every slot
    // statement with no blocks — both decoded by ch::decode, as a real driver does.
    clickhouse::Block ch_header_block() {
        clickhouse::Block block;
        block.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
        block.AppendColumn("name", std::make_shared<clickhouse::ColumnString>());
        return block;
    }

    class ch_header_driver final : public conn::IDriver {
    public:
        explicit ch_header_driver(std::pmr::memory_resource* resource)
            : resource_(resource) {}

        core::error_t connect() override { return core::error_t::no_error(); }
        core::error_t ping() override { return core::error_t::no_error(); }
        void close() noexcept override {}
        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<ch_header_driver>(resource_);
        }

        conn::statement_outcome_t execute(const conn::statement_t& statement) override {
            ch::select_result_t answer;
            if (otterstax::test::is_discovery(statement)) {
                answer.blocks.push_back(ch_header_block());
            }
            return ch::decode(resource_, answer, statement);
        }

    private:
        std::pmr::memory_resource* resource_;
    };

} // namespace

// Spawns the full scheduler + connection managers stack with mock connectors and
// fires N parallel Scheduler::execute() messages. Validates that N parallel
// sessions don't hang and each returns a typed result.
TEST_CASE("scheduler handles N parallel sessions without hanging") {
    using namespace std::chrono_literals;

    constexpr size_t N = 32;
    constexpr auto WAIT_TIMEOUT = 60000ms;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    REQUIRE(resource != nullptr);

    auto az_scheduler = make_az_scheduler();

    // OtterbrixManager must exist before the CatalogManager: the current
    // CatalogManager ctor takes the OtterbrixManager address (OID-centric
    // catalog registration).
    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1").contains_error());
    REQUIRE_FALSE(backends.add_mysql("2").contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        worker_pool_size(),
        &make_mock_parser,
        no_aliases(),
        backends.registry(),
        otterbrix_manager->address(),
        catalog_manager->address());
    REQUIRE(scheduler != nullptr);

    const std::string sql = "SELECT 1 AS test";

    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(N);

    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < N; ++i) {
        futs.push_back(actor_zeta::send(scheduler->address(),
                                        &Scheduler::execute,
                                        static_cast<session_hash_t>(i + 1),
                                        sql)
                           .second);
    }

    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto end = std::chrono::steady_clock::now();

    for (size_t i = 0; i < N; ++i) {
        INFO("session " << i);
        REQUIRE_FALSE(results[i].has_error());
        REQUIRE(results[i].value().size() == 2);
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    WARN("Scheduler N=" << N << " parallel sessions completed in " << duration_ms << " ms");
    REQUIRE(duration_ms < WAIT_TIMEOUT.count());

    az_scheduler->stop();
}

// Parallel sessions against a deliberately slow MySQL mock driver. Each Worker
// owns its backend and only awaits the statement it sent to the alias's pool,
// which runs up to max_size (4 by default) statements side by side on as many
// connections — so SESSIONS slow statements overlap instead of queueing behind
// one another.
TEST_CASE("slow MySQL connector does not starve other sessions") {
    using namespace std::chrono_literals;

    constexpr size_t SESSIONS = 4;
    constexpr auto WAIT_TIMEOUT = 30000ms;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    REQUIRE(resource != nullptr);

    auto az_scheduler = make_az_scheduler();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1", {.wait_time = SLOW_WAIT}).contains_error());
    REQUIRE_FALSE(backends.add_mysql("2", {.wait_time = SLOW_WAIT}).contains_error());

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        worker_pool_size(),
        &make_mock_parser,
        no_aliases(),
        backends.registry(),
        otterbrix_manager->address(),
        catalog_manager->address());
    REQUIRE(scheduler != nullptr);

    // Baseline: time for a single session.
    auto baseline_start = std::chrono::steady_clock::now();
    {
        auto fut = actor_zeta::send(scheduler->address(),
                                    &Scheduler::execute,
                                    static_cast<session_hash_t>(1000),
                                    std::string("SELECT 1"))
                       .second;
        auto r = await_session(std::move(fut), WAIT_TIMEOUT, resource);
        REQUIRE_FALSE(r.has_error());
    }
    auto baseline_end = std::chrono::steady_clock::now();
    auto baseline_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(baseline_end - baseline_start).count();

    // SESSIONS in parallel.
    std::vector<actor_zeta::unique_future<core::result_wrapper_t<session_payload>>> futs;
    futs.reserve(SESSIONS);
    auto parallel_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < SESSIONS; ++i) {
        futs.push_back(actor_zeta::send(scheduler->address(),
                                        &Scheduler::execute,
                                        static_cast<session_hash_t>(i + 1),
                                        std::string("SELECT 1"))
                           .second);
    }
    std::vector<core::result_wrapper_t<session_payload>> results;
    results.reserve(SESSIONS);
    for (size_t i = 0; i < SESSIONS; ++i) {
        results.push_back(await_session(std::move(futs[i]), WAIT_TIMEOUT, resource));
    }
    auto parallel_end = std::chrono::steady_clock::now();
    auto parallel_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(parallel_end - parallel_start).count();

    for (size_t i = 0; i < SESSIONS; ++i) {
        REQUIRE_FALSE(results[i].has_error());
    }

    double ratio = baseline_ms > 0 ? static_cast<double>(parallel_ms) / static_cast<double>(baseline_ms) : 0.0;
    WARN("baseline=" << baseline_ms << "ms, parallel(" << SESSIONS << ")=" << parallel_ms
                     << "ms, ratio=" << ratio);

    REQUIRE(parallel_ms < WAIT_TIMEOUT.count());
    // Run one after another they would take SESSIONS × SLOW_WAIT.
    REQUIRE(parallel_ms < static_cast<long long>(SESSIONS) * SLOW_WAIT.count());

    az_scheduler->stop();
}

namespace {
    // CPU time (user + system) this process used so far.
    std::chrono::milliseconds process_cpu_time() {
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        const auto ms = [](const timeval& t) {
            return std::chrono::milliseconds(static_cast<long long>(t.tv_sec) * 1000 + t.tv_usec / 1000);
        };
        return ms(usage.ru_utime) + ms(usage.ru_stime);
    }
} // namespace

// A Worker whose statement waits on a slow backend leaves the scheduler (Worker::resume parks
// it, the answer's waker enqueues it again) instead of being re-run until the answer arrives
// — the busy loop actor-zeta gives an awaiting actor, a core per waiting session. The process's
// CPU time over the waits is what tells them apart: polled, one scheduler thread spins through
// every wait; parked, the process is close to idle (the engine's and the Scheduler loop's
// own 100 µs pumps aside).
TEST_CASE("a Worker waiting on a slow backend parks instead of spinning") {
    using namespace std::chrono_literals;
    constexpr auto wait = 400ms;
    constexpr int statements = 3;

    db::otterbrix_engine_ptr otterbrix = init_default_test_otterbrix();
    auto resource = otterbrix->dispatcher()->resource();
    auto az_scheduler = make_az_scheduler();
    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource,
                                                std::make_unique<SimpleMockOtterbrixManager>(
                                                    mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends.add_mysql("1", {.wait_time = wait}).contains_error());
    REQUIRE_FALSE(backends.add_mysql("2", {.wait_time = wait}).contains_error());
    auto scheduler = actor_zeta::spawn<Scheduler>(resource,
                                                  az_scheduler.get(),
                                                  worker_pool_size(),
                                                  &make_mock_parser,
                                                  no_aliases(),
                                                  backends.registry(),
                                                  otterbrix_manager->address(),
                                                  catalog_manager->address());

    const auto run = [&](session_hash_t id) {
        auto fut = actor_zeta::send(scheduler->address(), &Scheduler::execute, id, std::string("SELECT 1")).second;
        auto r = await_session(std::move(fut), 30000ms, resource);
        REQUIRE_FALSE(r.has_error());
    };
    run(1); // registration of the table, first connections

    const auto cpu_before = process_cpu_time();
    const auto started = std::chrono::steady_clock::now();
    for (int i = 0; i < statements; ++i) {
        run(static_cast<session_hash_t>(100 + i));
    }
    const auto wall = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    const auto cpu = process_cpu_time() - cpu_before;

    WARN("waits " << wall.count() << " ms, process CPU " << cpu.count() << " ms");
    REQUIRE(wall >= statements * wait);
    // A spinning Worker alone would use most of a core for the whole wait.
    CHECK(cpu.count() < wall.count() / 4);

    az_scheduler->stop();
}

// The PostgreSQL ENUM metadata is written by discovery (the catalog's
// add_connection_schema, re-run here for an already registered table) and read
// by every backend's execute — one backend per thread, as every Worker owns its
// own over the shared pools and metadata. pg_metadata_t orders the two with a
// shared_mutex; ThreadSanitizer checks the invariant while they interleave.
TEST_CASE("PostgreSQL backends: parallel execute and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);

    // uid "1" is what the mock parser stamps on its external node.
    REQUIRE_FALSE(backends.add_postgresql("1", "pgdb", "public", "t").contains_error());

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                db::PostgresBackend backend(resource, &backends.registry().postgresql, &backends.registry().pg_metadata);
                for (size_t r = 0; r < ROUNDS; ++r) {
                    auto& verdict = verdicts[t * ROUNDS + r];
                    if (t % 2 == 0) {
                        auto data = raw_select_statement(resource);
                        if (!data) {
                            verdict.what = "mock parse failed";
                            continue;
                        }
                        auto result = drive(resource,
                                            backend.execute(static_cast<session_hash_t>(t * ROUNDS + r + 1),
                                                            std::move(data)),
                                            THREAD_POLL_TIMEOUT);
                        if (result.has_error()) {
                            verdict.what = std::string{result.error().what.c_str()};
                            continue;
                        }
                        const auto* rows = fetched_slot(result.value());
                        if (rows == nullptr) {
                            verdict.what = "slot was not fetched";
                            continue;
                        }
                        verdict.rows = rows->size();
                        verdict.ok = true;
                    } else {
                        auto [needs_sched, future] =
                            actor_zeta::send(catalog_manager->address(),
                                             &mysql::CatalogManager::add_connection_schema,
                                             qualified_name_t{"1", "pgdb", "public", "t"},
                                             catalog_ext::ConnectionType::PostgreSQL);
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "add_connection_schema did not settle";
                            continue;
                        }
                        auto err = std::move(future).take_ready();
                        if (err.contains_error()) {
                            verdict.what = std::string{err.what.c_str()};
                            continue;
                        }
                        verdict.ok = true;
                    }
                }
            });
        }
    }

    for (size_t t = 0; t < THREADS; ++t) {
        for (size_t r = 0; r < ROUNDS; ++r) {
            const auto& verdict = verdicts[t * ROUNDS + r];
            INFO("thread " << t << " round " << r << ": " << verdict.what);
            REQUIRE(verdict.ok);
            if (t % 2 == 0) {
                REQUIRE(verdict.rows == 2);
            }
        }
    }
}

// Same shape for ClickHouse: discovery rewrites the per-table named-type
// overrides (ch_metadata_t) every backend's execute decodes with.
TEST_CASE("ClickHouse backends: parallel execute and re-discovery share no unsynchronised state") {
    constexpr size_t THREADS = 8;
    constexpr size_t ROUNDS = 6;

    auto* resource = std::pmr::new_delete_resource();
    auto otterbrix_manager = actor_zeta::spawn<db::OtterbrixManager>(
        resource,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = resource}));
    auto catalog_manager = actor_zeta::spawn<mysql::CatalogManager>(resource, otterbrix_manager->address(), no_aliases());
    mock_backends backends(resource, *catalog_manager);
    REQUIRE_FALSE(backends
                      .add(backend_type_t::ClickHouse,
                           "1",
                           std::make_unique<ch_header_driver>(resource),
                           qualified_name_t("1", "ev", "", "1"))
                      .contains_error());

    std::vector<thread_verdict> verdicts(THREADS * ROUNDS);
    {
        std::vector<std::jthread> threads;
        threads.reserve(THREADS);
        for (size_t t = 0; t < THREADS; ++t) {
            threads.emplace_back([&, t] {
                db::ClickhouseBackend backend(resource, &backends.registry().clickhouse, &backends.registry().ch_metadata);
                for (size_t r = 0; r < ROUNDS; ++r) {
                    auto& verdict = verdicts[t * ROUNDS + r];
                    if (t % 2 == 0) {
                        auto data = raw_select_statement(resource);
                        if (!data) {
                            verdict.what = "mock parse failed";
                            continue;
                        }
                        auto result = drive(resource,
                                            backend.execute(static_cast<session_hash_t>(t * ROUNDS + r + 1),
                                                            std::move(data)),
                                            THREAD_POLL_TIMEOUT);
                        if (result.has_error()) {
                            verdict.what = std::string{result.error().what.c_str()};
                            continue;
                        }
                        if (fetched_slot(result.value()) == nullptr) {
                            verdict.what = "slot was not fetched";
                            continue;
                        }
                        verdict.ok = true;
                    } else {
                        auto [needs_sched, future] =
                            actor_zeta::send(catalog_manager->address(),
                                             &mysql::CatalogManager::add_connection_schema,
                                             qualified_name_t{"1", "ev", "", "1"},
                                             catalog_ext::ConnectionType::ClickHouse);
                        if (!poll_until_ready(future, THREAD_POLL_TIMEOUT)) {
                            verdict.what = "add_connection_schema did not settle";
                            continue;
                        }
                        auto err = std::move(future).take_ready();
                        if (err.contains_error()) {
                            verdict.what = std::string{err.what.c_str()};
                            continue;
                        }
                        verdict.ok = true;
                    }
                }
            });
        }
    }

    for (size_t t = 0; t < THREADS; ++t) {
        for (size_t r = 0; r < ROUNDS; ++r) {
            const auto& verdict = verdicts[t * ROUNDS + r];
            INFO("thread " << t << " round " << r << ": " << verdict.what);
            REQUIRE(verdict.ok);
        }
    }
}
