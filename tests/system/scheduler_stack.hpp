// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A full Scheduler→Worker stack over a real Otterbrix engine, for system tests
// that need to drive SQL the way a frontend does rather than call the engine
// directly; the pool, bridge and sizing helpers are shared by every system
// test that spawns a Scheduler.
//
// The backend registry has no connection on purpose. A statement with no
// external nodes is classified Otterbrix by the Worker itself and never reaches
// a backend, and the external-table path is intercepted before any backend
// routing; it runs on the registry's local-file transfer pool. The catalog is
// real (with no connections registered): every CREATE DATABASE / DROP
// DATABASE asks it whether the name belongs to a connection.

#pragma once

#include "catalog/catalog_manager.hpp"
#include "connectors/file/driver.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "integration/backends.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "../mock/aliases.hpp"
#include "otterbrix/parser/parser.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler/session_data.hpp"
#include "test_helpers.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <core/result_wrapper.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <memory_resource>
#include <string>
#include <thread>
#include <utility>

namespace otterstax::test {

    struct scheduler_stack {
        actor_zeta::address_t scheduler;
        db::otterbrix_engine_ptr otterbrix;
        std::pmr::memory_resource* resource;
    };

    // Sessions are routed `id % worker_count`, so a test that needs two
    // statements to land on the SAME Worker (and therefore the same parser
    // instance) picks `id` and `id + worker_pool_size()`.
    inline std::size_t worker_pool_size() { return std::max<std::size_t>(2, std::thread::hardware_concurrency()); }

    // The worker pool runs on an actor-zeta sharing_scheduler; mirrors the helper
    // used by test_scheduler.cpp / test_scheduler_concurrent.cpp.
    inline std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> make_az_scheduler() {
        auto sched = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(worker_pool_size(),
                                                                               /*max_throughput*/ 1000);
        sched->start();
        return sched;
    }

    inline db::otterbrix_engine_ptr init_test_otterbrix(const std::string& data_dir) {
        auto config = configuration::config::create_config(data_dir);
        initialize_all_loggers(config.log.path.string());
        return db::make_otterbrix_engine(std::move(config));
    }

    // An engine over an empty data_dir: nothing a previous run checkpointed
    // there is visible to the test.
    inline db::otterbrix_engine_ptr init_fresh_test_otterbrix(const std::string& data_dir) {
        std::filesystem::remove_all(data_dir);
        return init_test_otterbrix(data_dir);
    }

    // Drive the Scheduler's returned future from the test thread through the asio
    // bridge — the same poll-based path the frontends use.
    inline core::result_wrapper_t<session_payload>
    await_session(actor_zeta::unique_future<core::result_wrapper_t<session_payload>> fut,
                  std::chrono::milliseconds timeout,
                  std::pmr::memory_resource* resource) {
        return otterstax::await_future_blocking<session_payload>(std::move(fut), resource, timeout);
    }

    // A registry without connections whose local-file transfer pool is open, as
    // register_connections leaves it for a config without connections.
    inline std::unique_ptr<db::backend_registry_t> make_registry(std::pmr::memory_resource* resource) {
        auto registry = std::make_unique<db::backend_registry_t>(resource);
        auto opened = registry->transfers.set_files(std::make_shared<conn::file::driver_t>(resource), {});
        REQUIRE_FALSE(opened.contains_error());
        return registry;
    }

    // Owns otterbrix + the backend registry (no connections, a local-file transfer
    // pool) + a Scheduler over `parser_factory`.
    // Teardown runs in the destructor in dependency order (actors before
    // otterbrix, whose dtor checkpoints to data_dir), so it happens whether the
    // test body returns or a failed REQUIRE unwinds through it.
    class scheduler_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the
        // ones whose addresses it takes, and the pmr deleters carry the engine's
        // resource, so the members are initialised here rather than assigned.
        scheduler_stack_owner(const std::string& data_dir, Scheduler::parser_factory_fn parser_factory)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address(), no_aliases()))
            , backends_(make_registry(resource_))
            , scheduler_(actor_zeta::spawn<Scheduler>(resource_,
                                                      az_scheduler_.get(),
                                                      worker_pool_size(),
                                                      parser_factory,
                                                      no_aliases(),
                                                      *backends_,
                                                      otb_mgr_->address(),
                                                      catalog_->address())) {}

        scheduler_stack_owner(const scheduler_stack_owner&) = delete;
        scheduler_stack_owner& operator=(const scheduler_stack_owner&) = delete;

        ~scheduler_stack_owner() {
            scheduler_.reset();
            backends_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

    private:
        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<db::backend_registry_t> backends_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    // A full Scheduler stack over a real engine, a real catalog and the real
    // parser, whose connections are drivers a test registers through `add`
    // (mock_backends::add / add_mysql / ...) before the Scheduler exists — the
    // order register_connections keeps. `aliases` must outlive the owner.
    class backend_stack_owner {
    public:
        using add_fn = std::function<void(mock_backends&, std::pmr::memory_resource*)>;

        backend_stack_owner(const std::string& data_dir, const otterstax::names::alias_registry_t& aliases, add_fn add)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address(), aliases))
            , backends_(std::make_unique<mock_backends>(resource_, *catalog_))
            , scheduler_(spawn_scheduler(aliases, add)) {}

        backend_stack_owner(const backend_stack_owner&) = delete;
        backend_stack_owner& operator=(const backend_stack_owner&) = delete;

        ~backend_stack_owner() {
            scheduler_.reset();
            backends_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }
        mock_backends& backends() const { return *backends_; }
        actor_zeta::address_t catalog() const { return catalog_->address(); }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t>
        spawn_scheduler(const otterstax::names::alias_registry_t& aliases, const add_fn& add) {
            add(*backends_, resource_);
            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                aliases,
                                                backends_->registry(),
                                                otb_mgr_->address(),
                                                catalog_->address());
        }

        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<mock_backends> backends_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    template<typename Fn>
    void with_scheduler_stack_parser(const std::string& data_dir,
                                     Scheduler::parser_factory_fn parser_factory,
                                     Fn&& body) {
        scheduler_stack_owner owner(data_dir, parser_factory);
        body(owner.stack());
    }

    template<typename Fn>
    void with_scheduler_stack(const std::string& data_dir, Fn&& body) {
        with_scheduler_stack_parser(data_dir, &make_parser, std::forward<Fn>(body));
    }

    // Run `sql` through Scheduler::execute and block on the returned future.
    // Returns the payload itself — the shape of a DML result is exactly what
    // test_dml_result_shape.cpp asserts on.
    inline core::result_wrapper_t<session_payload>
    run_scheduler_sql_payload(const scheduler_stack& s,
                              session_hash_t id,
                              const std::string& sql,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::execute, id, sql);
        return await_session(std::move(fut), timeout, s.resource);
    }

    // Convenience wrapper for tests that only care whether the statement ran;
    // `err` carries the message in the error case.
    inline bool
    run_scheduler_sql(const scheduler_stack& s, session_hash_t id, const std::string& sql, std::string& err) {
        auto r = run_scheduler_sql_payload(s, id, sql);
        if (r.has_error()) {
            err = std::string{r.error().what.c_str()};
            return false;
        }
        err.clear();
        return true;
    }

    // The four extended-protocol messages, one call each, so a test can drive
    // the prepared-statement lifecycle step by step (and repeat a step).
    inline core::result_wrapper_t<session_payload>
    prepare_scheduler_sql(const scheduler_stack& s,
                          session_hash_t id,
                          const std::string& sql,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::prepare_schema, id, sql);
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    execute_scheduler_statement(const scheduler_stack& s,
                                session_hash_t id,
                                std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::execute_statement, id);
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    execute_scheduler_prepared(const scheduler_stack& s,
                               session_hash_t id,
                               std::pmr::vector<components::types::logical_value_t> parameters,
                               std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] =
            actor_zeta::send(s.scheduler, &Scheduler::execute_prepared_statement, id, std::move(parameters));
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    close_scheduler_statement(const scheduler_stack& s,
                              session_hash_t id,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::close_statement, id);
        return await_session(std::move(fut), timeout, s.resource);
    }

    // The two-phase path every extended-protocol client uses: Parse/GetFlightInfo
    // stores the statement via prepare_schema, Execute/DoGet runs it from that
    // stored state. It is a genuinely different route through the Worker than
    // Scheduler::execute — backend classification happens in a different place —
    // so a statement shape that works one way can still be broken the other.
    // Returns the EXECUTE phase's payload; `prepare_err` carries a prepare-phase
    // failure (in which case execute is not attempted and the prepare error is
    // returned unchanged, code included).
    inline core::result_wrapper_t<session_payload>
    run_scheduler_prepared(const scheduler_stack& s,
                           session_hash_t id,
                           const std::string& sql,
                           std::string& prepare_err,
                           std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto prepared = prepare_scheduler_sql(s, id, sql, timeout);
        if (prepared.has_error()) {
            prepare_err = std::string{prepared.error().what.c_str()};
            return prepared;
        }
        prepare_err.clear();
        return execute_scheduler_statement(s, id, timeout);
    }

    // Row count of `db.tbl` read straight from the engine. A failed SELECT is a
    // test failure, not a count of zero.
    inline size_t engine_row_count(const scheduler_stack& s, const std::string& db, const std::string& tbl) {
        session_id sid;
        auto cur = s.otterbrix->dispatcher()->execute_sql(sid, "SELECT * FROM " + db + "." + tbl + ";");
        if (!cur || cur->is_error()) {
            FAIL("engine_row_count: SELECT * FROM " << db << "." << tbl << " failed: "
                                                    << (cur ? cur->get_error().what.c_str() : "null cursor"));
        }
        return cur->size();
    }

    // Fresh dmldb.people (id bigint, name string) with `rows` rows, inserted in
    // multi-row VALUES batches so a table larger than one chunk is cheap to seed.
    inline void seed_people_bulk(const scheduler_stack& s, session_hash_t& id, size_t rows) {
        std::string err;
        const bool created_db = run_scheduler_sql(s, id++, "CREATE DATABASE dmldb;", err);
        INFO("CREATE DATABASE: " << err);
        REQUIRE(created_db);
        const bool created_tbl = run_scheduler_sql(s, id++, "CREATE TABLE dmldb.people (id bigint, name string);", err);
        INFO("CREATE TABLE: " << err);
        REQUIRE(created_tbl);

        constexpr size_t batch = 500;
        for (size_t first = 0; first < rows; first += batch) {
            const size_t last = std::min(rows, first + batch);
            std::string sql = "INSERT INTO dmldb.people (id, name) VALUES ";
            for (size_t i = first; i < last; ++i) {
                if (i != first) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i + 1) + ", 'name_" + std::to_string(i) + "')";
            }
            sql += ";";
            const bool inserted = run_scheduler_sql(s, id++, sql, err);
            INFO("INSERT rows [" << first << ", " << last << "): " << err);
            REQUIRE(inserted);
        }
    }

} // namespace otterstax::test
