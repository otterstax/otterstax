// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The connector pools and the actors that await them:
//   - BM_pool_round_trip_*     one no-op statement through a connection_pool_t: the hop to a
//                              pool thread and back, picked up by polling or by its waker;
//   - BM_pool_parallel         N slow statements on a four-connection pool (wall ≈ ceil(N/4)
//                              statement times);
//   - BM_actor_await_*         actors on a sharing_scheduler awaiting pool answers, spinning
//                              (actor-zeta's own polling) or parked (scheduler/parking.hpp);
//                              counters carry the process CPU per await;
//   - BM_engine_select_1_*     OtterbrixManager::execute of `SELECT 1` on a real engine from
//                              1/4/8 threads, concurrent (as now) and serialized by one mutex
//                              (the lock OtterbrixManager held around every statement before).

#include "connectors/common/connection_pool.hpp"
#include "connectors/common/driver.hpp"
#include "connectors/common/statement.hpp"
#include "connectors/common/wait.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "scheduler/parking.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <benchmark/benchmark.h>
#include <otterbrix/otterbrix.hpp>

#include <sys/resource.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

    using namespace std::chrono_literals;

    constexpr const char* bench_alias = "bench";

    // A connection that answers every statement with an empty result after `wait`.
    class sleep_driver_t final : public conn::IDriver {
    public:
        explicit sleep_driver_t(std::chrono::microseconds wait)
            : wait_(wait) {}

        core::error_t connect() override { return core::error_t::no_error(); }
        core::error_t ping() override { return core::error_t::no_error(); }
        void close() noexcept override {}
        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<sleep_driver_t>(wait_);
        }

        conn::statement_outcome_t execute(const conn::statement_t&) override {
            if (wait_.count() > 0) {
                std::this_thread::sleep_for(wait_);
            }
            return conn::statement_result_t{};
        }

    private:
        std::chrono::microseconds wait_;
    };

    // A pool of `connections` sessions, all opened up front, for alias `bench`.
    std::unique_ptr<conn::connection_pool_t> make_pool(std::chrono::microseconds wait, std::size_t connections) {
        auto pool = std::make_unique<conn::connection_pool_t>(std::pmr::get_default_resource());
        conn::pool_config_t config;
        config.min_size = connections;
        config.max_size = connections;
        auto added = pool->add(bench_alias, std::make_unique<sleep_driver_t>(wait), config);
        if (added.contains_error()) {
            throw std::runtime_error("bench pool: add failed");
        }
        return pool;
    }

    template<typename T>
    T take_when_ready(actor_zeta::unique_future<T>& future) {
        while (!future.is_ready()) {
            std::this_thread::yield();
        }
        return std::move(future).take_ready();
    }

    // User + system CPU of the whole process: every scheduler, pool and benchmark thread.
    std::chrono::microseconds process_cpu_time() {
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        auto to_us = [](const timeval& tv) {
            return std::chrono::seconds(tv.tv_sec) + std::chrono::microseconds(tv.tv_usec);
        };
        return std::chrono::duration_cast<std::chrono::microseconds>(to_us(usage.ru_utime) + to_us(usage.ru_stime));
    }

} // namespace

// ─── Pool round trip ─────────────────────────────────────────────────────────

// The caller polls the future, as drive() and CatalogManager's enqueue_impl do.
static void BM_pool_round_trip_poll(benchmark::State& state) {
    auto pool = make_pool(0us, 1);
    for (auto _ : state) {
        auto answer = pool->execute(bench_alias, conn::statement_t::rows("SELECT 1"));
        auto outcome = take_when_ready(answer);
        benchmark::DoNotOptimize(outcome);
    }
}
BENCHMARK(BM_pool_round_trip_poll)->UseRealTime();

// The caller sleeps until the request's waker runs, as a parked actor does.
static void BM_pool_round_trip_waker(benchmark::State& state) {
    auto pool = make_pool(0us, 1);
    std::binary_semaphore answered{0};
    for (auto _ : state) {
        auto answer = pool->execute(bench_alias, conn::statement_t::rows("SELECT 1"), [&answered] {
            answered.release();
        });
        answered.acquire();
        auto outcome = take_when_ready(answer); // settled before the waker ran
        benchmark::DoNotOptimize(outcome);
    }
}
BENCHMARK(BM_pool_round_trip_waker)->UseRealTime();

// ─── Pool fan-out ────────────────────────────────────────────────────────────

// N statements of 5 ms each on four connections: whichever session is free takes the next.
static void BM_pool_parallel(benchmark::State& state) {
    const auto statements = static_cast<std::size_t>(state.range(0));
    auto pool = make_pool(5ms, 4);
    std::counting_semaphore<> answered{0};
    std::vector<conn::statement_future_t> answers;
    answers.reserve(statements);
    for (auto _ : state) {
        answers.clear();
        for (std::size_t i = 0; i < statements; ++i) {
            answers.push_back(pool->execute(bench_alias, conn::statement_t::rows("SELECT 1"), [&answered] {
                answered.release();
            }));
        }
        for (std::size_t i = 0; i < statements; ++i) {
            answered.acquire();
        }
        for (auto& answer : answers) {
            benchmark::DoNotOptimize(take_when_ready(answer));
        }
    }
    state.counters["statements_per_s"] =
        benchmark::Counter(static_cast<double>(statements), benchmark::Counter::kIsIterationInvariantRate);
}
BENCHMARK(BM_pool_parallel)->Arg(1)->Arg(4)->Arg(8)->Arg(16)->UseRealTime()->Unit(benchmark::kMillisecond);

// ─── An actor awaiting pool answers: spin vs park ────────────────────────────

namespace {

    // A cooperative actor whose handler awaits a pool statement, shaped like a Worker awaiting
    // a backend: the request carries actor_parking_t's waker and the coroutine reports its ticket.
    // With `parks` false resume() stays the base one, so actor-zeta re-runs it until the answer is in.
    class pool_awaiter_t final : public actor_zeta::basic_actor<pool_awaiter_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        pool_awaiter_t(std::pmr::memory_resource* res,
                       actor_zeta::scheduler_raw scheduler,
                       conn::connection_pool_t* pool,
                       bool parks)
            : actor_zeta::basic_actor<pool_awaiter_t>(res)
            , parking_(this, scheduler)
            , pool_(pool)
            , parks_(parks) {}

        // Releases `done` once the answer is in.
        unique_future<void> run(std::counting_semaphore<>* done) {
            auto ticket = std::make_shared<conn::wait_ticket_t>();
            auto answer = pool_->execute(bench_alias, conn::statement_t::rows("SELECT 1"), parking_.watch(ticket));
            const bool waits = !answer.is_ready();
            if (waits) {
                parking_.suspending_on(ticket);
            }
            auto outcome = co_await std::move(answer);
            if (waits) {
                parking_.resumed();
            }
            benchmark::DoNotOptimize(outcome);
            done->release();
        }

        unique_future<void> wake() { co_return; }

        actor_zeta::scheduler::resume_info resume(std::size_t max_throughput) noexcept {
            if (parks_ && parking_.park()) {
                return actor_zeta::scheduler::resume_info(actor_zeta::scheduler::resume_result::awaiting, 0);
            }
            return actor_zeta::basic_actor<pool_awaiter_t>::resume(max_throughput);
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&pool_awaiter_t::run, &pool_awaiter_t::wake>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg) {
            auto cmd = msg->command();
            if (cmd == actor_zeta::msg_id<pool_awaiter_t, &pool_awaiter_t::run>) {
                co_await actor_zeta::dispatch(this, &pool_awaiter_t::run, msg);
            } else if (cmd == actor_zeta::msg_id<pool_awaiter_t, &pool_awaiter_t::wake>) {
                co_await actor_zeta::dispatch(this, &pool_awaiter_t::wake, msg);
            }
        }

    private:
        actor_parking_t<pool_awaiter_t> parking_;
        conn::connection_pool_t* pool_;
        bool parks_;
    };

    // state.range(0): the statement's time in ms; state.range(1): actors awaiting at once,
    // each on a connection of its own, on a four-thread sharing_scheduler.
    void actor_await(benchmark::State& state, bool parks) {
        const auto wait = std::chrono::milliseconds(state.range(0));
        const auto actors = static_cast<std::size_t>(state.range(1));
        auto* resource = std::pmr::get_default_resource();

        auto pool = make_pool(wait, actors);
        auto scheduler = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(4, /*max_throughput*/ 1000);
        scheduler->start();
        std::vector<std::unique_ptr<pool_awaiter_t, actor_zeta::pmr::deleter_t>> awaiters;
        for (std::size_t i = 0; i < actors; ++i) {
            awaiters.push_back(actor_zeta::spawn<pool_awaiter_t>(resource, scheduler.get(), pool.get(), parks));
        }

        std::counting_semaphore<> done{0};
        std::vector<actor_zeta::unique_future<void>> runs;
        runs.reserve(actors);
        const auto cpu_before = process_cpu_time();
        const auto wall_before = std::chrono::steady_clock::now();
        for (auto _ : state) {
            runs.clear();
            for (auto& awaiter : awaiters) {
                auto [needs_sched, future] = actor_zeta::send(awaiter->address(), &pool_awaiter_t::run, &done);
                if (needs_sched) {
                    scheduler->enqueue(awaiter.get());
                }
                runs.push_back(std::move(future));
            }
            for (std::size_t i = 0; i < actors; ++i) {
                done.acquire();
            }
            for (auto& run : runs) {
                take_when_ready(run);
            }
        }
        const auto wall = std::chrono::steady_clock::now() - wall_before;
        const auto cpu = process_cpu_time() - cpu_before;

        scheduler->stop();
        awaiters.clear();
        pool.reset();

        const double awaits = static_cast<double>(state.iterations()) * static_cast<double>(actors);
        const double cpu_ms = std::chrono::duration<double, std::milli>(cpu).count();
        const double wall_ms = std::chrono::duration<double, std::milli>(wall).count();
        state.counters["cpu_ms_per_await"] = benchmark::Counter(awaits > 0 ? cpu_ms / awaits : 0.0);
        state.counters["cpu_cores"] = benchmark::Counter(wall_ms > 0 ? cpu_ms / wall_ms : 0.0);
    }

} // namespace

static void BM_actor_await_spin(benchmark::State& state) { actor_await(state, /*parks*/ false); }
static void BM_actor_await_park(benchmark::State& state) { actor_await(state, /*parks*/ true); }

#define OTX_ACTOR_AWAIT_ARGS                                                                                           \
    ArgNames({"wait_ms", "actors"})                                                                                    \
        ->Args({0, 1})                                                                                                 \
        ->Args({2, 1})                                                                                                 \
        ->Args({20, 1})                                                                                                \
        ->Args({20, 4})                                                                                                \
        ->UseRealTime()                                                                                                \
        ->Unit(benchmark::kMicrosecond)

BENCHMARK(BM_actor_await_spin)->OTX_ACTOR_AWAIT_ARGS;
BENCHMARK(BM_actor_await_park)->OTX_ACTOR_AWAIT_ARGS;

#undef OTX_ACTOR_AWAIT_ARGS

// ─── OtterbrixManager statements from several threads ────────────────────────

namespace {

    // One engine and its OtterbrixManager for the whole run, built on first use after
    // bench_parser.cpp's static initializer registers the loggers. The function-local static dies
    // before spdlog::shutdown (registered earlier with atexit): members die in reverse, manager, then engine.
    struct engine_fixture_t {
        static constexpr const char* data_dir = "/tmp/otterstax_bench_engine";

        engine_fixture_t()
            : engine(make_engine())
            , resource(engine->dispatcher()->resource())
            , manager(actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(engine))) {}

        static db::otterbrix_engine_ptr make_engine() {
            std::filesystem::remove_all(data_dir);
            return db::make_otterbrix_engine(configuration::config::create_config(data_dir));
        }

        db::otterbrix_engine_ptr engine;
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> manager;
    };

    engine_fixture_t& engine_fixture() {
        static engine_fixture_t fixture;
        return fixture;
    }

    const otterstax::names::alias_registry_t& no_aliases() {
        static const otterstax::names::alias_registry_t empty;
        return empty;
    }

    // Each benchmark thread parses `SELECT 1` with a parser of its own (a Worker owns one)
    // and sends the plan to the manager, whose handler runs on the sending thread.
    void engine_select_1(benchmark::State& state, std::mutex* serialize) {
        auto& fixture = engine_fixture();
        GreenplumParser parser(fixture.resource, no_aliases());
        const session_hash_t id{static_cast<session_hash_t>(1000 + state.thread_index())};
        for (auto _ : state) {
            auto parsed = parser.parse("SELECT 1;");
            if (parsed.has_error()) {
                state.SkipWithError("parse failed");
                break;
            }
            auto statement = std::move(parsed.value()->otterbrix_params);
            std::unique_lock<std::mutex> guard;
            if (serialize != nullptr) {
                guard = std::unique_lock(*serialize);
            }
            auto [needs_sched, future] =
                actor_zeta::send(fixture.manager->address(), &db::OtterbrixManager::execute, id, std::move(statement));
            auto cursor = take_when_ready(future);
            if (guard.owns_lock()) {
                guard.unlock();
            }
            if (!cursor || !cursor->is_success()) {
                state.SkipWithError("execute failed");
                break;
            }
        }
        state.counters["statements_per_s"] = benchmark::Counter(static_cast<double>(state.iterations()),
                                                                benchmark::Counter::kIsRate);
    }

    std::mutex old_manager_lock;

} // namespace

static void BM_engine_select_1_concurrent(benchmark::State& state) { engine_select_1(state, nullptr); }
static void BM_engine_select_1_serialized(benchmark::State& state) { engine_select_1(state, &old_manager_lock); }

BENCHMARK(BM_engine_select_1_concurrent)->Threads(1)->Threads(4)->Threads(8)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_engine_select_1_serialized)->Threads(1)->Threads(4)->Threads(8)->UseRealTime()->Unit(benchmark::kMillisecond);
