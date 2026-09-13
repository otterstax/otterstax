// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Contract of utility/wait_barrier.hpp — the io_context worker -> consumer thread
// boundary for connector results.
//
// Two invariants are pinned here. (1) A connector outcome always reaches the
// consumer as a VALUE: a value the handler produced, an error the connector
// reported, an exception thrown on the io thread, and an io_context torn down
// before the query ran all arrive through future.get() without an exception.
// (2) QueryHandleWaiter::wait() fills `results` one-per-future, in order: three
// ConnectorManagers index wait_guard.results[j] positionally right after the call
// (integration/{sql,postgresql,clickhouse}/connection_manager.cpp), so anything
// that breaks the one-per-future postcondition turns those subscripts into
// out-of-bounds reads in a -O3 -DNDEBUG build, on exactly the backend-failure path.
// (3) run_with_owned_handler owns a query's handler for as long as the query runs:
// the connector's runQuery sees it only through a non-owning function_ref_t.

#include "utility/function_ref.hpp"
#include "utility/wait_barrier.hpp"

#include <catch2/catch_all.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

namespace {

    boost::asio::awaitable<core::result_wrapper_t<int>> answer(int value) { co_return value; }

    boost::asio::awaitable<core::result_wrapper_t<int>> throw_std_exception() {
        throw std::runtime_error("driver failed");
        co_return 0;
    }

    boost::asio::awaitable<core::result_wrapper_t<int>> throw_non_std() {
        throw 42;
        co_return 0;
    }

    boost::asio::awaitable<core::result_wrapper_t<int>> report_value_error(core::error_t error) {
        co_return std::move(error);
    }

    boost::asio::awaitable<core::error_t> report_error_only(core::error_t error) { co_return std::move(error); }

    using strand_t = boost::asio::strand<boost::asio::io_context::executor_type>;

    // Whether the body runs inside `strand` — what the MySQL connector relies on
    // when it spawns its connect on the connection's strand executor.
    boost::asio::awaitable<core::result_wrapper_t<bool>> running_in(const strand_t& strand) {
        co_return strand.running_in_this_thread();
    }

} // namespace

TEST_CASE("QueryHandleWaiter: wait() fills results one-per-future, in order") {
    auto* resource = std::pmr::new_delete_resource();
    otterstax::QueryHandleWaiter<int> waiter{resource};

    std::promise<core::result_wrapper_t<int>> p0, p1, p2;
    waiter.futures.push_back(p0.get_future());
    waiter.futures.push_back(p1.get_future());
    waiter.futures.push_back(p2.get_future());

    // Complete out of order: wait() must preserve FUTURE order, not completion order.
    p2.set_value(30);
    p0.set_value(10);
    p1.set_value(20);

    auto barrier = waiter.wait();
    REQUIRE_FALSE(barrier.has_error());

    REQUIRE(waiter.results.size() == waiter.futures.size());
    REQUIRE(waiter.results[0] == 10);
    REQUIRE(waiter.results[1] == 20);
    REQUIRE(waiter.results[2] == 30);
}

TEST_CASE("QueryHandleWaiter: every futures index has a matching results index") {
    // The exact invariant the three managers rely on, written as a loop so a
    // failure names the bad index. T is move-only here, like the real
    // unique_ptr<data_chunk_t> payload.
    auto* resource = std::pmr::new_delete_resource();
    otterstax::QueryHandleWaiter<std::unique_ptr<std::string>> waiter{resource};

    constexpr size_t N = 4;
    std::vector<std::promise<core::result_wrapper_t<std::unique_ptr<std::string>>>> promises(N);
    for (size_t i = 0; i < N; ++i) {
        waiter.futures.push_back(promises[i].get_future());
    }
    for (size_t i = 0; i < N; ++i) {
        promises[i].set_value(std::make_unique<std::string>("chunk" + std::to_string(i)));
    }

    auto barrier = waiter.wait();
    REQUIRE_FALSE(barrier.has_error());

    REQUIRE(waiter.results.size() == N);
    for (size_t j = 0; j < waiter.futures.size(); ++j) {
        INFO("index " << j);
        REQUIRE(j < waiter.results.size());
        REQUIRE(waiter.results[j] != nullptr);
        REQUIRE(*waiter.results[j] == "chunk" + std::to_string(j));
    }
}

TEST_CASE("QueryHandleWaiter: wait() reports the first error and leaves a results prefix") {
    auto* resource = std::pmr::new_delete_resource();
    otterstax::QueryHandleWaiter<int> waiter{resource};

    std::promise<core::result_wrapper_t<int>> good, bad, never_read;
    waiter.futures.push_back(good.get_future());
    waiter.futures.push_back(bad.get_future());
    waiter.futures.push_back(never_read.get_future());

    // Connector errors arrive as a VALUE (core::error_t inside result_wrapper_t),
    // never as a future exception — that is the io->consumer boundary contract in
    // wait_barrier.hpp. wait() hands that error back on the consumer thread.
    good.set_value(7);
    bad.set_value(core::error_t(core::error_code_t::io_error, std::pmr::string{"simulated DB failure", resource}));
    never_read.set_value(9);

    auto barrier = waiter.wait();
    REQUIRE(barrier.has_error());
    REQUIRE(barrier.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{barrier.error().what.c_str()} == "simulated DB failure");

    // results is a PREFIX: the third future was never consumed. Managers must
    // therefore never index past results.size().
    REQUIRE(waiter.results.size() == 1);
    REQUIRE(waiter.results[0] == 7);
    REQUIRE(waiter.results.size() < waiter.futures.size());
}

TEST_CASE("QueryHandleWaiter: futures and results live on the given memory_resource") {
    // The containers must be pmr containers bound to the caller's resource — never
    // the default resource — so a manager's arena owns every marshaled payload.
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    otterstax::QueryHandleWaiter<int> waiter{&arena};
    REQUIRE(waiter.futures.get_allocator().resource() == &arena);
    REQUIRE(waiter.results.get_allocator().resource() == &arena);
}

TEST_CASE("QueryHandleWaiter: the destructor waits for the futures wait() left unconsumed") {
    // wait() returns at the first error, but the handlers behind the failed one
    // may still be running on io threads with references into the caller's
    // frame: the waiter must not go away before each of them has settled.
    auto* resource = std::pmr::new_delete_resource();
    std::atomic<bool> tail_settled{false};
    std::thread producer;
    {
        otterstax::QueryHandleWaiter<int> waiter{resource};
        // Declared after the waiter: should an assertion below fail before the
        // producer takes the tail promise, the promises die first and the drain
        // sees a broken promise instead of waiting forever.
        std::promise<core::result_wrapper_t<int>> failed;
        std::promise<core::result_wrapper_t<int>> tail;
        waiter.futures.push_back(failed.get_future());
        waiter.futures.push_back(tail.get_future());
        failed.set_value(
            core::error_t(core::error_code_t::io_error, std::pmr::string{"simulated DB failure", resource}));

        auto barrier = waiter.wait();
        REQUIRE(barrier.has_error());
        REQUIRE(waiter.results.empty());
        REQUIRE(waiter.futures[1].valid());

        producer = std::thread{[promise = std::move(tail), &tail_settled]() mutable {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            tail_settled.store(true);
            promise.set_value(9);
        }};
    }
    // Read before the join: only a destructor that blocked on the tail future
    // lets the producer's store happen before this line.
    const bool drained = tail_settled.load();
    producer.join();
    REQUIRE(drained);
}

TEST_CASE("make_failed_future: reports the error without touching an io_context") {
    auto* resource = std::pmr::new_delete_resource();

    // The synchronous-failure path of executeQuery (unknown uuid, closed connector,
    // failed reconnect): a ready future carrying the error, so the function has one
    // single way to report a failure instead of a throw plus a value.
    auto fut = otterstax::make_failed_future<std::unique_ptr<std::string>>(
        core::error_t(core::error_code_t::do_not_exists, std::pmr::string{"no such connection", resource}));

    auto outcome = fut.get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::do_not_exists);
}

TEST_CASE("make_failed_future: error-only handlers collapse to a bare core::error_t") {
    auto* resource = std::pmr::new_delete_resource();

    // query_result_t maps an asio_error_t handler onto core::error_t rather than
    // result_wrapper_t<asio_error_t>, so a failure is never carried twice behind two
    // different predicates.
    static_assert(std::is_same_v<otterstax::query_result_t<otterstax::asio_error_t>, core::error_t>);

    auto fut = otterstax::make_failed_future<otterstax::asio_error_t>(
        core::error_t(core::error_code_t::io_error, std::pmr::string{"connector is not connected", resource}));

    auto err = fut.get();
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
}

TEST_CASE("as_query_result: shapes a handler's return into the marshaled outcome") {
    auto* resource = std::pmr::new_delete_resource();

    auto value = otterstax::as_query_result<int>(5);
    REQUIRE_FALSE(value.has_error());
    REQUIRE(value.value() == 5);

    auto clean = otterstax::as_query_result<otterstax::asio_error_t>(otterstax::asio_error_t{});
    REQUIRE_FALSE(clean.contains_error());

    auto failed = otterstax::as_query_result<otterstax::asio_error_t>(
        otterstax::asio_error_t{core::error_t(core::error_code_t::schema_error, std::pmr::string{"bad", resource})});
    REQUIRE(failed.contains_error());
    REQUIRE(failed.type == core::error_code_t::schema_error);
}

TEST_CASE("spawn_marshaled: a value produced on the io thread arrives through the future") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;

    auto fut = otterstax::spawn_marshaled<int>(io, answer(7), resource);
    std::thread worker{[&io] { io.run(); }};
    auto outcome = fut.get();
    worker.join();

    REQUIRE_FALSE(outcome.has_error());
    REQUIRE(outcome.value() == 7);
}

TEST_CASE("spawn_marshaled: accepts an executor and runs the awaitable on it") {
    // The first argument is an executor or an execution context, whatever
    // co_spawn takes: the ConnectorManagers pass their io_context, the MySQL
    // connector passes its connection's strand so every completion of the
    // connect — the operation's and its deadline timer's — is serialized.
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;
    strand_t strand = boost::asio::make_strand(io);

    auto on_strand = otterstax::spawn_marshaled<bool>(strand, running_in(strand), resource);
    // The type-erased form any_connection::get_executor() hands back.
    auto erased = otterstax::spawn_marshaled<int>(boost::asio::any_io_executor{strand}, answer(7), resource);
    std::thread worker{[&io] { io.run(); }};
    auto outcome = on_strand.get();
    auto value = erased.get();
    worker.join();

    REQUIRE_FALSE(outcome.has_error());
    REQUIRE(outcome.value());
    REQUIRE_FALSE(value.has_error());
    REQUIRE(value.value() == 7);
}

TEST_CASE("spawn_marshaled: a std::exception thrown on the io thread arrives as io_error") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;

    auto fut = otterstax::spawn_marshaled<int>(io, throw_std_exception(), resource);
    io.run();

    core::result_wrapper_t<int> outcome{0};
    REQUIRE_NOTHROW(outcome = fut.get());
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{outcome.error().what.c_str()} == "driver failed");
}

TEST_CASE("spawn_marshaled: a non-std exception thrown on the io thread arrives as io_error") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;

    auto fut = otterstax::spawn_marshaled<int>(io, throw_non_std(), resource);
    io.run();

    core::result_wrapper_t<int> outcome{0};
    REQUIRE_NOTHROW(outcome = fut.get());
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{outcome.error().what.c_str()} == "unknown connector error");
}

TEST_CASE("spawn_marshaled: an error the connector reports keeps its code") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;

    auto fut = otterstax::spawn_marshaled<int>(
        io,
        report_value_error(
            core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{"syntax error near FROM", resource})),
        resource);
    io.run();

    auto outcome = fut.get();
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::sql_parse_error);
    REQUIRE(std::string{outcome.error().what.c_str()} == "syntax error near FROM");
}

TEST_CASE("spawn_marshaled: an asio_error_t handler yields a bare core::error_t") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;

    auto failed = otterstax::spawn_marshaled<otterstax::asio_error_t>(
        io,
        report_error_only(core::error_t(core::error_code_t::table_not_exists, std::pmr::string{"no table", resource})),
        resource);
    auto clean = otterstax::spawn_marshaled<otterstax::asio_error_t>(io, report_error_only(core::error_t::no_error()),
                                                                     resource);
    io.run();

    static_assert(std::is_same_v<decltype(failed), std::future<core::error_t>>);
    auto err = failed.get();
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::table_not_exists);
    REQUIRE_FALSE(clean.get().contains_error());
}

TEST_CASE("spawn_marshaled: an io_context destroyed before the query ran yields io_error, not broken_promise") {
    auto* resource = std::pmr::new_delete_resource();
    std::future<core::result_wrapper_t<int>> fut;
    {
        boost::asio::io_context io;
        fut = otterstax::spawn_marshaled<int>(io, answer(7), resource);
        // Never run: the queued launch is destroyed together with the io_context.
    }

    REQUIRE(fut.valid());
    REQUIRE(fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
    core::result_wrapper_t<int> outcome{0};
    REQUIRE_NOTHROW(outcome = fut.get());
    REQUIRE(outcome.has_error());
    REQUIRE(outcome.error().type == core::error_code_t::io_error);
}

TEST_CASE("spawn_marshaled: an error-only query dropped with its io_context also yields io_error") {
    auto* resource = std::pmr::new_delete_resource();
    std::future<core::error_t> fut;
    {
        boost::asio::io_context io;
        fut = otterstax::spawn_marshaled<otterstax::asio_error_t>(io, report_error_only(core::error_t::no_error()),
                                                                  resource);
    }

    REQUIRE(fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
    auto err = fut.get();
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
}

namespace {

    // Stands in for an IConnector: suspends once, so the handler runs after the
    // full-expression that spawned the query is over, then calls it on `query`.
    struct deferred_connector {
        boost::asio::awaitable<core::result_wrapper_t<std::string>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::string(std::string_view)> handler) {
            auto executor = co_await boost::asio::this_coro::executor;
            co_await boost::asio::post(executor, boost::asio::use_awaitable);
            co_return handler(query);
        }
    };

} // namespace

TEST_CASE("run_with_owned_handler: a temporary handler outlives the expression that spawned the query") {
    auto* resource = std::pmr::new_delete_resource();
    boost::asio::io_context io;
    deferred_connector connector;

    // The lambda, and the heap string it owns, is a temporary of the spawning
    // full-expression, which is over before io.run(): the handler the connector
    // calls is the copy in run_with_owned_handler's frame.
    auto fut = otterstax::spawn_marshaled<std::string>(
        io,
        otterstax::run_with_owned_handler<std::string, std::string_view>(
            connector,
            "SELECT 1",
            [prefix = std::string(64, 'x')](std::string_view query) { return prefix + std::string{query}; }),
        resource);
    io.run();

    auto outcome = fut.get();
    REQUIRE_FALSE(outcome.has_error());
    REQUIRE(outcome.value() == std::string(64, 'x') + "SELECT 1");
}

TEST_CASE("function_ref_t: binds a named non-const callable only and calls it through the reference") {
    using ref_t = otterstax::function_ref_t<int(int)>;
    int calls = 0;
    auto add_one = [&calls](int value) {
        ++calls;
        return value + 1;
    };
    using callable_t = decltype(add_one);

    STATIC_REQUIRE(std::is_constructible_v<ref_t, callable_t&>);
    STATIC_REQUIRE_FALSE(std::is_constructible_v<ref_t, callable_t&&>);
    STATIC_REQUIRE_FALSE(std::is_constructible_v<ref_t, const callable_t&>);
    STATIC_REQUIRE_FALSE(std::is_convertible_v<callable_t&, ref_t>);

    const ref_t ref{add_one};
    const ref_t copy = ref;
    REQUIRE(ref(1) == 2);
    REQUIRE(copy(41) == 42);
    REQUIRE(calls == 2);
}
