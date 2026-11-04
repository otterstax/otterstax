// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Reads of a remote table: pages of at most 1024 rows under a cursor, the projected columns only and
// in their order, an empty table, a backend's refusal.

#include "federation_fixture.hpp"

#include <atomic>
#include <sys/resource.h>
#include <thread>

using namespace fedtest;

namespace {

    std::unique_ptr<fed_stack_t> pg_stack(std::size_t rows) {
        auto owned = std::make_unique<fed_stack_t>(
            std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public"}});
        std::vector<row_t> data;
        for (std::size_t i = 0; i < rows; ++i) {
            data.push_back({static_cast<std::int64_t>(i + 1), "n" + std::to_string(i % 7)});
        }
        owned->backend("pg").add_table({"shop",
                                       "public",
                                       "t",
                                       {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
                                       std::move(data)});
        return owned;
    }

} // namespace

TEST_CASE("scans: 2500 rows reach the cursor in chunks of at most 1024") {
    auto owned = pg_stack(2500);
    auto& stack = *owned;
    stack.open();
    auto cursor = stack.ok("SELECT id, note FROM pg.shop.public.t;");
    CHECK(rows_of(cursor).size() == 2500);
    for (const auto& chunk : cursor->chunks()) {
        CHECK(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
    }
    auto count = stack.ok("SELECT count(*) FROM pg.shop.public.t;");
    CHECK(rows_of(count) == std::vector<std::string>{"2500"});
}

TEST_CASE("scans: the read names only the projected columns in their order") {
    auto owned = pg_stack(3);
    auto& stack = *owned;
    stack.open();
    auto cursor = stack.ok("SELECT note, id FROM pg.shop.public.t ORDER BY id;");
    CHECK(rows_of(cursor) == std::vector<std::string>{"n0|1", "n1|2", "n2|3"});
    const auto reads = starting_with(stack.backend("pg").log(), "scan ");
    REQUIRE(reads.size() == 1);
    INFO(reads.front());
    CHECK(reads.front().find("\"note\"") < reads.front().find("\"id\""));
}

TEST_CASE("scans: an empty table answers its typed columns and no row") {
    auto owned = pg_stack(0);
    auto& stack = *owned;
    stack.open();
    auto cursor = stack.ok("SELECT id, note FROM pg.shop.public.t;");
    CHECK(rows_of(cursor).empty());
}

TEST_CASE("scans: a backend's refusal reaches the cursor with its code") {
    auto owned = pg_stack(1);
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").on_scan([](const std::string&, std::pmr::memory_resource*)
                                    -> std::optional<components::vector::data_chunk_t> { return std::nullopt; });
    stack.backend("pg").drop_table("t");
    CHECK(stack.refused("SELECT id FROM pg.shop.public.t;").type == core::error_code_t::undefined_object);
}

TEST_CASE("scans: a JOIN with a local table reads the remote side and joins in otterbrix") {
    auto owned = pg_stack(5);
    auto& stack = *owned;
    stack.open();
    stack.ok("CREATE TABLE main.public.l (id BIGINT, tag TEXT);");
    stack.ok("INSERT INTO main.public.l VALUES (2, 'two'), (4, 'four');");
    auto cursor = stack.ok("SELECT t.id, l.tag FROM pg.shop.public.t t JOIN main.public.l l ON t.id = l.id ORDER BY t.id;");
    CHECK(rows_of(cursor) == std::vector<std::string>{"2|two", "4|four"});
    CHECK(stack.backend("pg").changes().empty());
}

TEST_CASE("scans: answers settled on the backend's threads resume the reads of two sessions") {
    auto owned = pg_stack(1500);
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").answer_later(std::chrono::milliseconds(30));
    std::vector<std::thread> clients;
    std::atomic<int> good{0};
    for (int client = 0; client < 2; ++client) {
        clients.emplace_back([&stack, &good] {
            components::session::session_id_t session;
            for (int i = 0; i < 5; ++i) {
                auto cursor = stack.sql(session, "SELECT id, note FROM pg.shop.public.t;");
                if (cursor->is_success() && rows_of(cursor).size() == 1500) {
                    ++good;
                }
            }
        });
    }
    for (auto& client : clients) {
        client.join();
    }
    CHECK(good.load() == 10);
}

namespace {

    double cpu_seconds() {
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        return static_cast<double>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) +
               static_cast<double>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
    }

} // namespace

TEST_CASE("scans: the loop sleeps while a remote statement runs and wakes on its answer") {
    auto owned = pg_stack(10);
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").answer_later(std::chrono::milliseconds(1000));
    const auto passes_before = stack.actor("pg").loop_passes();
    const auto cpu_before = cpu_seconds();
    const auto started = std::chrono::steady_clock::now();
    auto cursor = stack.ok("SELECT id, note FROM pg.shop.public.t;");
    const auto elapsed = std::chrono::steady_clock::now() - started;
    const auto passes = stack.actor("pg").loop_passes() - passes_before;
    const auto cpu = cpu_seconds() - cpu_before;
    CHECK(rows_of(cursor).size() == 10);
    CHECK(elapsed >= std::chrono::milliseconds(1000));
    UNSCOPED_INFO("loop passes " << passes << ", process CPU " << cpu << " s over "
                                 << std::chrono::duration<double>(elapsed).count() << " s");
    // A pass per message and per answer, not one per millisecond.
    CHECK(passes < 20);
    WARN("loop passes " << passes << ", process CPU " << cpu << " s");
}

TEST_CASE("scans: with every agent busy a read waits in the actor's queue, not on a thread") {
    auto owned = std::make_unique<fed_stack_t>(
        std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public", 1}});
    auto& stack = *owned;
    stack.backend("pg").add_table({"shop",
                                   "public",
                                   "t",
                                   {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
                                   {{std::int64_t{1}, std::string{"a"}}}});
    stack.open();
    stack.backend("pg").answer_later(std::chrono::milliseconds(100));
    std::vector<std::thread> clients;
    std::atomic<int> good{0};
    for (int client = 0; client < 3; ++client) {
        clients.emplace_back([&stack, &good] {
            components::session::session_id_t session;
            auto cursor = stack.sql(session, "SELECT id, note FROM pg.shop.public.t;");
            if (cursor->is_success() && rows_of(cursor).size() == 1) {
                ++good;
            }
        });
    }
    for (auto& client : clients) {
        client.join();
    }
    CHECK(good.load() == 3);
    // One physical connection: every read ran on it, one after another.
    for (auto on : stack.backend("pg").connections()) {
        CHECK(on == stack.backend("pg").connections().front());
    }
}

TEST_CASE("scans: a read waiting longer than the checkout timeout is io_error") {
    auto owned = std::make_unique<fed_stack_t>(
        std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public", 1}});
    auto& stack = *owned;
    stack.backend("pg").add_table({"shop", "public", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
    stack.open();
    stack.actor("pg").set_checkout_timeout(std::chrono::milliseconds(100));
    // A block holds the one agent; a read of another session waits for it.
    stack.ok("BEGIN;");
    stack.ok("SELECT id FROM pg.shop.public.t;");
    components::session::session_id_t other;
    auto refused = stack.sql(other, "SELECT id FROM pg.shop.public.t;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::io_error);
    stack.ok("COMMIT;");
    CHECK(rows_of(stack.sql(other, "SELECT id FROM pg.shop.public.t;")) == std::vector<std::string>{"1"});
}

TEST_CASE("scans: a connection whose connect failed answers at once until the interval passed") {
    fed_stack_t stack({{"pg", backend_type_t::PostgreSQL, "shop", "public", 1}});
    stack.backend("pg").add_table({"shop", "public", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
    stack.open();
    stack.actor("pg").set_unreachable_interval(std::chrono::milliseconds(1500));
    auto timed = [&stack](const std::string& sql) {
        const auto started = std::chrono::steady_clock::now();
        auto cursor = stack.sql(sql);
        return std::pair{std::move(cursor), std::chrono::steady_clock::now() - started};
    };
    // The host stops answering: a connect takes 600 ms to fail.
    stack.backend("pg").set_down(true, std::chrono::milliseconds(600));
    auto [first, first_took] = timed("SELECT id FROM pg.shop.public.t;");
    REQUIRE(first->is_error());
    CHECK(first->get_error().type == core::error_code_t::io_error);
    CHECK(std::string{first->get_error().what}.find("is unreachable") != std::string::npos);
    CHECK(first_took >= std::chrono::milliseconds(600));
    CHECK(stack.backend("pg").connects() == 1);
    // Within the interval: refused at once, nobody connects.
    auto [second, second_took] = timed("SELECT id FROM pg.shop.public.t;");
    REQUIRE(second->is_error());
    CHECK(second->get_error().type == core::error_code_t::io_error);
    CHECK(std::string{second->get_error().what}.find("connection \"pg\" is unreachable") != std::string::npos);
    CHECK(second_took < std::chrono::milliseconds(300));
    // A discovery (the registration after a miss) too.
    const qualified_name_t scope{core::connection_name_t{"pg"}, core::dbname_t{"shop"}, core::schema_t{"public"}, core::relname_t{"t"}};
    auto [needs_sched, future] = actor_zeta::send(&stack.actor("pg"), &fed::backend_actor_t::discover, scope);
    static_cast<void>(needs_sched);
    const auto discover_started = std::chrono::steady_clock::now();
    auto discovered = wait_ready(std::move(future));
    CHECK(discovered.has_error());
    CHECK(std::chrono::steady_clock::now() - discover_started < std::chrono::milliseconds(300));
    CHECK(stack.backend("pg").connects() == 1);
    // The backend is back, the mark not expired yet: still refused without a try.
    stack.backend("pg").set_down(false);
    CHECK(stack.sql("SELECT id FROM pg.shop.public.t;")->is_error());
    CHECK(stack.backend("pg").connects() == 1);
    // Past the interval the next statement connects, and it reads.
    std::this_thread::sleep_for(std::chrono::milliseconds(1600));
    CHECK(rows_of(stack.ok("SELECT id FROM pg.shop.public.t;")) == std::vector<std::string>{"1"});
    CHECK(stack.backend("pg").connects() == 2);
    // Reachable again: the next one runs on the connection as it is.
    CHECK(rows_of(stack.ok("SELECT id FROM pg.shop.public.t;")) == std::vector<std::string>{"1"});
    CHECK(stack.backend("pg").connects() == 2);
}

TEST_CASE("scans: past the interval a connection still down is tried again") {
    fed_stack_t stack({{"pg", backend_type_t::PostgreSQL, "shop", "public", 1}});
    stack.backend("pg").add_table({"shop", "public", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
    stack.open();
    stack.actor("pg").set_unreachable_interval(std::chrono::milliseconds(300));
    stack.backend("pg").set_down(true);
    CHECK(stack.sql("SELECT id FROM pg.shop.public.t;")->is_error());
    CHECK(stack.backend("pg").connects() == 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    CHECK(stack.sql("SELECT id FROM pg.shop.public.t;")->is_error());
    CHECK(stack.backend("pg").connects() == 2);
}
