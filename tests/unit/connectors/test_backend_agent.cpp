// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The backend agents (otterbrix/federation/backend_agent.hpp) and how a backend actor hands them its
// statements, over fake connectors (tests/mock/fake_backend.hpp), on a pool of the test's own and
// without an engine: the transaction steps in each dialect, a failed COMMIT replacing the session,
// an unpin after BEGIN, ClickHouse without transactions, cancel(); a transaction holding one agent,
// statements waiting in the actor's queue while every agent is busy, the shutdown.

#include "otterbrix/federation/backend_actor.hpp"
#include "otterbrix/federation/backend_agent.hpp"
#include "tests/mock/fake_backend.hpp"

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/spawn.hpp>
#include <catch2/catch_all.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <vector>

using namespace fakebackend;
using fed::transaction_step_t;

namespace {

    using pool_t = actor_zeta::scheduler::sharing_scheduler;

    // A pool that stops before what ran on it goes (declare it before the agents / actors).
    struct pool_holder_t {
        explicit pool_holder_t(std::size_t threads)
            : pool(std::make_unique<pool_t>(threads, 1000)) {
            pool->start();
        }
        ~pool_holder_t() { stop(); }
        void stop() {
            if (running) {
                pool->stop();
                running = false;
            }
        }
        std::unique_ptr<pool_t> pool;
        bool running{true};
    };

    // One agent over one fake connector, its owner a counter of wake-ups.
    struct agent_stack_t {
        explicit agent_stack_t(backend_type_t kind)
            : server(kind)
            , agent(fake_agent(&resource, "c", kind, server, 1)) {
            agent->set_owner(&agent_stack_t::woken, this);
        }
        ~agent_stack_t() {
            agent->cancel();
            pool.stop();
        }

        template<typename Method, typename... Args>
        auto send(Method method, Args&&... args) {
            auto [needs_sched, future] = actor_zeta::send(agent.get(), method, std::forward<Args>(args)...);
            if (needs_sched) {
                pool.pool->enqueue(agent.get());
            }
            return std::move(future);
        }
        core::error_t step(transaction_step_t step, std::string savepoint = {}) {
            return wait_ready(send(&fed::backend_agent_t::step, step, std::move(savepoint)));
        }

        static void woken(void* self) noexcept { ++static_cast<agent_stack_t*>(self)->wakes; }

        std::pmr::synchronized_pool_resource resource;
        fake_backend_t server;
        pool_holder_t pool{2};
        fed::backend_agent_ptr agent;
        std::atomic<int> wakes{0};
    };

} // namespace

// ── the statements of the steps ──────────────────────────────────────────────

TEST_CASE("transaction statements: each step in each dialect") {
    using fed::transaction_statement;
    constexpr auto pg = backend_type_t::PostgreSQL;
    constexpr auto my = backend_type_t::MySQL;
    constexpr auto clickhouse = backend_type_t::ClickHouse;

    CHECK(transaction_statement(pg, transaction_step_t::begin, "") == "BEGIN");
    CHECK(transaction_statement(pg, transaction_step_t::commit, "") == "COMMIT");
    CHECK(transaction_statement(pg, transaction_step_t::rollback, "") == "ROLLBACK");
    CHECK(transaction_statement(pg, transaction_step_t::savepoint, "s1") == "SAVEPOINT \"s1\"");
    CHECK(transaction_statement(pg, transaction_step_t::rollback_to_savepoint, "s1") == "ROLLBACK TO SAVEPOINT \"s1\"");
    CHECK(transaction_statement(pg, transaction_step_t::release_savepoint, "s1") == "RELEASE SAVEPOINT \"s1\"");
    CHECK(transaction_statement(pg, transaction_step_t::savepoint, "a\"b") == "SAVEPOINT \"a\"\"b\"");
    CHECK(transaction_statement(pg, transaction_step_t::unpin, "") == "");

    CHECK(transaction_statement(my, transaction_step_t::begin, "") == "START TRANSACTION");
    CHECK(transaction_statement(my, transaction_step_t::commit, "") == "COMMIT");
    CHECK(transaction_statement(my, transaction_step_t::rollback, "") == "ROLLBACK");
    CHECK(transaction_statement(my, transaction_step_t::savepoint, "s1") == "SAVEPOINT `s1`");
    CHECK(transaction_statement(my, transaction_step_t::rollback_to_savepoint, "s1") == "ROLLBACK TO SAVEPOINT `s1`");
    CHECK(transaction_statement(my, transaction_step_t::release_savepoint, "s1") == "RELEASE SAVEPOINT `s1`");
    CHECK(transaction_statement(my, transaction_step_t::savepoint, "a`b") == "SAVEPOINT `a``b`");
    CHECK(transaction_statement(my, transaction_step_t::unpin, "") == "");

    // A savepoint step needs its name.
    CHECK_FALSE(transaction_statement(pg, transaction_step_t::savepoint, "").has_value());
    CHECK_FALSE(transaction_statement(my, transaction_step_t::rollback_to_savepoint, "").has_value());

    // ClickHouse has no transactions: unpin only.
    CHECK(transaction_statement(clickhouse, transaction_step_t::unpin, "") == "");
    CHECK_FALSE(transaction_statement(clickhouse, transaction_step_t::begin, "").has_value());
    CHECK_FALSE(transaction_statement(clickhouse, transaction_step_t::commit, "").has_value());
    CHECK_FALSE(transaction_statement(clickhouse, transaction_step_t::savepoint, "s").has_value());
}

TEST_CASE("mysql_update_rows_matched: the first of the three numbers whatever the language") {
    using fed::mysql_update_rows_matched;
    CHECK(mysql_update_rows_matched("Rows matched: 3  Changed: 1  Warnings: 0") == std::optional<std::uint64_t>{3});
    CHECK(mysql_update_rows_matched("Rows matched: 0  Changed: 0  Warnings: 0") == std::optional<std::uint64_t>{0});
    CHECK(mysql_update_rows_matched("Datensätze gefunden: 12  Geändert: 0  Warnungen: 0") ==
          std::optional<std::uint64_t>{12});
    CHECK(mysql_update_rows_matched("Записей найдено: 2  Изменено: 2  Предупреждений: 0") ==
          std::optional<std::uint64_t>{2});
    CHECK(mysql_update_rows_matched("Rows matched: 18446744073709551615  Changed: 0  Warnings: 0") ==
          std::optional<std::uint64_t>{18446744073709551615ULL});
    CHECK_FALSE(mysql_update_rows_matched("").has_value());
    CHECK_FALSE(mysql_update_rows_matched("Rows matched: 1  Changed: 1").has_value());
    CHECK_FALSE(mysql_update_rows_matched("Records: 1  Duplicates: 0  Warnings: 0  Extra: 4").has_value());
    CHECK_FALSE(mysql_update_rows_matched("Rows matched: 99999999999999999999  Changed: 0  Warnings: 0").has_value());
}

// ── one agent ────────────────────────────────────────────────────────────────

TEST_CASE("agent: BEGIN, SAVEPOINT and ROLLBACK TO in each dialect, on its own connection") {
    for (auto kind : {backend_type_t::PostgreSQL, backend_type_t::MySQL}) {
        agent_stack_t stack(kind);
        REQUIRE_FALSE(stack.step(transaction_step_t::begin).contains_error());
        REQUIRE_FALSE(stack.step(transaction_step_t::savepoint, "s1").contains_error());
        REQUIRE_FALSE(stack.step(transaction_step_t::rollback_to_savepoint, "s1").contains_error());
        REQUIRE_FALSE(stack.step(transaction_step_t::release_savepoint, "s1").contains_error());
        REQUIRE_FALSE(stack.step(transaction_step_t::commit).contains_error());
        CHECK(stack.server.log() == std::vector<std::string>{"begin",
                                                             "savepoint s1",
                                                             "rollback_to_savepoint s1",
                                                             "release_savepoint s1",
                                                             "commit"});
        // A savepoint without a name is the caller's mistake.
        CHECK(stack.step(transaction_step_t::savepoint).type == core::error_code_t::invalid_parameter);
        // Every answer woke the owner.
        CHECK(stack.wakes.load() == 6);
    }
}

TEST_CASE("agent: a failed COMMIT replaces the session: close, then reconnect") {
    std::pmr::synchronized_pool_resource resource;
    fake_backend_t server(backend_type_t::PostgreSQL);
    pool_holder_t pool(1);
    auto agent = actor_zeta::spawn<fed::backend_agent_t>(&resource, "pg");
    auto connector = std::make_unique<fake_pg_t>(server, 1);
    auto* raw = connector.get();
    agent->attach(std::move(connector));
    auto run = [&](transaction_step_t step) {
        auto [needs_sched, future] = actor_zeta::send(agent.get(), &fed::backend_agent_t::step, step, std::string{});
        if (needs_sched) {
            pool.pool->enqueue(agent.get());
        }
        return wait_ready(std::move(future));
    };
    REQUIRE_FALSE(run(transaction_step_t::begin).contains_error());
    server.fail_commits(true);
    CHECK(run(transaction_step_t::commit).type == core::error_code_t::io_error);
    CHECK(raw->closes.load() == 1);
    CHECK(raw->reconnects.load() == 1);
    // A COMMIT that went through keeps the session.
    server.fail_commits(false);
    REQUIRE_FALSE(run(transaction_step_t::begin).contains_error());
    REQUIRE_FALSE(run(transaction_step_t::commit).contains_error());
    CHECK(raw->closes.load() == 1);
    pool.stop();
}

TEST_CASE("agent: unpin after BEGIN rolls back and answers invalid_parameter, a reader's unpin runs nothing") {
    agent_stack_t stack(backend_type_t::MySQL);
    REQUIRE_FALSE(stack.step(transaction_step_t::unpin).contains_error());
    CHECK(stack.server.log().empty());
    REQUIRE_FALSE(stack.step(transaction_step_t::begin).contains_error());
    CHECK(stack.step(transaction_step_t::unpin).type == core::error_code_t::invalid_parameter);
    CHECK(stack.server.log() == std::vector<std::string>{"begin", "rollback"});
    // The transaction is gone: the next unpin has nothing to end.
    REQUIRE_FALSE(stack.step(transaction_step_t::unpin).contains_error());
}

TEST_CASE("agent: ClickHouse has no transactions, unpin is the one step it answers") {
    agent_stack_t stack(backend_type_t::ClickHouse);
    CHECK_FALSE(stack.step(transaction_step_t::unpin).contains_error());
    for (auto step : {transaction_step_t::begin,
                      transaction_step_t::commit,
                      transaction_step_t::rollback,
                      transaction_step_t::savepoint,
                      transaction_step_t::rollback_to_savepoint,
                      transaction_step_t::release_savepoint}) {
        CHECK(stack.step(step, "s1").type == core::error_code_t::unimplemented_yet);
    }
    CHECK(stack.server.log().empty());
}

TEST_CASE("agent: a read answers the rows of its statement, a write the backend's count") {
    agent_stack_t stack(backend_type_t::PostgreSQL);
    stack.server.add_table(
        {"shop", "public", "t", {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
         {{std::int64_t{1}, std::string{"a"}}, {std::int64_t{2}, std::string{"b"}}}});
    auto rows = wait_ready(stack.send(&fed::backend_agent_t::read,
                                      std::string{R"(SELECT "note" FROM "public"."t")"},
                                      std::vector<std::optional<std::string>>{},
                                      std::uint64_t{1}));
    REQUIRE_FALSE(rows.has_error());
    REQUIRE(rows.value().size() == 1);
    CHECK(rows.value().front().size() == 2);
    CHECK(rows.value().front().column_count() == 1);
    auto written = wait_ready(stack.send(&fed::backend_agent_t::write,
                                         std::string{R"(INSERT INTO "public"."t" VALUES (3, 'c'), (4, 'd'))"},
                                         fed::write_kind_t::insert));
    REQUIRE_FALSE(written.has_error());
    CHECK(written.value() == 2);
}

TEST_CASE("agent: a MySQL discovery reads the server's listing and probes") {
    agent_stack_t stack(backend_type_t::MySQL);
    stack.server.add_table({"Bill", "", "Orders", {column("id", logical_type::BIGINT), column("Note", logical_type::STRING_LITERAL)}, {}});
    auto reply = wait_ready(stack.send(&fed::backend_agent_t::discover,
                                       qualified_name_t{core::connection_name_t{"my"},
                                                        core::dbname_t{"Bill"},
                                                        core::schema_t{},
                                                        core::relname_t{}}));
    INFO((reply.has_error() ? std::string{reply.error().what} : std::string{}));
    REQUIRE_FALSE(reply.has_error());
    REQUIRE(reply.value().size() == 1);
    const auto& table = reply.value().front();
    CHECK(table.name.collection.t == "Orders");
    REQUIRE(table.schema.child_types().size() == 2);
    CHECK(table.schema.child_types()[0].type() == logical_type::BIGINT);
    CHECK(table.schema.child_types()[1].alias() == "Note");
}

TEST_CASE("agent: cancel ends a statement the backend never answers and refuses the next one") {
    agent_stack_t stack(backend_type_t::PostgreSQL);
    stack.server.block_reads(true);
    auto hanging = stack.send(&fed::backend_agent_t::read,
                              std::string{R"(SELECT "id" FROM "public"."t")"},
                              std::vector<std::optional<std::string>>{},
                              std::uint64_t{1});
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (stack.server.hanging() == 0) {
        REQUIRE(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    stack.agent->cancel();
    auto answered = wait_ready(std::move(hanging));
    REQUIRE(answered.has_error());
    CHECK(answered.error().type == core::error_code_t::io_error);
    stack.server.block_reads(false);
    auto refused = wait_ready(stack.send(&fed::backend_agent_t::read,
                                         std::string{R"(SELECT "id" FROM "public"."t")"},
                                         std::vector<std::optional<std::string>>{},
                                         std::uint64_t{1}));
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
}

// ── an actor and its agents ──────────────────────────────────────────────────

namespace {

    constexpr fed::oid_t table_oid = 100;

    // A PostgreSQL connection's actor over `agents` fake agents and one registered table
    // pg.shop.public.t (id, note).
    struct actor_stack_t {
        explicit actor_stack_t(int agents_count, std::size_t threads = 4)
            : server(backend_type_t::PostgreSQL)
            , pool(threads) {
            server.add_table(
                {"shop", "public", "t", {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
                 {{std::int64_t{1}, std::string{"a"}}}});
            std::vector<fed::backend_agent_ptr> agents;
            for (int i = 0; i < agents_count; ++i) {
                agents.push_back(fake_agent(&resource, "pg", backend_type_t::PostgreSQL, server, i + 1));
            }
            actor = actor_zeta::spawn<fed::backend_actor_t>(&resource,
                                                             fed::connection_t{"pg", backend_type_t::PostgreSQL},
                                                             components::session::session_id_t::generate_uid(),
                                                             std::move(agents),
                                                             pool.pool.get());
            REQUIRE_FALSE(actor->construction_error().contains_error());
            auto id = column("id", logical_type::BIGINT);
            auto note = column("note", logical_type::STRING_LITERAL);
            actor->load_tables({fed::registered_table_t{
                table_oid,
                qualified_name_t{core::connection_name_t{"pg"}, core::dbname_t{"shop"}, core::schema_t{"public"}, core::relname_t{"t"}},
                "public",
                "t",
                {fed::registered_column_t{1, "id", id, "id"}, fed::registered_column_t{2, "note", note, "note"}}}});
        }
        // main's shutdown: the agents cancelled, the pool stopped, the actor destroyed.
        ~actor_stack_t() { shut_down(); }
        void shut_down() {
            if (actor) {
                actor->cancel_agents();
                pool.stop();
                actor.reset();
            }
        }

        // A read of the table by engine transaction `txn`, inside a block or not.
        auto read(std::uint64_t txn, bool block) {
            components::table::transaction_data data{txn, 0};
            data.command_id = block ? 1 : 0;
            auto [needs_sched, future] = actor_zeta::send(actor.get(),
                                                          &fed::backend_actor_t::storage_fetch_next_batch,
                                                          components::session::session_id_t{},
                                                          table_oid,
                                                          std::uint64_t{0},
                                                          std::int64_t{-1},
                                                          std::vector<size_t>{},
                                                          data);
            static_cast<void>(needs_sched);
            return std::move(future);
        }
        void release(std::uint64_t txn) {
            components::execution_context_t ctx;
            ctx.txn = components::table::transaction_data{txn, 0};
            auto [needs_sched, future] = actor_zeta::send(actor.get(),
                                                          &fed::backend_actor_t::storage_release_prepared,
                                                          ctx,
                                                          std::pmr::set<fed::oid_t>{&resource});
            static_cast<void>(needs_sched);
            wait_ready(std::move(future));
        }

        std::pmr::synchronized_pool_resource resource;
        fake_backend_t server;
        pool_holder_t pool;
        fed::backend_actor_ptr actor{nullptr, actor_zeta::pmr::deleter_t{std::pmr::new_delete_resource()}};
    };

    bool answered_rows(const core::result_wrapper_t<services::disk::fetch_batch_t>& page) {
        return !page.has_error() && page.value().batch && page.value().batch->size() == 1;
    }

} // namespace

TEST_CASE("actor: a transaction holds one agent until it ends, another takes the next, a third waits") {
    actor_stack_t stack(2);
    stack.actor->set_checkout_timeout(std::chrono::milliseconds(200));
    CHECK(answered_rows(wait_ready(stack.read(1, true))));
    CHECK(stack.actor->pinned() == 1);
    CHECK(answered_rows(wait_ready(stack.read(2, true))));
    CHECK(stack.actor->pinned() == 2);
    // Each transaction keeps its own agent.
    CHECK(answered_rows(wait_ready(stack.read(1, true))));
    CHECK(answered_rows(wait_ready(stack.read(2, true))));
    // The reads (each block's BEGIN before its first one aside).
    std::vector<int> on;
    const auto log = stack.server.log();
    const auto all = stack.server.connections();
    for (std::size_t i = 0; i < log.size(); ++i) {
        if (log[i].rfind("scan ", 0) == 0) {
            on.push_back(all[i]);
        }
    }
    REQUIRE(on.size() == 4);
    CHECK(on[0] == on[2]);
    CHECK(on[1] == on[3]);
    CHECK(on[0] != on[1]);
    // Every agent is held: a third transaction waits, past the checkout timeout here.
    auto refused = wait_ready(stack.read(3, true));
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
    // A release frees its agent: the next one takes it.
    stack.release(1);
    CHECK(stack.actor->pinned() == 1);
    CHECK(answered_rows(wait_ready(stack.read(3, true))));
    CHECK(stack.actor->pinned() == 2);
    // An autocommit read holds no agent past its statement.
    stack.release(2);
    stack.release(3);
    CHECK(answered_rows(wait_ready(stack.read(4, false))));
    CHECK(stack.actor->pinned() == 0);
}

TEST_CASE("actor: with its one agent busy, statements wait in the queue and run one after another") {
    actor_stack_t stack(1);
    stack.server.answer_later(std::chrono::milliseconds(50));
    std::vector<actor_zeta::unique_future<core::result_wrapper_t<services::disk::fetch_batch_t>>> futures;
    for (std::uint64_t txn = 1; txn <= 4; ++txn) {
        futures.push_back(stack.read(txn, false));
    }
    for (auto& future : futures) {
        CHECK(answered_rows(wait_ready(std::move(future))));
    }
    const auto on = stack.server.connections();
    REQUIRE(on.size() == 4);
    for (auto connection : on) {
        CHECK(connection == on.front());
    }
}

TEST_CASE("actor: the shutdown answers a statement the backend never answers, then the pool stops") {
    actor_stack_t stack(2, 2);
    stack.server.block_reads(true);
    auto hanging = stack.read(1, false);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (stack.server.hanging() == 0) {
        REQUIRE(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto started = std::chrono::steady_clock::now();
    stack.actor->cancel_agents();
    auto answered = wait_ready(std::move(hanging));
    REQUIRE(answered.has_error());
    CHECK(answered.error().type == core::error_code_t::io_error);
    stack.shut_down();
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(5));
}

TEST_CASE("actor: four open blocks on four agents do not wait for each other, a fifth waits for one") {
    actor_stack_t stack(4);
    stack.actor->set_checkout_timeout(std::chrono::milliseconds(200));
    for (std::uint64_t txn = 1; txn <= 4; ++txn) {
        CHECK(answered_rows(wait_ready(stack.read(txn, true))));
    }
    CHECK(stack.actor->pinned() == 4);
    // Each block is a backend transaction of its own agent from its first read on.
    auto on = stack.server.connections();
    const auto log = stack.server.log();
    REQUIRE(log.size() == 8);
    for (std::size_t i = 0; i < 8; i += 2) {
        CHECK(log[i] == "begin");
        CHECK(log[i + 1].ends_with("@txn"));
        CHECK(on[i] == on[i + 1]);
    }
    CHECK(std::set<int>{on.begin(), on.end()}.size() == 4);
    // The four keep their agents: a second read of each runs at once.
    for (std::uint64_t txn = 1; txn <= 4; ++txn) {
        CHECK(answered_rows(wait_ready(stack.read(txn, true))));
    }
    // A fifth block finds every agent held: it waits, past the checkout timeout here.
    auto refused = wait_ready(stack.read(5, true));
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
    stack.release(2);
    CHECK(answered_rows(wait_ready(stack.read(5, true))));
    CHECK(stack.actor->pinned() == 4);
}

TEST_CASE("actor: a statement waiting for its own transaction's agent waits past the checkout timeout") {
    actor_stack_t stack(1);
    stack.actor->set_checkout_timeout(std::chrono::milliseconds(100));
    stack.server.answer_later(std::chrono::milliseconds(400));
    // Two reads of one block at once (two scans of one statement): one waits for the other, on the
    // agent the block holds — no other agent could serve it.
    auto first = stack.read(1, true);
    auto second = stack.read(1, true);
    CHECK(answered_rows(wait_ready(std::move(first))));
    CHECK(answered_rows(wait_ready(std::move(second))));
    stack.release(1);
    CHECK(stack.actor->pinned() == 0);
}

TEST_CASE("pg_commit_rolled_back: the tag a COMMIT of an aborted transaction answers") {
    CHECK(fed::pg_commit_rolled_back("ROLLBACK"));
    CHECK_FALSE(fed::pg_commit_rolled_back("COMMIT"));
    CHECK_FALSE(fed::pg_commit_rolled_back(""));
}

TEST_CASE("agent: a failed statement breaks a PostgreSQL transaction until a ROLLBACK TO a savepoint before it") {
    agent_stack_t stack(backend_type_t::PostgreSQL);
    stack.server.add_table({"shop", "public", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
    stack.server.fail_statements("broken", core::error_code_t::internal_error);
    auto read = [&](std::string sql) {
        return wait_ready(stack.send(&fed::backend_agent_t::read,
                                     std::move(sql),
                                     std::vector<std::optional<std::string>>{},
                                     std::uint64_t{1}));
    };
    REQUIRE_FALSE(stack.step(transaction_step_t::begin).contains_error());
    REQUIRE_FALSE(stack.step(transaction_step_t::savepoint, "s1").contains_error());
    CHECK(read("SELECT broken").has_error());
    // Nothing runs on it now but its end or the ROLLBACK TO.
    CHECK(read("SELECT \"id\" FROM \"public\".\"t\"").error().type == core::error_code_t::transaction_finalized);
    CHECK(stack.step(transaction_step_t::savepoint, "s2").type == core::error_code_t::transaction_finalized);
    REQUIRE_FALSE(stack.step(transaction_step_t::rollback_to_savepoint, "s1").contains_error());
    CHECK_FALSE(read("SELECT \"id\" FROM \"public\".\"t\"").has_error());
    REQUIRE_FALSE(stack.step(transaction_step_t::commit).contains_error());
    // Without a savepoint before the failure its COMMIT is refused, and a ROLLBACK goes instead.
    REQUIRE_FALSE(stack.step(transaction_step_t::begin).contains_error());
    CHECK(read("SELECT broken").has_error());
    stack.server.clear();
    CHECK(stack.step(transaction_step_t::commit).type == core::error_code_t::transaction_finalized);
    CHECK(stack.server.log() == std::vector<std::string>{"rollback"});
}

TEST_CASE("agent: a MySQL deadlock ends the transaction and its COMMIT is refused") {
    agent_stack_t stack(backend_type_t::MySQL);
    stack.server.fail_statements("dead", core::error_code_t::write_conflict);
    auto write = [&](std::string sql) {
        return wait_ready(stack.send(&fed::backend_agent_t::write, std::move(sql), fed::write_kind_t::insert));
    };
    REQUIRE_FALSE(stack.step(transaction_step_t::begin).contains_error());
    REQUIRE_FALSE(write("INSERT ok").has_error());
    REQUIRE_FALSE(stack.step(transaction_step_t::savepoint, "s1").contains_error());
    CHECK(write("INSERT dead").error().type == core::error_code_t::write_conflict);
    // InnoDB rolled it all back: no savepoint recovers it, and no later write lands outside it.
    CHECK(stack.step(transaction_step_t::rollback_to_savepoint, "s1").type == core::error_code_t::transaction_finalized);
    CHECK(write("INSERT later").error().type == core::error_code_t::transaction_finalized);
    CHECK(stack.step(transaction_step_t::commit).type == core::error_code_t::transaction_finalized);
    const auto log = stack.server.log();
    CHECK(std::find(log.begin(), log.end(), "commit") == log.end());
    CHECK(std::none_of(log.begin(), log.end(), [](const auto& line) { return line.find("later") != std::string::npos; }));
}
