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
#include <map>
#include <memory>
#include <memory_resource>
#include <set>
#include <string>
#include <thread>
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
                                         fed::write_kind_t::insert,
                                         false,
                                         qualified_name_t{}));
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

// An UPDATE whose narrowing assignment the generator left to MySQL's store into the column
// (sql_gen::statement_needs_t::strict_store) runs only on a session whose sql_mode is strict:
// elsewhere MySQL would clamp an out-of-range value where the engine refuses it.
TEST_CASE("agent: a write relying on a strict sql_mode runs only on a strict MySQL session") {
    const std::string update = "UPDATE `bill`.`orders` SET `a` = (`a` + 1);";
    auto write = [](agent_stack_t& stack, const std::string& sql, bool strict_store) {
        return wait_ready(stack.send(&fed::backend_agent_t::write,
                                     std::string{sql},
                                     fed::write_kind_t::update,
                                     strict_store,
                                     qualified_name_t{}));
    };
    SECTION("not strict: refused with nothing sent, a write not relying on it runs") {
        agent_stack_t stack(backend_type_t::MySQL);
        stack.server.set_sql_mode("NO_ENGINE_SUBSTITUTION");
        REQUIRE_FALSE(stack.agent->connect().contains_error());
        stack.server.clear();
        auto refused = write(stack, update, true);
        REQUIRE(refused.has_error());
        CHECK(refused.error().type == core::error_code_t::unimplemented_yet);
        CHECK(std::string{refused.error().what}.find("strict sql_mode") != std::string::npos);
        CHECK(stack.server.log().empty());
        CHECK_FALSE(write(stack, update, false).has_error());
        CHECK(stack.server.log().size() == 1);
    }
    for (const char* mode : {"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION", "STRICT_ALL_TABLES"}) {
        DYNAMIC_SECTION("strict: " << mode) {
            agent_stack_t stack(backend_type_t::MySQL);
            stack.server.set_sql_mode(mode);
            REQUIRE_FALSE(stack.agent->connect().contains_error());
            stack.server.clear();
            CHECK_FALSE(write(stack, update, true).has_error());
            CHECK(stack.server.log() == std::vector<std::string>{"write " + update});
        }
    }
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
        auto read(std::uint64_t txn, bool block, components::session::session_id_t session = {}) {
            components::table::transaction_data data{txn, 0};
            data.command_id = block ? 1 : 0;
            auto [needs_sched, future] = actor_zeta::send(actor.get(),
                                                          &fed::backend_actor_t::storage_fetch_next_batch,
                                                          session,
                                                          table_oid,
                                                          std::uint64_t{0},
                                                          std::int64_t{-1},
                                                          std::vector<size_t>{},
                                                          data);
            static_cast<void>(needs_sched);
            return std::move(future);
        }
        // The client's connection of `session` closed (query_session_t::close).
        void end_session(const components::session::session_id_t& session) {
            auto [needs_sched, future] =
                actor_zeta::send(actor.get(), &fed::backend_actor_t::end_session, session);
            static_cast<void>(needs_sched);
            wait_ready(std::move(future));
        }
        // otterbrix's ROLLBACK of transaction `txn`.
        void abort(std::uint64_t txn) {
            components::execution_context_t ctx;
            ctx.txn = components::table::transaction_data{txn, 0};
            auto [needs_sched, future] = actor_zeta::send(actor.get(),
                                                          &fed::backend_actor_t::abort_transaction,
                                                          ctx,
                                                          std::uint64_t{0},
                                                          std::pmr::set<fed::oid_t>{&resource});
            static_cast<void>(needs_sched);
            CHECK_FALSE(wait_ready(std::move(future)).contains_error());
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

TEST_CASE("actor: a closed connection's block ends on its agent at once and its waiting statement leaves the queue") {
    actor_stack_t stack(2);
    stack.actor->set_checkout_timeout(std::chrono::seconds(30));
    using session_id = components::session::session_id_t;
    const session_id s1, s2, s3, s4;
    CHECK(answered_rows(wait_ready(stack.read(1, true, s1))));
    CHECK(answered_rows(wait_ready(stack.read(2, true, s2))));
    REQUIRE(stack.actor->pinned() == 2);
    // Two more blocks wait for an agent.
    auto third = stack.read(3, true, s3);
    auto fourth = stack.read(4, true, s4);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (stack.actor->waiting() < 2) {
        REQUIRE(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // The first block's connection closes: its ROLLBACK runs on its agent, which serves the third.
    stack.end_session(s1);
    CHECK(answered_rows(wait_ready(std::move(third))));
    // The fourth's connection closes while its first statement waits: it leaves the queue at once
    // and never took an agent.
    stack.end_session(s4);
    auto refused = wait_ready(std::move(fourth));
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::connection_closed);
    CHECK(stack.actor->waiting() == 0);
    // otterbrix's own ROLLBACK of the closed sessions comes later: nothing more reaches the backend.
    stack.abort(1);
    stack.abort(4);
    stack.end_session(s2);
    stack.end_session(s3);
    CHECK(stack.actor->pinned() == 0);
    // Every backend transaction that began ended on its own connection: no agent keeps one open.
    std::map<int, int> open;
    const auto log = stack.server.log();
    const auto on = stack.server.connections();
    int begins = 0;
    for (std::size_t i = 0; i < log.size(); ++i) {
        if (log[i] == "begin") {
            ++begins;
            ++open[on[i]];
        } else if (log[i] == "rollback" || log[i] == "commit") {
            --open[on[i]];
        }
    }
    CHECK(begins == 3);
    for (const auto& [connection, count] : open) {
        INFO("connection " << connection);
        CHECK(count == 0);
    }
    // A closed connection's transaction takes no agent any more, a new block takes one at once.
    auto late = wait_ready(stack.read(2, true, s2));
    REQUIRE(late.has_error());
    CHECK(late.error().type == core::error_code_t::connection_closed);
    CHECK(answered_rows(wait_ready(stack.read(5, true))));
    CHECK(stack.actor->pinned() == 1);
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
        return wait_ready(
            stack.send(&fed::backend_agent_t::write, std::move(sql), fed::write_kind_t::insert, false, qualified_name_t{}));
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

TEST_CASE("ch_failed_mutations: the mutations ClickHouse names in a failed synchronous mutation") {
    using ids_t = std::vector<std::string>;
    CHECK(fed::ch_failed_mutations("Code: 341. DB::Exception: Exception happened during execution of mutation "
                                   "'mutation_4.txt' with part 'all_1_1_0' reason: 'Code: 70. DB::Exception: Value in "
                                   "column Int64 cannot be safely converted into type Int32'.") == ids_t{"mutation_4.txt"});
    CHECK(fed::ch_failed_mutations("Exception happened during execution of mutations 'mutation_2.txt, mutation_3.txt' "
                                   "with part 'all_1_1_0' reason: 'Code: 153. Division by zero'") ==
          ids_t{"mutation_2.txt", "mutation_3.txt"});
    CHECK(fed::ch_failed_mutations("Exception happened during execution of mutation '0000000005' with part "
                                   "'all_0_0_0_4' reason: 'x'") == ids_t{"0000000005"});
    // A statement refused before its mutation existed, a killed one, a text of another shape.
    CHECK(fed::ch_failed_mutations("Code: 47. DB::Exception: Missing columns: 'nope'").empty());
    CHECK(fed::ch_failed_mutations("Code: 341. DB::Exception: Mutation mutation_4.txt was killed").empty());
    CHECK(fed::ch_failed_mutations("Exception happened during execution of mutation 'mutation_4.txt").empty());
    CHECK(fed::ch_failed_mutations("Exception happened during execution of mutation '' with part 'p'").empty());
}

// A ClickHouse mutation that failed on a part (ALTER TABLE ... UPDATE under mutations_sync = 2)
// stays in system.mutations and is retried forever, failing every later mutation of the table: the
// agent kills exactly the mutation ClickHouse names, then answers the statement's own error.
TEST_CASE("agent: a failed ClickHouse mutation is killed and the statement answers its own error") {
    const std::string update =
        "ALTER TABLE `chdb`.`pc` UPDATE `i` = accurateCast((`i` * 3000000000), 'Nullable(Int32)') WHERE 1 "
        "SETTINGS mutations_sync = 2;";
    const std::string reason = "Code: 341. DB::Exception: Exception happened during execution of mutation "
                               "'mutation_4.txt' with part 'all_1_1_0' reason: 'Code: 70. DB::Exception: Value in "
                               "column Int64 cannot be safely converted into type Int32'";
    const std::string kill =
        "exec KILL MUTATION WHERE database = 'chdb' AND table = 'pc' AND mutation_id = 'mutation_4.txt'";
    const qualified_name_t table{core::dbname_t{"chdb"}, core::schema_t{}, core::relname_t{"pc"}};
    auto write = [&table](agent_stack_t& stack, const std::string& sql, fed::write_kind_t kind) {
        return wait_ready(stack.send(&fed::backend_agent_t::write, std::string{sql}, kind, false, table));
    };
    SECTION("the mutation named is killed, the answer is the mutation's error") {
        agent_stack_t stack(backend_type_t::ClickHouse);
        stack.server.fail_statements("ALTER TABLE", core::error_code_t::internal_error, reason);
        auto written = write(stack, update, fed::write_kind_t::update);
        REQUIRE(written.has_error());
        CHECK(written.error().type == core::error_code_t::internal_error);
        CHECK(std::string{written.error().what} == "fake backend: refused " + update + ": " + reason);
        CHECK(stack.server.log() == std::vector<std::string>{"write " + update, kill});
    }
    SECTION("a lightweight DELETE that failed: the same") {
        agent_stack_t stack(backend_type_t::ClickHouse);
        const std::string remove = "DELETE FROM `chdb`.`pc` WHERE (intDiv(`i`, 0) = 1);";
        stack.server.fail_statements("DELETE FROM", core::error_code_t::internal_error, reason);
        CHECK(write(stack, remove, fed::write_kind_t::delete_).has_error());
        CHECK(stack.server.log() == std::vector<std::string>{"write " + remove, kill});
    }
    SECTION("a KILL that failed: both errors in the answer") {
        agent_stack_t stack(backend_type_t::ClickHouse);
        stack.server.fail_statements("ALTER TABLE", core::error_code_t::internal_error, reason);
        stack.server.fail_statements("KILL MUTATION", core::error_code_t::io_error, "connection lost");
        auto written = write(stack, update, fed::write_kind_t::update);
        REQUIRE(written.has_error());
        CHECK(written.error().type == core::error_code_t::internal_error);
        const std::string what{written.error().what};
        CHECK(what.rfind("fake backend: refused " + update + ": " + reason, 0) == 0);
        CHECK(what.find("the failed mutation mutation_4.txt was not killed") != std::string::npos);
        CHECK(what.find("connection lost") != std::string::npos);
    }
    SECTION("several mutations named together: none is killed, the answer says so") {
        agent_stack_t stack(backend_type_t::ClickHouse);
        stack.server.fail_statements("ALTER TABLE",
                                     core::error_code_t::internal_error,
                                     "Exception happened during execution of mutations 'mutation_2.txt, "
                                     "mutation_3.txt' with part 'all_1_1_0' reason: 'x'");
        auto written = write(stack, update, fed::write_kind_t::update);
        REQUIRE(written.has_error());
        CHECK(std::string{written.error().what}.find("mutation_2.txt, mutation_3.txt as failed together") !=
              std::string::npos);
        CHECK(stack.server.log() == std::vector<std::string>{"write " + update});
    }
    SECTION("a refusal naming no mutation, the statement's own literal, an INSERT: nothing killed") {
        agent_stack_t stack(backend_type_t::ClickHouse);
        stack.server.fail_statements("ALTER TABLE", core::error_code_t::undefined_column, "Missing columns: 'nope'");
        CHECK(write(stack, update, fed::write_kind_t::update).has_error());
        const std::string literal = "ALTER TABLE `chdb`.`pc` UPDATE `s` = 'Exception happened during execution of "
                                    "mutation ''m'' with part ''p''' WHERE 1 SETTINGS mutations_sync = 2;";
        CHECK(write(stack, literal, fed::write_kind_t::update).has_error());
        stack.server.fail_statements("INSERT", core::error_code_t::internal_error, reason);
        const std::string insert = "INSERT INTO `chdb`.`pc` (`i`) VALUES (1);";
        CHECK(write(stack, insert, fed::write_kind_t::insert).has_error());
        CHECK(stack.server.log() == std::vector<std::string>{"write " + update, "write " + literal, "write " + insert});
    }
}
