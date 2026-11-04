// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The backend transaction an otterbrix transaction runs on: BEGIN before the first write, COMMIT at
// prepare, ROLLBACK / ROLLBACK TO s<k> at abort, the pinned connection returned by a release without
// a prepare; ClickHouse writes only outside a transaction block.

#include "federation_fixture.hpp"

using namespace fedtest;

namespace {

    std::unique_ptr<fed_stack_t> make_stack() {
        auto owned = std::make_unique<fed_stack_t>(std::vector<connection_spec_t>{
            {"pg", backend_type_t::PostgreSQL, "shop", "public"}, {"ch", backend_type_t::ClickHouse, "ev", ""}});
        auto& stack = *owned;
        stack.backend("pg").add_table({"shop",
                                       "public",
                                       "t",
                                       {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
                                       {{std::int64_t{1}, std::string{"a"}}}});
        stack.backend("ch").add_table({"ev", "", "e", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
        return owned;
    }

    // The backend's steps and statements, a statement shortened to its verb.
    std::vector<std::string> steps(fake_backend_t& backend) {
        std::vector<std::string> out;
        for (const auto& line : backend.changes()) {
            if (line.rfind("write ", 0) == 0) {
                out.push_back(line.substr(6, line.find(' ', 6) - 6) + (line.ends_with("@txn") ? " @txn" : ""));
            } else {
                out.push_back(line);
            }
        }
        return out;
    }

} // namespace

TEST_CASE("transactions: an autocommit write is BEGIN, the write and COMMIT") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "commit"});
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: a block's writes share one BEGIN and one COMMIT") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (3, 'c');");
    stack.ok("COMMIT;");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "INSERT @txn", "commit"});
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: ROLLBACK reaches the backend") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    stack.ok("ROLLBACK;");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "rollback"});
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: ROLLBACK TO a savepoint is the backend's ROLLBACK TO") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    stack.ok("SAVEPOINT a;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (3, 'c');");
    stack.ok("ROLLBACK TO a;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (4, 'd');");
    stack.ok("ROLLBACK TO a;");
    stack.ok("COMMIT;");
    const auto seen = steps(stack.backend("pg"));
    INFO(seen.size());
    REQUIRE(seen.size() == 9);
    CHECK(seen[0] == "begin");
    CHECK(seen[1] == "INSERT @txn");
    CHECK(seen[2].rfind("savepoint s", 0) == 0);
    CHECK(seen[3] == "INSERT @txn");
    CHECK(seen[4] == "rollback_to_savepoint " + seen[2].substr(10));
    // The level after ROLLBACK TO is a new one: its savepoint is set before its write.
    CHECK(seen[5].rfind("savepoint s", 0) == 0);
    CHECK(seen[6] == "INSERT @txn");
    CHECK(seen[7] == "rollback_to_savepoint " + seen[2].substr(10));
    CHECK(seen[8] == "commit");
}

TEST_CASE("transactions: a block that only read is one backend transaction and returns its connection") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("SELECT id FROM pg.shop.public.t;");
    CHECK(stack.actor("pg").pinned() == 1);
    stack.ok("SELECT note FROM pg.shop.public.t;");
    stack.ok("COMMIT;");
    // BEGIN before the block's first read, so both reads see one snapshot; nothing was written, so
    // the release rolls it back, and the agent is free again.
    CHECK(stack.backend("pg").changes() == std::vector<std::string>{"begin", "rollback"});
    const auto reads = starting_with(stack.backend("pg").log(), "scan ");
    REQUIRE(reads.size() == 2);
    CHECK(reads[0].ends_with("@txn"));
    CHECK(reads[1].ends_with("@txn"));
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: an autocommit read pins no connection") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("SELECT id FROM pg.shop.public.t;");
    CHECK(stack.backend("pg").changes().empty());
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: a refused write fails the statement and rolls the backend back") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").fail_writes(core::error_code_t::unique_violation);
    CHECK(stack.refused("INSERT INTO pg.shop.public.t VALUES (2, 'b');").type == core::error_code_t::unique_violation);
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "rollback"});
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: ClickHouse writes outside a block and refuses one inside") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("INSERT INTO ch.ev.public.e VALUES (2);");
    CHECK(steps(stack.backend("ch")) == std::vector<std::string>{"INSERT"});
    stack.backend("ch").clear();
    stack.ok("BEGIN;");
    CHECK(stack.refused("INSERT INTO ch.ev.public.e VALUES (3);").type == core::error_code_t::unimplemented_yet);
    stack.ok("ROLLBACK;");
    CHECK(stack.backend("ch").changes().empty());
}

TEST_CASE("transactions: a client's DDL on PostgreSQL runs in the backend transaction, a DROP at COMMIT") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("CREATE TABLE pg.shop.public.fresh (a BIGINT, b TEXT);");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "CREATE @txn", "commit"});
    stack.backend("pg").clear();
    stack.ok("ALTER TABLE pg.shop.public.fresh ADD COLUMN c BIGINT;");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "ALTER @txn", "commit"});
    stack.backend("pg").clear();
    stack.ok("DROP TABLE pg.shop.public.fresh;");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "DROP @txn", "commit"});
    CHECK(stack.actor("pg").pinned() == 0);
    static_cast<void>(stack.refused("SELECT a FROM pg.shop.public.fresh;"));
}

TEST_CASE("transactions: a client's DDL on ClickHouse runs at once and survives a restart") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("CREATE TABLE ch.ev.public.fresh (a BIGINT);");
    const auto created = steps(stack.backend("ch"));
    REQUIRE(created.size() == 1);
    CHECK(created[0] == "CREATE");
    stack.backend("ch").add_table({"ev", "", "fresh", {column("a", logical_type::BIGINT)}, {{std::int64_t{4}}}});
    stack.backend("ch").clear();
    stack.reopen();
    CHECK(stack.backend("ch").changes().empty());
    CHECK(rows_of(stack.ok("SELECT a FROM ch.ev.public.fresh;")) == std::vector<std::string>{"4"});
}

TEST_CASE("transactions: every statement of a block runs on the one agent it holds") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("SELECT id FROM pg.shop.public.t;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    stack.ok("SELECT id FROM pg.shop.public.t;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (3, 'c');");
    stack.ok("COMMIT;");
    const auto log = stack.backend("pg").log();
    const auto on = stack.backend("pg").connections();
    REQUIRE(log.size() == on.size());
    REQUIRE_FALSE(on.empty());
    for (std::size_t i = 0; i < on.size(); ++i) {
        INFO(log[i]);
        CHECK(on[i] == on.front());
    }
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: a failed COMMIT fails the statement and frees the agent") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").fail_commits(true);
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    CHECK(stack.refused("COMMIT;").type == core::error_code_t::io_error);
    CHECK(stack.actor("pg").pinned() == 0);
    stack.backend("pg").fail_commits(false);
    stack.backend("pg").clear();
    // The agent serves the next statement (its session was replaced).
    stack.ok("INSERT INTO pg.shop.public.t VALUES (3, 'c');");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "commit"});
}

// ---- refusals inside the backend transaction ----

namespace {

    // A PostgreSQL connection of one agent, a MySQL and a ClickHouse one, each with a table t(id, note)
    // holding (1, 'a') and PostgreSQL a table u.
    std::unique_ptr<fed_stack_t> make_ddl_stack() {
        auto owned = std::make_unique<fed_stack_t>(std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public", 1},
                                                                                  {"my", backend_type_t::MySQL, "bill", ""},
                                                                                  {"ch", backend_type_t::ClickHouse, "ev", ""}});
        auto& stack = *owned;
        const std::vector<complex_logical_type> columns{column("id", logical_type::BIGINT),
                                                        column("note", logical_type::STRING_LITERAL)};
        const std::vector<row_t> rows{{std::int64_t{1}, std::string{"a"}}};
        stack.backend("pg").add_table({"shop", "public", "t", columns, rows});
        stack.backend("pg").add_table({"shop", "public", "u", {column("id", logical_type::BIGINT)}, {{std::int64_t{9}}}});
        stack.backend("my").add_table({"bill", "", "t", columns, rows});
        stack.backend("ch").add_table({"ev", "", "t", columns, rows});
        return owned;
    }

    bool mentions(const std::vector<std::string>& log, const std::string& text) {
        return std::any_of(log.begin(), log.end(), [&](const auto& line) { return line.find(text) != std::string::npos; });
    }

} // namespace

TEST_CASE("transactions: a CREATE TABLE the backend refuses drops nothing") {
    for (const std::string connection : {"my", "ch"}) {
        INFO(connection);
        auto owned = make_ddl_stack();
        auto& stack = *owned;
        stack.open();
        // The table is on the backend already, not in the catalog.
        stack.backend(connection).fail_statements("CREATE TABLE", core::error_code_t::table_already_exists);
        const auto database = connection == "my" ? "bill" : "ev";
        CHECK(stack.refused("CREATE TABLE " + connection + "." + database + ".public.kept (a BIGINT);").type ==
              core::error_code_t::table_already_exists);
        CHECK_FALSE(mentions(stack.backend(connection).changes(), "DROP"));
    }
}

TEST_CASE("transactions: an ADD COLUMN the backend refuses drops nothing and reads keep their columns") {
    for (const std::string connection : {"my", "ch"}) {
        INFO(connection);
        auto owned = make_ddl_stack();
        auto& stack = *owned;
        stack.open();
        stack.backend(connection).fail_statements("ADD COLUMN", core::error_code_t::duplicate_column);
        const std::string table = connection + (connection == "my" ? ".bill" : ".ev") + ".public.t";
        static_cast<void>(stack.refused("ALTER TABLE " + table + " ADD COLUMN extra BIGINT;"));
        CHECK_FALSE(mentions(stack.backend(connection).changes(), "DROP"));
        CHECK(rows_of(stack.ok("SELECT * FROM " + table + ";")) == std::vector<std::string>{"1|a"});
        CHECK(rows_of(stack.ok("SELECT note FROM " + table + ";")) == std::vector<std::string>{"a"});
    }
}

TEST_CASE("transactions: a client's ADD COLUMN and DROP COLUMN reach the reads once committed") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    for (const std::string table : {"my.bill.public.t", "pg.shop.public.t"}) {
        INFO(table);
        stack.ok("ALTER TABLE " + table + " ADD COLUMN extra BIGINT;");
        CHECK(rows_of(stack.ok("SELECT id, note FROM " + table + ";")) == std::vector<std::string>{"1|a"});
        stack.ok("ALTER TABLE " + table + " DROP COLUMN note;");
        CHECK(rows_of(stack.ok("SELECT id FROM " + table + ";")) == std::vector<std::string>{"1"});
    }
    CHECK(stack.actor("pg").pinned() == 0);
    CHECK(stack.actor("my").pinned() == 0);
}

TEST_CASE("transactions: a DROP COLUMN the COMMIT refuses keeps the column") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    stack.actor("pg").set_checkout_timeout(std::chrono::milliseconds(2000));
    stack.backend("pg").fail_statements("DROP COLUMN", core::error_code_t::dependent_objects_still_exist);
    static_cast<void>(stack.refused("ALTER TABLE pg.shop.public.t DROP COLUMN id;"));
    CHECK(stack.actor("pg").pinned() == 0);
    CHECK(rows_of(stack.ok("SELECT note FROM pg.shop.public.t;")) == std::vector<std::string>{"a"});
    CHECK(rows_of(stack.ok("SELECT id, note FROM pg.shop.public.t;")) == std::vector<std::string>{"1|a"});
}

TEST_CASE("transactions: a deferred DROP TABLE the backend refuses frees the agent") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    // pool_size 1: an agent left pinned would hold every later statement until the checkout timeout.
    stack.actor("pg").set_checkout_timeout(std::chrono::milliseconds(2000));
    stack.backend("pg").fail_statements("DROP TABLE", core::error_code_t::dependent_objects_still_exist);
    static_cast<void>(stack.refused("DROP TABLE pg.shop.public.t;"));
    CHECK(stack.actor("pg").pinned() == 0);
    CHECK(stack.backend("pg").changes().back() == "rollback");
    stack.backend("pg").clear();
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    CHECK(steps(stack.backend("pg")) == std::vector<std::string>{"begin", "INSERT @txn", "commit"});
    CHECK(rows_of(stack.ok("SELECT note FROM pg.shop.public.t;")) == std::vector<std::string>{"a"});
}

TEST_CASE("transactions: a read refused under a savepoint is rolled back to it and the COMMIT keeps the write") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    stack.backend("pg").fail_statements("\"u\"", core::error_code_t::internal_error);
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO pg.shop.public.t VALUES (2, 'b');");
    stack.ok("SAVEPOINT a;");
    static_cast<void>(stack.refused("SELECT id FROM pg.shop.public.u;"));
    stack.ok("ROLLBACK TO a;");
    stack.ok("COMMIT;");
    const auto seen = stack.backend("pg").changes();
    INFO(seen.size());
    CHECK_FALSE(mentions(seen, "commit-rolled-back"));
    CHECK(seen.back() == "commit");
    CHECK(stack.actor("pg").pinned() == 0);
}

TEST_CASE("transactions: a MySQL deadlock inside a block fails its COMMIT") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    stack.backend("my").fail_statements("'dead'", core::error_code_t::write_conflict);
    stack.ok("BEGIN;");
    stack.ok("INSERT INTO my.bill.public.t VALUES (2, 'b');");
    stack.ok("SAVEPOINT a;");
    CHECK(stack.refused("INSERT INTO my.bill.public.t VALUES (3, 'dead');").type == core::error_code_t::write_conflict);
    // MySQL rolled the whole transaction back with the deadlock: the savepoint is gone with it.
    static_cast<void>(stack.sql("ROLLBACK TO a;"));
    static_cast<void>(stack.refused("COMMIT;"));
    CHECK_FALSE(mentions(stack.backend("my").changes(), "commit"));
    CHECK(stack.actor("my").pinned() == 0);
    stack.backend("my").clear();
    stack.ok("INSERT INTO my.bill.public.t VALUES (4, 'd');");
    CHECK(steps(stack.backend("my")) == std::vector<std::string>{"begin", "INSERT @txn", "commit"});
}

TEST_CASE("transactions: a MySQL block's reads share one backend transaction") {
    auto owned = make_ddl_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("BEGIN;");
    stack.ok("SELECT id FROM my.bill.public.t;");
    stack.ok("SELECT note FROM my.bill.public.t;");
    stack.ok("COMMIT;");
    // START TRANSACTION before the first read: one REPEATABLE READ snapshot for both.
    const auto log = stack.backend("my").log();
    const auto on = stack.backend("my").connections();
    REQUIRE(log.size() == 4);
    CHECK(log[0] == "begin");
    CHECK(log[1].ends_with("@txn"));
    CHECK(log[2].ends_with("@txn"));
    CHECK(log[3] == "rollback");
    CHECK(std::set<int>{on.begin(), on.end()}.size() == 1);
    CHECK(stack.actor("my").pinned() == 0);
}
