// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// What a statement that missed a connection's table answers, through the query
// session over fake MySQL servers (fake_servers.hpp):
//
// - a connection whose config.yaml names one `table:` is that table only: a
//   miss of another table of it is the statement's own error, and no discovery
//   reaches the backend;
// - a connection the miss cannot reach (its server down) answers the failure
//   of the connection, not that the table or the database does not exist; a
//   backend that answers without the table leaves the statement's own miss;
// - a MySQL server under sql_mode NO_BACKSLASH_ESCAPES is left out at startup
//   on the first answer (no retry): no actor, not in the engine's map, and a
//   statement naming it is refused, its earlier catalog rows included;
// - a session a MySQL agent opens later (a reconnect) is checked too: under
//   NO_BACKSLASH_ESCAPES the statement is refused before any SQL of it is sent,
//   and the next statement connects and asks again;
// - a connection configured with one `table:` refuses DDL of any other table of
//   it before the backend (invalid_parameter), the configured table's own DDL
//   and a connection without `table:` reaching the backend as before.

#include "fake_servers.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

using namespace otterstax::test;
using components::types::logical_type;
using fakebackend::column;
using code = core::error_code_t;

namespace {
    // shop.orders (id BIGINT): 1, 2.
    fakebackend::canned_table_t table_in_shop(const char* name) {
        return {"shop", "public", name, {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}, {std::int64_t{2}}}};
    }

    config::MysqlConnectionDesc mysql_entry(const char* alias, const char* table = "") {
        return config::MysqlConnectionDesc{.alias = alias, .host = "fake", .database = "shop", .table = table};
    }

    std::uint64_t rows_of(const core::result_wrapper_t<session_payload>& result) {
        INFO((result.has_error() ? result.error().what.c_str() : "ok"));
        REQUIRE_FALSE(result.has_error());
        return result.value().reported_rows();
    }

    bool mentions(const core::error_t& error, const std::string& text) {
        return std::string{error.what.c_str()}.find(text) != std::string::npos;
    }

    // A data dir two starts share, removed before and after.
    struct data_dir_guard {
        explicit data_dir_guard(std::string path)
            : path(std::move(path)) {
            std::filesystem::remove_all(this->path);
        }
        ~data_dir_guard() { std::filesystem::remove_all(path); }
        std::string path;
    };
} // namespace

TEST_CASE("registration scope: a connection configured with one table registers no other table by a miss") {
    fake_servers_t servers;
    servers.add("one", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    stack_options_t options;
    options.connectors = fake_servers_t::connectors();
    options.connections.mysql.push_back(mysql_entry("one", "orders"));
    session_stack_owner owner(unique_test_dir("otterstax_scope_one_table"), options);

    CHECK(rows_of(run_session_sql_payload(owner.stack(), 1, "SELECT id FROM one.shop.orders;")) == 2);

    // A table the backend got after startup, of a connection that is one table only.
    servers.server("one").add_table(table_in_shop("other"));
    servers.forget_sent();
    auto missed = run_session_sql_payload(owner.stack(), 2, "SELECT id FROM one.shop.other;");
    REQUIRE(missed.has_error());
    INFO(missed.error().what.c_str());
    CHECK(missed.error().type == code::table_not_exists);
    CHECK(servers.server("one").discoveries() == 0);
    CHECK(servers.sent("one").empty());

    // The configured table still answers.
    CHECK(rows_of(run_session_sql_payload(owner.stack(), 3, "SELECT id FROM one.shop.orders;")) == 2);
}

TEST_CASE("registration scope: a miss over an unreachable connection answers the connection's failure") {
    fake_servers_t servers;
    servers.add("down", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    servers.server("down").set_down(true);
    servers.add("up", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    stack_options_t options;
    options.connectors = fake_servers_t::connectors();
    options.connections.mysql.push_back(mysql_entry("down"));
    options.connections.mysql.push_back(mysql_entry("up"));
    session_stack_owner owner(unique_test_dir("otterstax_scope_unreachable"), options);
    REQUIRE(owner.remote().discovered().size() == 2);
    REQUIRE_FALSE(owner.remote().discovered().front().discovered);
    // A failed connect marks the connection for no time: the last statement below connects again.
    owner.remote().actor("down")->set_unreachable_interval(std::chrono::milliseconds{0});

    // Nothing of `down` is registered: the miss asks its backend, which cannot be reached.
    auto unreachable = run_session_sql_payload(owner.stack(), 1, "SELECT id FROM down.shop.orders;");
    REQUIRE(unreachable.has_error());
    INFO(unreachable.error().what.c_str());
    CHECK(unreachable.error().type == code::io_error);
    CHECK(mentions(unreachable.error(), "unreachable"));
    CHECK_FALSE(mentions(unreachable.error(), "does not exist"));

    // A backend that answers it has no such table: the statement's own miss.
    auto missing = run_session_sql_payload(owner.stack(), 2, "SELECT id FROM up.shop.nothing;");
    REQUIRE(missing.has_error());
    INFO(missing.error().what.c_str());
    CHECK(missing.error().type == code::table_not_exists);

    // Back up: the miss registers the table and the statement runs.
    servers.server("down").set_down(false);
    CHECK(rows_of(run_session_sql_payload(owner.stack(), 3, "SELECT id FROM down.shop.orders;")) == 2);
}

TEST_CASE("registration scope: a MySQL server under NO_BACKSLASH_ESCAPES is left out on its first answer") {
    data_dir_guard dir(unique_test_dir("otterstax_scope_nbe"));
    fake_servers_t servers;
    servers.add("nbe", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    servers.add("plain", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    stack_options_t options;
    options.connectors = fake_servers_t::connectors();
    options.connections.mysql.push_back(mysql_entry("nbe"));
    options.connections.mysql.push_back(mysql_entry("plain"));
    options.retry = config::ConnectionRetryConfig{.max_attempts = 3, .delay_ms = 0};
    options.fresh = false;

    // A first start under the default sql_mode registers the table.
    {
        session_stack_owner first(dir.path, options);
        CHECK(rows_of(run_session_sql_payload(first.stack(), 1, "SELECT id FROM nbe.shop.orders;")) == 2);
    }

    servers.server("nbe").set_sql_mode("STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES");
    servers.server("nbe").clear();
    session_stack_owner owner(dir.path, options);
    // One session checked, nothing discovered: the answer is the same on every attempt, so none is
    // retried.
    CHECK(servers.server("nbe").session_checks() == 1);
    CHECK(servers.server("nbe").discoveries() == 0);
    const auto& discovered = owner.remote().discovered();
    const auto nbe =
        std::find_if(discovered.begin(), discovered.end(), [](const auto& d) { return d.connection.name == "nbe"; });
    REQUIRE(nbe != discovered.end());
    CHECK_FALSE(nbe->discovered);
    // Left out: no actor, no address in the engine's map, not among the session's connections.
    CHECK(nbe->connection.actor == nullptr);
    CHECK(owner.remote().actor("nbe") == nullptr);
    const auto map = owner.remote().primitives().backends;
    CHECK(std::none_of(map.begin(), map.end(), [](const auto& entry) { return entry.connection == "nbe"; }));
    const auto connections = owner.remote().connections();
    CHECK(std::none_of(connections.begin(), connections.end(), [](const auto& c) { return c.name == "nbe"; }));

    // Its catalog rows of the first start answer that the connection is not there; the process serves on.
    auto refused = run_session_sql_payload(owner.stack(), 2, "SELECT id FROM nbe.shop.orders;");
    REQUIRE(refused.has_error());
    INFO(refused.error().what.c_str());
    CHECK(refused.error().type == code::undefined_object);
    CHECK(servers.server("nbe").log().empty());
    CHECK(rows_of(run_session_sql_payload(owner.stack(), 3, "SELECT id FROM plain.shop.orders;")) == 2);
}

TEST_CASE(
    "registration scope: a MySQL session opened by a reconnect under NO_BACKSLASH_ESCAPES refuses its statements") {
    fake_servers_t servers;
    servers.add("m", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    stack_options_t options;
    options.connectors = fake_servers_t::connectors();
    auto entry = mysql_entry("m");
    entry.pool_size = 1; // one agent: every statement below runs on the session it holds
    options.connections.mysql.push_back(entry);
    session_stack_owner owner(unique_test_dir("otterstax_scope_nbe_reconnect"), options);
    owner.remote().actor("m")->set_unreachable_interval(std::chrono::milliseconds{0});
    CHECK(rows_of(run_session_sql_payload(owner.stack(), 1, "SELECT id FROM m.shop.orders;")) == 2);

    // The server goes away and comes back under NO_BACKSLASH_ESCAPES (replaced, or its global
    // sql_mode changed): the connection is lost, the agent connects again.
    servers.server("m").set_sql_mode("STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES");
    servers.server("m").set_down(true);
    auto lost = run_session_sql_payload(owner.stack(), 2, "SELECT id FROM m.shop.orders;");
    REQUIRE(lost.has_error());
    CHECK(lost.error().type == code::io_error);
    servers.server("m").set_down(false);
    servers.forget_sent();

    for (int attempt = 0; attempt < 2; ++attempt) {
        auto refused = run_session_sql_payload(owner.stack(), 3 + attempt, "SELECT id FROM m.shop.orders;");
        REQUIRE(refused.has_error());
        INFO(refused.error().what.c_str());
        CHECK(refused.error().type == code::unimplemented_yet);
        CHECK(mentions(refused.error(), "NO_BACKSLASH_ESCAPES"));
        // Asked of every session it opened; the statement itself never reached the server.
        CHECK(servers.server("m").session_checks() == attempt + 1);
        CHECK(servers.sent("m").empty());
    }

    // Back to the default sql_mode: the next session passes and the statement runs.
    servers.server("m").set_sql_mode("");
    CHECK(rows_of(run_session_sql_payload(owner.stack(), 5, "SELECT id FROM m.shop.orders;")) == 2);
    CHECK(servers.server("m").session_checks() == 3);
}

TEST_CASE("registration scope: DDL of another table of a one-table connection is refused before the backend") {
    fake_servers_t servers;
    servers.add("one", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    servers.add("every", backend_type_t::MySQL).add_table(table_in_shop("orders"));
    stack_options_t options;
    options.connectors = fake_servers_t::connectors();
    options.connections.mysql.push_back(mysql_entry("one", "orders"));
    options.connections.mysql.push_back(mysql_entry("every"));
    session_stack_owner owner(unique_test_dir("otterstax_scope_one_table_ddl"), options);
    servers.forget_sent();

    auto refused_before_backend = [&](const core::result_wrapper_t<session_payload>& result) {
        REQUIRE(result.has_error());
        INFO(result.error().what.c_str());
        CHECK(result.error().type == code::invalid_parameter);
        CHECK(mentions(result.error(), "connection \"one\" is configured with table \"orders\""));
        CHECK(servers.sent("one").empty());
        CHECK(servers.server("one").discoveries() == 0);
    };
    session_hash_t id = 1;
    for (const char* sql : {"CREATE TABLE one.shop.other (id BIGINT);",
                            "CREATE TABLE ONE.Shop.Other (id BIGINT);",
                            "DROP TABLE one.shop.other;",
                            "DROP TABLE IF EXISTS one.shop.other;",
                            "ALTER TABLE one.shop.other ADD COLUMN v BIGINT;",
                            "CREATE INDEX other_id ON one.shop.other (id);",
                            // The file is never read: the target is refused first.
                            "CREATE EXTERNAL TABLE one.shop.other WITH (location = '/nonexistent/other.csv', "
                            "format = 'csv');"}) {
        INFO(sql);
        refused_before_backend(run_session_sql_payload(owner.stack(), id++, sql));
    }
    // A prepare is refused the same way.
    refused_before_backend(prepare_session_sql(owner.stack(), id++, "CREATE TABLE one.shop.other (id BIGINT);"));

    // The configured table's own DDL reaches the backend as before.
    auto altered = run_session_sql_payload(owner.stack(), id++, "ALTER TABLE one.shop.orders ADD COLUMN v BIGINT;");
    INFO((altered.has_error() ? altered.error().what.c_str() : "ok"));
    CHECK_FALSE(altered.has_error());
    auto dropped = run_session_sql_payload(owner.stack(), id++, "DROP TABLE one.shop.orders;");
    INFO((dropped.has_error() ? dropped.error().what.c_str() : "ok"));
    CHECK_FALSE(dropped.has_error());
    const auto sent_one = servers.sent("one");
    CHECK(std::any_of(sent_one.begin(), sent_one.end(), [](const auto& s) {
        return s.find("ALTER TABLE") != std::string::npos;
    }));
    CHECK(std::any_of(sent_one.begin(), sent_one.end(), [](const auto& s) {
        return s.find("DROP TABLE") != std::string::npos;
    }));

    // A connection without `table:` creates any table on the backend, as before.
    auto created = run_session_sql_payload(owner.stack(), id++, "CREATE TABLE every.shop.other (id BIGINT);");
    INFO((created.has_error() ? created.error().what.c_str() : "ok"));
    CHECK_FALSE(created.has_error());
    CHECK(servers.sent("every") == std::vector<std::string>{"CREATE TABLE `shop`.`other` (`id` bigint);"});
}
