// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The registration of the connections' tables in otterbrix's catalog (catalog/registration.hpp):
// on the actor's registration session, without SQL to a backend; a restart brings the catalog in
// line with the discovery; a table created on the backend is registered after a statement missed it.

#include "federation_fixture.hpp"

#include "catalog/ownership.hpp"

using namespace fedtest;

namespace {

    std::vector<connection_spec_t> three_backends() {
        return {{"Pg", backend_type_t::PostgreSQL, "shop", ""},
                {"my", backend_type_t::MySQL, "Bill", ""},
                {"ch", backend_type_t::ClickHouse, "ev", ""}};
    }

    void seed(fed_stack_t& stack) {
        stack.backend("Pg").add_table({"shop",
                                       "Sales",
                                       "Orders",
                                       {column("Id", logical_type::BIGINT), column("Note", logical_type::STRING_LITERAL)},
                                       {{std::int64_t{1}, std::string{"a"}}, {std::int64_t{2}, std::string{"b"}}}});
        stack.backend("my").add_table({"Bill",
                                       "",
                                       "Orders",
                                       {column("id", logical_type::BIGINT), column("Amount", logical_type::BIGINT)},
                                       {{std::int64_t{1}, std::int64_t{10}}}});
        stack.backend("ch").add_table(
            {"ev", "", "events", {column("id", logical_type::BIGINT)}, {{std::int64_t{7}}, {std::int64_t{8}}}});
    }

    std::size_t all_changes(fed_stack_t& stack) {
        return stack.backend("Pg").changes().size() + stack.backend("my").changes().size() +
               stack.backend("ch").changes().size();
    }

} // namespace

TEST_CASE("registration: discovered tables are catalog rows and no SQL reaches a backend") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    CHECK(all_changes(stack) == 0);
    CHECK(stack.backend("Pg").log().empty());

    // Lower case in otterbrix, the backend's spelling on the wire.
    auto pg = stack.ok("SELECT id, note FROM pg.shop.sales.orders ORDER BY id;");
    CHECK(rows_of(pg) == std::vector<std::string>{"1|a", "2|b"});
    const auto reads = starting_with(stack.backend("Pg").log(), "scan ");
    REQUIRE_FALSE(reads.empty());
    CHECK(reads.front().find("\"Sales\".\"Orders\"") != std::string::npos);
    CHECK(reads.front().find("\"Id\"") != std::string::npos);

    auto my = stack.ok("SELECT amount FROM my.bill.public.orders;");
    CHECK(rows_of(my) == std::vector<std::string>{"10"});
    CHECK(starting_with(stack.backend("my").log(), "scan ").front().find("`Bill`.`Orders`") != std::string::npos);

    auto ch = stack.ok("SELECT count(*) FROM ch.ev.public.events;");
    CHECK(rows_of(ch).size() == 1);
    CHECK(all_changes(stack) == 0);
}

TEST_CASE("registration: a restart sends no DDL and keeps the backend spelling") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    stack.reopen();
    CHECK(all_changes(stack) == 0);
    auto pg = stack.ok("SELECT note FROM pg.shop.sales.orders ORDER BY id;");
    CHECK(rows_of(pg) == std::vector<std::string>{"a", "b"});
    CHECK(starting_with(stack.backend("Pg").log(), "scan ").back().find("\"Sales\".\"Orders\"") != std::string::npos);
    CHECK(all_changes(stack) == 0);
}

TEST_CASE("registration: a restart drops a table the backend lost and follows a changed column") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    stack.backend("ch").drop_table("events");
    auto* orders = stack.backend("my").find("Orders");
    orders->columns = {column("id", logical_type::BIGINT), column("Total", logical_type::STRING_LITERAL)};
    orders->rows = {{std::int64_t{1}, std::string{"x"}}};
    stack.reopen();
    CHECK(all_changes(stack) == 0);
    CHECK(stack.refused("SELECT * FROM ch.ev.public.events;").type == core::error_code_t::table_not_exists);
    static_cast<void>(stack.refused("SELECT amount FROM my.bill.public.orders;"));
    auto total = stack.ok("SELECT total FROM my.bill.public.orders;");
    CHECK(rows_of(total) == std::vector<std::string>{"x"});
    CHECK(starting_with(stack.backend("my").log(), "scan ").back().find("`Total`") != std::string::npos);
}

TEST_CASE("registration: a connection unreachable at startup keeps its tables") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    stack.backend("Pg").drop_table("Orders");
    stack.reopen(std::set<std::string>{"my", "ch"});
    // Still in the catalog: the statement reaches the backend, which answers it.
    auto refused = stack.refused("SELECT id FROM pg.shop.sales.orders;");
    CHECK(refused.type == core::error_code_t::undefined_object);
}

TEST_CASE("registration: a table created on the backend is registered after a statement missed it") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    stack.backend("my").add_table({"Bill",
                                   "",
                                   "fresh",
                                   {column("k", logical_type::BIGINT)},
                                   {{std::int64_t{5}}, {std::int64_t{6}}}});
    auto missed = stack.refused("SELECT k FROM my.bill.public.fresh ORDER BY k;");
    const qualified_name_t written{core::connection_name_t{"my"},
                                   core::dbname_t{"bill"},
                                   core::schema_t{"public"},
                                   core::relname_t{"fresh"}};
    const auto connections = stack.connections();
    auto target = cat::registration_target(missed, written, connections);
    REQUIRE(target.has_value());
    CHECK(target->second.database.t == "Bill");
    // The connection's actor discovers the table on one of its agents (the agent's MySQL discovery
    // over the fake server's result sets), answering the test thread through a future.
    auto [needs_sched, future] =
        actor_zeta::send(target->first.actor, &fed::backend_actor_t::discover, target->second);
    static_cast<void>(needs_sched);
    auto discovered = wait_ready(std::move(future));
    INFO((discovered.has_error() ? std::string{discovered.error().what} : std::string{}));
    REQUIRE_FALSE(discovered.has_error());
    REQUIRE(discovered.value().size() == 1);
    CHECK(discovered.value().front().name.collection.t == "fresh");
    for (auto& plan : cat::registration_plans(stack.resource(), target->first, discovered.value())) {
        auto cursor = stack.dispatcher()->execute_plan(stack.registration_session(), std::move(plan));
        const std::string why = cursor->is_error() ? std::string{cursor->get_error().what} : std::string{};
        INFO(why);
        REQUIRE(cursor->is_success());
    }
    CHECK(rows_of(stack.ok("SELECT k FROM my.bill.public.fresh ORDER BY k;")) == std::vector<std::string>{"5", "6"});
    CHECK(stack.backend("my").changes().empty());

    // Not the federation's: a local three-part name, another error, an unknown connection.
    CHECK_FALSE(cat::registration_target(missed,
                                         qualified_name_t{core::dbname_t{"bill"}, core::schema_t{"public"}, core::relname_t{"x"}},
                                         connections)
                    .has_value());
    CHECK_FALSE(cat::registration_target(core::error_t{core::error_code_t::datatype_mismatch, std::pmr::string{"x"}},
                                         written,
                                         connections)
                    .has_value());
    CHECK_FALSE(cat::registration_target(
        missed,
        qualified_name_t{core::connection_name_t{"nobody"}, core::dbname_t{"bill"}, core::schema_t{"public"}, core::relname_t{"x"}},
        connections)
                    .has_value());
}

TEST_CASE("registration: a configured connection's name is not a local database") {
    std::pmr::monotonic_buffer_resource resource;
    const std::vector<std::string> aliases{"Pg", "my"};
    CHECK(cat::check_database_ownership(&resource, "pg", aliases).type == core::error_code_t::invalid_parameter);
    CHECK(cat::check_database_ownership(&resource, "MY", aliases).type == core::error_code_t::invalid_parameter);
    CHECK(cat::check_database_ownership(&resource, "federation", aliases).type == core::error_code_t::invalid_parameter);
    // The Kafka objects are the schema main.kafka, so `kafka` is a database name like any other.
    CHECK_FALSE(cat::check_database_ownership(&resource, "kafka", aliases).contains_error());
    CHECK_FALSE(cat::check_database_ownership(&resource, "shop", aliases).contains_error());
}

TEST_CASE("registration: the host table keeps the backend's spelling with its container") {
    fed_stack_t stack(three_backends());
    seed(stack);
    stack.open();
    auto names = stack.ok("SELECT remote_name, remote_container FROM federation.public.remote_names WHERE attoid = 0 "
                          "ORDER BY remote_name, remote_container;");
    CHECK(rows_of(names) == std::vector<std::string>{"Orders|Bill", "Orders|Sales", "events|ev"});
    auto columns = stack.ok("SELECT remote_name FROM federation.public.remote_names WHERE attoid <> 0 AND "
                            "remote_container IS NULL ORDER BY remote_name;");
    CHECK(rows_of(columns) == std::vector<std::string>{"Amount", "Id", "Note", "id", "id"});
}

TEST_CASE("registration: PostgreSQL without a configured schema registers every user schema") {
    for (const std::string configured : {"", "b"}) {
        INFO("schema: '" << configured << "'");
        fed_stack_t stack({{"pg", backend_type_t::PostgreSQL, "shop", configured}});
        stack.backend("pg").add_table({"shop", "a", "t1", {column("x", logical_type::BIGINT)}, {{std::int64_t{1}}}});
        stack.backend("pg").add_table({"shop", "b", "t2", {column("y", logical_type::BIGINT)}, {{std::int64_t{2}}}});
        stack.open();
        auto listed = stack.ok("SELECT n.nspname, c.relname FROM pg_catalog.pg_database d JOIN pg_catalog.pg_namespace n "
                               "ON n.nspdatabase = d.oid JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid WHERE "
                               "d.datconnection = 'pg' AND c.relkind = 'r' ORDER BY c.relname;");
        if (configured.empty()) {
            CHECK(rows_of(listed) == std::vector<std::string>{"a|t1", "b|t2"});
            CHECK(rows_of(stack.ok("SELECT x FROM pg.shop.a.t1;")) == std::vector<std::string>{"1"});
        } else {
            CHECK(rows_of(listed) == std::vector<std::string>{"b|t2"});
        }
        CHECK(rows_of(stack.ok("SELECT y FROM pg.shop.b.t2;")) == std::vector<std::string>{"2"});
    }
}

TEST_CASE("registration: a restart keeps a table a local view holds and starts") {
    fed_stack_t stack({{"pg", backend_type_t::PostgreSQL, "shop", "public"}});
    stack.backend("pg").add_table({"shop",
                                   "public",
                                   "t",
                                   {column("id", logical_type::BIGINT), column("note", logical_type::STRING_LITERAL)},
                                   {{std::int64_t{1}, std::string{"a"}}}});
    stack.backend("pg").add_table({"shop", "public", "u", {column("id", logical_type::BIGINT)}, {{std::int64_t{9}}}});
    stack.backend("pg").add_table({"shop", "public", "w", {column("k", logical_type::BIGINT)}, {{std::int64_t{3}}}});
    stack.open();
    stack.ok("CREATE DATABASE lv;");
    stack.ok("CREATE VIEW lv.public.of_u AS SELECT id FROM pg.shop.public.u;");
    stack.ok("CREATE VIEW lv.public.of_note AS SELECT note FROM pg.shop.public.t;");
    // The backend lost u, and t's note changed its type; w is gone too, with no view over it.
    stack.backend("pg").drop_table("u");
    stack.backend("pg").drop_table("w");
    auto* t = stack.backend("pg").find("t");
    t->columns = {column("id", logical_type::BIGINT), column("note", logical_type::BIGINT)};
    t->rows = {{std::int64_t{1}, std::int64_t{5}}};
    // The DROP TABLE / DROP COLUMN the views refuse (RESTRICT) are warnings: the server starts.
    stack.reopen();
    CHECK(stack.backend("pg").changes().empty());
    // Kept as the catalog had them: still there, the backend answers their reads.
    static_cast<void>(stack.refused("SELECT id FROM pg.shop.public.u;"));
    CHECK(stack.refused("SELECT * FROM pg.shop.public.w;").type == core::error_code_t::table_not_exists);
    CHECK(rows_of(stack.ok("SELECT id FROM pg.shop.public.t;")) == std::vector<std::string>{"1"});
}

// G1 (a local database capturing a connection's names) is gone with the four-part model: a connection's table is a row of a database
// of the connection (pg_database.datconnection), named c.d.s.t, and a local one is d.s.t / s.t / t.
// A local database named as a connection's database — or as the connection itself — answers its own
// rows, and the connection's table keeps answering the backend's: neither captures the other.
TEST_CASE("names: a local table never answers a connection's name, nor the connection a local one") {
    fed_stack_t stack({{"sales", backend_type_t::MySQL, "bill", ""}});
    stack.backend("sales").add_table(
        {"bill", "", "orders", {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}, {std::int64_t{2}}, {std::int64_t{3}}}});
    stack.open();
    // The connection's database name, and the connection's own name, as local databases (the
    // session's ownership guard refuses the second; the engine itself takes it).
    stack.ok("CREATE DATABASE bill;");
    stack.ok("CREATE TABLE bill.public.orders (id BIGINT);");
    stack.ok("INSERT INTO bill.public.orders (id) VALUES (77);");
    stack.ok("CREATE DATABASE sales;");
    stack.ok("CREATE TABLE sales.public.orders (id BIGINT);");
    stack.ok("INSERT INTO sales.public.orders (id) VALUES (42);");
    stack.backend("sales").clear();

    CHECK(rows_of(stack.ok("SELECT id FROM sales.bill.public.orders ORDER BY id;")) ==
          std::vector<std::string>{"1", "2", "3"});
    CHECK(starting_with(stack.backend("sales").log(), "scan ").size() == 1);
    stack.backend("sales").clear();

    CHECK(rows_of(stack.ok("SELECT id FROM bill.public.orders;")) == std::vector<std::string>{"77"});
    CHECK(rows_of(stack.ok("SELECT id FROM sales.public.orders;")) == std::vector<std::string>{"42"});
    // Three parts are database.schema.table: the local database sales has no schema bill, and the
    // connection is not asked.
    CHECK(stack.refused("SELECT id FROM sales.bill.orders;").type == core::error_code_t::table_not_exists);
    // Two parts are schema.table of the session's database.
    CHECK(stack.refused("SELECT id FROM bill.orders;").type == core::error_code_t::table_not_exists);
    CHECK(stack.backend("sales").log().empty());

    // Both databases named bill are in the catalog, told apart by their connection.
    auto databases = stack.ok("SELECT datname, datconnection FROM pg_catalog.pg_database WHERE datname = 'bill' "
                              "ORDER BY datconnection;");
    CHECK(rows_of(databases) == std::vector<std::string>{"bill|", "bill|sales"});
}

// A connection whose config.yaml names one `table:` is that table only: a miss of any other name of
// it is no registration target (the statement's own error answers, no discovery is asked), the
// configured table is one — MySQL by its database, PostgreSQL by its configured schema too.
TEST_CASE("registration: a connection configured with one table is a target for that table alone") {
    fed_stack_t stack({{"my", backend_type_t::MySQL, "Bill", "", 2, "Orders"},
                       {"pg", backend_type_t::PostgreSQL, "shop", "Sales", 2, "Orders"}});
    for (const char* table : {"Orders", "other"}) {
        stack.backend("my").add_table({"Bill", "", table, {column("id", logical_type::BIGINT)}, {{std::int64_t{1}}}});
        stack.backend("pg").add_table({"shop", "Sales", table, {column("id", logical_type::BIGINT)}, {{std::int64_t{2}}}});
    }
    stack.open();
    // The startup discovery is scoped by the table: only it is in the catalog.
    CHECK(rows_of(stack.ok("SELECT id FROM my.bill.public.orders;")) == std::vector<std::string>{"1"});
    CHECK(rows_of(stack.ok("SELECT id FROM pg.shop.sales.orders;")) == std::vector<std::string>{"2"});
    const auto connections = stack.connections();

    auto missed = stack.refused("SELECT id FROM my.bill.public.other;");
    CHECK(missed.type == core::error_code_t::table_not_exists);
    const auto name = [](const char* connection, const char* database, const char* schema, const char* table) {
        return qualified_name_t{core::connection_name_t{connection},
                                core::dbname_t{database},
                                core::schema_t{schema},
                                core::relname_t{table}};
    };
    CHECK_FALSE(cat::registration_target(missed, name("my", "bill", "public", "other"), connections).has_value());
    CHECK_FALSE(cat::registration_target(missed, name("pg", "shop", "sales", "other"), connections).has_value());

    // The configured table, written in any case, is the target (a table the startup did not find).
    auto mine = cat::registration_target(missed, name("my", "bill", "public", "orders"), connections);
    REQUIRE(mine.has_value());
    CHECK(mine->second.collection.t == "Orders");
    auto pg = cat::registration_target(missed, name("pg", "shop", "sales", "orders"), connections);
    REQUIRE(pg.has_value());
    CHECK(pg->second.schema.t == "Sales");
    CHECK(pg->second.collection.t == "Orders");
}
