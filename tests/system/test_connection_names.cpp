// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "fake_servers.hpp"
#include "session_stack.hpp"

#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"
#include "raw_wire_client.hpp"

#include <spark/connect/relations.pb.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <string>
#include <vector>

using namespace otterstax::test;
using components::types::logical_type;
using fakebackend::column;
using code = core::error_code_t;

namespace {
    using rows_t = std::vector<std::string>;

    rows_t rows_of(const session_payload& payload) {
        rows_t out;
        for (const auto& chunk : payload.chunks) {
            for (std::uint64_t r = 0; r < chunk.size(); ++r) {
                std::string line;
                for (std::uint64_t c = 0; c < chunk.column_count(); ++c) {
                    if (c) {
                        line += "|";
                    }
                    const auto& v = chunk.data[c];
                    const auto type = v.type().type();
                    if (v.is_null(r)) {
                        line += "null";
                    } else if (type == logical_type::BIGINT || type == logical_type::INTEGER) {
                        line += std::to_string(v.get_value<std::int64_t>(r));
                    } else if (type == logical_type::UBIGINT) {
                        line += std::to_string(v.get_value<std::uint64_t>(r));
                    } else if (type == logical_type::STRING_LITERAL) {
                        line += std::string{v.get_value<std::string_view>(r)};
                    } else {
                        line += "<type " + std::to_string(static_cast<int>(type)) + ">";
                    }
                }
                out.push_back(std::move(line));
            }
        }
        return out;
    }

    rows_t sorted(rows_t rows) {
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    // my: bill.orders (id, customer, amount) and ops.t (id).
    fakebackend::canned_table_t bill_orders() {
        return {"bill",
                "",
                "orders",
                {column("id", logical_type::BIGINT),
                 column("customer", logical_type::STRING_LITERAL),
                 column("amount", logical_type::BIGINT)},
                {{std::int64_t{1}, std::string{"ann"}, std::int64_t{10}},
                 {std::int64_t{2}, std::string{"bob"}, std::int64_t{20}},
                 {std::int64_t{3}, std::string{"ann"}, std::int64_t{30}}}};
    }
    fakebackend::canned_table_t ops_t() {
        return {"ops", "", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{7}}, {std::int64_t{8}}}};
    }
    // pg: shop.customers (id, name) and shop.t; pg_sales: sales.t (the same base, the other schema).
    fakebackend::canned_table_t shop_customers() {
        return {"shop_db",
                "shop",
                "customers",
                {column("id", logical_type::BIGINT), column("name", logical_type::STRING_LITERAL)},
                {{std::int64_t{1}, std::string{"ann"}},
                 {std::int64_t{2}, std::string{"bob"}},
                 {std::int64_t{3}, std::string{"cid"}}}};
    }
    fakebackend::canned_table_t shop_t() {
        return {"shop_db", "shop", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{5}}}};
    }
    fakebackend::canned_table_t sales_t() {
        return {"shop_db", "sales", "t", {column("id", logical_type::BIGINT)}, {{std::int64_t{5}}}};
    }

    bool seed(fake_servers_t& servers) {
        servers.add("my", backend_type_t::MySQL).add_table(bill_orders());
        servers.server("my").add_table(ops_t());
        servers.add("pg", backend_type_t::PostgreSQL).add_table(shop_customers());
        servers.server("pg").add_table(shop_t());
        servers.add("pg_sales", backend_type_t::PostgreSQL).add_table(sales_t());
        return true;
    }

    stack_options_t fake_options() {
        stack_options_t options;
        options.connectors = fake_servers_t::connectors();
        options.connections.mysql.push_back(
            config::MysqlConnectionDesc{.alias = "my", .host = "fake", .database = "bill"});
        options.connections.postgresql.push_back(
            config::PgConnectionDesc{.alias = "pg", .host = "fake", .database = "shop_db", .schema = "shop"});
        options.connections.postgresql.push_back(
            config::PgConnectionDesc{.alias = "pg_sales", .host = "fake", .database = "shop_db", .schema = "sales"});
        return options;
    }

    // How a backend's SQL names a table.
    std::string pg_table(const char* schema, const char* name) {
        return std::string{"\""} + schema + "\".\"" + name + "\"";
    }
    std::string my_table(const char* database, const char* name) {
        return std::string{"`"} + database + "`.`" + name + "`";
    }

    bool mentions(const std::vector<std::string>& sent, const std::string& fragment) {
        return sent.size() == 1 && sent.front().find(fragment) != std::string::npos;
    }

    // The servers, the stack over them, and the local base: schema eg of local
    // for the cases' own tables.
    class names_t {
    public:
        explicit names_t(const char* name)
            : seeded_(seed(servers_))
            , owner_(unique_test_dir(name), fake_options()) {
            ok("SET search_path = 'local.public';");
            ok("CREATE SCHEMA eg;");
            ok("CREATE TABLE local.eg.regions (id bigint, label string);");
            ok("INSERT INTO local.eg.regions (id, label) VALUES (1, 'ann'), (2, 'bob');");
            ok("SET search_path = '';");
        }

        core::result_wrapper_t<session_payload> run(const std::string& sql) {
            servers_.forget_sent();
            return run_session_sql_payload(owner_.stack(), id_++, sql);
        }
        session_payload ok(const std::string& sql) {
            auto r = run(sql);
            INFO(sql << " -> " << (r.has_error() ? r.error().what.c_str() : "ok"));
            REQUIRE_FALSE(r.has_error());
            return std::move(r.value());
        }
        rows_t rows(const std::string& sql) { return rows_of(ok(sql)); }
        core::error_t refused(const std::string& sql) {
            auto r = run(sql);
            INFO(sql);
            REQUIRE(r.has_error());
            return r.error();
        }
        // reaches: the statement answers rows and exactly one statement mentioning `fragment`
        // reached `alias`; no other backend was asked.
        void reaches(const std::string& sql, const char* alias, const std::string& fragment) {
            INFO(sql);
            auto r = run(sql);
            INFO((r.has_error() ? r.error().what.c_str() : "ok"));
            REQUIRE_FALSE(r.has_error());
            for (const char* a : aliases()) {
                INFO("sent to " << a);
                if (std::string_view{a} == alias) {
                    CHECK(mentions(servers_.sent(a), fragment));
                } else {
                    CHECK(servers_.sent(a).empty());
                }
            }
        }
        session_stack stack() const { return owner_.stack(); }
        session_hash_t next_id() { return id_++; }
        std::vector<std::string> sent(const char* alias) { return servers_.sent(alias); }

        static constexpr std::array<const char*, 3> aliases() { return {"my", "pg", "pg_sales"}; }

    private:
        fake_servers_t servers_;
        bool seeded_;
        session_stack_owner owner_;
        session_hash_t id_{1};
    };

    // One refusal: a statement refused before any backend was asked, naming what it names.
    struct refusal_t {
        const char* sql;
        const char* text;    // nullptr: the text is not pinned
        bool probes = false; // a missed table of a connection is looked for on its backend first
    };
} // namespace

TEST_CASE("connection names: a uid.ns.table read reaches its backend") {
    names_t f("otterstax_cn_reaches");

    // MySQL gets bill.orders.
    f.reaches("SELECT id FROM my.bill.orders;", "my", my_table("bill", "orders"));
    CHECK(sorted(rows_of(f.ok("SELECT id FROM my.bill.orders;"))) == rows_t{"1", "2", "3"});
    // The PostgreSQL connection on shop_db gets shop.customers.
    f.reaches("SELECT id FROM pg.shop.customers;", "pg", pg_table("shop", "customers"));
    // Two aliases of one PostgreSQL base, one schema each — both are asked.
    {
        INFO("pg.shop.t x pg_sales.sales.t");
        auto r = f.run("SELECT a.id FROM pg.shop.t a JOIN pg_sales.sales.t b ON a.id = b.id;");
        REQUIRE_FALSE(r.has_error());
        CHECK(sorted(rows_of(r.value())) == rows_t{"5"});
        CHECK(mentions(f.sent("pg"), pg_table("shop", "t")));
        CHECK(mentions(f.sent("pg_sales"), pg_table("sales", "t")));
    }
    // The session's place pg.shop completes the bare name.
    f.ok("SET search_path = 'pg.shop';");
    f.reaches("SELECT id FROM customers;", "pg", pg_table("shop", "customers"));
    CHECK(f.rows("SELECT id FROM customers;") == rows_t{"1", "2", "3"});
    // The place also completes schema.table: sales of pg_sales's base is not pg's.
    CHECK(f.refused("SET search_path = 'pg.sales';").type == code::schema_not_exists);
    // A column by its table, its database, its connection or its alias.
    f.reaches("SELECT orders.id, bill.orders.customer, my.bill.orders.amount FROM my.bill.orders;",
              "my",
              my_table("bill", "orders"));
    f.reaches("SELECT o.id FROM my.bill.orders o;", "my", my_table("bill", "orders"));
    // A table under an alias is named by its alias only, as in PostgreSQL.
    const auto hidden = f.refused("SELECT orders.id, o.id FROM my.bill.orders o;");
    CHECK(std::string{hidden.what.c_str()}.find("under the alias \"o\"") != std::string::npos);
}

TEST_CASE("connection names: a JOIN of two connections and a local table answers the rows") {
    names_t f("otterstax_cn_join3");
    CHECK(sorted(rows_of(f.ok("SELECT o.id, c.name, r.label FROM my.bill.orders o JOIN pg.shop.customers c "
                              "ON o.customer = c.name JOIN local.eg.regions r ON r.label = c.name;"))) ==
          rows_t{"1|ann|ann", "2|bob|bob", "3|ann|ann"});
    CHECK(mentions(f.sent("my"), my_table("bill", "orders")));
    CHECK(mentions(f.sent("pg"), pg_table("shop", "customers")));
}

namespace {
    // One position: the statement, the fragment each involved backend must see
    // in its SQL, the rows.
    struct position_t {
        const char* name;
        const char* sql;
        std::vector<std::pair<const char*, const char*>> fragments; // alias -> table fragment in its SQL
        std::optional<rows_t> rows;
    };

    const std::vector<position_t>& positions() {
        static const std::vector<position_t> out{
            {"FROM",
             "SELECT name FROM pg.shop.customers;",
             {{"pg", R"("shop"."customers")"}},
             rows_t{"ann", "bob", "cid"}},
            {"JOIN",
             "SELECT o.id FROM my.bill.orders o JOIN pg.shop.customers c ON o.customer = c.name;",
             {{"my", "`bill`.`orders`"}, {"pg", R"("shop"."customers")"}},
             rows_t{"1", "2", "3"}},
            {"derived table",
             "SELECT d.id FROM (SELECT id FROM my.bill.orders) d;",
             {{"my", "`bill`.`orders`"}},
             rows_t{"1", "2", "3"}},
            {"CTE body",
             "WITH x AS (SELECT id FROM pg.shop.customers) SELECT id FROM x;",
             {{"pg", R"("shop"."customers")"}},
             rows_t{"1", "2", "3"}},
            {"IN (SELECT)",
             "SELECT id FROM local.eg.regions WHERE label IN (SELECT name FROM pg.shop.customers);",
             {{"pg", R"("shop"."customers")"}},
             rows_t{"1", "2"}},
            {"scalar sub-query in the select list",
             "SELECT (SELECT count(*) FROM my.bill.orders) AS n FROM local.eg.regions;",
             {{"my", "`bill`.`orders`"}},
             rows_t{"3", "3"}},
            {"INSERT ... SELECT",
             "INSERT INTO local.eg.regions (id, label) SELECT id, name FROM pg.shop.customers;",
             {{"pg", R"("shop"."customers")"}},
             std::nullopt},
            {"INSERT target", "INSERT INTO my.bill.orders (id) VALUES (9);", {{"my", "`bill`.`orders`"}}, std::nullopt},
            {"UPDATE target",
             "UPDATE pg.shop.customers SET name = 'x' WHERE id = 3;",
             {{"pg", R"("shop"."customers")"}},
             std::nullopt},
            {"DELETE target",
             "DELETE FROM pg.shop.customers WHERE id = 3;",
             {{"pg", R"("shop"."customers")"}},
             std::nullopt},
        };
        return out;
    }
} // namespace

TEST_CASE("connection names: every position of a statement reaches the same table") {
    names_t f("otterstax_cn_positions");
    for (const auto& position : positions()) {
        INFO("position " << position.name << ": " << position.sql);
        auto r = f.run(position.sql);
        INFO((r.has_error() ? r.error().what.c_str() : "ok"));
        REQUIRE_FALSE(r.has_error());
        for (const char* alias : names_t::aliases()) {
            INFO("sent to " << alias);
            const auto expected = std::find_if(position.fragments.begin(),
                                               position.fragments.end(),
                                               [alias](const auto& p) { return std::string_view{p.first} == alias; });
            if (expected == position.fragments.end()) {
                CHECK(f.sent(alias).empty());
            } else {
                CHECK(mentions(f.sent(alias), expected->second));
            }
        }
        if (position.rows) {
            CHECK(sorted(rows_of(r.value())) == sorted(*position.rows));
        }
    }
    // A fake backend does not retain its writes: the reads still answer the canned rows.
    CHECK(sorted(rows_of(f.ok("SELECT id FROM my.bill.orders;"))) == rows_t{"1", "2", "3"});
}

TEST_CASE("connection names: a Spark named-table read reaches the backend") {
    names_t f("otterstax_cn_spark");
    auto& resource = *f.stack().resource;
    spark::connect::Plan plan;
    auto* read = plan.mutable_root()->mutable_read();
    read->mutable_named_table()->set_unparsed_identifier("pg.shop.customers");
    auto translated = frontend::spark::relation_to_plan(plan, &resource);
    REQUIRE_FALSE(translated.has_error());
    auto r = execute_session_plan(f.stack(), f.next_id(), std::move(translated.value().parsed_data));
    INFO("execute_plan -> " << (r.has_error() ? r.error().what.c_str() : "ok"));
    REQUIRE_FALSE(r.has_error());
    CHECK(sorted(rows_of(r.value())) == rows_t{"1|ann", "2|bob", "3|cid"});
    CHECK(mentions(f.sent("pg"), pg_table("shop", "customers")));
    CHECK(f.sent("my").empty());
}

TEST_CASE("connection names: local tables and kafka objects keep their short names") {
    names_t f("otterstax_cn_local_kafka");
    // A local table through the connection-less spelling; no backend asked.
    f.ok("CREATE TABLE local.eg.t (id bigint);");
    f.ok("INSERT INTO local.eg.t (id) VALUES (4);");
    CHECK(f.rows("SELECT id FROM local.eg.t;") == rows_t{"4"});
    for (const char* alias : names_t::aliases()) {
        CHECK(f.sent(alias).empty());
    }
    // Kafka objects by the short name from a place in local (no broker needed to
    // create a SOURCE and read its table).
    f.ok("SET search_path = 'local.public';");
    f.ok("CREATE SOURCE s (id INT, name VARCHAR) "
         "WITH (KAFKA_TOPIC='cn', BOOTSTRAP_SERVERS='localhost:9092')");
    f.ok("SELECT * FROM kafka.s;");
    f.ok("SELECT * FROM local.kafka.s;");
    for (const char* alias : names_t::aliases()) {
        CHECK(f.sent(alias).empty());
    }
}

TEST_CASE("connection names: the wrong forms are refused before any backend") {
    names_t f("otterstax_cn_refusals");
    const std::vector<refusal_t> refusals{
        // Four segments are the engine's own second full name of one table.
        {"SELECT id FROM pg.shop_db.shop.customers;", nullptr},
        // MySQL has no schema level.
        {"SELECT id FROM my.bill.public.orders;", nullptr},
        // The local database is local, not eg.
        {"SELECT id FROM eg.public.t;", nullptr},
        // A column is at most four segments.
        {"SELECT my.def.bill.orders.id FROM my.bill.orders;", nullptr},
        // No connection, no local base; the text names the name as written.
        {"SELECT id FROM nope.bill.orders;", "nope.bill.orders"},
        // A table the connection's namespace has not; the text names it as written.
        {"SELECT id FROM pg.shop.nope;", "pg.shop.nope", true},
        // Two parts in a session whose place names no connection.
        {"SELECT id FROM bill.orders;", nullptr},
        // The namespaces of a connection are discovery's.
        {"CREATE SCHEMA pg.x;", nullptr},
    };
    for (const auto& refusal : refusals) {
        INFO(refusal.sql);
        auto r = f.run(refusal.sql);
        REQUIRE(r.has_error());
        if (refusal.text) {
            CHECK(std::string{r.error().what.c_str()}.find(refusal.text) != std::string::npos);
        }
        for (const char* alias : names_t::aliases()) {
            INFO("sent to " << alias);
            CHECK((f.sent(alias).empty() || (refusal.probes && alias == std::string_view{"pg"})));
        }
    }
}

TEST_CASE("connection names: CREATE DATABASE is refused to a client whose database is a catalog") {
    fake_servers_t servers;
    seed(servers);
    auto options = fake_options();
    options.database_term = otterstax::session::database_term_t::catalog;
    session_stack_owner owner(unique_test_dir("otterstax_cn_catalog_term"), std::move(options));
    const auto& s = owner.stack();
    session_hash_t id = 700;
    for (const char* sql : {"CREATE DATABASE pg;",
                            "CREATE DATABASE local;",
                            "CREATE DATABASE federation;",
                            "CREATE DATABASE mine;",
                            "DROP DATABASE mine;"}) {
        INFO(sql);
        auto r = run_session_sql_payload(s, id++, sql);
        REQUIRE(r.has_error());
        CHECK(r.error().type == code::invalid_parameter);
    }
}

TEST_CASE("connection names: CREATE DATABASE named like a connection is a schema of local") {
    names_t f("otterstax_cn_schema_term");
    f.ok("SET search_path = 'local.public';");
    f.ok("CREATE DATABASE pg;");
    f.ok("CREATE TABLE pg.t (id bigint);");
    CHECK(f.rows("SELECT id FROM local.pg.t;").empty());
    f.reaches("SELECT id FROM pg.shop.customers;", "pg", pg_table("shop", "customers"));
}

TEST_CASE("connection names: client DDL maps by frontend") {
    fake_servers_t servers;
    seed(servers);
    session_stack_owner owner(unique_test_dir("otterstax_cn_client_ddl"), fake_options());
    const auto stack = owner.stack();

    frontend::frontend_server_config config{.resource = stack.resource,
                                            .port = 0,
                                            .sessions = stack.sessions,
                                            .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
                                            .accept_retry_delay =
                                                std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
                                            .pool_size = 4};
    frontend::mysql::mysql_server mysql(config);
    frontend::postgres::postgres_server postgres(config);
    mysql.start();
    postgres.start();

    constexpr uint8_t COM_QUERY = 0x03;
    constexpr uint8_t OK_HEADER = 0x00;
    const auto mysql_query = [](wire::raw_client& client, const std::string& sql) {
        std::vector<uint8_t> payload{COM_QUERY};
        payload.insert(payload.end(), sql.begin(), sql.end());
        client.write(wire::mysql_frame(0, payload));
        return wire::mysql_read_packet(client);
    };

    // The MySQL client's CREATE DATABASE eg is the schema local.eg.
    wire::raw_client my(mysql.local_port());
    wire::mysql_handshake(my);
    {
        INFO("CREATE DATABASE eg over the MySQL wire");
        const auto answer = mysql_query(my, "CREATE DATABASE eg");
        REQUIRE(answer[0] == OK_HEADER); // OK, not ERR
    }
    {
        INFO("CREATE TABLE local.eg.t after the client's CREATE DATABASE");
        REQUIRE(mysql_query(my, "CREATE TABLE local.eg.t (id bigint)")[0] == OK_HEADER);
    }

    // The PostgreSQL client's CREATE DATABASE is refused: a new PostgreSQL base is a
    // new connection, and connections come from config.yaml alone.
    wire::raw_client pg(postgres.local_port());
    wire::pg_startup(pg);
    const std::string create_database = "CREATE DATABASE eg2";
    std::vector<uint8_t> query{create_database.begin(), create_database.end()};
    query.push_back(0x00);
    pg.write(wire::pg_frame('Q', query));
    bool refused = false;
    for (int guard = 0; guard < 64; ++guard) {
        const auto message = wire::pg_read_message(pg);
        if (message.type == 'E') {
            refused = true;
            break;
        }
        if (message.type == 'Z') {
            break;
        }
    }
    CHECK(refused);
    mysql.stop();
    postgres.stop();
}

TEST_CASE("connection names: a namespace statement lands in local") {
    names_t f("otterstax_cn_namespaces");
    // A CREATE DATABASE through the session runs once and is the schema
    // local.eg_spark (the Spark frontend runs spark.sql through this route).
    f.ok("CREATE DATABASE eg_spark;");
    f.ok("CREATE TABLE local.eg_spark.t (id bigint);");
    CHECK(f.rows("SELECT id FROM local.eg_spark.t;").empty());
    // CREATE SCHEMA under the place pg.shop is the schema local.eg_pg, not a
    // namespace of the connection's base.
    f.ok("SET search_path = 'pg.shop';");
    f.ok("CREATE SCHEMA eg_pg;");
    f.ok("CREATE TABLE local.eg_pg.t (id bigint);");
    CHECK(f.rows("SELECT id FROM local.eg_pg.t;").empty());
    for (const char* alias : names_t::aliases()) {
        CHECK(f.sent(alias).empty());
    }
}
