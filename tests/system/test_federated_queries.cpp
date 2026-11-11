// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Federated statements through the query session over fake MySQL and PostgreSQL
// servers (fake_servers.hpp): what the client gets, and the SQL each backend
// received for it.
//
// - A CTE over one remote table is one statement: the CTE block is pushed as a
//   derived table, the query over it with it (a CTE read twice is one JOIN of
//   two derived tables; JOINed with a local table, the CTE alone is pushed and
//   the JOIN runs in the engine).
// - A remote table in a sub-query: `IN (SELECT ...)`, `NOT IN`, a correlated
//   `EXISTS` / `NOT EXISTS` (decorrelated by the engine into a read of the
//   correlation key, `__k0`), a scalar sub-query in the select list and in a
//   WHERE (read first, its value sent as a literal of the outer statement), a
//   derived table (one statement with its GROUP BY), a local table JOINed with
//   a derived table over a remote one, a remote table filtered by a sub-query
//   over a local one (the local one runs first, its values become the remote
//   statement's IN list), a sub-query over another server.
// - A column named by its full name: five parts (`c.d.s.t.col`), four, three,
//   two, a table alias; an alias qualified, or a qualification no FROM element
//   is written with, is the engine's refusal naming the nearest element.
// - The name rules of the model (c.d.s.t is a connection's table, d.s.t / s.t /
//   t local), each a statement and what it must answer: rows or an error code,
//   and the exact statement each backend received (a rule naming none reaches
//   no backend).
// - SAVEPOINT / ROLLBACK TO / RELEASE over a remote write: the engine's
//   savepoint levels are the backend's `SAVEPOINT s<k>` / `ROLLBACK TO
//   SAVEPOINT s<k>`, the local rows of a level rolled back with it.
//
// A canned read of a fake server answers every row of the table it names; a
// statement whose answer depends on its WHERE, its GROUP BY or an aggregate is
// answered with `answer()` — the rows the backend would answer for exactly the
// text the test expects it to receive — so a statement sent in another form
// shows up as a wrong row or a wrong text, never as a silent pass.

#include "fake_servers.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <map>
#include <optional>
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
                    if (v.is_null(r)) {
                        line += "null";
                        continue;
                    }
                    switch (v.type().type()) {
                        case logical_type::BIGINT:
                            line += std::to_string(v.get_value<std::int64_t>(r));
                            break;
                        case logical_type::INTEGER:
                            line += std::to_string(v.get_value<std::int32_t>(r));
                            break;
                        case logical_type::UBIGINT:
                            line += std::to_string(v.get_value<std::uint64_t>(r));
                            break;
                        case logical_type::STRING_LITERAL:
                            line += std::string{v.get_value<std::string_view>(r)};
                            break;
                        default:
                            FAIL("rows_of: no text for type " << static_cast<int>(v.type().type()));
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

    // The dialect of a fake server: its connection, its namespace (PostgreSQL schema / MySQL
    // database), how it spells shop.orders, its quote.
    struct dialect_t {
        const char* alias;
        const char* ns;
        const char* orders;
        char quote;
    };
    const dialect_t pg_dialect{"pg", "public", R"("public"."orders")", '"'};
    const dialect_t my_dialect{"my", "shop", "`shop`.`orders`", '`'};

    std::string replace_all(std::string text, const std::string& from, const std::string& to) {
        for (auto at = text.find(from); at != std::string::npos; at = text.find(from, at + to.size())) {
            text.replace(at, from.size(), to);
        }
        return text;
    }
    // A statement of the test, `{a}` its connection, `{n}` its namespace.
    std::string query(const dialect_t& d, const std::string& text) {
        return replace_all(replace_all(text, "{a}", d.alias), "{n}", d.ns);
    }
    // A statement as the backend receives it, written with PostgreSQL's quote: `{T}` the table,
    // every `"` the dialect's quote.
    std::string backend_sql(const dialect_t& d, const std::string& text) {
        auto out = replace_all(text, "{T}", "\x01");
        std::replace(out.begin(), out.end(), '"', d.quote);
        return replace_all(out, "\x01", d.orders);
    }

    // orders (id BIGINT, customer TEXT, amount BIGINT): (1, ann, 10), (2, bob, 20), (3, ann, 30).
    fakebackend::canned_table_t orders_in(const char* schema) {
        return {"shop",
                schema,
                "orders",
                {column("id", logical_type::BIGINT),
                 column("customer", logical_type::STRING_LITERAL),
                 column("amount", logical_type::BIGINT)},
                {{std::int64_t{1}, std::string{"ann"}, std::int64_t{10}},
                 {std::int64_t{2}, std::string{"bob"}, std::int64_t{20}},
                 {std::int64_t{3}, std::string{"ann"}, std::int64_t{30}}}};
    }

    // pg (PostgreSQL shop, schema public), pgb (the same database, schema sales; its server
    // also has public.orders, which pgb does not register) and my (MySQL shop). pg also has the
    // mixed-case table "Events" ("Id"), my the table items (id, label).
    bool seed(fake_servers_t& servers) {
        servers.add("pg", backend_type_t::PostgreSQL).add_table(orders_in("public"));
        servers.server("pg").add_table(
            {"shop", "public", "Events", {column("Id", logical_type::BIGINT)}, {{std::int64_t{7}}, {std::int64_t{8}}}});
        servers.add("pgb", backend_type_t::PostgreSQL).add_table(orders_in("sales"));
        servers.server("pgb").add_table(orders_in("public"));
        servers.add("my", backend_type_t::MySQL).add_table(orders_in("public"));
        servers.server("my").add_table(
            {"shop",
             "public",
             "items",
             {column("id", logical_type::BIGINT), column("label", logical_type::STRING_LITERAL)},
             {{std::int64_t{1}, std::string{"x"}}, {std::int64_t{3}, std::string{"z"}}}});
        return true;
    }

    stack_options_t fake_options() {
        stack_options_t options;
        options.connectors = fake_servers_t::connectors();
        options.connections.postgresql.push_back(
            config::PgConnectionDesc{.alias = "pg", .host = "fake", .database = "shop", .schema = "public"});
        options.connections.postgresql.push_back(
            config::PgConnectionDesc{.alias = "pgb", .host = "fake", .database = "shop", .schema = "sales"});
        options.connections.mysql.push_back(
            config::MysqlConnectionDesc{.alias = "my", .host = "fake", .database = "shop"});
        return options;
    }

    // The servers, the stack over them and a local table loc.cust (name, city): ann rome,
    // bob oslo, cid kyiv.
    class federated_t {
    public:
        explicit federated_t(const std::string& name)
            : seeded_(seed(servers_))
            , owner_(unique_test_dir(name), fake_options()) {
            for (const char* sql : {"CREATE DATABASE loc;",
                                    "CREATE TABLE loc.cust (name string, city string);",
                                    "INSERT INTO loc.cust (name, city) VALUES ('ann', 'rome'), ('bob', "
                                    "'oslo'), ('cid', 'kyiv');"}) {
                ok(sql);
            }
        }

        core::result_wrapper_t<session_payload> run(const std::string& sql) {
            if (!keep_sent_) {
                servers_.forget_sent();
            }
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

        std::vector<std::string> sent(const std::string& alias) { return servers_.sent(alias); }
        // Whether no server received a statement since the last one the test sent.
        bool nothing_sent() {
            for (const char* alias : {"pg", "pgb", "my"}) {
                if (!sent(alias).empty()) {
                    return false;
                }
            }
            return true;
        }
        void answer(const std::string& alias,
                    std::string sql,
                    std::vector<components::types::complex_logical_type> columns,
                    std::vector<fakebackend::row_t> rows) {
            servers_.answer(alias, std::move(sql), std::move(columns), std::move(rows));
        }
        fake_servers_t& servers() { return servers_; }
        session_stack stack() const { return owner_.stack(); }
        session_hash_t next_id() { return id_++; }
        // Keep what the servers received across statements (each statement forgets it otherwise).
        void keep_sent(bool keep) { keep_sent_ = keep; }

    private:
        fake_servers_t servers_;
        bool seeded_;
        session_stack_owner owner_;
        session_hash_t id_{1};
        bool keep_sent_{false};
    };

    components::types::complex_logical_type bigint(const char* name) { return column(name, logical_type::BIGINT); }
    components::types::complex_logical_type text(const char* name) {
        return column(name, logical_type::STRING_LITERAL);
    }
    fakebackend::value_t i(std::int64_t v) { return v; }
    fakebackend::value_t s(const char* v) { return std::string{v}; }
} // namespace

// ─── CTE ────────────────────────────────────────────────────────────────────

TEST_CASE("federated queries: a CTE over one remote table is one statement of its backend") {
    federated_t f("otterstax_fq_cte");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto pushed = backend_sql(
            d,
            R"(SELECT "d0"."id" FROM (SELECT "id" AS "id", "amount" AS "amount" FROM {T} WHERE "amount" > 15) AS "d0";)");
        f.answer(d.alias, pushed, {bigint("id")}, {{i(2)}, {i(3)}});
        CHECK(sorted(f.rows(query(d,
                                  "WITH x AS (SELECT id, amount FROM {a}.{n}.orders WHERE amount > 15) "
                                  "SELECT id FROM x;"))) == rows_t{"2", "3"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{pushed});

        // A CTE read twice (a self-JOIN of it) is one JOIN of two derived tables.
        const auto twice = backend_sql(
            d,
            R"(SELECT "d0"."id", "d1"."id" FROM (SELECT "id" AS "id", "amount" AS "amount" FROM {T} WHERE "amount" > 15) )"
            R"(AS "d0" JOIN (SELECT "id" AS "id", "amount" AS "amount" FROM {T} WHERE "amount" > 15) AS "d1" ON )"
            R"("d0"."id" = "d1"."id";)");
        f.answer(d.alias, twice, {bigint("id"), bigint("id")}, {{i(2), i(2)}, {i(3), i(3)}});
        CHECK(sorted(f.rows(query(d,
                                  "WITH x AS (SELECT id, amount FROM {a}.{n}.orders WHERE amount > 15) "
                                  "SELECT a.id, b.id FROM x a JOIN x b ON a.id = b.id;"))) == rows_t{"2|2", "3|3"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{twice});

        // A filter over the CTE's aggregate goes with it: a WHERE over the derived table.
        const auto grouped = backend_sql(d,
                                         R"(SELECT "d0"."customer" FROM (SELECT "customer" AS "customer", )"
                                         R"(SUM("amount") AS "total" FROM {T} GROUP BY "customer") AS "d0" )"
                                         R"(WHERE "d0"."total" > 25;)");
        f.answer(d.alias, grouped, {text("customer")}, {{s("ann")}});
        CHECK(f.rows(query(d,
                           "WITH x AS (SELECT customer, sum(amount) AS total FROM {a}.{n}.orders GROUP BY "
                           "customer) SELECT customer FROM x WHERE total > 25;")) == rows_t{"ann"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{grouped});
    }
}

TEST_CASE("federated queries: a CTE over a remote table JOINed with a local one pushes the CTE alone") {
    federated_t f("otterstax_fq_cte_local");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto pushed = backend_sql(d,
                                        R"(SELECT "d0"."customer", "d0"."amount" FROM (SELECT "customer" AS )"
                                        R"("customer", "amount" AS "amount" FROM {T} WHERE "amount" > 15) AS "d0";)");
        f.answer(d.alias, pushed, {text("customer"), bigint("amount")}, {{s("bob"), i(20)}, {s("ann"), i(30)}});
        CHECK(f.rows(query(d,
                           "WITH big AS (SELECT customer, amount FROM {a}.{n}.orders WHERE amount > 15) "
                           "SELECT c.city, big.amount FROM loc.cust c JOIN big ON c.name = big.customer "
                           "ORDER BY big.amount;")) == rows_t{"oslo|20", "rome|30"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{pushed});
    }
}

// ─── Sub-queries ────────────────────────────────────────────────────────────

TEST_CASE("federated queries: a remote table in IN and NOT IN under a local query") {
    federated_t f("otterstax_fq_in");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto big = backend_sql(d, R"(SELECT "customer" FROM {T} WHERE "amount" > 25;)");
        f.answer(d.alias, big, {text("customer")}, {{s("ann")}});
        CHECK(f.rows(query(d,
                           "SELECT name FROM loc.cust WHERE name IN (SELECT customer FROM "
                           "{a}.{n}.orders WHERE amount > 25);")) == rows_t{"ann"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{big});

        // NOT IN over the whole table (the canned read: ann, bob, ann).
        CHECK(f.rows(query(d,
                           "SELECT name FROM loc.cust WHERE name NOT IN (SELECT customer FROM "
                           "{a}.{n}.orders);")) == rows_t{"cid"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{backend_sql(d, R"(SELECT "customer" FROM {T};)")});
    }
}

TEST_CASE("federated queries: a correlated EXISTS over a remote table reads its correlation key once") {
    federated_t f("otterstax_fq_exists");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        // The engine decorrelates the sub-query: one read of the key under its WHERE, then a semi-join.
        const auto keys = backend_sql(
            d,
            R"(SELECT "d0"."__k0" FROM (SELECT "customer" AS "__k0" FROM {T} WHERE "amount" > 25) AS "d0";)");
        f.answer(d.alias, keys, {text("__k0")}, {{s("ann")}});
        CHECK(f.rows(query(d,
                           "SELECT name FROM loc.cust c WHERE EXISTS (SELECT 1 FROM {a}.{n}.orders o "
                           "WHERE o.customer = c.name AND o.amount > 25);")) == rows_t{"ann"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{keys});

        const auto all_keys =
            backend_sql(d, R"(SELECT "d0"."__k0" FROM (SELECT "customer" AS "__k0" FROM {T}) AS "d0";)");
        f.answer(d.alias, all_keys, {text("__k0")}, {{s("ann")}, {s("bob")}, {s("ann")}});
        CHECK(f.rows(query(d,
                           "SELECT name FROM loc.cust c WHERE NOT EXISTS (SELECT 1 FROM "
                           "{a}.{n}.orders o WHERE o.customer = c.name);")) == rows_t{"cid"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{all_keys});
    }
}

TEST_CASE("federated queries: a scalar sub-query over a remote table") {
    federated_t f("otterstax_fq_scalar");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto max = backend_sql(d, R"(SELECT MAX("amount") AS "max" FROM {T};)");
        f.answer(d.alias, max, {bigint("max")}, {{i(30)}});
        CHECK(f.rows(query(d,
                           "SELECT name, (SELECT max(amount) FROM {a}.{n}.orders) AS m FROM loc.cust "
                           "ORDER BY name;")) == rows_t{"ann|30", "bob|30", "cid|30"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{max});

        // In the WHERE of a remote query of the same server: the sub-query runs first and its value
        // is a literal of the outer statement.
        const auto outer = backend_sql(d, R"(SELECT "id" FROM {T} WHERE "amount" < 30;)");
        f.answer(d.alias, outer, {bigint("id")}, {{i(1)}, {i(2)}});
        CHECK(sorted(f.rows(query(d,
                                  "SELECT id FROM {a}.{n}.orders WHERE amount < (SELECT max(amount) FROM "
                                  "{a}.{n}.orders);"))) == rows_t{"1", "2"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{max, outer});
    }
}

TEST_CASE("federated queries: a derived table over a remote table, alone and JOINed with a local table") {
    federated_t f("otterstax_fq_derived");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto grouped =
            backend_sql(d,
                        R"(SELECT "d0"."customer", "d0"."total" FROM (SELECT "customer" AS )"
                        R"("customer", SUM("amount") AS "total" FROM {T} GROUP BY "customer") AS "d0";)");
        f.answer(d.alias, grouped, {text("customer"), bigint("total")}, {{s("ann"), i(40)}, {s("bob"), i(20)}});
        CHECK(sorted(f.rows(query(d,
                                  "SELECT d.customer, d.total FROM (SELECT customer, sum(amount) AS total FROM "
                                  "{a}.{n}.orders GROUP BY customer) d;"))) == rows_t{"ann|40", "bob|20"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{grouped});

        CHECK(f.rows(query(d,
                           "SELECT c.city, d.total FROM loc.cust c JOIN (SELECT customer, sum(amount) AS total "
                           "FROM {a}.{n}.orders GROUP BY customer) d ON c.name = d.customer ORDER BY "
                           "c.city;")) == rows_t{"oslo|20", "rome|40"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{grouped});
    }
}

TEST_CASE("federated queries: a remote table filtered by a sub-query over a local one gets its values") {
    federated_t f("otterstax_fq_local_in");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto pushed = backend_sql(d, R"(SELECT "id" FROM {T} WHERE "customer" IN ('ann');)");
        f.answer(d.alias, pushed, {bigint("id")}, {{i(1)}, {i(3)}});
        CHECK(sorted(f.rows(query(d,
                                  "SELECT id FROM {a}.{n}.orders WHERE customer IN (SELECT name FROM "
                                  "loc.cust WHERE city = 'rome');"))) == rows_t{"1", "3"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{pushed});
    }
}

TEST_CASE("federated queries: a sub-query over another server is read first") {
    federated_t f("otterstax_fq_cross_scalar");
    const auto min = backend_sql(pg_dialect, R"(SELECT MIN("amount") AS "min" FROM {T};)");
    f.answer("pg", min, {bigint("min")}, {{i(10)}});
    const auto outer = backend_sql(my_dialect, R"(SELECT "id" FROM {T} WHERE "amount" > 10;)");
    f.answer("my", outer, {bigint("id")}, {{i(2)}, {i(3)}});
    CHECK(sorted(f.rows("SELECT id FROM my.shop.orders WHERE amount > (SELECT min(amount) FROM "
                        "pg.public.orders);")) == rows_t{"2", "3"});
    CHECK(f.sent("pg") == std::vector<std::string>{min});
    CHECK(f.sent("my") == std::vector<std::string>{outer});
}

// ─── Column names ───────────────────────────────────────────────────────────

TEST_CASE("federated queries: a column named by the full name of its table, or by any suffix of it") {
    federated_t f("otterstax_fq_columns");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto read = backend_sql(d, R"(SELECT "id" FROM {T};)");
        for (const char* sql : {"SELECT {a}.{n}.orders.id FROM {a}.{n}.orders;",
                                "SELECT {n}.orders.id FROM {a}.{n}.orders;",
                                "SELECT orders.id FROM {a}.{n}.orders;",
                                "SELECT o.id FROM {a}.{n}.orders o;"}) {
            INFO(query(d, sql));
            CHECK(sorted(f.rows(query(d, sql))) == rows_t{"1", "2", "3"});
            CHECK(f.sent(d.alias) == std::vector<std::string>{read});
        }

        // In WHERE as well: the pushed condition names the backend's column.
        const auto filtered = backend_sql(d, R"(SELECT "id" FROM {T} WHERE "amount" > 25;)");
        f.answer(d.alias, filtered, {bigint("id")}, {{i(3)}});
        CHECK(f.rows(query(d, "SELECT {a}.{n}.orders.id FROM {a}.{n}.orders WHERE {a}.{n}.orders.amount > 25;")) ==
              rows_t{"3"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{filtered});

        // In GROUP BY: the block is one statement, grouped by the backend's column.
        const auto grouped = backend_sql(d, R"(SELECT "customer", COUNT(*) AS "count" FROM {T} GROUP BY "customer";)");
        f.answer(d.alias, grouped, {text("customer"), bigint("count")}, {{s("ann"), i(2)}, {s("bob"), i(1)}});
        CHECK(sorted(f.rows(query(d,
                                  "SELECT {a}.{n}.orders.customer, count(*) FROM {a}.{n}.orders "
                                  "GROUP BY {a}.{n}.orders.customer;"))) == rows_t{"ann|2", "bob|1"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{grouped});

        // An alias takes no qualification, and a qualification no FROM element is written with is
        // refused with the element it is nearest to: the engine's own refusals
        // (components/sql/transformer/utils.cpp, refusal()), before any backend is asked.
        auto qualified_alias = f.refused(query(d, "SELECT {a}.{n}.o.id FROM {a}.{n}.orders o;"));
        CHECK(qualified_alias.type == code::table_not_exists);
        CHECK(std::string{qualified_alias.what.c_str()}.find("\"o\" is an alias and takes no qualification") !=
              std::string::npos);
        CHECK(f.nothing_sent());
        auto other = f.refused(query(d, "SELECT nowhere.orders.id FROM {a}.{n}.orders;"));
        CHECK(other.type == code::table_not_exists);
        CHECK(std::string{other.what.c_str()}.find(query(d, "the nearest is \"{a}.{n}.orders\"")) != std::string::npos);
        CHECK(f.nothing_sent());
        // A table is named in three parts: a column by more than four is no column.
        CHECK(f.refused(query(d, "SELECT {a}.x.{n}.orders.id FROM {a}.{n}.orders;")).type == code::table_not_exists);
        CHECK(f.nothing_sent());
    }

    // Two servers in one JOIN, each column by its table's full name.
    CHECK(sorted(f.rows("SELECT pg.public.orders.id, my.shop.orders.amount FROM pg.public.orders "
                        "JOIN my.shop.orders ON pg.public.orders.id = my.shop.orders.id;")) ==
          rows_t{"1|10", "2|20", "3|30"});
    CHECK(f.sent("pg") == std::vector<std::string>{backend_sql(pg_dialect, R"(SELECT "id" FROM {T};)")});
    CHECK(f.sent("my") == std::vector<std::string>{backend_sql(my_dialect, R"(SELECT "id", "amount" FROM {T};)")});
    // The table name alone is then ambiguous.
    CHECK(f.refused("SELECT orders.id FROM pg.public.orders JOIN my.shop.orders ON true;").type ==
          code::ambiguous_alias);
}

// ─── The name rules ─────────────────────────────────────────────────────────

namespace {
    // One rule: a statement and what it answers — rows (any order) or an error code — and the
    // statements each backend received; a backend not named received none.
    struct name_rule_t {
        const char* rule;
        std::string sql;
        std::optional<code> error;
        std::map<std::string, std::vector<std::string>> sent;
        std::optional<rows_t> rows = std::nullopt;
    };

    const std::string pg_ids = R"(SELECT "id" FROM "public"."orders";)";
    const std::string pgb_ids = R"(SELECT "id" FROM "sales"."orders";)";
    const std::string my_ids = "SELECT `id` FROM `shop`.`orders`;";

    const std::vector<name_rule_t>& name_rules() {
        static const std::vector<name_rule_t> rules{
            // connection.namespace.table: the namespace is a PostgreSQL schema, a MySQL database.
            {"three parts name a connection's table",
             "SELECT id FROM pg.public.orders;",
             {},
             {{"pg", {pg_ids}}},
             rows_t{"1", "2", "3"}},
            {"a second connection of one PostgreSQL database names its own schema",
             "SELECT id FROM pgb.sales.orders;",
             {},
             {{"pgb", {pgb_ids}}},
             rows_t{"1", "2", "3"}},
            {"a schema its connection does not register is no table, even where the backend has it",
             "SELECT id FROM pgb.public.orders;",
             code::table_not_exists,
             {}},
            {"a PostgreSQL connection reaches its configured schema only",
             "SELECT id FROM pg.sales.orders;",
             code::table_not_exists,
             {}},
            {"four parts are a second full name of one table, refused",
             "SELECT id FROM pg.shop.public.orders;",
             code::invalid_parameter,
             {}},
            {"three parts under a name that is no connection are a local database's",
             "SELECT id FROM nosuch.shop.orders;",
             code::table_not_exists,
             {}},
            {"MySQL: the namespace is the backend's database",
             "SELECT id FROM my.shop.orders;",
             {},
             {{"my", {my_ids}}},
             rows_t{"1", "2", "3"}},
            {"MySQL has no schema level: four parts are refused",
             "SELECT id FROM my.shop.public.orders;",
             code::invalid_parameter,
             {}},
            {"a backend's mixed-case table is named in lower case and sent in its own spelling",
             "SELECT id FROM pg.public.events;",
             {},
             {{"pg", {R"(SELECT "Id" FROM "public"."Events";)"}}},
             rows_t{"7", "8"}},
            {"the backend's spelling of a table is not its catalog name",
             R"(SELECT id FROM pg.public."Events";)",
             code::table_not_exists,
             {}},
            {"nor the backend's spelling of a column",
             R"(SELECT "Id" FROM pg.public.events;)",
             code::undefined_column,
             {}},

            // Local names: local.namespace.table, or completed by the session's place (local.public).
            {"two parts are schema.table of the session's place",
             "SELECT id FROM pg.orders;",
             code::table_not_exists,
             {}},
            {"a local schema named as a connection's namespace is the local one",
             "SELECT id FROM shop.orders;",
             {},
             {},
             rows_t{"42"}},
            {"and the connection's table stays the remote one",
             "SELECT id FROM my.shop.orders;",
             {},
             {{"my", {my_ids}}},
             rows_t{"1", "2", "3"}},
            {"a local table JOINed with the remote table of the same names",
             "SELECT l.id, r.id FROM shop.orders l JOIN my.shop.orders r ON l.id = r.id + 41;",
             {},
             {{"my", {my_ids}}},
             rows_t{"42|1"}},
            {"two local schemas",
             "SELECT a.id FROM local.shop.orders a JOIN local.shop2.orders b ON a.id = b.id;",
             {},
             {},
             rows_t{}},
            {"a bare table is in the session's place", "SELECT id FROM orders;", {}, {}, rows_t{"5"}},
            {"schema.table in the session's database", "SELECT id FROM public.orders;", {}, {}, rows_t{"5"}},
            {"the local database written out", "SELECT id FROM local.public.orders;", {}, {}, rows_t{"5"}},
            {"a local write target in a schema that does not exist",
             "INSERT INTO local.whatever.orders (id) VALUES (1);",
             code::schema_not_exists,
             {}},

            // JOINs.
            {"two backends with the same namespace and table",
             "SELECT p.id FROM pg.public.orders p JOIN my.shop.orders m ON p.id = m.id;",
             {},
             {{"pg", {pg_ids}}, {"my", {my_ids}}},
             rows_t{"1", "2", "3"}},
            {"two connections of one PostgreSQL database, one per schema",
             "SELECT a.id FROM pg.public.orders a JOIN pgb.sales.orders b ON a.id = b.id;",
             {},
             {{"pg", {pg_ids}}, {"pgb", {pgb_ids}}},
             rows_t{"1", "2", "3"}},
            {"JOIN USING across two backends",
             "SELECT id FROM pg.public.orders JOIN my.shop.items USING (id);",
             {},
             {{"pg", {pg_ids}}, {"my", {"SELECT `id` FROM `shop`.`items`;"}}},
             rows_t{"1", "3"}},
            {"a derived table over a remote table",
             "SELECT s.id FROM (SELECT id FROM pg.public.orders) s;",
             {},
             {{"pg", {R"(SELECT "d0"."id" FROM (SELECT "id" AS "id" FROM "public"."orders") AS "d0";)"}}},
             rows_t{"1", "2", "3"}},

            // Writes.
            {"INSERT into a connection's table",
             "INSERT INTO pg.public.orders (id) VALUES (1);",
             {},
             {{"pg", {R"(INSERT INTO "public"."orders" ("id", "customer", "amount") VALUES (1, NULL, NULL);)"}}}},
            {"INSERT on the second schema",
             "INSERT INTO pgb.sales.orders (id) VALUES (1);",
             {},
             {{"pgb", {R"(INSERT INTO "sales"."orders" ("id", "customer", "amount") VALUES (1, NULL, NULL);)"}}}},
            {"INSERT on MySQL",
             "INSERT INTO my.shop.orders (id) VALUES (1);",
             {},
             {{"my", {"INSERT INTO `shop`.`orders` (`id`, `customer`, `amount`) VALUES (1, NULL, NULL);"}}}},
            {"UPDATE",
             "UPDATE pg.public.orders SET amount = 1 WHERE id = 1;",
             {},
             {{"pg", {R"(UPDATE "public"."orders" SET "amount" = 1 WHERE "id" = 1;)"}}}},
            {"UPDATE on MySQL",
             "UPDATE my.shop.orders SET amount = 1 WHERE id = 1;",
             {},
             {{"my", {"UPDATE `shop`.`orders` SET `amount` = 1 WHERE `id` = 1;"}}}},
            {"DELETE",
             "DELETE FROM pg.public.orders WHERE id = 1;",
             {},
             {{"pg", {R"(DELETE FROM "public"."orders" WHERE "id" = 1;)"}}}},
            {"DELETE on MySQL",
             "DELETE FROM my.shop.orders WHERE id = 1;",
             {},
             {{"my", {"DELETE FROM `shop`.`orders` WHERE `id` = 1;"}}}},
            {"a write target sharing namespace and table with a source on another connection",
             "INSERT INTO pg.public.orders (id) SELECT id FROM my.shop.orders;",
             {},
             {{"my", {my_ids}},
              {"pg",
               {R"(INSERT INTO "public"."orders" ("id", "customer", "amount") VALUES (1, NULL, NULL), (2, NULL, NULL), (3, NULL, NULL);)"}}}},
            {"a local write target fed by the remote table of the same names",
             "INSERT INTO shop.orders (id) SELECT id FROM my.shop.orders;",
             {},
             {{"my", {my_ids}}}},

            // DDL.
            {"CREATE TABLE on a connection",
             "CREATE TABLE pg.public.fresh (id INT);",
             {},
             {{"pg", {R"(CREATE TABLE "public"."fresh" ("id" int4);)"}}}},
            {"CREATE TABLE on the second schema",
             "CREATE TABLE pgb.sales.fresh (id INT);",
             {},
             {{"pgb", {R"(CREATE TABLE "sales"."fresh" ("id" int4);)"}}}},
            {"CREATE TABLE on MySQL",
             "CREATE TABLE my.shop.fresh (id INT);",
             {},
             {{"my", {"CREATE TABLE `shop`.`fresh` (`id` int);"}}}},
            {"DROP TABLE on a connection",
             "DROP TABLE pg.public.fresh;",
             {},
             {{"pg", {R"(DROP TABLE "public"."fresh";)"}}}},
            {"DROP TABLE on MySQL", "DROP TABLE my.shop.fresh;", {}, {{"my", {"DROP TABLE `shop`.`fresh`;"}}}},
            {"CREATE DATABASE named as a connection is a schema of local", "CREATE DATABASE pg;", {}, {}},
        };
        return rules;
    }
} // namespace

TEST_CASE("federated names: every statement reaches the table its name means", "[names]") {
    federated_t f("otterstax_fq_names");
    // The local side of the rules: a local schema named as a connection's namespace, a second one, and
    // a table of the session's own place (local.public).
    for (const char* sql : {"CREATE DATABASE shop;",
                            "CREATE TABLE shop.orders (id bigint, customer string, amount bigint);",
                            "INSERT INTO shop.orders (id, customer, amount) VALUES (42, 'loc', 1);",
                            "CREATE DATABASE shop2;",
                            "CREATE TABLE shop2.orders (id bigint);",
                            "CREATE TABLE public.orders (id bigint);",
                            "INSERT INTO public.orders (id) VALUES (5);"}) {
        f.ok(sql);
    }
    for (const auto& rule : name_rules()) {
        auto result = f.run(rule.sql);
        INFO("rule: " << rule.rule);
        INFO("sql: " << rule.sql);
        INFO("result: " << (result.has_error() ? result.error().what.c_str() : "ok"));
        for (const char* alias : {"pg", "pgb", "my"}) {
            const auto expected = rule.sent.contains(alias) ? rule.sent.at(alias) : std::vector<std::string>{};
            INFO("sent to " << alias);
            CHECK(f.sent(alias) == expected);
        }
        if (rule.error) {
            CHECK(result.has_error());
            if (result.has_error()) {
                CHECK(result.error().type == *rule.error);
            }
            continue;
        }
        CHECK_FALSE(result.has_error());
        if (rule.rows && !result.has_error()) {
            CHECK(sorted(rows_of(result.value())) == sorted(*rule.rows));
        }
    }
}

TEST_CASE("federated names: a table the backend got after startup is registered by the statement that missed it") {
    federated_t f("otterstax_fq_names_late");
    f.servers().server("pg").add_table({"shop", "public", "late", {bigint("k")}, {{i(11)}}});
    CHECK(f.rows("SELECT k FROM pg.public.late;") == rows_t{"11"});
    CHECK(f.sent("pg") == std::vector<std::string>{R"(SELECT "k" FROM "public"."late";)"});
    // Under a schema its connection does not register, the miss stays a miss.
    f.servers().server("pgb").add_table({"shop", "public", "late", {bigint("k")}, {{i(11)}}});
    CHECK(f.refused("SELECT k FROM pgb.public.late;").type == code::table_not_exists);
    CHECK(f.nothing_sent());
}

// ─── Savepoints over a remote write ─────────────────────────────────────────

TEST_CASE("federated savepoints: ROLLBACK TO is the backend's ROLLBACK TO the level's savepoint") {
    federated_t f("otterstax_fq_savepoints");
    f.ok("CREATE TABLE loc.t (id bigint);");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        f.ok("DELETE FROM loc.t;");
        auto& server = f.servers().server(d.alias);
        server.clear();
        f.keep_sent(true);
        const auto insert = [&](int id) {
            return query(d,
                         "INSERT INTO {a}.{n}.orders (id, customer, amount) VALUES (" + std::to_string(id) +
                             ", 'x', 1);");
        };
        f.ok("BEGIN;");
        f.ok(insert(4));
        f.ok("INSERT INTO loc.t (id) VALUES (1);");
        f.ok("SAVEPOINT a;");
        f.ok(insert(5));
        f.ok("INSERT INTO loc.t (id) VALUES (2);");
        f.ok("SAVEPOINT b;");
        f.ok(insert(6));
        // Back to a: the writes of a's level and of b's are rolled back on the backend (to the
        // oldest backend savepoint of a level at or above a's) and in the engine.
        f.ok("ROLLBACK TO SAVEPOINT a;");
        f.ok(insert(7));
        f.ok("RELEASE SAVEPOINT a;");
        f.ok("COMMIT;");
        f.keep_sent(false);
        const auto log = server.log();
        CHECK(f.rows("SELECT id FROM loc.t;") == rows_t{"1"});

        const auto row = [&](int id) {
            return "write " + backend_sql(d, R"(INSERT INTO {T} ("id", "customer", "amount") VALUES ()") +
                   std::to_string(id) + ", 'x', 1); @txn";
        };
        INFO("log: " << Catch::rangeToString(log));
        REQUIRE(log.size() == 10);
        CHECK(log[0] == "begin");
        CHECK(log[1] == row(4));
        // Each level's savepoint is set before its first statement on the backend.
        REQUIRE(log[2].rfind("savepoint s", 0) == 0);
        CHECK(log[3] == row(5));
        REQUIRE(log[4].rfind("savepoint s", 0) == 0);
        CHECK(log[4] != log[2]);
        CHECK(log[5] == row(6));
        CHECK(log[6] == "rollback_to_savepoint " + log[2].substr(std::string{"savepoint "}.size()));
        // The level after ROLLBACK TO is a new one with a savepoint of its own; RELEASE sends
        // nothing (the backend's savepoints end with its transaction).
        REQUIRE(log[7].rfind("savepoint s", 0) == 0);
        CHECK(log[8] == row(7));
        CHECK(log[9] == "commit");
    }
}

TEST_CASE("federated savepoints: a block writes to one connection") {
    federated_t f("otterstax_fq_savepoints_two");
    f.ok("BEGIN;");
    f.ok("SAVEPOINT a;");
    f.ok("INSERT INTO pg.public.orders (id, customer, amount) VALUES (8, 'b', 2);");
    // A second connection's write in the block is refused, the block fails until ROLLBACK, and
    // the ROLLBACK reaches the first backend.
    CHECK(f.refused("INSERT INTO my.shop.orders (id, customer, amount) VALUES (8, 'b', 2);").type ==
          code::unimplemented_yet);
    CHECK(f.nothing_sent());
    CHECK(f.refused("RELEASE SAVEPOINT a;").type == code::transaction_finalized);
    f.ok("ROLLBACK;");
    CHECK(f.servers().server("pg").changes().back() == "rollback");
}

// ─── A prepared select-list parameter over a remote table ───────────────────

TEST_CASE("federated queries: a prepared SELECT $1 FROM a remote table answers the bound value per row") {
    federated_t f("otterstax_fq_select_param");
    for (const auto& d : {pg_dialect, my_dialect}) {
        INFO("connection " << d.alias);
        const auto stack = f.stack();
        const auto statement = f.next_id();
        auto prepared = prepare_session_sql(stack, statement, query(d, "SELECT $1 FROM {a}.{n}.orders"));
        INFO((prepared.has_error() ? prepared.error().what.c_str() : ""));
        REQUIRE_FALSE(prepared.has_error());
        CHECK(prepared.value().parameter_count == 1);
        f.servers().forget_sent();
        // The bound value is a literal of the pushed statement; the backend answers it per row.
        const auto pushed = backend_sql(d, "SELECT 5 FROM {T};");
        f.answer(d.alias, pushed, {bigint("?column?")}, {{i(5)}, {i(5)}, {i(5)}});
        std::pmr::vector<components::vector::vector_t> values{stack.resource};
        values.push_back(components::vector::make_scalar(stack.resource, std::int64_t{5}));
        auto executed = execute_session_prepared(stack, statement, std::move(values));
        INFO((executed.has_error() ? executed.error().what.c_str() : ""));
        REQUIRE_FALSE(executed.has_error());
        CHECK(rows_of(executed.value()) == rows_t{"5", "5", "5"});
        CHECK(f.sent(d.alias) == std::vector<std::string>{pushed});
    }
}
