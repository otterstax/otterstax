// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// JOIN shapes the old mixed-plan path was tested through, over the federation: a JOIN of two
// servers grouped by a key of either type, and a remote table JOINed with a local composite table
// on a struct-field key and on a plain key, over an empty and a seeded local table (engine defect
// D1: a build side left with zero chunks once crashed operator_join_t). Four separate TEST_CASEs
// for D1, never SECTIONs: a crash aborts the process and each variant must be attributable.

#include "federation_fixture.hpp"

#include "otterbrix/translators/input/ch_to_chunk.hpp"

#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

using namespace fedtest;

namespace {

    canned_table_t answer(std::vector<complex_logical_type> columns, std::vector<row_t> rows) {
        return canned_table_t{std::move(columns), std::move(rows), std::nullopt, {}};
    }

    // campaigns on a MySQL server, products on a PostgreSQL one; campaign_name of `name_type`.
    struct two_servers_t {
        std::shared_ptr<fake_backend_t> campaigns = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
        std::shared_ptr<fake_backend_t> products = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
        std::unique_ptr<engine_t> engine;

        two_servers_t(const std::string& leaf, bool string_names) {
            const std::vector<complex_logical_type> campaign_columns{
                column(logical_type::INTEGER, "campaign_id"),
                column(string_names ? logical_type::STRING_LITERAL : logical_type::INTEGER, "campaign_name"),
                column(logical_type::DOUBLE, "budget")};
            const std::vector<complex_logical_type> product_columns{column(logical_type::INTEGER, "product_id"),
                                                                    column(logical_type::INTEGER, "campaign_id"),
                                                                    column(logical_type::DOUBLE, "price")};
            auto name = [string_names](int i) -> value_t {
                return string_names ? value_t{std::string{"name_" + std::to_string(i)}} : value_t{std::int32_t{i * 10}};
            };
            campaigns->set_table("campaigns",
                                 answer(campaign_columns,
                                        {{std::int32_t{1}, name(1), 100.5}, {std::int32_t{2}, name(2), 201.0}}));
            products->set_table("products",
                                answer(product_columns,
                                       {{std::int32_t{1}, std::int32_t{1}, 10.0},
                                        {std::int32_t{2}, std::int32_t{1}, 20.0},
                                        {std::int32_t{3}, std::int32_t{2}, 30.0}}));
            install({server("campaigns", campaigns), server("products", products)});
            engine = open_engine(leaf);
            engine->bootstrap();
            engine->upsert(backend_type_t::MySQL,
                           scope("campaigns", "db1", ""),
                           tables({discovered("campaigns", "db1", "", "campaigns", campaign_columns)}));
            engine->upsert(backend_type_t::PostgreSQL,
                           scope("products", "pgdb", "public"),
                           tables({discovered("products", "pgdb", "public", "products", product_columns)}));
        }
    };

    constexpr const char* group_by_sql =
        "SELECT c.campaign_name, count(p.product_id) AS product_count, avg(p.price) AS avg_product_price "
        "FROM campaigns.db1.campaigns c INNER JOIN products.pgdb.public.products p ON p.campaign_id = c.campaign_id "
        "GROUP BY c.campaign_name ORDER BY product_count DESC;";

} // namespace

TEST_CASE("federation joins: a JOIN of two servers grouped by a string key") {
    two_servers_t servers("join_group_string", true);
    auto grouped = servers.engine->sql(group_by_sql);
    INFO(error_of(grouped));
    REQUIRE(grouped->is_success());
    CHECK(rows_of(grouped) == text_rows_t{{"name_1", "2", "15.000000"}, {"name_2", "1", "30.000000"}});
    CHECK(servers.campaigns->scans() == 1);
    CHECK(servers.products->scans() == 1);
}

TEST_CASE("federation joins: a JOIN of two servers grouped by an integer key") {
    two_servers_t servers("join_group_integer", false);
    auto grouped = servers.engine->sql(group_by_sql);
    INFO(error_of(grouped));
    REQUIRE(grouped->is_success());
    CHECK(rows_of(grouped) == text_rows_t{{"10", "2", "15.000000"}, {"20", "1", "30.000000"}});
}

namespace {

    // pg.shop.public.customers: Ann in Berlin, Bob in Tel Aviv. Local otter.warehouses in the
    // demo's step_3a shape (ENUM + two composites + bigints); `seed_rows` adds the single
    // warehouse BER-1 located in Berlin. A correct engine answers `expected_rows`.
    void run_remote_join_over_local_composite(const char* tag, const char* sql, bool seed_rows, size_t expected_rows) {
        auto pg = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
        const std::vector<complex_logical_type> columns{column(logical_type::STRING_LITERAL, "name"),
                                                        column(logical_type::STRING_LITERAL, "addr_city")};
        pg->set_table("customers",
                      answer(columns,
                             {{std::string{"Ann"}, std::string{"Berlin"}}, {std::string{"Bob"}, std::string{"Tel Aviv"}}}));
        install({server("pg", pg)});
        auto engine = open_engine(std::string{"join_d1_"} + tag);
        engine->bootstrap();
        engine->upsert(backend_type_t::PostgreSQL,
                       scope("pg", "shop", "public"),
                       tables({discovered("pg", "shop", "public", "customers", columns)}));

        for (const char* ddl : {"CREATE DATABASE otter;",
                                "CREATE TYPE tier_t AS ENUM('bronze','silver','gold');",
                                "CREATE TYPE address_t AS (city STRING, country STRING, zip STRING);",
                                "CREATE TYPE spec_t AS (weight_1 INT, weight_2 INT, weight_3 INT, weight_4 INT, "
                                "weight_5 INT, weight_6 INT, primary_barcode STRING, secondary_barcode STRING, "
                                "photo STRING);",
                                "CREATE TABLE otter.warehouses (warehouse_id STRING, code STRING, tier tier_t, "
                                "location address_t, spec spec_t, priority_high BIGINT, priority_med BIGINT, "
                                "priority_low BIGINT);"}) {
            auto created = engine->sql(ddl);
            INFO(ddl << ": " << error_of(created));
            REQUIRE(created->is_success());
        }
        if (seed_rows) {
            auto seeded = engine->sql("INSERT INTO otter.warehouses (warehouse_id, code, tier, location, priority_high) "
                                      "VALUES ('w-1', 'BER-1', 'silver', ROW('Berlin','DE','10115'), 1);");
            INFO(error_of(seeded));
            REQUIRE(seeded->is_success());
        }

        auto joined = engine->sql(sql);
        INFO(error_of(joined));
        REQUIRE(joined->is_success());
        CHECK(joined->size() == expected_rows);
    }

    // Struct field access on the JOIN key and the demo's local-side predicate (step_4).
    constexpr const char* d1_struct_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                          "INNER JOIN otter.warehouses w ON c.addr_city = (w.location).city "
                                          "WHERE (w.location).country IN ('DE','IL','US');";
    // Plain column on the JOIN key; the same build side and predicate.
    constexpr const char* d1_plain_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                         "INNER JOIN otter.warehouses w ON c.addr_city = w.code "
                                         "WHERE (w.location).country IN ('DE','IL','US');";

} // namespace

// No warehouse: the predicate leaves the build side with zero chunks.
TEST_CASE("D1: remote table JOIN local composite table on struct key over empty local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite("struct_empty", d1_struct_key, false, 0);
}

// BER-1 is located in Berlin, DE: Ann matches, Bob does not.
TEST_CASE("D1: remote table JOIN local composite table on struct key over seeded local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite("struct_seeded", d1_struct_key, true, 1);
}

TEST_CASE("D1: remote table JOIN local composite table on plain key over empty local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite("plain_empty", d1_plain_key, false, 0);
}

// The code 'BER-1' is no customer's city: the JOIN runs to completion with no match.
TEST_CASE("D1: remote table JOIN local composite table on plain key over seeded local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite("plain_seeded", d1_plain_key, true, 0);
}

// ── one server: the JOIN is one remote statement ─────────────────────────────

namespace {

    complex_logical_type decimal_column(std::uint8_t width, std::uint8_t scale, const std::string& name) {
        return complex_logical_type::create_decimal(std::pmr::get_default_resource(), width, scale, name).value();
    }

    // shop.products (product_id INTEGER key, product_name STRING, price DECIMAL(10, 2)) and
    // shop.orders (order_id INTEGER key, product_id INTEGER, quantity INTEGER) on one server of
    // `kind` — MySQL / ClickHouse database `db`, PostgreSQL schema `public`.
    struct one_server_t {
        std::shared_ptr<fake_backend_t> backend;
        std::unique_ptr<engine_t> engine;
        std::string products;
        std::string orders;

        static std::vector<complex_logical_type> product_columns() {
            return {column(logical_type::INTEGER, "product_id"),
                    column(logical_type::STRING_LITERAL, "product_name"),
                    decimal_column(10, 2, "price")};
        }
        static std::vector<complex_logical_type> order_columns() {
            return {column(logical_type::INTEGER, "order_id"),
                    column(logical_type::INTEGER, "product_id"),
                    column(logical_type::INTEGER, "quantity")};
        }

        one_server_t(backend_type_t kind, const std::string& leaf)
            : backend(std::make_shared<fake_backend_t>(kind)) {
            backend->set_table("products",
                               answer(product_columns(),
                                      {{std::int32_t{1}, std::string{"apple"}, std::string{"0.10"}},
                                       {std::int32_t{2}, std::string{"pear"}, std::string{"0.20"}},
                                       {std::int32_t{3}, std::string{"plum"}, std::string{"50.00"}}}));
            backend->set_table("orders",
                               answer(order_columns(),
                                      {{std::int32_t{10}, std::int32_t{1}, std::int32_t{3}},
                                       {std::int32_t{11}, std::int32_t{1}, std::int32_t{2}},
                                       {std::int32_t{12}, std::int32_t{2}, std::int32_t{5}}}));
            install({server("shop", backend)});
            engine = open_engine(leaf);
            engine->bootstrap();
            const bool pg = kind == backend_type_t::PostgreSQL;
            const std::string database = pg ? "pgdb" : "db";
            const std::string schema = pg ? "public" : "";
            engine->upsert(kind,
                           scope("shop", database, schema),
                           tables({discovered("shop", database, schema, "products", product_columns(), {"product_id"}),
                                   discovered("shop", database, schema, "orders", order_columns(), {"order_id"})}));
            products = pg ? "shop.public.products" : "shop.db.products";
            orders = pg ? "shop.public.orders" : "shop.db.orders";
        }

        // The JOIN of the benchmark's join_same_instance shape.
        std::string join_sql(const std::string& kind_of_join = "JOIN", const std::string& tail = "") const {
            return "SELECT p.product_id, p.product_name, o.quantity FROM " + products + " p " + kind_of_join + " " +
                   orders + " o ON p.product_id = o.product_id" + tail + ";";
        }
    };

    std::vector<logical_type> types_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<logical_type> out;
        if (!cursor->chunks().empty()) {
            for (const auto& type : cursor->chunks().front().types()) {
                out.push_back(type.type());
            }
        }
        return out;
    }

    std::vector<std::string> sorted(std::vector<std::string> lines) {
        std::sort(lines.begin(), lines.end());
        return lines;
    }

    canned_table_t joined_rows() {
        return answer({column(logical_type::INTEGER, "product_id"),
                       column(logical_type::STRING_LITERAL, "product_name"),
                       column(logical_type::INTEGER, "quantity")},
                      {{std::int32_t{1}, std::string{"apple"}, std::int32_t{3}},
                       {std::int32_t{1}, std::string{"apple"}, std::int32_t{2}},
                       {std::int32_t{2}, std::string{"pear"}, std::int32_t{5}}});
    }

} // namespace

// The benchmark's join_same_instance: every table on one MySQL server, so the JOIN, its WHERE and
// its projection are one statement — each table under an alias of its own, every column qualified
// by it — and no table is scanned.
TEST_CASE("federation joins: a JOIN of two tables of one MySQL server is one remote statement") {
    one_server_t shop(backend_type_t::MySQL, "join_one_mysql");
    const std::string pushed = "SELECT `t0`.`product_id`, `t0`.`product_name`, `t1`.`quantity` FROM `db`.`products` "
                               "AS `t0` JOIN `db`.`orders` AS `t1` ON `t0`.`product_id` = `t1`.`product_id` WHERE "
                               "`t0`.`product_id` <= 67;";
    shop.backend->script(pushed, joined_rows());
    auto joined = shop.engine->sql(shop.join_sql("JOIN", " WHERE p.product_id <= 67"));
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"1", "apple", "2"}, {"1", "apple", "3"}, {"2", "pear", "5"}});
    CHECK(column_names(joined) == std::vector<std::string>{"product_id", "product_name", "quantity"});
    CHECK(types_of(joined) ==
          std::vector<logical_type>{logical_type::INTEGER, logical_type::STRING_LITERAL, logical_type::INTEGER});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// PostgreSQL: double-quoted names, an aggregate over a LEFT JOIN grouped, filtered by HAVING (an
// output expanded: PostgreSQL resolves no alias there), ordered by an output alias and windowed —
// all of it on the backend. PostgreSQL answers count(...) as int8 and sum(int4) as int8: the result
// keeps the engine's UBIGINT and BIGINT.
TEST_CASE("federation joins: an aggregate over a LEFT JOIN of one PostgreSQL server is one remote statement") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_one_pg");
    const std::string pushed =
        R"(SELECT "t0"."product_name" AS "product_name", COUNT("t1"."order_id") AS "n", SUM("t1"."quantity") AS "q" FROM "public"."products" AS "t0" LEFT JOIN "public"."orders" AS "t1" ON "t0"."product_id" = "t1"."product_id" GROUP BY "t0"."product_name" HAVING COUNT("t1"."order_id") > 0 ORDER BY "q" DESC NULLS FIRST LIMIT 10;)";
    shop.backend->script(pushed,
                         answer({column(logical_type::STRING_LITERAL, "product_name"),
                                 column(logical_type::BIGINT, "n"),
                                 column(logical_type::BIGINT, "q")},
                                {{std::string{"pear"}, std::int64_t{1}, std::int64_t{5}},
                                 {std::string{"apple"}, std::int64_t{2}, std::int64_t{5}}}));
    auto grouped = shop.engine->sql("SELECT p.product_name, count(o.order_id) AS n, sum(o.quantity) AS q FROM " +
                                    shop.products + " p LEFT JOIN " + shop.orders +
                                    " o ON p.product_id = o.product_id GROUP BY p.product_name "
                                    "HAVING count(o.order_id) > 0 ORDER BY q DESC LIMIT 10;");
    INFO(error_of(grouped));
    REQUIRE(grouped->is_success());
    CHECK(rows_of(grouped) == text_rows_t{{"pear", "1", "5"}, {"apple", "2", "5"}});
    CHECK(types_of(grouped) ==
          std::vector<logical_type>{logical_type::STRING_LITERAL, logical_type::UBIGINT, logical_type::BIGINT});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// ClickHouse: an outer JOIN pads a row without a match with the column type's default (0, '')
// unless join_use_nulls is set, so the statement sets it; a condition on the side the LEFT JOIN
// does not preserve is ClickHouse's to run in ON. The strictness is written (ALL): without one the
// server's join_default_strictness decides.
TEST_CASE("federation joins: a ClickHouse LEFT JOIN sets join_use_nulls") {
    one_server_t shop(backend_type_t::ClickHouse, "join_one_ch");
    const std::string pushed = "SELECT `t0`.`product_id`, `t0`.`product_name`, `t1`.`quantity` FROM `db`.`products` "
                               "AS `t0` ALL LEFT JOIN `db`.`orders` AS `t1` ON (`t0`.`product_id` = `t1`.`product_id` AND "
                               "`t1`.`quantity` > 2) SETTINGS join_use_nulls = 1;";
    shop.backend->script(pushed,
                         answer({column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::STRING_LITERAL, "product_name"),
                                 column(logical_type::INTEGER, "quantity")},
                                {{std::int32_t{1}, std::string{"apple"}, std::int32_t{3}},
                                 {std::int32_t{2}, std::string{"pear"}, std::int32_t{5}},
                                 {std::int32_t{3}, std::string{"plum"}, null_t{}}}));
    auto joined = shop.engine->sql(shop.join_sql("LEFT JOIN", " AND o.quantity > 2"));
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) ==
          text_rows_t{{"1", "apple", "3"}, {"2", "pear", "5"}, {"3", "plum", "NULL"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// A condition on the side a ClickHouse LEFT JOIN preserves has no ON that ClickHouse runs with the
// engine's semantics (every left row stays, padded where the condition fails): the engine runs the
// JOIN over a scan of each table.
TEST_CASE("federation joins: a ClickHouse LEFT JOIN with a condition on its preserved side stays in the engine") {
    one_server_t shop(backend_type_t::ClickHouse, "join_one_ch_preserved");
    auto joined = shop.engine->sql(shop.join_sql("LEFT JOIN", " AND p.product_id > 1"));
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) ==
          text_rows_t{{"1", "apple", "NULL"}, {"2", "pear", "5"}, {"3", "plum", "NULL"}});
    CHECK(shop.backend->scans() == 2);
}

// MySQL has no FULL JOIN: the engine runs it over a scan of each table, with SQL's answer.
TEST_CASE("federation joins: a MySQL FULL JOIN stays in the engine") {
    one_server_t shop(backend_type_t::MySQL, "join_one_mysql_full");
    auto joined = shop.engine->sql(shop.join_sql("FULL JOIN"));
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) ==
          text_rows_t{{"1", "apple", "2"}, {"1", "apple", "3"}, {"2", "pear", "5"}, {"3", "plum", "NULL"}});
    CHECK(shop.backend->scans() == 2);
}

// A derived table of the same server joined: `(SELECT ...) AS d0`, its outputs named as the engine
// names them, which the query around it reads them by.
TEST_CASE("federation joins: a derived table JOINed on one MySQL server is one remote statement") {
    one_server_t shop(backend_type_t::MySQL, "join_one_derived");
    const std::string pushed =
        "SELECT `d0`.`pid`, `d0`.`total`, `t1`.`product_name` FROM (SELECT `product_id` AS `pid`, SUM(`quantity`) AS "
        "`total` FROM `db`.`orders` GROUP BY `product_id`) AS `d0` JOIN `db`.`products` AS `t1` ON `t1`.`product_id` "
        "= `d0`.`pid` WHERE `d0`.`total` > 1;";
    shop.backend->script(pushed,
                         answer({column(logical_type::INTEGER, "pid"),
                                 column(logical_type::STRING_LITERAL, "total"), // MySQL's DECIMAL text of SUM(int)
                                 column(logical_type::STRING_LITERAL, "product_name")},
                                {{std::int32_t{1}, std::string{"5"}, std::string{"apple"}},
                                 {std::int32_t{2}, std::string{"5"}, std::string{"pear"}}}));
    auto joined = shop.engine->sql("SELECT d.pid, d.total, p.product_name FROM (SELECT product_id AS pid, "
                                   "sum(quantity) AS total FROM " +
                                   shop.orders + " GROUP BY product_id) d JOIN " + shop.products +
                                   " p ON p.product_id = d.pid WHERE d.total > 1;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"1", "5", "apple"}, {"2", "5", "pear"}});
    CHECK(types_of(joined) ==
          std::vector<logical_type>{logical_type::INTEGER, logical_type::BIGINT, logical_type::STRING_LITERAL});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// `SELECT *` over a JOIN keeps both sides' columns in the engine's layout, a name both tables have
// (product_id) once per side, as the engine answers it.
TEST_CASE("federation joins: SELECT star over a JOIN of one server keeps both key columns in order") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_one_star");
    const std::string pushed =
        R"(SELECT "t0"."product_id", "t0"."product_name", "t0"."price", "t1"."order_id", "t1"."product_id", "t1"."quantity" FROM "public"."products" AS "t0" JOIN "public"."orders" AS "t1" ON "t0"."product_id" = "t1"."product_id";)";
    shop.backend->script(pushed,
                         answer({column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::STRING_LITERAL, "product_name"),
                                 decimal_column(10, 2, "price"),
                                 column(logical_type::INTEGER, "order_id"),
                                 column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::INTEGER, "quantity")},
                                {{std::int32_t{2}, std::string{"pear"}, std::string{"0.20"}, std::int32_t{12},
                                  std::int32_t{2}, std::int32_t{5}}}));
    auto joined = shop.engine->sql("SELECT * FROM " + shop.products + " p JOIN " + shop.orders +
                                   " o ON p.product_id = o.product_id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(rows_of(joined) == text_rows_t{{"2", "pear", "0.20", "12", "2", "5"}});
    CHECK(column_names(joined) ==
          std::vector<std::string>{"product_id", "product_name", "price", "order_id", "product_id", "quantity"});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// Three tables of one server, left-deep: one statement on PostgreSQL.
TEST_CASE("federation joins: three tables of one server are one remote statement") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_one_three");
    const std::string pushed =
        R"(SELECT "t0"."product_name", "t2"."quantity" FROM "public"."products" AS "t0" JOIN "public"."orders" AS "t1" ON "t0"."product_id" = "t1"."product_id" JOIN "public"."orders" AS "t2" ON "t2"."order_id" = "t1"."order_id";)";
    shop.backend->script(pushed,
                         answer({column(logical_type::STRING_LITERAL, "product_name"),
                                 column(logical_type::INTEGER, "quantity")},
                                {{std::string{"apple"}, std::int32_t{3}}}));
    auto joined = shop.engine->sql("SELECT a.product_name, c.quantity FROM " + shop.products + " a JOIN " +
                                   shop.orders + " b ON a.product_id = b.product_id JOIN " + shop.orders +
                                   " c ON c.order_id = b.order_id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(rows_of(joined) == text_rows_t{{"apple", "3"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// What the dialect cannot say above the JOIN stays with the engine over the JOIN's own remote
// statement: MySQL has no cube root, so the JOIN alone is pushed (every column it outputs, in the
// engine's layout) and the engine computes cbrt over its rows.
TEST_CASE("federation joins: the JOIN alone is pushed when the block above it is not expressible") {
    one_server_t shop(backend_type_t::MySQL, "join_one_inner_only");
    const std::string pushed =
        "SELECT `t0`.`product_id`, `t0`.`product_name`, `t0`.`price`, `t1`.`order_id`, `t1`.`product_id`, "
        "`t1`.`quantity` FROM `db`.`products` AS `t0` JOIN `db`.`orders` AS `t1` ON `t0`.`product_id` = "
        "`t1`.`product_id`;";
    shop.backend->script(pushed,
                         answer({column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::STRING_LITERAL, "product_name"),
                                 decimal_column(10, 2, "price"),
                                 column(logical_type::INTEGER, "order_id"),
                                 column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::INTEGER, "quantity")},
                                {{std::int32_t{1}, std::string{"apple"}, std::string{"0.10"}, std::int32_t{10},
                                  std::int32_t{1}, std::int32_t{8}},
                                 {std::int32_t{2}, std::string{"pear"}, std::string{"0.20"}, std::int32_t{12},
                                  std::int32_t{2}, std::int32_t{27}}}));
    auto joined = shop.engine->sql("SELECT p.product_name, cbrt(o.quantity) AS c FROM " + shop.products + " p JOIN " +
                                   shop.orders + " o ON p.product_id = o.product_id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"apple", "2.000000"}, {"pear", "3.000000"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// ── DISTINCT ON over a JOIN (PostgreSQL) ─────────────────────────────────────

// Over a plain column list the validator numbers a DISTINCT ON key by what the FROM outputs, and
// the key is the column it names there.
TEST_CASE("federation joins: DISTINCT ON over a JOIN of plain columns names the FROM column") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_distinct_on_plain");
    const std::string expected =
        R"(SELECT DISTINCT ON ("t1"."product_name") "t1"."product_name", "t0"."quantity" FROM "public"."orders" AS "t0" JOIN "public"."products" AS "t1" ON "t0"."product_id" = "t1"."product_id";)";
    shop.backend->script(expected,
                         answer({column(logical_type::STRING_LITERAL, "product_name"),
                                 column(logical_type::INTEGER, "quantity")},
                                {{std::string{"apple"}, std::int32_t{3}}, {std::string{"pear"}, std::int32_t{5}}}));
    auto distinct = shop.engine->sql("SELECT DISTINCT ON (p.product_name) p.product_name, o.quantity FROM " +
                                     shop.orders + " o JOIN " + shop.products + " p ON o.product_id = p.product_id;");
    INFO(error_of(distinct));
    REQUIRE(distinct->is_success());
    CHECK(sorted_rows_of(distinct) == text_rows_t{{"apple", "3"}, {"pear", "5"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{expected});
}

// Over a grouped projection the validator numbers the DISTINCT ON key by the group's outputs
// (validate_logical_plan: find_types(on_key, result)), and the engine deduplicates on those outputs:
// p.product_name is output 0, while the FROM's column 0 is o.order_id. The key is written as the
// output's expression, with ORDER BY and without. (Grouped by product_name alone the DISTINCT ON
// would be redundant and the optimizer drops it: drop_redundant_distinct.)
TEST_CASE("federation joins: DISTINCT ON over a grouped JOIN names the group's output") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_distinct_on_group");
    const canned_table_t rows = answer({column(logical_type::STRING_LITERAL, "product_name"),
                                        column(logical_type::INTEGER, "quantity"),
                                        column(logical_type::BIGINT, "n")},
                                       {{std::string{"apple"}, std::int32_t{3}, std::int64_t{1}},
                                        {std::string{"pear"}, std::int32_t{5}, std::int64_t{1}}});
    const std::string body =
        R"(SELECT DISTINCT ON ("t1"."product_name") "t1"."product_name" AS "product_name", "t0"."quantity" AS "quantity", COUNT("t0"."order_id") AS "n" FROM "public"."orders" AS "t0" JOIN "public"."products" AS "t1" ON "t0"."product_id" = "t1"."product_id" GROUP BY "t1"."product_name", "t0"."quantity")";
    const std::string query =
        "SELECT DISTINCT ON (p.product_name) p.product_name, o.quantity, count(o.order_id) AS n FROM " + shop.orders +
        " o JOIN " + shop.products + " p ON o.product_id = p.product_id GROUP BY p.product_name, o.quantity";
    SECTION("without ORDER BY") {
        shop.backend->script(body + ";", rows);
        auto distinct = shop.engine->sql(query + ";");
        INFO(error_of(distinct));
        REQUIRE(distinct->is_success());
        CHECK(sorted_rows_of(distinct) == text_rows_t{{"apple", "3", "1"}, {"pear", "5", "1"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{body + ";"});
    }
    SECTION("with ORDER BY") {
        const std::string pushed = body + R"( ORDER BY "product_name" ASC NULLS LAST, "quantity" DESC NULLS FIRST;)";
        shop.backend->script(pushed, rows);
        auto distinct = shop.engine->sql(query + " ORDER BY p.product_name, o.quantity DESC;");
        INFO(error_of(distinct));
        REQUIRE(distinct->is_success());
        CHECK(rows_of(distinct) == text_rows_t{{"apple", "3", "1"}, {"pear", "5", "1"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
    }
}

// One table, grouped: the key is the output it names, written as that output's expression.
TEST_CASE("federation joins: DISTINCT ON over one grouped table names the group's output") {
    one_server_t shop(backend_type_t::PostgreSQL, "join_distinct_on_single");
    const canned_table_t rows = answer({column(logical_type::INTEGER, "product_id"),
                                        column(logical_type::INTEGER, "quantity"),
                                        column(logical_type::BIGINT, "n")},
                                       {{std::int32_t{1}, std::int32_t{3}, std::int64_t{1}},
                                        {std::int32_t{2}, std::int32_t{5}, std::int64_t{1}}});
    const std::string body =
        R"(SELECT DISTINCT ON ("product_id") "product_id", "quantity", COUNT("order_id") AS "n" FROM "public"."orders" GROUP BY "product_id", "quantity")";
    const std::string query = "SELECT DISTINCT ON (product_id) product_id, quantity, count(order_id) AS n FROM " +
                              shop.orders + " GROUP BY product_id, quantity";
    SECTION("without ORDER BY") {
        shop.backend->script(body + ";", rows);
        auto distinct = shop.engine->sql(query + ";");
        INFO(error_of(distinct));
        REQUIRE(distinct->is_success());
        CHECK(sorted_rows_of(distinct) == text_rows_t{{"1", "3", "1"}, {"2", "5", "1"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{body + ";"});
    }
    SECTION("with ORDER BY") {
        const std::string pushed = body + R"( ORDER BY "product_id" ASC NULLS LAST, "quantity" DESC NULLS FIRST;)";
        shop.backend->script(pushed, rows);
        auto distinct = shop.engine->sql(query + " ORDER BY product_id, quantity DESC;");
        INFO(error_of(distinct));
        REQUIRE(distinct->is_success());
        CHECK(rows_of(distinct) == text_rows_t{{"1", "3", "1"}, {"2", "5", "1"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
    }
}

// ── ORDER BY a name both tables have ─────────────────────────────────────────

// product_id is a column of both tables. Every output of a block over a JOIN is named (`AS`), so
// ORDER BY reads the output: a bare `product_id` would be ambiguous on MySQL (1052) and the left
// table's column on ClickHouse.
TEST_CASE("federation joins: ORDER BY an output named like a column of both tables names the output") {
    struct dialect_t {
        backend_type_t kind;
        const char* leaf;
        std::string pushed;
    };
    const std::vector<dialect_t> dialects{
        {backend_type_t::MySQL,
         "join_order_name_mysql",
         "SELECT `t1`.`product_id` AS `product_id`, `t0`.`product_name` AS `product_name`, COUNT(`t1`.`order_id`) AS `n` "
         "FROM `db`.`products` AS `t0` JOIN `db`.`orders` AS `t1` ON `t0`.`product_id` = `t1`.`product_id` GROUP BY "
         "`t1`.`product_id`, `t0`.`product_name` ORDER BY `t1`.`product_id` IS NULL DESC, `product_id` DESC LIMIT 2;"},
        {backend_type_t::PostgreSQL,
         "join_order_name_pg",
         R"(SELECT "t1"."product_id" AS "product_id", "t0"."product_name" AS "product_name", COUNT("t1"."order_id") AS "n" FROM "public"."products" AS "t0" JOIN "public"."orders" AS "t1" ON "t0"."product_id" = "t1"."product_id" GROUP BY "t1"."product_id", "t0"."product_name" ORDER BY "product_id" DESC NULLS FIRST LIMIT 2;)"},
        {backend_type_t::ClickHouse,
         "join_order_name_ch",
         "SELECT `t1`.`product_id` AS `product_id`, `t0`.`product_name` AS `product_name`, COUNT(`t1`.`order_id`) AS `n` "
         "FROM `db`.`products` AS `t0` ALL INNER JOIN `db`.`orders` AS `t1` ON `t0`.`product_id` = `t1`.`product_id` "
         "GROUP BY `t1`.`product_id`, `t0`.`product_name` ORDER BY `product_id` DESC NULLS FIRST LIMIT 2;"}};
    for (const auto& dialect : dialects) {
        CAPTURE(dialect.leaf);
        one_server_t shop(dialect.kind, dialect.leaf);
        shop.backend->script(dialect.pushed,
                             answer({column(logical_type::INTEGER, "product_id"),
                                     column(logical_type::STRING_LITERAL, "product_name"),
                                     column(logical_type::BIGINT, "n")},
                                    {{std::int32_t{2}, std::string{"pear"}, std::int64_t{1}},
                                     {std::int32_t{1}, std::string{"apple"}, std::int64_t{2}}}));
        auto ordered = shop.engine->sql("SELECT o.product_id, p.product_name, count(o.order_id) AS n FROM " +
                                        shop.products + " p JOIN " + shop.orders +
                                        " o ON p.product_id = o.product_id GROUP BY o.product_id, p.product_name "
                                        "ORDER BY product_id DESC LIMIT 2;");
        INFO(error_of(ordered));
        REQUIRE(ordered->is_success());
        CHECK(rows_of(ordered) == text_rows_t{{"2", "pear", "1"}, {"1", "apple", "2"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{dialect.pushed});
    }
}

// Two outputs of one name (product_id of each table): each takes a name of its own, the first its
// own name, the second the first free suffix.
TEST_CASE("federation joins: two outputs of one name over a JOIN take distinct names") {
    one_server_t shop(backend_type_t::MySQL, "join_output_names");
    const std::string pushed =
        "SELECT `t0`.`product_id` AS `product_id`, `t1`.`product_id` AS `product_id_1`, COUNT(`t1`.`order_id`) AS `n` "
        "FROM `db`.`products` AS `t0` JOIN `db`.`orders` AS `t1` ON `t0`.`product_id` = `t1`.`product_id` GROUP BY "
        "`t0`.`product_id`, `t1`.`product_id` ORDER BY COUNT(`t1`.`order_id`) IS NULL DESC, `n` DESC LIMIT 1;";
    shop.backend->script(pushed,
                         answer({column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::INTEGER, "product_id"),
                                 column(logical_type::BIGINT, "n")},
                                {{std::int32_t{1}, std::int32_t{1}, std::int64_t{2}}}));
    auto ordered = shop.engine->sql("SELECT p.product_id, o.product_id, count(o.order_id) AS n FROM " + shop.products +
                                    " p JOIN " + shop.orders +
                                    " o ON p.product_id = o.product_id GROUP BY p.product_id, o.product_id "
                                    "ORDER BY n DESC LIMIT 1;");
    INFO(error_of(ordered));
    REQUIRE(ordered->is_success());
    CHECK(rows_of(ordered) == text_rows_t{{"1", "1", "2"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
}

// ── ClickHouse: the declared types of every table of a pushed JOIN ───────────

namespace {

    complex_logical_type rec_type() {
        std::pmr::vector<complex_logical_type> fields{std::pmr::get_default_resource()};
        fields.push_back(column(logical_type::INTEGER, "id"));
        fields.push_back(column(logical_type::STRING_LITERAL, "label"));
        auto type = complex_logical_type::create_struct("rec", fields);
        type.set_alias("rec");
        return type;
    }

    using wire_answers_t = std::map<std::string, std::vector<clickhouse::Block>>;
    using named_types_t = std::map<std::string, std::map<std::string, std::string>>;

    // ClickHouse server `shop` with products (product_id Int32, product_name String, three rows) and
    // flags (product_id Int32, ok Bool, rec Tuple(id Int32, label String); products 1 and 2). The fake
    // answers a read of flags the way ClickhouseManager::scan does: the wire's blocks — UInt8 for
    // Bool, a Tuple without its field names — read by tsl::ch_to_chunk under the system.columns type
    // of the table column each result column reads, by position (scan_request_t::columns).
    struct clickhouse_flags_t {
        std::shared_ptr<fake_backend_t> backend = std::make_shared<fake_backend_t>(backend_type_t::ClickHouse);
        std::shared_ptr<wire_answers_t> wire = std::make_shared<wire_answers_t>();
        std::unique_ptr<engine_t> engine;

        static std::vector<complex_logical_type> product_columns() {
            return {column(logical_type::INTEGER, "product_id"), column(logical_type::STRING_LITERAL, "product_name")};
        }
        static std::vector<complex_logical_type> flag_columns() {
            return {column(logical_type::INTEGER, "product_id"), column(logical_type::BOOLEAN, "ok"), rec_type()};
        }

        explicit clickhouse_flags_t(const std::string& leaf) {
            backend->set_table("products",
                               answer(product_columns(),
                                      {{std::int32_t{1}, std::string{"apple"}},
                                       {std::int32_t{2}, std::string{"pear"}},
                                       {std::int32_t{3}, std::string{"plum"}}}));
            const named_types_t named{
                {"products", {{"product_id", "Int32"}, {"product_name", "String"}}},
                {"flags", {{"product_id", "Int32"}, {"ok", "Bool"}, {"rec", "Tuple(id Int32, label String)"}}}};
            backend->on_scan([wire = wire, named](const fed::scan_request_t& request, std::pmr::memory_resource* resource)
                                 -> std::optional<core::result_wrapper_t<components::vector::data_chunk_t>> {
                auto it = wire->find(request.sql);
                if (it == wire->end()) {
                    return std::nullopt;
                }
                std::vector<std::optional<std::string>> declared(request.columns.size());
                for (std::size_t i = 0; i < request.columns.size(); ++i) {
                    const auto& origin = request.columns[i];
                    if (!origin) {
                        continue;
                    }
                    auto table = named.find(origin->table.table);
                    if (table == named.end()) {
                        continue;
                    }
                    if (auto type = table->second.find(origin->column); type != table->second.end()) {
                        declared[i] = type->second;
                    }
                }
                return tsl::ch_to_chunk(resource, it->second, declared);
            });
            install({server("shop", backend)});
            engine = open_engine(leaf);
            engine->bootstrap();
            engine->upsert(backend_type_t::ClickHouse,
                           scope("shop", "db", ""),
                           tables({discovered("shop", "db", "", "products", product_columns()),
                                   discovered("shop", "db", "", "flags", flag_columns())}));
            (*wire)["SELECT `product_id`, `ok`, `rec` FROM `db`.`flags`"] = {
                block({{"product_id", int32s({1, 2})}, {"ok", bools({1, 0})}, {"rec", recs({{10, "a"}, {20, "b"}})}})};
        }

        static clickhouse::ColumnRef int32s(const std::vector<std::int32_t>& values) {
            auto out = std::make_shared<clickhouse::ColumnInt32>();
            for (auto v : values) {
                out->Append(v);
            }
            return out;
        }
        static clickhouse::ColumnRef bools(const std::vector<std::uint8_t>& values) {
            auto out = std::make_shared<clickhouse::ColumnUInt8>();
            for (auto v : values) {
                out->Append(v);
            }
            return out;
        }
        static clickhouse::ColumnRef strings(const std::vector<std::string>& values) {
            auto out = std::make_shared<clickhouse::ColumnString>();
            for (const auto& v : values) {
                out->Append(v);
            }
            return out;
        }
        static clickhouse::ColumnRef recs(const std::vector<std::pair<std::int32_t, std::string>>& values) {
            std::vector<std::int32_t> ids;
            std::vector<std::string> labels;
            for (const auto& [id, label] : values) {
                ids.push_back(id);
                labels.push_back(label);
            }
            return std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{int32s(ids), strings(labels)});
        }
        static clickhouse::Block block(const std::vector<std::pair<std::string, clickhouse::ColumnRef>>& columns) {
            clickhouse::Block out;
            for (const auto& [name, values] : columns) {
                out.AppendColumn(name, values);
            }
            return out;
        }
    };

    // The STRUCT fields of a result column, by name, and the row's (id, label).
    std::vector<std::string> field_names(const complex_logical_type& type) {
        std::vector<std::string> names;
        for (const auto& field : type.child_types()) {
            names.push_back(field.has_alias() ? field.alias() : std::string{});
        }
        return names;
    }

} // namespace

// The second table's Bool and named Tuple reach the engine as BOOLEAN and STRUCT(id, label): the
// statement names, per result column, the table column it reads (scan_request_t::columns), and
// the second table's system.columns types are applied to its columns — without them its Bool is
// UInt8 and its Tuple loses its field names, which conform_to_result reads as a changed schema.
TEST_CASE("federation joins: a ClickHouse JOIN reads the second table's Bool and named Tuple as declared") {
    clickhouse_flags_t shop("join_ch_named_types");
    const std::string pushed = "SELECT `t0`.`product_name`, `t1`.`ok`, `t1`.`rec` FROM `db`.`products` AS `t0` ALL "
                               "INNER JOIN `db`.`flags` AS `t1` ON `t0`.`product_id` = `t1`.`product_id`;";
    (*shop.wire)[pushed] = {clickhouse_flags_t::block({{"t0.product_name", clickhouse_flags_t::strings({"apple", "pear"})},
                                                       {"ok", clickhouse_flags_t::bools({1, 0})},
                                                       {"rec", clickhouse_flags_t::recs({{10, "a"}, {20, "b"}})}})};
    auto joined = shop.engine->sql("SELECT p.product_name, f.ok, f.rec FROM shop.db.products p JOIN shop.db.flags f "
                                   "ON p.product_id = f.product_id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
    const auto requests = shop.backend->scan_requests();
    REQUIRE(requests.size() == 1);
    REQUIRE(requests.front().columns.size() == 3);
    CHECK(requests.front().columns[0]->table.table == "products");
    CHECK(requests.front().columns[0]->column == "product_name");
    CHECK(requests.front().columns[1]->table.table == "flags");
    CHECK(requests.front().columns[1]->column == "ok");
    CHECK(requests.front().columns[2]->table.table == "flags");
    CHECK(requests.front().columns[2]->column == "rec");
    REQUIRE(joined->chunks().size() == 1);
    const auto& chunk = joined->chunks().front();
    REQUIRE(chunk.size() == 2);
    CHECK(chunk.types()[1].type() == logical_type::BOOLEAN);
    REQUIRE(chunk.types()[2].type() == logical_type::STRUCT);
    CHECK(field_names(chunk.types()[2]) == std::vector<std::string>{"id", "label"});
    CHECK(cell(chunk, 0, 0) == "apple");
    CHECK(cell(chunk, 1, 0) == "true");
    CHECK(cell(chunk, 1, 1) == "false");
    const auto rec = chunk.value(2, 1);
    REQUIRE(rec.children().size() == 2);
    CHECK(rec.children()[0].value<std::int32_t>() == 20);
    CHECK(rec.children()[1].value<std::string_view>() == "b");
}

// A LEFT JOIN pads plum's flags: join_use_nulls makes the padded Bool a NULL, so a block reading it
// is one statement; a Tuple cannot be Nullable and would come back as an empty tuple, so a block
// reading the padded Tuple stays with the engine, which JOINs a scan of each table.
TEST_CASE("federation joins: a ClickHouse outer JOIN reading a padded Tuple stays in the engine") {
    clickhouse_flags_t shop("join_ch_padded_tuple");
    const std::string pushed = "SELECT `t0`.`product_name`, `t1`.`ok` FROM `db`.`products` AS `t0` ALL LEFT JOIN "
                               "`db`.`flags` AS `t1` ON `t0`.`product_id` = `t1`.`product_id` SETTINGS "
                               "join_use_nulls = 1;";
    {
        auto nullable_flags = std::make_shared<clickhouse::ColumnNullable>(
            clickhouse_flags_t::bools({1, 0, 0}),
            clickhouse_flags_t::bools({0, 0, 1}));
        (*shop.wire)[pushed] = {clickhouse_flags_t::block(
            {{"product_name", clickhouse_flags_t::strings({"apple", "pear", "plum"})}, {"ok", nullable_flags}})};
    }
    SECTION("the padded Bool: one statement") {
        auto joined = shop.engine->sql("SELECT p.product_name, f.ok FROM shop.db.products p LEFT JOIN "
                                       "shop.db.flags f ON p.product_id = f.product_id;");
        INFO(error_of(joined));
        REQUIRE(joined->is_success());
        CHECK(sorted_rows_of(joined) == text_rows_t{{"apple", "true"}, {"pear", "false"}, {"plum", "NULL"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{pushed});
    }
    SECTION("the padded Tuple: a scan of each table") {
        auto joined = shop.engine->sql("SELECT p.product_name, f.rec FROM shop.db.products p LEFT JOIN "
                                       "shop.db.flags f ON p.product_id = f.product_id;");
        INFO(error_of(joined));
        REQUIRE(joined->is_success());
        CHECK(sorted(shop.backend->sql_log()) ==
              std::vector<std::string>{"SELECT `product_id`, `ok`, `rec` FROM `db`.`flags`",
                                       "SELECT `product_id`, `product_name` FROM `db`.`products`"});
        std::size_t rows = 0;
        std::size_t padded = 0;
        for (const auto& chunk : joined->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                ++rows;
                if (chunk.value(1, row).is_null()) {
                    CHECK(cell(chunk, 0, row) == "plum");
                    ++padded;
                }
            }
        }
        CHECK(rows == 3);
        CHECK(padded == 1);
    }
}
