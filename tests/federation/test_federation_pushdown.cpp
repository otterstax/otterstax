// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The pushdown rule (otterbrix/federation/rules/pushdown.hpp) per dialect over a real engine: the
// statement each backend is sent, the answer held to the engine's types, what stays with the engine
// (a JOIN over two scans, remote with local and remote with remote), and the windows a wide pushed
// answer is handed out in.

#include "federation_fixture.hpp"

using namespace fedtest;

namespace {

    std::vector<logical_type> types_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<logical_type> out;
        if (!cursor->chunks().empty()) {
            for (const auto& type : cursor->chunks().front().types()) {
                out.push_back(type.type());
            }
        }
        return out;
    }

    canned_table_t answer(std::vector<complex_logical_type> columns, std::vector<row_t> rows) {
        return canned_table_t{std::move(columns), std::move(rows), std::nullopt, {}};
    }

    std::vector<std::string> sorted(std::vector<std::string> lines) {
        std::sort(lines.begin(), lines.end());
        return lines;
    }

} // namespace

TEST_CASE("federation pushdown: PostgreSQL statements name schema and table in double quotes") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "Id"),
                                                    column(logical_type::STRING_LITERAL, "Label")};
    backend->set_table("Items",
                       answer(columns, {{std::int64_t{1}, std::string{"x"}}, {std::int64_t{2}, std::string{"y"}}}));
    install({server("pg", backend)});
    auto engine = open_engine("pushdown_pg");
    engine->bootstrap();
    engine->upsert(backend_type_t::PostgreSQL,
                   scope("pg", "shopdb", "Shop"),
                   tables({discovered("pg", "shopdb", "Shop", "Items", columns, {"Id"})}));

    const std::string filter_sql = R"(SELECT "Label" FROM "Shop"."Items" WHERE "Id" = 2;)";
    backend->script(filter_sql, answer({column(logical_type::STRING_LITERAL, "Label")}, {{std::string{"y"}}}));
    auto filtered = engine->sql("SELECT label FROM pg.shop.items WHERE id = 2;");
    INFO(error_of(filtered));
    REQUIRE(filtered->is_success());
    CHECK(rows_of(filtered) == text_rows_t{{"y"}});
    CHECK(backend->sql_log().back() == filter_sql);

    // PostgreSQL answers AVG of int8 as numeric (DOUBLE through the translator) and COUNT as int8:
    // the result is the engine's DOUBLE and UBIGINT.
    const std::string avg_sql = R"(SELECT COUNT(*) AS "n", AVG("Id") AS "a" FROM "Shop"."Items";)";
    backend->script(avg_sql,
                    answer({column(logical_type::BIGINT, "n"), column(logical_type::DOUBLE, "a")},
                           {{std::int64_t{2}, 1.5}}));
    auto averaged = engine->sql("SELECT count(*) AS n, avg(id) AS a FROM pg.anydb.shop.items;");
    INFO(error_of(averaged));
    REQUIRE(averaged->is_success());
    CHECK(rows_of(averaged) == text_rows_t{{"2", "1.500000"}});
    CHECK(types_of(averaged) == std::vector<logical_type>{logical_type::UBIGINT, logical_type::DOUBLE});
    CHECK(backend->sql_log().back() == avg_sql);

    // ORDER BY spells out where NULLs go, as the engine places them by default.
    const std::string sorted_sql =
        R"(SELECT "Label" FROM "Shop"."Items" ORDER BY "Label" DESC NULLS FIRST, "Id" ASC NULLS LAST;)";
    backend->script(sorted_sql,
                    answer({column(logical_type::STRING_LITERAL, "Label")}, {{std::string{"y"}}, {std::string{"x"}}}));
    auto sorted = engine->sql("SELECT label FROM pg.shop.items ORDER BY label DESC, id;");
    INFO(error_of(sorted));
    REQUIRE(sorted->is_success());
    CHECK(rows_of(sorted) == text_rows_t{{"y"}, {"x"}});
    CHECK(backend->sql_log().back() == sorted_sql);

    // A row change picked by the engine is named on PostgreSQL by its primary key, one row per
    // statement.
    REQUIRE(engine->sql("CREATE DATABASE loc;")->is_success());
    REQUIRE(engine->sql("CREATE TABLE loc.ids (id BIGINT);")->is_success());
    REQUIRE(engine->sql("INSERT INTO loc.ids (id) VALUES (2);")->is_success());
    auto deleted = engine->sql("DELETE FROM pg.shop.items WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 1);
    CHECK(backend->write_log() == std::vector<std::string>{R"(DELETE FROM "Shop"."Items" WHERE "Id" = 2)"});
    CHECK(backend->rows("Items") == std::vector<row_t>{{std::int64_t{1}, std::string{"x"}}});
}

TEST_CASE("federation pushdown: MySQL statements quote backticks and spell the MySQL dialect") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "Id"),
                                                    column(logical_type::BIGINT, "Qty"),
                                                    column(logical_type::STRING_LITERAL, "Name")};
    backend->set_table("Goods",
                       answer(columns,
                              {{std::int64_t{1}, std::int64_t{7}, std::string{"a\\b"}},
                               {std::int64_t{2}, std::int64_t{-7}, std::string{"it's"}}}));
    install({server("my", backend)});
    auto engine = open_engine("pushdown_mysql");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("my", "Shop", ""),
                   tables({discovered("my", "Shop", "", "Goods", columns, {"Id"})}));

    // A string literal doubles the quote and the backslash (MySQL reads a backslash as an escape).
    const std::string filter_sql = R"(SELECT `Id` FROM `Shop`.`Goods` WHERE (`Name` = 'a\\b' OR `Name` = 'it''s');)";
    backend->script(filter_sql, answer({column(logical_type::BIGINT, "Id")}, {{std::int64_t{1}}, {std::int64_t{2}}}));
    auto filtered = engine->sql(R"(SELECT id FROM my.shop.goods WHERE name = 'a\b' OR name = 'it''s';)");
    INFO(error_of(filtered));
    REQUIRE(filtered->is_success());
    CHECK(sorted_rows_of(filtered) == text_rows_t{{"1"}, {"2"}});
    CHECK(backend->sql_log().back() == filter_sql);

    // Integer division is MySQL's DIV (truncating toward zero, as the engine's `/` of two integers).
    const std::string div_sql = "SELECT (`Qty` DIV 2) AS `half` FROM `Shop`.`Goods`;";
    backend->script(div_sql, answer({column(logical_type::BIGINT, "half")}, {{std::int64_t{3}}, {std::int64_t{-3}}}));
    auto halved = engine->sql("SELECT qty / 2 AS half FROM my.shop.goods;");
    INFO(error_of(halved));
    REQUIRE(halved->is_success());
    CHECK(sorted_rows_of(halved) == text_rows_t{{"-3"}, {"3"}});
    CHECK(backend->sql_log().back() == div_sql);

    // MySQL has no NULLS FIRST / LAST: an IS NULL key places the NULLs where the engine does.
    const std::string sorted_sql =
        "SELECT `Name` FROM `Shop`.`Goods` ORDER BY `Name` IS NULL ASC, `Name` ASC, `Qty` IS NULL DESC, `Qty` DESC;";
    backend->script(sorted_sql,
                    answer({column(logical_type::STRING_LITERAL, "Name")},
                           {{std::string{"a\\b"}}, {std::string{"it's"}}}));
    auto sorted = engine->sql("SELECT name FROM my.shop.goods ORDER BY name, qty DESC;");
    INFO(error_of(sorted));
    REQUIRE(sorted->is_success());
    CHECK(rows_of(sorted) == text_rows_t{{"a\\b"}, {"it's"}});
    CHECK(backend->sql_log().back() == sorted_sql);

    // LIMIT with OFFSET and a pushed UPDATE whose count is the backend's.
    const std::string window_sql = "SELECT `Id` FROM `Shop`.`Goods` ORDER BY `Id` IS NULL ASC, `Id` ASC LIMIT 1 OFFSET 1;";
    backend->script(window_sql, answer({column(logical_type::BIGINT, "Id")}, {{std::int64_t{2}}}));
    auto window = engine->sql("SELECT id FROM my.shop.goods ORDER BY id LIMIT 1 OFFSET 1;");
    INFO(error_of(window));
    REQUIRE(window->is_success());
    CHECK(rows_of(window) == text_rows_t{{"2"}});
    CHECK(backend->sql_log().back() == window_sql);
    auto updated = engine->sql("UPDATE my.shop.goods SET qty = 0 WHERE id = 2;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    CHECK(backend->write_log().back() == "UPDATE `Shop`.`Goods` SET `Qty` = 0 WHERE `Id` = 2;");

    // UPDATE ... FROM the target itself is no statement either dialect accepts: the engine picks
    // the rows and the storage changes them by key.
    backend->clear_logs();
    auto self = engine->sql("UPDATE my.shop.goods SET qty = 5 FROM my.shop.goods AS g WHERE g.id = 1 AND goods.id = g.id;");
    INFO(error_of(self));
    REQUIRE(self->is_success());
    CHECK(self->affected_rows() == 1);
    CHECK(backend->write_log() == std::vector<std::string>{"UPDATE `Shop`.`Goods` SET `Qty` = 5 WHERE `Id` = 1"});
}

TEST_CASE("federation pushdown: an INTEGER column set to an integer in its range is one MySQL statement") {
    // The validator reads `SET qty = 6` over an INTEGER column as CAST($1 AS INTEGER) of a BIGINT
    // value — a narrowing cast MySQL's CAST cannot spell. The value lies in INTEGER's range, so the
    // cast leaves it unchanged and the statement is pushed whole: a table without a primary key
    // needs none. A value out of the range is the engine's to refuse; nothing reaches the backend.
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::INTEGER, "Id"),
                                                    column(logical_type::INTEGER, "Qty")};
    backend->set_table("Goods",
                       answer(columns,
                              {{std::int32_t{1}, std::int32_t{5}},
                               {std::int32_t{1}, std::int32_t{5}},
                               {std::int32_t{2}, std::int32_t{5}}}));
    install({server("my", backend)});
    auto engine = open_engine("pushdown_mysql_int_set");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("my", "Shop", ""),
                   tables({discovered("my", "Shop", "", "Goods", columns)}));

    auto updated = engine->sql("UPDATE my.shop.goods SET qty = 6 WHERE id = 2;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    CHECK(backend->write_log() == std::vector<std::string>{"UPDATE `Shop`.`Goods` SET `Qty` = CAST(6 AS SIGNED) WHERE `Id` = 2;"});
    CHECK(backend->rows("Goods") == std::vector<row_t>{{std::int32_t{1}, std::int32_t{5}},
                                                       {std::int32_t{1}, std::int32_t{5}},
                                                       {std::int32_t{2}, std::int32_t{6}}});

    backend->clear_logs();
    auto too_big = engine->sql("UPDATE my.shop.goods SET qty = 5000000000 WHERE id = 2;");
    REQUIRE(too_big->is_error());
    CHECK(backend->write_log().empty());
}

TEST_CASE("federation pushdown: ClickHouse statements quote backticks and backslashes") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::ClickHouse);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                                    column(logical_type::STRING_LITERAL, "a\\b`c")};
    backend->set_table("events", answer(columns, {{std::int64_t{1}, std::string{"v"}}}));
    install({server("ch", backend)});
    auto engine = open_engine("pushdown_ch");
    engine->bootstrap();
    engine->upsert(backend_type_t::ClickHouse,
                   scope("ch", "ev", ""),
                   tables({discovered("ch", "ev", "", "events", columns)}));

    const std::string sql = "SELECT `a\\\\b``c` FROM `ev`.`events` WHERE `id` > 0;";
    backend->script(sql, answer({column(logical_type::STRING_LITERAL, "a\\b`c")}, {{std::string{"v"}}}));
    auto read = engine->sql("SELECT \"a\\b`c\" FROM ch.ev.events WHERE id > 0;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(rows_of(read) == text_rows_t{{"v"}});
    CHECK(backend->sql_log().back() == sql);

    // ClickHouse answers count() as UInt64 and sum of Int64 as Int64: already the engine's types.
    const std::string count_sql = "SELECT COUNT(*) AS `n`, SUM(`id`) AS `s` FROM `ev`.`events`;";
    backend->script(count_sql,
                    answer({column(logical_type::BIGINT, "n"), column(logical_type::BIGINT, "s")},
                           {{std::int64_t{1}, std::int64_t{1}}}));
    auto counted = engine->sql("SELECT count(*) AS n, sum(id) AS s FROM ch.ev.events;");
    INFO(error_of(counted));
    REQUIRE(counted->is_success());
    CHECK(rows_of(counted) == text_rows_t{{"1", "1"}});
    CHECK(backend->sql_log().back() == count_sql);

    // Integer division is intDiv; ORDER BY names where NULLs go (ClickHouse puts them last both ways).
    const std::string div_sql =
        "SELECT intDiv(`id`, 2) AS `h` FROM `ev`.`events` ORDER BY `id` DESC NULLS FIRST;";
    backend->script(div_sql, answer({column(logical_type::BIGINT, "h")}, {{std::int64_t{0}}}));
    auto halved = engine->sql("SELECT id / 2 AS h FROM ch.ev.events ORDER BY id DESC;");
    INFO(error_of(halved));
    REQUIRE(halved->is_success());
    CHECK(rows_of(halved) == text_rows_t{{"0"}});
    CHECK(backend->sql_log().back() == div_sql);

    // DELETE is the lightweight delete; a predicate-less one gets WHERE 1.
    auto deleted = engine->sql("DELETE FROM ch.ev.events;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(backend->write_log() == std::vector<std::string>{"DELETE FROM `ev`.`events` WHERE 1;"});
}

TEST_CASE("federation pushdown: a JOIN of two servers runs in the engine over one scan of each") {
    auto sales = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    auto pg = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
    const std::vector<complex_logical_type> orders{column(logical_type::BIGINT, "id"),
                                                   column(logical_type::BIGINT, "customer")};
    const std::vector<complex_logical_type> customers{column(logical_type::BIGINT, "id"),
                                                      column(logical_type::STRING_LITERAL, "name")};
    sales->set_table("orders",
                     answer(orders,
                            {{std::int64_t{10}, std::int64_t{1}},
                             {std::int64_t{11}, std::int64_t{2}},
                             {std::int64_t{12}, std::int64_t{1}}}));
    pg->set_table("customers", answer(customers, {{std::int64_t{1}, std::string{"ann"}}, {std::int64_t{2}, std::string{"bob"}}}));
    install({server("sales", sales), server("pg", pg)});
    auto engine = open_engine("pushdown_two_servers");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "orders", orders)}));
    engine->upsert(backend_type_t::PostgreSQL,
                   scope("pg", "shopdb", "public"),
                   tables({discovered("pg", "shopdb", "public", "customers", customers)}));

    auto joined = engine->sql("SELECT o.id, c.name FROM sales.bill.orders AS o "
                              "JOIN pg.shopdb.public.customers AS c ON o.customer = c.id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"10", "ann"}, {"11", "bob"}, {"12", "ann"}});
    CHECK(sales->sql_log() == std::vector<std::string>{"SELECT `id`, `customer` FROM `bill`.`orders`"});
    CHECK(pg->sql_log() == std::vector<std::string>{R"(SELECT "id", "name" FROM "public"."customers")"});

    // The aggregate over the join is the engine's too.
    auto counted = engine->sql("SELECT c.name, count(*) AS n FROM sales.bill.orders AS o "
                               "JOIN pg.shopdb.public.customers AS c ON o.customer = c.id GROUP BY c.name;");
    INFO(error_of(counted));
    REQUIRE(counted->is_success());
    CHECK(sorted_rows_of(counted) == text_rows_t{{"ann", "2"}, {"bob", "1"}});
}

TEST_CASE("federation pushdown: a wide pushed answer is handed out in windows within the bound") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "n")};
    canned_table_t wide = answer(columns, {});
    for (std::int64_t i = 0; i < 2500; ++i) {
        wide.rows.push_back({i});
    }
    backend->set_table("numbers", answer(columns, {}));
    backend->script("SELECT `n` FROM `bill`.`numbers` WHERE `n` >= 0;", wide);
    install({server("sales", backend)});
    auto engine = open_engine("pushdown_windows");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "numbers", columns)}));

    auto all = engine->sql("SELECT n FROM sales.bill.numbers WHERE n >= 0;");
    INFO(error_of(all));
    REQUIRE(all->is_success());
    CHECK(all->size() == 2500);
    std::int64_t expected = 0;
    bool ordered = true;
    for (const auto& chunk : all->chunks()) {
        CHECK(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            ordered = ordered && chunk.value(0, row).value<std::int64_t>() == expected++;
        }
    }
    CHECK(ordered);
}

TEST_CASE("federation pushdown: an answer value with no conversion to the result type names the statement") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "n")};
    backend->set_table("numbers", answer(columns, {}));
    const std::string sql = "SELECT SUM(`n`) AS `s` FROM `bill`.`numbers`;";
    backend->script(sql, answer({column(logical_type::STRING_LITERAL, "s")}, {{std::string{"not a number"}}}));
    install({server("sales", backend)});
    auto engine = open_engine("pushdown_conversion");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "numbers", columns)}));

    auto summed = engine->sql("SELECT sum(n) AS s FROM sales.bill.numbers;");
    REQUIRE(summed->is_error());
    CHECK(summed->get_error().type == core::error_code_t::schema_error);
    CHECK(error_of(summed).find(sql) != std::string::npos);
}

TEST_CASE("federation pushdown: a value the dialect has no literal for leaves the block to the engine") {
    // The rule decides with every parameter written as `?`; MySQL has no literal for an infinite
    // DOUBLE nor for an INTERVAL. The operator, which has the values, plans the block as the engine
    // would have: over the storage's scan (and its row-change sink for a DML).
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                                    column(logical_type::DOUBLE, "price")};
    backend->set_table("goods", answer(columns, {{std::int64_t{1}, 1.5}, {std::int64_t{2}, 2.5}}));
    install({server("sales", backend)});
    auto engine = open_engine("pushdown_no_literal");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "goods", columns, {"id"})}));
    const std::vector<std::string> scan{"SELECT `id`, `price` FROM `bill`.`goods`"};

    auto below = engine->sql("SELECT id FROM sales.bill.goods WHERE price < CAST('Infinity' AS DOUBLE);");
    INFO(error_of(below));
    REQUIRE(below->is_success());
    CHECK(sorted_rows_of(below) == text_rows_t{{"1"}, {"2"}});
    CHECK(backend->sql_log() == scan);

    backend->clear_logs();
    auto interval = engine->sql("SELECT id, INTERVAL '1 day' AS i FROM sales.bill.goods WHERE id = 1;");
    INFO(error_of(interval));
    REQUIRE(interval->is_success());
    CHECK(rows_of(interval).size() == 1);
    CHECK(backend->sql_log() == scan);

    backend->clear_logs();
    auto deleted = engine->sql("DELETE FROM sales.bill.goods WHERE price < CAST('Infinity' AS DOUBLE);");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 2);
    CHECK(backend->sql_log() == scan);
    CHECK(sorted(backend->write_log()) == std::vector<std::string>{"DELETE FROM `bill`.`goods` WHERE `id` = 1",
                                                                  "DELETE FROM `bill`.`goods` WHERE `id` = 2"});
}

TEST_CASE("federation pushdown: a user's numeric CAST reaches each dialect and the answer is the engine's number") {
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                                    column(logical_type::INTEGER, "a"),
                                                    column(logical_type::INTEGER, "b"),
                                                    column(logical_type::DOUBLE, "price")};
    const std::vector<row_t> rows{{std::int64_t{1}, std::int32_t{5}, std::int32_t{2}, 0.5},
                                  {std::int64_t{2}, std::int32_t{7}, std::int32_t{2}, 1.4},
                                  {std::int64_t{3}, std::int32_t{9}, std::int32_t{2}, 1.5},
                                  {std::int64_t{4}, std::int32_t{1}, std::int32_t{2}, 2.5},
                                  {std::int64_t{5}, std::int32_t{3}, std::int32_t{2}, 0.9}};
    auto pg = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
    auto my = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    auto ch = std::make_shared<fake_backend_t>(backend_type_t::ClickHouse);
    for (const auto& backend : {pg, my, ch}) {
        backend->set_table("nums", answer(columns, rows));
    }
    install({server("pg", pg), server("my", my), server("ch", ch)});
    auto engine = open_engine("pushdown_casts");
    engine->bootstrap();
    engine->upsert(backend_type_t::PostgreSQL,
                   scope("pg", "db", "shop"),
                   tables({discovered("pg", "db", "shop", "nums", columns, {"id"})}));
    engine->upsert(backend_type_t::MySQL,
                   scope("my", "shop", ""),
                   tables({discovered("my", "shop", "", "nums", columns, {"id"})}));
    engine->upsert(backend_type_t::ClickHouse,
                   scope("ch", "shop", ""),
                   tables({discovered("ch", "shop", "", "nums", columns)}));
    const auto quotient = answer({column(logical_type::DOUBLE, "q")}, {{2.5}});

    SECTION("CAST(a AS DOUBLE) / CAST(b AS DOUBLE) divides as DOUBLE on every dialect") {
        const std::vector<std::pair<std::shared_ptr<fake_backend_t>, std::string>> sent{
            {pg, R"(SELECT (CAST("a" AS float8) / CAST("b" AS float8)) AS "q" FROM "shop"."nums" WHERE "id" = 1;)"},
            {my, "SELECT (CAST(`a` AS DOUBLE) / CAST(`b` AS DOUBLE)) AS `q` FROM `shop`.`nums` WHERE `id` = 1;"},
            {ch, "SELECT (CAST(`a` AS Float64) / CAST(`b` AS Float64)) AS `q` FROM `shop`.`nums` WHERE `id` = 1;"}};
        for (const auto& [backend, sql] : sent) {
            // The answer is scripted for exactly the statement that divides as DOUBLE: an integer
            // division sent instead would be answered with the table's own columns and fail.
            backend->script(sql, quotient);
        }
        for (const char* uid : {"pg", "my", "ch"}) {
            CAPTURE(uid);
            auto divided = engine->sql(std::string{"SELECT CAST(a AS DOUBLE) / CAST(b AS DOUBLE) AS q FROM "} + uid +
                                       ".shop.nums WHERE id = 1;");
            INFO(error_of(divided));
            REQUIRE(divided->is_success());
            CHECK(rows_of(divided) == text_rows_t{{"2.500000"}});
        }
        CHECK(pg->sql_log().back() == sent[0].second);
        CHECK(my->sql_log().back() == sent[1].second);
        CHECK(ch->sql_log().back() == sent[2].second);
    }

    SECTION("in the engine the same expression is 2.5 too, over the storage's scan") {
        REQUIRE(engine->sql("CREATE DATABASE loc;")->is_success());
        REQUIRE(engine->sql("CREATE TABLE loc.ids (id BIGINT);")->is_success());
        REQUIRE(engine->sql("INSERT INTO loc.ids (id) VALUES (1);")->is_success());
        auto joined = engine->sql("SELECT CAST(n.a AS DOUBLE) / CAST(n.b AS DOUBLE) AS q FROM my.shop.nums AS n "
                                  "JOIN loc.ids AS i ON n.id = i.id;");
        INFO(error_of(joined));
        REQUIRE(joined->is_success());
        CHECK(rows_of(joined) == text_rows_t{{"2.500000"}});
    }

    SECTION("WHERE CAST(price AS INTEGER) = 1 narrows on PostgreSQL and stays in the engine elsewhere") {
        const std::string pg_sql = R"(SELECT "id" FROM "shop"."nums" WHERE CAST("price" AS int4) = 1;)";
        pg->script(pg_sql, answer({column(logical_type::BIGINT, "id")}, {{std::int64_t{2}}, {std::int64_t{5}}}));
        auto on_pg = engine->sql("SELECT id FROM pg.shop.nums WHERE CAST(price AS INTEGER) = 1;");
        INFO(error_of(on_pg));
        REQUIRE(on_pg->is_success());
        CHECK(sorted_rows_of(on_pg) == text_rows_t{{"2"}, {"5"}});
        CHECK(pg->sql_log().back() == pg_sql);

        // MySQL rounds a tie away from zero and ClickHouse truncates: the engine rounds to even over
        // the scan — 0.5 -> 0, 1.4 -> 1, 1.5 -> 2, 2.5 -> 2, 0.9 -> 1 — never `price = 1`.
        for (const auto& [uid, backend] : {std::pair{"my", my}, std::pair{"ch", ch}}) {
            CAPTURE(uid);
            backend->clear_logs();
            auto narrowed = engine->sql(std::string{"SELECT id FROM "} + uid +
                                        ".shop.nums WHERE CAST(price AS INTEGER) = 1;");
            INFO(error_of(narrowed));
            REQUIRE(narrowed->is_success());
            CHECK(sorted_rows_of(narrowed) == text_rows_t{{"2"}, {"5"}});
            REQUIRE(backend->sql_log().size() == 1);
            CHECK(backend->sql_log().front().find("CAST") == std::string::npos);
        }
    }

    SECTION("a comparison over an INTEGER column and a BIGINT literal keeps the client's shape") {
        const std::string sql = R"(SELECT "id" FROM "shop"."nums" WHERE "a" = 7;)";
        pg->script(sql, answer({column(logical_type::BIGINT, "id")}, {{std::int64_t{2}}}));
        auto compared = engine->sql("SELECT id FROM pg.shop.nums WHERE a = 7;");
        INFO(error_of(compared));
        REQUIRE(compared->is_success());
        CHECK(rows_of(compared) == text_rows_t{{"2"}});
        CHECK(pg->sql_log().back() == sql);
    }
}

// ── DECIMAL ──────────────────────────────────────────────────────────────────

namespace {

    complex_logical_type decimal_column(std::uint8_t width, std::uint8_t scale, const std::string& name) {
        return complex_logical_type::create_decimal(std::pmr::get_default_resource(), width, scale, name).value();
    }

    // goods (id BIGINT, price DECIMAL(width, scale)) on a server of `kind` — MySQL / ClickHouse
    // database `db` (shop.db.goods), PostgreSQL schema `public` (shop.public.goods).
    struct priced_t {
        std::shared_ptr<fake_backend_t> backend;
        std::unique_ptr<engine_t> engine;
        std::string goods;

        priced_t(backend_type_t kind,
                 const std::string& leaf,
                 std::vector<row_t> rows = {{std::int64_t{1}, std::string{"0.10"}},
                                            {std::int64_t{2}, std::string{"0.20"}},
                                            {std::int64_t{3}, std::string{"0.20"}}},
                 std::uint8_t width = 10,
                 std::uint8_t scale = 2)
            : backend(std::make_shared<fake_backend_t>(kind)) {
            const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                                            decimal_column(width, scale, "price")};
            backend->set_table("goods", answer(columns, std::move(rows)));
            install({server("shop", backend)});
            engine = open_engine(leaf);
            engine->bootstrap();
            const bool pg = kind == backend_type_t::PostgreSQL;
            const std::string database = pg ? "pgdb" : "db";
            const std::string schema = pg ? "public" : "";
            engine->upsert(kind,
                           scope("shop", database, schema),
                           tables({discovered("shop", database, schema, "goods", columns, {"id"})}));
            goods = pg ? "shop.public.goods" : "shop.db.goods";
        }
    };

} // namespace

// `price > 50` over a DECIMAL(10, 2) column: the validator casts the BIGINT 50 into the column's
// DECIMAL, a cast that leaves 50 as it is, so it is the lossless CAST every dialect does exactly and
// the filter reaches the backend; the answer keeps the column's DECIMAL(10, 2).
TEST_CASE("federation pushdown: a comparison with a DECIMAL column is pushed with an exact DECIMAL literal") {
    priced_t shop(backend_type_t::MySQL, "pushdown_decimal_filter");
    const std::string pushed = "SELECT `id`, `price` FROM `db`.`goods` WHERE `price` > CAST(50 AS DECIMAL(10, 2));";
    shop.backend->script(pushed,
                         answer({column(logical_type::BIGINT, "id"), decimal_column(10, 2, "price")},
                                {{std::int64_t{7}, std::string{"50.01"}}}));
    auto filtered = shop.engine->sql("SELECT id, price FROM shop.db.goods WHERE price > 50;");
    INFO(error_of(filtered));
    REQUIRE(filtered->is_success());
    CHECK(rows_of(filtered) == text_rows_t{{"7", "50.01"}});
    CHECK(types_of(filtered) == std::vector<logical_type>{logical_type::BIGINT, logical_type::DECIMAL});
    CHECK(shop.backend->sql_log().back() == pushed);
}

// SUM over DECIMAL(10, 2): MySQL answers DECIMAL(32, 2), the engine's type is DECIMAL(38, 2) — the
// cents are kept. AVG: MySQL's own AVG keeps only s + 4 fraction digits, so the mean is written over
// a sum widened to DECIMAL(65, 30), whose text (wider than 38 digits) is read into the engine's
// DECIMAL(10, 2), rounded half away from zero: 0.5 / 3 is 0.17.
TEST_CASE("federation pushdown: SUM and AVG of a DECIMAL column keep every cent on MySQL") {
    priced_t shop(backend_type_t::MySQL, "pushdown_decimal_sum");
    const std::string pushed = "SELECT SUM(`price`) AS `s`, (CAST(SUM(`price`) AS DECIMAL(65, 30)) / COUNT(`price`)) "
                               "AS `a` FROM `db`.`goods`;";
    shop.backend->script(pushed,
                         answer({decimal_column(32, 2, "s"), column(logical_type::STRING_LITERAL, "a")},
                                {{std::string{"0.50"}, std::string{"0.166666666666666666666666666667"}}}));
    auto summed = shop.engine->sql("SELECT sum(price) AS s, avg(price) AS a FROM shop.db.goods;");
    INFO(error_of(summed));
    REQUIRE(summed->is_success());
    CHECK(rows_of(summed) == text_rows_t{{"0.50", "0.17"}});
    CHECK(types_of(summed) == std::vector<logical_type>{logical_type::DECIMAL, logical_type::DECIMAL});
    CHECK(shop.backend->sql_log().back() == pushed);
}

// ClickHouse's avg of a Decimal is a Float64: the mean stays with the engine, over the scan, which
// answers the exact DECIMAL mean (0.5 / 3 = 0.17).
TEST_CASE("federation pushdown: AVG of a DECIMAL column stays in the engine on ClickHouse") {
    priced_t shop(backend_type_t::ClickHouse, "pushdown_decimal_avg_ch");
    auto averaged = shop.engine->sql("SELECT avg(price) AS a FROM shop.db.goods;");
    INFO(error_of(averaged));
    REQUIRE(averaged->is_success());
    CHECK(rows_of(averaged) == text_rows_t{{"0.17"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{"SELECT `id`, `price` FROM `db`.`goods`"});
}

// MySQL / MariaDB refuse an output alias of an aggregate inside an ORDER BY expression ("Reference
// 'total' not supported (reference to group function)"): the IS NULL key that places the NULLs
// spells the aggregate, the key itself keeps the alias.
TEST_CASE("federation pushdown: MySQL orders by an aggregate alias with the aggregate in its IS NULL key") {
    priced_t shop(backend_type_t::MySQL, "pushdown_mysql_order_aggregate");
    const std::string pushed = "SELECT `id`, SUM(`price`) AS `total` FROM `db`.`goods` GROUP BY `id` ORDER BY "
                               "SUM(`price`) IS NULL DESC, `total` DESC LIMIT 2;";
    shop.backend->script(pushed,
                         answer({column(logical_type::BIGINT, "id"), decimal_column(32, 2, "total")},
                                {{std::int64_t{2}, std::string{"0.20"}}, {std::int64_t{3}, std::string{"0.20"}}}));
    auto ordered = shop.engine->sql("SELECT id, sum(price) AS total FROM shop.db.goods GROUP BY id "
                                    "ORDER BY total DESC LIMIT 2;");
    INFO(error_of(ordered));
    REQUIRE(ordered->is_success());
    CHECK(rows_of(ordered) == text_rows_t{{"2", "0.20"}, {"3", "0.20"}});
    CHECK(shop.backend->sql_log().back() == pushed);
}

// AVG over DECIMAL(10, 2) is the engine's exact mean rounded once, half away from zero. A backend
// division rounds its quotient to r fraction digits and the answer is rounded again to the scale;
// the two agree with one rounding for any count n < 10^(r - s), so the sum is widened to keep
// s + 19 digits (COUNT < 2^63 < 10^19): PostgreSQL numeric(48, 21) — its own AVG keeps only 16
// significant digits — and MySQL DECIMAL(65, 30) — its own AVG keeps s + 4. The answers below are
// what such a division returns: a tie (0.125 / -0.125) rounds away from zero, and
// 0.124999999999999999995, a mean just under the tie that a 16-digit AVG would have rounded to
// 0.1250000000000000 (then 0.13), stays 0.12.
TEST_CASE("federation pushdown: AVG of a DECIMAL column is the sum widened past the scale over the count") {
    struct dialect_t {
        backend_type_t kind;
        const char* leaf;
        std::string pushed;
    };
    const std::vector<dialect_t> dialects{
        {backend_type_t::PostgreSQL,
         "pushdown_decimal_avg_pg",
         R"(SELECT "id", (CAST(SUM("price") AS numeric(48, 21)) / COUNT("price")) AS "a" FROM "public"."goods" GROUP BY "id" ORDER BY "id" ASC NULLS LAST;)"},
        {backend_type_t::MySQL,
         "pushdown_decimal_avg_mysql",
         "SELECT `id`, (CAST(SUM(`price`) AS DECIMAL(65, 30)) / COUNT(`price`)) AS `a` FROM `db`.`goods` GROUP BY `id` "
         "ORDER BY `id` IS NULL ASC, `id` ASC;"}};
    for (const auto& dialect : dialects) {
        CAPTURE(dialect.leaf);
        priced_t shop(dialect.kind, dialect.leaf);
        shop.backend->script(dialect.pushed,
                             answer({column(logical_type::BIGINT, "id"), column(logical_type::STRING_LITERAL, "a")},
                                    {{std::int64_t{1}, std::string{"0.125000000000000000000"}},
                                     {std::int64_t{2}, std::string{"-0.125000000000000000000"}},
                                     {std::int64_t{3}, std::string{"0.124999999999999999995"}},
                                     {std::int64_t{4}, std::string{"0.124950000000000000000"}}}));
        auto averaged =
            shop.engine->sql("SELECT id, avg(price) AS a FROM " + shop.goods + " GROUP BY id ORDER BY id;");
        INFO(error_of(averaged));
        REQUIRE(averaged->is_success());
        CHECK(rows_of(averaged) == text_rows_t{{"1", "0.13"}, {"2", "-0.13"}, {"3", "0.12"}, {"4", "0.12"}});
        CHECK(types_of(averaged) == std::vector<logical_type>{logical_type::BIGINT, logical_type::DECIMAL});
        CHECK(shop.backend->sql_log().back() == dialect.pushed);
    }
}

// The engine's own mean of the same tie (0.10 and 0.15: 0.125) is 0.13 and of its negation -0.13:
// ClickHouse leaves AVG of a DECIMAL to it.
TEST_CASE("federation pushdown: the engine rounds a DECIMAL mean on a tie away from zero") {
    priced_t shop(backend_type_t::ClickHouse,
                  "pushdown_decimal_avg_tie",
                  {{std::int64_t{1}, std::string{"0.10"}},
                   {std::int64_t{1}, std::string{"0.15"}},
                   {std::int64_t{2}, std::string{"-0.10"}},
                   {std::int64_t{2}, std::string{"-0.15"}}});
    auto averaged = shop.engine->sql("SELECT id, avg(price) AS a FROM shop.db.goods GROUP BY id ORDER BY id;");
    INFO(error_of(averaged));
    REQUIRE(averaged->is_success());
    CHECK(rows_of(averaged) == text_rows_t{{"1", "0.13"}, {"2", "-0.13"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{"SELECT `id`, `price` FROM `db`.`goods`"});
}

// DECIMAL(30, 2) on MySQL: its sum needs 30 - 2 + 19 integer digits, which leaves DECIMAL(65, t) only
// 18 fraction digits — fewer than the 19 past the scale one more rounding needs — so the mean stays
// with the engine, over the scan.
TEST_CASE("federation pushdown: AVG of a wide DECIMAL column stays in the engine on MySQL") {
    priced_t shop(backend_type_t::MySQL,
                  "pushdown_decimal_avg_wide",
                  {{std::int64_t{1}, std::string{"0.10"}}, {std::int64_t{2}, std::string{"0.15"}}},
                  30,
                  2);
    auto averaged = shop.engine->sql("SELECT avg(price) AS a FROM shop.db.goods;");
    INFO(error_of(averaged));
    REQUIRE(averaged->is_success());
    CHECK(rows_of(averaged) == text_rows_t{{"0.13"}});
    CHECK(shop.backend->sql_log() == std::vector<std::string>{"SELECT `id`, `price` FROM `db`.`goods`"});
}

// MySQL's DECIMAL holds at most 30 fraction digits and refuses CAST(… AS DECIMAL(38, 31)): the cast
// stays with the engine over the scan. PostgreSQL writes it.
TEST_CASE("federation pushdown: a CAST into a DECIMAL of more than 30 fraction digits stays off MySQL") {
    SECTION("MySQL: the engine casts over the scan") {
        priced_t shop(backend_type_t::MySQL, "pushdown_decimal_cast_scale_mysql");
        auto cast = shop.engine->sql("SELECT CAST(price AS DECIMAL(38, 31)) AS c FROM shop.db.goods WHERE id = 1;");
        INFO(error_of(cast));
        REQUIRE(cast->is_success());
        CHECK(rows_of(cast) == text_rows_t{{"0.1000000000000000000000000000000"}});
        CHECK(shop.backend->sql_log() == std::vector<std::string>{"SELECT `id`, `price` FROM `db`.`goods`"});
    }
    SECTION("PostgreSQL: one statement") {
        priced_t shop(backend_type_t::PostgreSQL, "pushdown_decimal_cast_scale_pg");
        const std::string pushed =
            R"(SELECT CAST("price" AS numeric(38, 31)) AS "c" FROM "public"."goods" WHERE "id" = 1;)";
        shop.backend->script(pushed,
                             answer({decimal_column(38, 31, "c")}, {{std::string{"0.1000000000000000000000000000000"}}}));
        auto cast = shop.engine->sql("SELECT CAST(price AS DECIMAL(38, 31)) AS c FROM shop.public.goods WHERE id = 1;");
        INFO(error_of(cast));
        REQUIRE(cast->is_success());
        CHECK(rows_of(cast) == text_rows_t{{"0.1000000000000000000000000000000"}});
        CHECK(shop.backend->sql_log().back() == pushed);
    }
}
