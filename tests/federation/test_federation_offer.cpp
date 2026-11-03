// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// storage_offer: the SQL each dialect is sent for what the backend takes of a block — a SELECT block
// whole, a JOIN of one connection as one statement, a WHERE with a residual left to otterbrix, an
// UPDATE / DELETE as one statement — and the refusals: an UPDATE / DELETE the backend cannot run whole,
// RETURNING outside PostgreSQL.

#include "federation_fixture.hpp"

using namespace fedtest;

namespace {

    struct dialect_t {
        std::string connection;
        backend_type_t kind;
        std::string database;
        std::string schema;
        std::string quote;
    };

    const std::vector<dialect_t>& dialects() {
        static const std::vector<dialect_t> all{{"pg", backend_type_t::PostgreSQL, "shop", "public", "\""},
                                                {"my", backend_type_t::MySQL, "shop", "", "`"},
                                                {"ch", backend_type_t::ClickHouse, "shop", "", "`"}};
        return all;
    }

    std::unique_ptr<fed_stack_t> make_stack() {
        std::vector<connection_spec_t> specs;
        for (const auto& d : dialects()) {
            specs.push_back({d.connection, d.kind, d.database, d.schema});
        }
        auto owned = std::make_unique<fed_stack_t>(std::move(specs));
        auto& stack = *owned;
        for (const auto& d : dialects()) {
            const std::string schema = d.kind == backend_type_t::PostgreSQL ? "public" : "";
            stack.backend(d.connection)
                .add_table({"shop",
                            schema,
                            "orders",
                            {column("id", logical_type::BIGINT),
                             column("amount", logical_type::BIGINT),
                             column("note", logical_type::STRING_LITERAL)},
                            {{std::int64_t{1}, std::int64_t{5}, std::string{"a"}},
                             {std::int64_t{2}, std::int64_t{50}, std::string{"b"}},
                             {std::int64_t{3}, std::int64_t{500}, std::string{"c"}}}});
            stack.backend(d.connection)
                .add_table({"shop",
                            schema,
                            "items",
                            {column("order_id", logical_type::BIGINT), column("qty", logical_type::BIGINT)},
                            {{std::int64_t{2}, std::int64_t{7}}}});
        }
        return owned;
    }

    std::string table_of(const dialect_t& d, const std::string& name) {
        return d.connection + ".shop.public." + name;
    }

    std::string last_read(fed_stack_t& stack, const dialect_t& d) {
        const auto reads = starting_with(stack.backend(d.connection).log(), "scan ");
        return reads.empty() ? std::string{} : reads.back();
    }

} // namespace

TEST_CASE("offer: a SELECT block of one table is one statement of each dialect") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    for (const auto& d : dialects()) {
        INFO(d.connection);
        // The fake answers the columns the SELECT list names; the block's own answer is checked by
        // its text.
        stack.backend(d.connection).on_scan([](const std::string& sql, std::pmr::memory_resource* resource)
                                                -> std::optional<components::vector::data_chunk_t> {
            if (sql.find("WHERE") == std::string::npos) {
                return std::nullopt;
            }
            return chunk_of(resource, {column("id", logical_type::BIGINT)}, {{std::int64_t{2}}, {std::int64_t{3}}});
        });
        auto cursor = stack.ok("SELECT id FROM " + table_of(d, "orders") + " WHERE amount > 10 ORDER BY id LIMIT 5;");
        CHECK(rows_of(cursor) == std::vector<std::string>{"2", "3"});
        const auto sql = last_read(stack, d);
        INFO(sql);
        CHECK(sql.find("WHERE") != std::string::npos);
        CHECK(sql.find(d.quote + "amount" + d.quote) != std::string::npos);
        CHECK(sql.find("> 10") != std::string::npos);
        CHECK(sql.find("ORDER BY") != std::string::npos);
        CHECK(sql.find("LIMIT 5") != std::string::npos);
    }
}

TEST_CASE("offer: a GROUP BY block is one statement and its answer is held to otterbrix's types") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    for (const auto& d : dialects()) {
        INFO(d.connection);
        stack.backend(d.connection).on_scan([](const std::string& sql, std::pmr::memory_resource* resource)
                                                -> std::optional<components::vector::data_chunk_t> {
            if (sql.find("GROUP BY") == std::string::npos) {
                return std::nullopt;
            }
            // A backend answering the sum as text (MySQL's DECIMAL of an integer SUM).
            return chunk_of(resource,
                            {column("note", logical_type::STRING_LITERAL), column("s", logical_type::STRING_LITERAL)},
                            {{std::string{"a"}, std::string{"5"}}});
        });
        auto cursor = stack.ok("SELECT note, sum(amount) AS s FROM " + table_of(d, "orders") + " GROUP BY note;");
        CHECK(rows_of(cursor) == std::vector<std::string>{"a|5"});
        CHECK(last_read(stack, d).find("GROUP BY") != std::string::npos);
    }
}

TEST_CASE("offer: a JOIN of two tables of one connection is one statement") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    for (const auto& d : dialects()) {
        INFO(d.connection);
        stack.backend(d.connection).on_scan([](const std::string& sql, std::pmr::memory_resource* resource)
                                                -> std::optional<components::vector::data_chunk_t> {
            if (sql.find("JOIN") == std::string::npos) {
                return std::nullopt;
            }
            return chunk_of(resource,
                            {column("id", logical_type::BIGINT), column("qty", logical_type::BIGINT)},
                            {{std::int64_t{2}, std::int64_t{7}}});
        });
        auto cursor = stack.ok("SELECT o.id, i.qty FROM " + table_of(d, "orders") + " o JOIN " + table_of(d, "items") +
                               " i ON o.id = i.order_id;");
        CHECK(rows_of(cursor) == std::vector<std::string>{"2|7"});
        const auto sql = last_read(stack, d);
        INFO(sql);
        CHECK(sql.find("JOIN") != std::string::npos);
        CHECK(starting_with(stack.backend(d.connection).log(), "scan ").size() >= 1);
    }
}

TEST_CASE("offer: a WHERE conjunct the dialect cannot say stays with otterbrix") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    const auto& d = dialects()[1]; // MySQL
    auto cursor = stack.ok("SELECT id FROM " + table_of(d, "orders") + " WHERE amount > 10 AND cbrt(amount) > 3 ORDER BY id;");
    CHECK(rows_of(cursor) == std::vector<std::string>{"2", "3"});
    const auto sql = last_read(stack, d);
    INFO(sql);
    CHECK(sql.find("> 10") != std::string::npos);
    CHECK(sql.find("cbrt") == std::string::npos);
}

TEST_CASE("offer: an UPDATE or DELETE of one table is one statement of the backend") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    for (const auto& d : dialects()) {
        INFO(d.connection);
        stack.backend(d.connection).clear();
        auto updated = stack.ok("UPDATE " + table_of(d, "orders") + " SET note = 'z' WHERE id = 2;");
        CHECK(updated->affected_rows() == 1);
        auto deleted = stack.ok("DELETE FROM " + table_of(d, "orders") + " WHERE amount > 100;");
        CHECK(deleted->affected_rows() == 1);
        const auto writes = starting_with(stack.backend(d.connection).log(), "write ");
        REQUIRE(writes.size() == 2);
        INFO(writes[0] << " / " << writes[1]);
        if (d.kind == backend_type_t::ClickHouse) {
            CHECK(writes[0].find("ALTER TABLE") != std::string::npos);
        } else {
            CHECK(writes[0].find("UPDATE") != std::string::npos);
        }
        CHECK(writes[1].find("DELETE") != std::string::npos);
    }
}

TEST_CASE("offer: an UPDATE the backend cannot run whole is refused") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    stack.ok("CREATE TABLE main.public.l (a BIGINT, t TEXT);");
    stack.ok("INSERT INTO main.public.l VALUES (2, 'x');");
    for (const auto& d : dialects()) {
        INFO(d.connection);
        stack.backend(d.connection).clear();
        auto refused = stack.refused("UPDATE " + table_of(d, "orders") + " SET note = l.t FROM main.public.l l WHERE id = l.a;");
        CHECK(refused.type == core::error_code_t::unimplemented_yet);
        CHECK(starting_with(stack.backend(d.connection).log(), "write ").empty());
    }
}

TEST_CASE("offer: RETURNING is PostgreSQL's alone") {
    auto owned = make_stack();
    auto& stack = *owned;
    stack.open();
    const auto& my = dialects()[1];
    CHECK(stack.refused("UPDATE " + table_of(my, "orders") + " SET note = 'z' WHERE id = 2 RETURNING id;").type ==
          core::error_code_t::unimplemented_yet);
    CHECK(stack.refused("INSERT INTO " + table_of(my, "orders") + " VALUES (9, 9, 'n') RETURNING id;").type ==
          core::error_code_t::unimplemented_yet);
    const auto& pg = dialects()[0];
    stack.backend(pg.connection).on_scan([](const std::string& sql, std::pmr::memory_resource* resource)
                                             -> std::optional<components::vector::data_chunk_t> {
        if (sql.find("RETURNING") == std::string::npos) {
            return std::nullopt;
        }
        return chunk_of(resource,
                        {column("id", logical_type::BIGINT),
                         column("amount", logical_type::BIGINT),
                         column("note", logical_type::STRING_LITERAL)},
                        {{std::int64_t{2}, std::int64_t{50}, std::string{"z"}}});
    });
    auto returned = stack.ok("UPDATE " + table_of(pg, "orders") + " SET note = 'z' WHERE id = 2 RETURNING id, note;");
    CHECK(rows_of(returned) == std::vector<std::string>{"2|z"});
}

// The demo's `c.tier = 'gold'::tier_t`: a PostgreSQL ENUM column (pg_enum: the engine's ENUM of the
// same labels) compared with a value cast to the engine's ENUM type. The block is one statement, the
// value its label as a string literal, which PostgreSQL reads as a value of the column's type; before,
// the offer's probe took the block (the value a `?`) and its run failed "unsupported value type".
TEST_CASE("offer: a value of an ENUM type is pushed as its label") {
    auto owned = std::make_unique<fed_stack_t>(
        std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public"}});
    auto& stack = *owned;
    auto mood = complex_logical_type::create_enum("mood", {{"a", 0}, {"b", 1}, {"c", 2}}, "m");
    stack.backend("pg").add_table({"shop", "public", "moods", {column("id", logical_type::BIGINT), mood}, {}});
    stack.open();
    stack.ok("CREATE TYPE mood AS ENUM ('a', 'b', 'c');");
    stack.backend("pg").on_scan([](const std::string& sql, std::pmr::memory_resource* resource)
                                    -> std::optional<components::vector::data_chunk_t> {
        if (sql.find("WHERE") == std::string::npos) {
            return std::nullopt;
        }
        return chunk_of(resource, {column("id", logical_type::BIGINT)}, {{std::int64_t{2}}});
    });
    auto cursor = stack.ok("SELECT id FROM pg.shop.public.moods WHERE m = 'b'::mood;");
    CHECK(rows_of(cursor) == std::vector<std::string>{"2"});
    const auto reads = starting_with(stack.backend("pg").log(), "scan ");
    REQUIRE_FALSE(reads.empty());
    INFO(reads.back());
    CHECK(reads.back().find("\"m\" = 'b'") != std::string::npos);
}

// otterbrix casts an UPDATE assignment whose value is not of its column's type to that type (a
// literal is BIGINT / DOUBLE): `SET i = i + 1` over an INTEGER column narrows a BIGINT. MySQL's
// CAST neither narrows nor range-checks, so the statement leaves the range check to the store into
// the column, which only a strict sql_mode makes an error; ClickHouse's mutation gets accurateCast.
TEST_CASE("offer: an UPDATE narrowing its value to the column is one statement where the store range-checks it") {
    auto owned = std::make_unique<fed_stack_t>(std::vector<connection_spec_t>{{"pg", backend_type_t::PostgreSQL, "shop", "public"},
                                                                              {"my", backend_type_t::MySQL, "shop", ""},
                                                                              {"lax", backend_type_t::MySQL, "shop", ""},
                                                                              {"ch", backend_type_t::ClickHouse, "shop", ""}});
    auto& stack = *owned;
    for (const std::string connection : {"pg", "my", "lax", "ch"}) {
        stack.backend(connection).add_table({"shop",
                                             connection == "pg" ? "public" : "",
                                             "counts",
                                             {column("id", logical_type::BIGINT),
                                              column("i", logical_type::INTEGER),
                                              column("f", logical_type::FLOAT)},
                                             {}});
    }
    stack.backend("my").set_sql_mode("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION");
    stack.backend("lax").set_sql_mode("NO_ENGINE_SUBSTITUTION");
    stack.open();
    auto written = [&](const std::string& connection, const std::string& set) {
        stack.backend(connection).clear();
        stack.ok("UPDATE " + connection + ".shop.public.counts SET " + set + " WHERE id = 1;");
        const auto writes = starting_with(stack.backend(connection).log(), "write ");
        REQUIRE(writes.size() == 1);
        return writes.front();
    };

    SECTION("an integer narrowed to INTEGER") {
        const auto pg = written("pg", "i = i + 1");
        INFO(pg);
        CHECK(pg.find("CAST(") != std::string::npos);
        // The validator's widening of `i` stays; the assignment's narrowing is the store's.
        CHECK(written("my", "i = i * 1000") ==
              "write UPDATE `shop`.`counts` SET `i` = (CAST(`i` AS SIGNED) * 1000) WHERE `id` = 1; @txn");
        const auto ch = written("ch", "i = i + 1");
        INFO(ch);
        CHECK(ch.find("accurateCast(") != std::string::npos);
        CHECK(ch.find("'Nullable(Int32)')") != std::string::npos);
        // The validator's widening of `i` holds a NULL row's NULL: a bare Int64 target refuses it.
        CHECK(ch.find("CAST(`i` AS Nullable(Int64))") != std::string::npos);
    }
    SECTION("a DOUBLE narrowed to FLOAT") {
        CHECK(written("my", "f = 2.5").find("`f` = 2.5 ") != std::string::npos);
        CHECK(written("my", "f = f + 1") ==
              "write UPDATE `shop`.`counts` SET `f` = (CAST(`f` AS DOUBLE) + CAST(1 AS DOUBLE)) WHERE `id` = 1; @txn");
        CHECK(written("ch", "f = 2.5").find("CAST(2.5 AS Float32)") != std::string::npos);
        stack.backend("ch").clear();
        auto refused = stack.refused("UPDATE ch.shop.public.counts SET f = f + 1 WHERE id = 1;");
        CHECK(refused.type == core::error_code_t::unimplemented_yet);
        CHECK(starting_with(stack.backend("ch").log(), "write ").empty());
    }
    SECTION("a MySQL session without a strict sql_mode takes no narrowing assignment") {
        stack.backend("lax").clear();
        auto refused = stack.refused("UPDATE lax.shop.public.counts SET i = i + 1 WHERE id = 1;");
        CHECK(refused.type == core::error_code_t::unimplemented_yet);
        CHECK(std::string{refused.what}.find("strict sql_mode") != std::string::npos);
        CHECK(starting_with(stack.backend("lax").log(), "write ").empty());
        // A bound value the engine's cast takes leaves nothing to range-check.
        CHECK(written("lax", "f = 2.5").find("`f` = 2.5 ") != std::string::npos);
        CHECK(written("lax", "i = 7").find("`i` = ") != std::string::npos);
    }
    SECTION("a fraction into an integer column stays refused on MySQL and ClickHouse") {
        for (const std::string connection : {"my", "ch"}) {
            INFO(connection);
            stack.backend(connection).clear();
            CHECK(stack.refused("UPDATE " + connection + ".shop.public.counts SET i = 2.5 WHERE id = 1;").type ==
                  core::error_code_t::unimplemented_yet);
            CHECK(starting_with(stack.backend(connection).log(), "write ").empty());
        }
    }
}

// A ClickHouse mutation that failed on a part stays in system.mutations, retried forever, and every
// later mutation of the table fails with its error: the agent kills the mutation ClickHouse names in
// the table the statement wrote (its backend spelling), the client gets the mutation's own error, and
// the next UPDATE of the table runs.
TEST_CASE("offer: a ClickHouse mutation that failed is killed and the table takes the next UPDATE") {
    auto owned = std::make_unique<fed_stack_t>(std::vector<connection_spec_t>{{"ch", backend_type_t::ClickHouse, "Shop", ""}});
    auto& stack = *owned;
    stack.backend("ch").add_table(
        {"Shop", "", "Counts", {column("id", logical_type::BIGINT), column("i", logical_type::INTEGER)}, {}});
    stack.open();
    const std::string reason = "Code: 341. DB::Exception: Exception happened during execution of mutation "
                               "'mutation_4.txt' with part 'all_1_1_0' reason: 'Code: 70. DB::Exception: Value in "
                               "column Int64 cannot be safely converted into type Int32'";
    stack.backend("ch").fail_statements("3000000000", core::error_code_t::internal_error, reason);
    stack.backend("ch").clear();
    auto refused = stack.refused("UPDATE ch.shop.public.counts SET i = i * 3000000000 WHERE id = 1;");
    CHECK(refused.type == core::error_code_t::internal_error);
    CHECK(std::string{refused.what}.find(reason) != std::string::npos);
    const auto log = stack.backend("ch").log();
    REQUIRE(log.size() == 2);
    CHECK(log[0].rfind("write ALTER TABLE `Shop`.`Counts` UPDATE ", 0) == 0);
    CHECK(log[1] == "exec KILL MUTATION WHERE database = 'Shop' AND table = 'Counts' AND mutation_id = 'mutation_4.txt'");
    stack.backend("ch").clear();
    stack.ok("UPDATE ch.shop.public.counts SET i = i + 1 WHERE id = 1;");
    CHECK(starting_with(stack.backend("ch").log(), "write ALTER TABLE `Shop`.`Counts` UPDATE ").size() == 1);
}
