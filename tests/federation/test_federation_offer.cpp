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
