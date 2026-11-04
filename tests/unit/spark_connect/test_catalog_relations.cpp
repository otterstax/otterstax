// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/spark_connect_server/catalog_rows.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <initializer_list>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

    namespace ct = components::types;
    namespace cv = components::vector;
    namespace rows = frontend::spark::catalog_rows;

    // A read's answer: text columns (`width` of them), one row per entry;
    // std::nullopt is a NULL cell.
    using cell_t = std::optional<std::string_view>;
    session_payload text_result(size_t width,
                                std::initializer_list<std::vector<cell_t>> input,
                                std::pmr::memory_resource* resource) {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        for (size_t i = 0; i < width; ++i) {
            types.emplace_back(ct::logical_type::STRING_LITERAL, "c" + std::to_string(i));
        }
        cv::data_chunk_t chunk(resource, types);
        uint64_t at = 0;
        for (const auto& row : input) {
            for (size_t column = 0; column < width; ++column) {
                if (row[column]) {
                    chunk.set_value(column, at, *row[column]);
                } else {
                    chunk.data[column].set_null(at, true);
                }
            }
            ++at;
        }
        chunk.set_cardinality(at);
        std::pmr::vector<cv::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(chunk));
        return session_payload{ct::complex_logical_type::create_struct("", types), std::move(chunks), 0, NodeTag::T_Null};
    }

    // What schemas_sql() answers: datconnection, datname, nspname.
    session_payload fixture_schemas(std::pmr::memory_resource* resource) {
        return text_result(3,
                           {{"", "main", "public"},
                            {"", "shop", "public"},
                            {"", "shop", "sales"},
                            {"", "empty_db", "public"},
                            {"mysql", "bill", "public"},
                            {"pg", "shop", "public"}},
                           resource);
    }

    // What tables_sql() answers: datconnection, datname, nspname, relname, relkind.
    session_payload fixture_tables(std::pmr::memory_resource* resource) {
        return text_result(5,
                           {{"", "main", "public", "loose", "r"},
                            {"", "shop", "public", "orders", "r"},
                            {"", "shop", "public", "orders_by_day", "v"},
                            {"", "shop", "public", "daily", "m"},
                            {"", "shop", "public", "docs", "g"},
                            {"", "shop", "sales", "orders", "r"},
                            {"mysql", "bill", "public", "payments", "r"},
                            {"pg", "shop", "public", "products", "r"}},
                           resource);
    }

    // What place_sql answers: current_database(), current_schema().
    session_payload place_of(cell_t database, cell_t schema, std::pmr::memory_resource* resource) {
        return text_result(2, {{database, schema}}, resource);
    }

    rows::catalog_view_t fixture_view(std::pmr::memory_resource* resource) {
        auto loaded = rows::read_catalog(fixture_schemas(resource),
                                         fixture_tables(resource),
                                         place_of("main", "public", resource),
                                         resource);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    std::vector<std::string> parts_of(const std::pmr::vector<std::pmr::string>& parts) {
        return std::vector<std::string>(parts.begin(), parts.end());
    }

    struct column_spec {
        std::string_view name;
        ct::logical_type type;
    };

    // The schema and every chunk carry exactly `expected`, in order.
    void require_layout(const session_payload& payload, std::initializer_list<column_spec> expected) {
        REQUIRE(payload.schema.type() == ct::logical_type::STRUCT);
        const auto& fields = payload.schema.child_types();
        REQUIRE(fields.size() == expected.size());
        REQUIRE_FALSE(payload.chunks.empty());
        size_t index = 0;
        for (const auto& spec : expected) {
            INFO("column " << index << " (" << spec.name << ")");
            REQUIRE(fields[index].has_alias());
            CHECK(fields[index].alias() == spec.name);
            CHECK(fields[index].type() == spec.type);
            for (const auto& chunk : payload.chunks) {
                REQUIRE(chunk.column_count() == expected.size());
                const auto& type = chunk.data[index].type();
                REQUIRE(type.has_alias());
                CHECK(type.alias() == spec.name);
                CHECK(type.type() == spec.type);
            }
            ++index;
        }
    }

    std::string_view text_at(const session_payload& payload, size_t column, size_t row) {
        return payload.chunks.front().data[column].get_value<std::string_view>(row);
    }

    bool flag_at(const session_payload& payload, size_t column, size_t row) {
        return payload.chunks.front().data[column].get_value<bool>(row);
    }

    bool null_at(const session_payload& payload, size_t column, size_t row) {
        return payload.chunks.front().data[column].is_null(row);
    }

    std::vector<std::string> list_at(const session_payload& payload, size_t column, size_t row) {
        const auto parts = payload.chunks.front().data[column].get_value<std::vector<std::string_view>>(row);
        return std::vector<std::string>(parts.begin(), parts.end());
    }

    const std::initializer_list<column_spec> database_layout = {{"name", ct::logical_type::STRING_LITERAL},
                                                                {"catalog", ct::logical_type::STRING_LITERAL},
                                                                {"description", ct::logical_type::STRING_LITERAL},
                                                                {"locationUri", ct::logical_type::STRING_LITERAL}};

    const std::initializer_list<column_spec> table_layout = {{"name", ct::logical_type::STRING_LITERAL},
                                                             {"catalog", ct::logical_type::STRING_LITERAL},
                                                             {"namespace", ct::logical_type::LIST},
                                                             {"description", ct::logical_type::STRING_LITERAL},
                                                             {"tableType", ct::logical_type::STRING_LITERAL},
                                                             {"isTemporary", ct::logical_type::BOOLEAN}};

} // namespace

// ── pg_catalog rows → what Spark sees ───────────────────────────────────────

TEST_CASE("spark catalog: a database is a schema, named as SQL writes it") {
    std::pmr::synchronized_pool_resource pool;
    const auto view = fixture_view(&pool);

    std::vector<std::string> databases(view.databases.begin(), view.databases.end());
    CHECK(databases == std::vector<std::string>{"empty_db.public",
                                                "main.public",
                                                "mysql.bill.public",
                                                "pg.shop.public",
                                                "shop.public",
                                                "shop.sales"});
    CHECK(parts_of(view.place) == std::vector<std::string>{"main", "public"});
    CHECK(view.current_database == "main.public");
}

TEST_CASE("spark catalog: tables are classified under their namespace") {
    std::pmr::synchronized_pool_resource pool;
    const auto view = fixture_view(&pool);

    // Sorted by database, then name.
    REQUIRE(view.tables.size() == 8);

    const auto& loose = view.tables[0];
    CHECK(loose.database == "main.public");
    CHECK(loose.name == "loose");
    CHECK(parts_of(loose.qualifier) == std::vector<std::string>{"main", "public"});
    CHECK_FALSE(loose.is_view);

    // A table of a connection's database: [c, d, s].
    const auto& payments = view.tables[1];
    CHECK(payments.database == "mysql.bill.public");
    CHECK(payments.name == "payments");
    CHECK(parts_of(payments.qualifier) == std::vector<std::string>{"mysql", "bill", "public"});

    const auto& products = view.tables[2];
    CHECK(products.database == "pg.shop.public");
    CHECK(parts_of(products.qualifier) == std::vector<std::string>{"pg", "shop", "public"});

    // A local database's schema: [d, s]; v / m are views, r / g tables.
    const std::vector<std::pair<std::string_view, bool>> shop = {{"daily", true},
                                                                 {"docs", false},
                                                                 {"orders", false},
                                                                 {"orders_by_day", true}};
    for (size_t i = 0; i < shop.size(); ++i) {
        const auto& table = view.tables[3 + i];
        INFO(shop[i].first);
        CHECK(table.database == "shop.public");
        CHECK(table.name == shop[i].first);
        CHECK(parts_of(table.qualifier) == std::vector<std::string>{"shop", "public"});
        CHECK(table.is_view == shop[i].second);
    }
    CHECK(view.tables[7].database == "shop.sales");
}

TEST_CASE("spark catalog: the reads hide the system schemas and the federation database") {
    for (const auto& sql : {rows::schemas_sql(), rows::tables_sql()}) {
        INFO(sql);
        CHECK(sql.find("NOT IN ('pg_catalog', 'information_schema')") != std::string::npos);
        CHECK(sql.find("d.datname = 'federation'") != std::string::npos);
    }
    CHECK(rows::tables_sql().find("c.relkind IN ('r', 'g', 'v', 'm')") != std::string::npos);
}

TEST_CASE("spark catalog: the place without a schema is the database alone") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    auto loaded =
        rows::read_catalog(fixture_schemas(resource), fixture_tables(resource), place_of("main", std::nullopt, resource), resource);
    REQUIRE_FALSE(loaded.has_error());
    CHECK(loaded.value().current_database == "main");
    auto bare = rows::find_table(loaded.value(), "loose", std::nullopt, resource);
    REQUIRE_FALSE(bare.has_error());
    CHECK(bare.value() == nullptr);
}

TEST_CASE("spark catalog: unreadable rows are an internal_error") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    SECTION("a NULL cell") {
        auto loaded = rows::read_catalog(fixture_schemas(resource),
                                         text_result(5, {{"", "shop", "public", std::nullopt, "r"}}, resource),
                                         place_of("main", "public", resource),
                                         resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::internal_error);
    }

    SECTION("a row of another width") {
        auto loaded = rows::read_catalog(text_result(2, {{"", "shop"}}, resource),
                                         fixture_tables(resource),
                                         place_of("main", "public", resource),
                                         resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::internal_error);
    }

    SECTION("no place") {
        auto loaded =
            rows::read_catalog(fixture_schemas(resource), fixture_tables(resource), text_result(2, {}, resource), resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::internal_error);
    }
}

// ── Spark name patterns ─────────────────────────────────────────────────────

TEST_CASE("spark catalog: patterns") {
    CHECK(rows::matches_pattern("*", "anything"));
    CHECK(rows::matches_pattern("*", ""));
    CHECK(rows::matches_pattern("sh*", "shop"));
    CHECK(rows::matches_pattern("SH*", "shop"));
    CHECK(rows::matches_pattern("sh*", "SHOP"));
    CHECK(rows::matches_pattern("*op", "shop"));
    CHECK(rows::matches_pattern("s*p", "shop"));
    CHECK(rows::matches_pattern("*a*b*", "xxaxxbxx"));
    CHECK(rows::matches_pattern("a**b", "ab"));
    CHECK(rows::matches_pattern("pg|mysql", "mysql"));
    CHECK(rows::matches_pattern("  shop  ", "shop"));
    CHECK(rows::matches_pattern("", ""));

    CHECK_FALSE(rows::matches_pattern("s*x", "shop"));
    CHECK_FALSE(rows::matches_pattern("shop", "shopping"));
    CHECK_FALSE(rows::matches_pattern("pg|mysql", "kafka"));
    CHECK_FALSE(rows::matches_pattern("", "a"));
    // Only '*' and '|' are special: a '.' is itself.
    CHECK_FALSE(rows::matches_pattern("a.b", "axb"));
}

// ── Qualified-name resolution ───────────────────────────────────────────────

TEST_CASE("spark catalog: a table is found by the name the SQL path reads") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);
    const auto found = [&](std::string_view name, std::optional<std::string_view> db) -> std::string {
        auto table = rows::find_table(view, name, db, resource);
        REQUIRE_FALSE(table.has_error());
        return table.value() == nullptr ? std::string{"-"}
                                         : std::string{table.value()->database} + "/" + std::string{table.value()->name};
    };

    CHECK(found("mysql.bill.public.payments", std::nullopt) == "mysql.bill.public/payments");
    CHECK(found("pg.shop.public.products", std::nullopt) == "pg.shop.public/products");
    CHECK(found("shop.sales.orders", std::nullopt) == "shop.sales/orders");
    CHECK(found("`shop`.`public`.orders", std::nullopt) == "shop.public/orders");
    // Completed with the place, main.public.
    CHECK(found("loose", std::nullopt) == "main.public/loose");
    CHECK(found("public.loose", std::nullopt) == "main.public/loose");

    CHECK(found("orders", std::nullopt) == "-");
    CHECK(found("shop.orders", std::nullopt) == "-"); // the schema shop of main
    CHECK(found("Shop.public.orders", std::nullopt) == "-"); // names are matched exactly
    CHECK(found("bill.public.payments", std::nullopt) == "-"); // three parts are a local database

    CHECK(found("payments", "mysql.bill.public") == "mysql.bill.public/payments");
    CHECK(found("orders", "shop.sales") == "shop.sales/orders");
    CHECK(found("shop.public.orders", "shop.public") == "-"); // with a database the name is not split
    CHECK(found("orders", "nope") == "-");

    auto five = rows::find_table(view, "a.b.c.d.e", std::nullopt, resource);
    REQUIRE(five.has_error());
    CHECK(five.error().type == core::error_code_t::sql_parse_error);
}

TEST_CASE("spark catalog: getTable answers the table or says why not") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    auto missing = rows::get_table(view, "orders", std::nullopt, resource);
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::table_not_exists);

    auto no_database = rows::get_table(view, "orders", "nope", resource);
    REQUIRE(no_database.has_error());
    CHECK(no_database.error().type == core::error_code_t::database_not_exists);

    auto not_there = rows::get_table(view, "nope", "shop.public", resource);
    REQUIRE(not_there.has_error());
    CHECK(not_there.error().type == core::error_code_t::table_not_exists);

    auto five = rows::get_table(view, "a.b.c.d.e", std::nullopt, resource);
    REQUIRE(five.has_error());
    CHECK(five.error().type == core::error_code_t::sql_parse_error);

    auto qualified = rows::get_table(view, "shop.sales.orders", std::nullopt, resource);
    REQUIRE_FALSE(qualified.has_error());
    CHECK(list_at(qualified.value(), 2, 0) == std::vector<std::string>{"shop", "sales"});
}

TEST_CASE("spark catalog: the column probe quotes every part") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    auto products = rows::find_table(view, "products", "pg.shop.public", resource);
    REQUIRE_FALSE(products.has_error());
    REQUIRE(products.value() != nullptr);
    CHECK(rows::select_all_query(*products.value()) == R"(SELECT * FROM "pg"."shop"."public"."products")");

    auto loose = rows::find_table(view, "loose", std::nullopt, resource);
    REQUIRE_FALSE(loose.has_error());
    REQUIRE(loose.value() != nullptr);
    CHECK(rows::select_all_query(*loose.value()) == R"(SELECT * FROM "main"."public"."loose")");

    rows::table_entry_t odd{std::pmr::string{"Shop.public", resource},
                            std::pmr::string{"we\"ird", resource},
                            std::pmr::vector<std::pmr::string>{resource},
                            false};
    odd.qualifier.emplace_back("Shop");
    odd.qualifier.emplace_back("public");
    CHECK(rows::select_all_query(odd) == R"(SELECT * FROM "Shop"."public"."we""ird")");
}

// ── Row layouts PySpark reads by position ───────────────────────────────────

TEST_CASE("spark catalog: single values") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto current = rows::string_payload("main.public", resource);
    require_layout(current, {{"value", ct::logical_type::STRING_LITERAL}});
    REQUIRE(current.size() == 1);
    CHECK(text_at(current, 0, 0) == "main.public");

    const auto exists = rows::boolean_payload(true, resource);
    require_layout(exists, {{"value", ct::logical_type::BOOLEAN}});
    REQUIRE(exists.size() == 1);
    CHECK(flag_at(exists, 0, 0));
}

TEST_CASE("spark catalog: CatalogMetadata rows") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto all = rows::list_catalogs(std::nullopt, resource);
    require_layout(all,
                   {{"name", ct::logical_type::STRING_LITERAL}, {"description", ct::logical_type::STRING_LITERAL}});
    REQUIRE(all.size() == 1);
    CHECK(text_at(all, 0, 0) == "otterstax");
    CHECK(null_at(all, 1, 0));

    CHECK(rows::list_catalogs("OTTER*", resource).size() == 1);
    const auto none = rows::list_catalogs("spark_catalog", resource);
    require_layout(none,
                   {{"name", ct::logical_type::STRING_LITERAL}, {"description", ct::logical_type::STRING_LITERAL}});
    CHECK(none.size() == 0);
}

TEST_CASE("spark catalog: Database rows") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    const auto all = rows::list_databases(view, std::nullopt, resource);
    require_layout(all, database_layout);
    REQUIRE(all.size() == 6);
    CHECK(text_at(all, 0, 0) == "empty_db.public");
    CHECK(text_at(all, 1, 0) == "otterstax");
    CHECK(null_at(all, 2, 0));
    CHECK(null_at(all, 3, 0));

    const auto matched = rows::list_databases(view, "m*|P*", resource);
    REQUIRE(matched.size() == 3);
    CHECK(text_at(matched, 0, 0) == "main.public");
    CHECK(text_at(matched, 0, 1) == "mysql.bill.public");
    CHECK(text_at(matched, 0, 2) == "pg.shop.public");

    auto sales = rows::get_database(view, "shop.sales", resource);
    REQUIRE_FALSE(sales.has_error());
    require_layout(sales.value(), database_layout);
    REQUIRE(sales.value().size() == 1);
    CHECK(text_at(sales.value(), 0, 0) == "shop.sales");

    auto missing = rows::get_database(view, "shop", resource);
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::database_not_exists);

    CHECK(rows::database_exists(view, "main.public"));
    CHECK(rows::database_exists(view, "empty_db.public"));
    CHECK_FALSE(rows::database_exists(view, "main.pg_catalog"));
}

TEST_CASE("spark catalog: Table rows") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    auto mysql = rows::list_tables(view, "mysql.bill.public", std::nullopt, resource);
    REQUIRE_FALSE(mysql.has_error());
    const auto& payload = mysql.value();
    require_layout(payload, table_layout);
    CHECK(payload.schema.child_types()[2].child_type().type() == ct::logical_type::STRING_LITERAL);
    CHECK(payload.chunks.front().data[2].type().child_type().type() == ct::logical_type::STRING_LITERAL);
    REQUIRE(payload.size() == 1);
    CHECK(text_at(payload, 0, 0) == "payments");
    CHECK(text_at(payload, 1, 0) == "otterstax");
    CHECK(list_at(payload, 2, 0) == std::vector<std::string>{"mysql", "bill", "public"});
    CHECK(null_at(payload, 3, 0));
    CHECK(text_at(payload, 4, 0) == "MANAGED");
    CHECK_FALSE(flag_at(payload, 5, 0));

    // Without a database: the current one, main.public.
    auto current = rows::list_tables(view, std::nullopt, std::nullopt, resource);
    REQUIRE_FALSE(current.has_error());
    REQUIRE(current.value().size() == 1);
    CHECK(text_at(current.value(), 0, 0) == "loose");
    CHECK(list_at(current.value(), 2, 0) == std::vector<std::string>{"main", "public"});

    auto orders = rows::list_tables(view, "shop.public", "orders*", resource);
    REQUIRE_FALSE(orders.has_error());
    REQUIRE(orders.value().size() == 2);
    CHECK(text_at(orders.value(), 0, 0) == "orders");
    CHECK(text_at(orders.value(), 4, 0) == "MANAGED");
    CHECK(text_at(orders.value(), 0, 1) == "orders_by_day");
    CHECK(text_at(orders.value(), 4, 1) == "VIEW");

    // No row is still the whole layout, in one empty chunk.
    auto empty = rows::list_tables(view, "empty_db.public", std::nullopt, resource);
    REQUIRE_FALSE(empty.has_error());
    require_layout(empty.value(), table_layout);
    CHECK(empty.value().chunks.size() == 1);
    CHECK(empty.value().size() == 0);

    auto missing = rows::list_tables(view, "nope", std::nullopt, resource);
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::database_not_exists);

    auto products = rows::get_table(view, "pg.shop.public.products", std::nullopt, resource);
    REQUIRE_FALSE(products.has_error());
    require_layout(products.value(), table_layout);
    REQUIRE(products.value().size() == 1);
    CHECK(text_at(products.value(), 0, 0) == "products");
    CHECK(list_at(products.value(), 2, 0) == std::vector<std::string>{"pg", "shop", "public"});
}

TEST_CASE("spark catalog: Column rows") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    std::pmr::vector<ct::complex_logical_type> fields(resource);
    fields.emplace_back(ct::logical_type::BIGINT, "id");
    fields.emplace_back(ct::logical_type::STRING_LITERAL, "name");
    auto price = ct::complex_logical_type::create_decimal(resource, 12, 2, "price");
    REQUIRE_FALSE(price.has_error());
    fields.push_back(price.value());
    fields.push_back(
        ct::complex_logical_type::create_list(ct::complex_logical_type{ct::logical_type::STRING_LITERAL}, "tags"));
    fields.emplace_back(ct::logical_type::UBIGINT, "hits");
    fields.emplace_back(ct::logical_type::BOOLEAN); // unnamed
    const auto schema = ct::complex_logical_type::create_struct("", fields);

    const auto columns = rows::list_columns(schema, resource);
    require_layout(columns,
                   {{"name", ct::logical_type::STRING_LITERAL},
                    {"description", ct::logical_type::STRING_LITERAL},
                    {"dataType", ct::logical_type::STRING_LITERAL},
                    {"nullable", ct::logical_type::BOOLEAN},
                    {"isPartition", ct::logical_type::BOOLEAN},
                    {"isBucket", ct::logical_type::BOOLEAN},
                    {"isCluster", ct::logical_type::BOOLEAN}});
    REQUIRE(columns.size() == 6);

    const std::vector<std::pair<std::string_view, std::string_view>> expected = {{"id", "bigint"},
                                                                                 {"name", "string"},
                                                                                 {"price", "decimal(12,2)"},
                                                                                 {"tags", "array<string>"},
                                                                                 {"hits", "bigint"},
                                                                                 {"col5", "boolean"}};
    for (size_t row = 0; row < expected.size(); ++row) {
        INFO("row " << row);
        CHECK(text_at(columns, 0, row) == expected[row].first);
        CHECK(null_at(columns, 1, row));
        CHECK(text_at(columns, 2, row) == expected[row].second);
        CHECK(flag_at(columns, 3, row));
        CHECK_FALSE(flag_at(columns, 4, row));
        CHECK_FALSE(flag_at(columns, 5, row));
        CHECK_FALSE(flag_at(columns, 6, row));
    }

    // A prepare that described nothing has no column.
    const auto none = rows::list_columns(ct::complex_logical_type{ct::logical_type::NA}, resource);
    CHECK(none.size() == 0);
    CHECK(none.column_count() == 7);
}

TEST_CASE("spark catalog: a long answer is split into DEFAULT_VECTOR_CAPACITY-row chunks") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    rows::catalog_view_t view(resource);
    for (size_t i = 0; i < 1500; ++i) {
        view.databases.emplace_back("db" + std::to_string(i));
    }
    const auto all = rows::list_databases(view, std::nullopt, resource);
    require_layout(all, database_layout);
    REQUIRE(all.chunks.size() == 2);
    CHECK(all.chunks[0].size() == cv::DEFAULT_VECTOR_CAPACITY);
    CHECK(all.chunks[1].size() == 1500 - cv::DEFAULT_VECTOR_CAPACITY);
    CHECK(all.chunks[1].data[0].get_value<std::string_view>(0) == "db1024");
}
