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

    struct namespace_row {
        uint32_t oid;
        std::string_view name;
    };

    struct class_row {
        uint32_t oid;
        std::string_view name;
        uint32_t namespace_oid;
        std::string_view kind;
    };

    session_payload payload_of(std::pmr::vector<ct::complex_logical_type> types,
                               cv::data_chunk_t chunk,
                               std::pmr::memory_resource* resource) {
        std::pmr::vector<cv::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(chunk));
        return session_payload{ct::complex_logical_type::create_struct("", types),
                               std::move(chunks),
                               0,
                               NodeTag::T_Null};
    }

    void set_oid(cv::data_chunk_t& chunk, uint64_t column, uint64_t row, uint32_t oid) {
        if (chunk.data[column].type().type() == ct::logical_type::BIGINT) {
            chunk.set_value(column, row, static_cast<int64_t>(oid));
        } else {
            chunk.set_value(column, row, oid);
        }
    }

    // What `SELECT oid, nspname FROM pg_catalog.pg_namespace` answers.
    session_payload namespaces_result(std::initializer_list<namespace_row> input, std::pmr::memory_resource* resource) {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::UINTEGER, "oid");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "nspname");
        cv::data_chunk_t chunk(resource, types);
        uint64_t at = 0;
        for (const auto& row : input) {
            set_oid(chunk, 0, at, row.oid);
            chunk.set_value(1, at, row.name);
            ++at;
        }
        chunk.set_cardinality(at);
        return payload_of(std::move(types), std::move(chunk), resource);
    }

    // What `SELECT oid, relname, relnamespace, relkind FROM pg_catalog.pg_class`
    // answers, the OIDs typed `oid_type`.
    session_payload classes_result(std::initializer_list<class_row> input,
                                   ct::logical_type oid_type,
                                   std::pmr::memory_resource* resource) {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(oid_type, "oid");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "relname");
        types.emplace_back(oid_type, "relnamespace");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "relkind");
        cv::data_chunk_t chunk(resource, types);
        uint64_t at = 0;
        for (const auto& row : input) {
            set_oid(chunk, 0, at, row.oid);
            chunk.set_value(1, at, row.name);
            set_oid(chunk, 2, at, row.namespace_oid);
            chunk.set_value(3, at, row.kind);
            ++at;
        }
        chunk.set_cardinality(at);
        return payload_of(std::move(types), std::move(chunk), resource);
    }

    session_payload fixture_namespaces(std::pmr::memory_resource* resource) {
        return namespaces_result({{1, "pg_catalog"},
                                  {2, "public"},
                                  {3, "information_schema"},
                                  {16384, "shop"},
                                  {16390, "mysql"},
                                  {16400, "pg"},
                                  {16410, "kafka"},
                                  {16420, "empty_db"}},
                                 resource);
    }

    session_payload fixture_classes(std::pmr::memory_resource* resource) {
        return classes_result({{10, "pg_namespace", 1, "r"},
                               {11, "pg_class", 1, "r"},
                               {16385, "orders", 16384, "r"},
                               {16386, "orders_pkey", 16384, "i"},
                               {16387, "orders_by_day", 16384, "v"},
                               {16388, "orders_id_seq", 16384, "S"},
                               {16389, "loose", 0, "r"},
                               {16391, "__otterstax_tables", 16390, "r"},
                               {16392, "bill::payments", 16390, "r"},
                               {16401, "__otterstax_tables", 16400, "r"},
                               {16402, "shop:public:products", 16400, "r"},
                               {16411, "__sources", 16410, "r"},
                               {16412, "clicks", 16410, "r"},
                               {16413, "clicks__offsets", 16410, "r"},
                               {16421, "point", 2, "c"},
                               {16422, "daily", 16384, "m"},
                               {16423, "docs", 16384, "g"},
                               {16424, "add_one", 16384, "F"},
                               {16425, "shadow", 2, "r"}},
                              ct::logical_type::UINTEGER,
                              resource);
    }

    rows::catalog_view_t fixture_view(std::pmr::memory_resource* resource) {
        auto loaded = rows::read_catalog(fixture_namespaces(resource), fixture_classes(resource), resource);
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

TEST_CASE("spark catalog: databases are the user namespaces plus default") {
    std::pmr::synchronized_pool_resource pool;
    const auto view = fixture_view(&pool);

    std::vector<std::string> databases(view.databases.begin(), view.databases.end());
    CHECK(databases == std::vector<std::string>{"default", "empty_db", "kafka", "mysql", "pg", "shop"});
}

TEST_CASE("spark catalog: tables are classified and the internal ones hidden") {
    std::pmr::synchronized_pool_resource pool;
    const auto view = fixture_view(&pool);

    // Sorted by database, then name. Hidden: system rows, the index, the
    // sequence, the composite type, the macro, a table in `public`, both
    // mirror manifests and the Kafka bookkeeping tables.
    REQUIRE(view.tables.size() == 8);

    const auto& loose = view.tables[0];
    CHECK(loose.database == "default");
    CHECK(loose.name == "loose");
    CHECK(loose.qualifier.empty());
    CHECK_FALSE(loose.is_view);

    const auto& clicks = view.tables[1];
    CHECK(clicks.database == "kafka");
    CHECK(clicks.name == "clicks");
    CHECK(parts_of(clicks.qualifier) == std::vector<std::string>{"kafka"});

    // MySQL / ClickHouse mirror `<db>::<table>`: [alias, db].
    const auto& payments = view.tables[2];
    CHECK(payments.database == "mysql");
    CHECK(payments.name == "payments");
    CHECK(parts_of(payments.qualifier) == std::vector<std::string>{"mysql", "bill"});
    CHECK_FALSE(payments.is_view);

    // PostgreSQL mirror `<db>:<schema>:<table>`: [alias, db, schema].
    const auto& products = view.tables[3];
    CHECK(products.database == "pg");
    CHECK(products.name == "products");
    CHECK(parts_of(products.qualifier) == std::vector<std::string>{"pg", "shop", "public"});

    // Local database: [d]; v / m are views, r / g tables.
    const std::vector<std::pair<std::string_view, bool>> shop = {{"daily", true},
                                                                 {"docs", false},
                                                                 {"orders", false},
                                                                 {"orders_by_day", true}};
    for (size_t i = 0; i < shop.size(); ++i) {
        const auto& table = view.tables[4 + i];
        INFO(shop[i].first);
        CHECK(table.database == "shop");
        CHECK(table.name == shop[i].first);
        CHECK(parts_of(table.qualifier) == std::vector<std::string>{"shop"});
        CHECK(table.is_view == shop[i].second);
    }
}

TEST_CASE("spark catalog: columns are found by name and an OID may be any integer type") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // pg_class with BIGINT OIDs and its columns in another order.
    std::pmr::vector<ct::complex_logical_type> types(resource);
    types.emplace_back(ct::logical_type::STRING_LITERAL, "relkind");
    types.emplace_back(ct::logical_type::BIGINT, "relnamespace");
    types.emplace_back(ct::logical_type::STRING_LITERAL, "relname");
    types.emplace_back(ct::logical_type::BIGINT, "oid");
    cv::data_chunk_t chunk(resource, types);
    chunk.set_value(0, 0, std::string_view{"r"});
    chunk.set_value(1, 0, int64_t{16384});
    chunk.set_value(2, 0, std::string_view{"orders"});
    chunk.set_value(3, 0, int64_t{16385});
    chunk.set_cardinality(1);
    const auto classes = payload_of(std::move(types), std::move(chunk), resource);

    auto loaded = rows::read_catalog(namespaces_result({{16384, "shop"}}, resource), classes, resource);
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().tables.size() == 1);
    CHECK(loaded.value().tables[0].database == "shop");
    CHECK(loaded.value().tables[0].name == "orders");
}

TEST_CASE("spark catalog: unreadable pg_catalog rows are a schema_error") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto namespaces = namespaces_result({{16384, "shop"}, {16390, "mysql"}}, resource);

    SECTION("a missing column") {
        std::pmr::vector<ct::complex_logical_type> types(resource);
        types.emplace_back(ct::logical_type::UINTEGER, "oid");
        types.emplace_back(ct::logical_type::STRING_LITERAL, "relname");
        cv::data_chunk_t chunk(resource, types);
        chunk.set_value(0, 0, uint32_t{16385});
        chunk.set_value(1, 0, std::string_view{"orders"});
        chunk.set_cardinality(1);
        auto loaded =
            rows::read_catalog(namespaces, payload_of(std::move(types), std::move(chunk), resource), resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::schema_error);
    }

    SECTION("a NULL oid") {
        auto classes = classes_result({{16385, "orders", 16384, "r"}}, ct::logical_type::UINTEGER, resource);
        classes.chunks.front().data[0].set_null(0, true);
        auto loaded = rows::read_catalog(namespaces, classes, resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::schema_error);
    }

    SECTION("a mirror name that is not <db>:<schema>:<table>") {
        auto loaded = rows::read_catalog(
            namespaces,
            classes_result({{16391, "__otterstax_tables", 16390, "r"}, {16392, "payments", 16390, "r"}},
                           ct::logical_type::UINTEGER,
                           resource),
            resource);
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::schema_error);
    }
}

TEST_CASE("spark catalog: a table whose database was dropped between the two reads is not listed") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    // pg_class, read first, still lists `orders` of namespace 16500; pg_namespace,
    // read after a DROP DATABASE (which cascades), no longer holds it.
    auto loaded = rows::read_catalog(namespaces_result({{16384, "shop"}}, resource),
                                     classes_result({{16385, "orders", 16500, "r"}, {16386, "items", 16384, "r"}},
                                                    ct::logical_type::UINTEGER,
                                                    resource),
                                     resource);
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(loaded.value().tables.size() == 1);
    CHECK(loaded.value().tables[0].name == "items");
    std::vector<std::string> databases(loaded.value().databases.begin(), loaded.value().databases.end());
    CHECK(databases == std::vector<std::string>{"default", "shop"});
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

TEST_CASE("spark catalog: a table is found by the identifier the SQL path reads") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);
    const auto count = [&](std::string_view name, std::optional<std::string_view> db) {
        return rows::find_tables(view, name, db, resource).size();
    };

    CHECK(count("mysql.bill.payments", std::nullopt) == 1);
    CHECK(count("pg.shop.public.products", std::nullopt) == 1);
    CHECK(count("shop.orders", std::nullopt) == 1);
    CHECK(count("loose", std::nullopt) == 1);

    CHECK(count("orders", std::nullopt) == 0); // only the unqualified tables answer a bare name
    CHECK(count("mysql.payments", std::nullopt) == 0);
    CHECK(count("bill.payments", std::nullopt) == 0);
    CHECK(count("Shop.orders", std::nullopt) == 0); // names are matched exactly
    CHECK(count("default.loose", std::nullopt) == 0);

    CHECK(count("payments", "mysql") == 1);
    CHECK(count("products", "pg") == 1);
    CHECK(count("loose", "default") == 1);
    CHECK(count("orders", "shop") == 1);
    CHECK(count("shop.orders", "shop") == 0); // with a database the name is not split
    CHECK(count("orders", "nope") == 0);

    const auto products = rows::find_tables(view, "pg.shop.public.products", std::nullopt, resource);
    REQUIRE(products.size() == 1);
    CHECK(products.front()->name == "products");
}

TEST_CASE("spark catalog: getTable answers one table or says why not") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    auto missing = rows::get_table(view, "orders", std::nullopt, resource);
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::table_not_exists);

    auto no_database = rows::get_table(view, "orders", "nope", resource);
    REQUIRE(no_database.has_error());
    CHECK(no_database.error().type == core::error_code_t::database_not_exists);

    auto not_there = rows::get_table(view, "nope", "shop", resource);
    REQUIRE(not_there.has_error());
    CHECK(not_there.error().type == core::error_code_t::table_not_exists);

    // Two mirrored databases of one connection, each with a `t`.
    auto loaded = rows::read_catalog(
        namespaces_result({{16390, "mysql"}}, resource),
        classes_result(
            {{16391, "__otterstax_tables", 16390, "r"}, {16392, "db1::t", 16390, "r"}, {16393, "db2::t", 16390, "r"}},
            ct::logical_type::UINTEGER,
            resource),
        resource);
    REQUIRE_FALSE(loaded.has_error());
    CHECK(rows::find_tables(loaded.value(), "t", "mysql", resource).size() == 2);
    auto ambiguous = rows::get_table(loaded.value(), "t", "mysql", resource);
    REQUIRE(ambiguous.has_error());
    CHECK(ambiguous.error().type == core::error_code_t::ambiguous_name);
    auto qualified = rows::get_table(loaded.value(), "mysql.db2.t", std::nullopt, resource);
    REQUIRE_FALSE(qualified.has_error());
    CHECK(list_at(qualified.value(), 2, 0) == std::vector<std::string>{"mysql", "db2"});
}

TEST_CASE("spark catalog: the column probe quotes every part") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    const auto products = rows::find_tables(view, "products", "pg", resource);
    REQUIRE(products.size() == 1);
    CHECK(rows::select_all_query(*products.front()) == R"(SELECT * FROM "pg"."shop"."public"."products")");

    const auto loose = rows::find_tables(view, "loose", "default", resource);
    REQUIRE(loose.size() == 1);
    CHECK(rows::select_all_query(*loose.front()) == R"(SELECT * FROM "loose")");

    rows::table_entry_t odd{std::pmr::string{"Shop", resource},
                            std::pmr::string{"we\"ird", resource},
                            std::pmr::vector<std::pmr::string>{resource},
                            false};
    odd.qualifier.emplace_back("Shop");
    CHECK(rows::select_all_query(odd) == R"(SELECT * FROM "Shop"."we""ird")");
}

// ── Row layouts PySpark reads by position ───────────────────────────────────

TEST_CASE("spark catalog: single values") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;

    const auto current = rows::string_payload(rows::default_database, resource);
    require_layout(current, {{"value", ct::logical_type::STRING_LITERAL}});
    REQUIRE(current.size() == 1);
    CHECK(text_at(current, 0, 0) == "default");

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
    CHECK(text_at(all, 0, 0) == "default");
    CHECK(text_at(all, 1, 0) == "otterstax");
    CHECK(null_at(all, 2, 0));
    CHECK(null_at(all, 3, 0));

    const auto matched = rows::list_databases(view, "m*|P*", resource);
    REQUIRE(matched.size() == 2);
    CHECK(text_at(matched, 0, 0) == "mysql");
    CHECK(text_at(matched, 0, 1) == "pg");

    auto shop = rows::get_database(view, "shop", resource);
    REQUIRE_FALSE(shop.has_error());
    require_layout(shop.value(), database_layout);
    REQUIRE(shop.value().size() == 1);
    CHECK(text_at(shop.value(), 0, 0) == "shop");

    auto missing = rows::get_database(view, "public", resource);
    REQUIRE(missing.has_error());
    CHECK(missing.error().type == core::error_code_t::database_not_exists);

    CHECK(rows::database_exists(view, "default"));
    CHECK(rows::database_exists(view, "empty_db"));
    CHECK_FALSE(rows::database_exists(view, "pg_catalog"));
}

TEST_CASE("spark catalog: Table rows") {
    std::pmr::synchronized_pool_resource pool;
    auto* resource = &pool;
    const auto view = fixture_view(resource);

    auto mysql = rows::list_tables(view, "mysql", std::nullopt, resource);
    REQUIRE_FALSE(mysql.has_error());
    const auto& payload = mysql.value();
    require_layout(payload, table_layout);
    CHECK(payload.schema.child_types()[2].child_type().type() == ct::logical_type::STRING_LITERAL);
    CHECK(payload.chunks.front().data[2].type().child_type().type() == ct::logical_type::STRING_LITERAL);
    REQUIRE(payload.size() == 1);
    CHECK(text_at(payload, 0, 0) == "payments");
    CHECK(text_at(payload, 1, 0) == "otterstax");
    CHECK(list_at(payload, 2, 0) == std::vector<std::string>{"mysql", "bill"});
    CHECK(null_at(payload, 3, 0));
    CHECK(text_at(payload, 4, 0) == "MANAGED");
    CHECK_FALSE(flag_at(payload, 5, 0));

    // Without a database: the current one, "default"; its namespace is empty, not NULL.
    auto current = rows::list_tables(view, std::nullopt, std::nullopt, resource);
    REQUIRE_FALSE(current.has_error());
    REQUIRE(current.value().size() == 1);
    CHECK(text_at(current.value(), 0, 0) == "loose");
    CHECK_FALSE(null_at(current.value(), 2, 0));
    CHECK(list_at(current.value(), 2, 0).empty());

    auto orders = rows::list_tables(view, "shop", "orders*", resource);
    REQUIRE_FALSE(orders.has_error());
    REQUIRE(orders.value().size() == 2);
    CHECK(text_at(orders.value(), 0, 0) == "orders");
    CHECK(text_at(orders.value(), 4, 0) == "MANAGED");
    CHECK(text_at(orders.value(), 0, 1) == "orders_by_day");
    CHECK(text_at(orders.value(), 4, 1) == "VIEW");

    // No row is still the whole layout, in one empty chunk.
    auto empty = rows::list_tables(view, "empty_db", std::nullopt, resource);
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
