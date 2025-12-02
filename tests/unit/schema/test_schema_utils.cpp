// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "../../../otterbrix/translators/input/mysql_to_chunk.hpp"
#include "../../../scheduler/schema_utils.hpp"

#include <catch2/catch.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components;
using namespace components::types;
using namespace schema_utils;

namespace {
    std::pair<logical_plan::node_ptr, logical_plan::parameter_node_ptr> parse(std::string sql) {
        auto* resource = std::pmr::get_default_resource();
        std::pmr::monotonic_buffer_resource arena_resource(resource);
        sql::transform::transformer transformer(resource);

        auto res = linitial(raw_parser(&arena_resource, sql.c_str()));
        auto transform_res = std::get<sql::transform::result_view>(
            transformer.transform(sql::transform::pg_cell_to_node_cast(res)).finalize());
        return {std::move(transform_res.node), std::move(transform_res.params)};
    }

    // map of "".test1 -> 1, "".test2 -> 2, etc...
    std::pmr::map<collection_full_name_t, size_t> fill_test(size_t n) {
        std::pmr::map<collection_full_name_t, size_t> dep;
        for (size_t i = 1; i <= n; ++i) {
            std::string name = "test" + std::to_string(i);
            dep.emplace(collection_full_name_t("", std::move(name)), i - 1);
        }

        return dep;
    }
} // namespace

TEST_CASE("aggregate: filter") {
    auto [node, params] = parse("SELECT id, name from test;");
    auto* resource = std::pmr::get_default_resource();
    std::vector<complex_logical_type> fields;
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_alias("id");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    fields.emplace_back(logical_type::FLOAT);
    fields.back().set_alias("dummy");
    auto schema = catalog::schema(resource, complex_logical_type::create_struct(fields));

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), schema);
    REQUIRE(filtered.child_types().size() == 2);
    REQUIRE(complex_logical_type::contains(filtered, logical_type::BIGINT));
    REQUIRE(complex_logical_type::contains(filtered, logical_type::STRING_LITERAL));
    REQUIRE_FALSE(complex_logical_type::contains(filtered, logical_type::FLOAT)); // dummy type
}

TEST_CASE("aggregate: constants & aggregations") {
    auto* resource = std::pmr::get_default_resource();
    auto schema = catalog::schema(resource, complex_logical_type::create_struct({}));
    {
        auto [node, params] = parse("SELECT 1, avg(smth) from test;");
        auto filtered =
            aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), schema);
        REQUIRE(filtered.child_types().size() == 2);
        REQUIRE(complex_logical_type::contains(filtered, [](const complex_logical_type& type) {
            return type.type() == logical_type::BIGINT && type.alias() == "1";
        }));
        REQUIRE(complex_logical_type::contains(filtered, logical_type::DOUBLE));
    }
    {
        auto [node, params] = parse("SELECT max(smth), count(smth), min(smth), max(smth), 'name' from test;");
        auto filtered =
            aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), schema);
        REQUIRE(filtered.child_types().size() == 5);
        for (size_t i = 0; i < 4; ++i) {
            REQUIRE(filtered.child_types()[i] == logical_type::BIGINT);
        }
        REQUIRE(filtered.child_types()[4] == logical_type::STRING_LITERAL);
        REQUIRE(filtered.child_types()[4].alias() == "name");
    }
}

TEST_CASE("join: simple") {
    auto [node, params] = parse("SELECT * from test1 cross join test2;");
    auto* resource = std::pmr::get_default_resource();
    std::vector<complex_logical_type> fields;
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_alias("id");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    auto struct_t = complex_logical_type::create_struct(fields);
    auto cursor = cursor::make_cursor(resource, {struct_t, struct_t});

    auto dependencies = fill_test(2);
    auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                               params.get(),
                                               std::move(cursor),
                                               dependencies);
    REQUIRE(joined_cur->type_data().size() == 1);

    auto joined = joined_cur->type_data()[0];
    REQUIRE(joined.type() == logical_type::STRUCT);
    REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
        return type.type() == logical_type::BIGINT && type.alias() == "id";
    }));
    REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
        return type.type() == logical_type::STRING_LITERAL && type.alias() == "name";
    }));
}

TEST_CASE("join: complex") {
    auto [node, params] = parse("SELECT * from test1 join test2 on x = y full outer join test3 on y = z;");
    auto* resource = std::pmr::get_default_resource();
    std::pmr::vector<complex_logical_type> catalog_vec;
    {
        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BIGINT);
        fields.back().set_alias("id");
        fields.emplace_back(logical_type::STRING_LITERAL);
        fields.back().set_alias("name");
        catalog_vec.emplace_back(complex_logical_type::create_struct(fields));
    }
    {
        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::FLOAT);
        fields.back().set_alias("value");
        fields.emplace_back(logical_type::DOUBLE);
        fields.back().set_alias("pi");
        catalog_vec.emplace_back(complex_logical_type::create_struct(fields));
    }
    {
        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BIGINT);
        fields.back().set_alias("id");
        fields.emplace_back(logical_type::BOOLEAN);
        fields.back().set_alias("is_something");
        catalog_vec.emplace_back(complex_logical_type::create_struct(fields));
    }

    auto dependencies = fill_test(3);
    SECTION("complex") {
        auto cursor = cursor::make_cursor(resource, std::move(catalog_vec));
        auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                                   params.get(),
                                                   std::move(cursor),
                                                   dependencies);
        REQUIRE(joined_cur->type_data().size() == 1);
        auto joined = joined_cur->type_data()[0];
        REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
            return type.type() == logical_type::BIGINT && type.alias() == "id";
        }));
        REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
            return type.type() == logical_type::STRING_LITERAL && type.alias() == "name";
        }));
        REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
            return type.type() == logical_type::FLOAT && type.alias() == "value";
        }));
        REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
            return type.type() == logical_type::DOUBLE && type.alias() == "pi";
        }));
        REQUIRE(complex_logical_type::contains(joined, [](const complex_logical_type& type) {
            return type.type() == logical_type::BOOLEAN && type.alias() == "is_something";
        }));
    }

    SECTION("NA propagation") {
        catalog_vec.erase(catalog_vec.begin());
        auto cursor_missing = cursor::make_cursor(resource, std::move(catalog_vec));
        auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                                   params.get(),
                                                   std::move(cursor_missing),
                                                   dependencies);
        REQUIRE(joined_cur->is_error());
    }
}