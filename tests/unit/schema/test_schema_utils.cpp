// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"

#include <catch2/catch_all.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <memory_resource>

using namespace components;
using namespace components::types;
using namespace schema_utils;

namespace {
    std::pair<logical_plan::node_ptr, logical_plan::parameter_node_ptr> parse(std::string sql) {
        auto* resource = std::pmr::new_delete_resource();
        std::pmr::monotonic_buffer_resource arena_resource(resource);
        sql::transform::transformer transformer(resource);

        auto res = linitial(raw_parser(&arena_resource, sql.c_str()));
        auto transform_res = transformer.transform(sql::transform::pg_cell_to_node_cast(res)).finalize();
        REQUIRE_FALSE(transform_res.has_error());
        auto& plan = transform_res.value();
        // a13 wraps table-referencing statements in a node_sequence_t
        // (catalog_resolve_* siblings + the consumer as the LAST child); the
        // tests exercise the aggregate consumer itself.
        auto node = plan.sub_queries.back();
        if (node->type() == logical_plan::node_type::sequence_t && !node->children().empty()) {
            node = node->children().back();
        }
        return {std::move(node), std::move(plan.parameters)};
    }

    // The column names of a schema in the order it holds them. The order is what
    // a frontend's RowDescription names and a client decodes a row by, so it is
    // asserted as a whole rather than by membership.
    std::vector<std::string> aliases(const complex_logical_type& schema) {
        std::vector<std::string> names;
        names.reserve(schema.child_types().size());
        for (const auto& column : schema.child_types()) {
            names.push_back(column.alias());
        }
        return names;
    }

    complex_logical_type named_column(logical_type type, const std::string& name) {
        complex_logical_type column{type};
        column.set_alias(name);
        return column;
    }

    // map of "".test1 -> 1, "".test2 -> 2, etc...
    std::pmr::map<qualified_name_t, size_t> fill_test(size_t n) {
        std::pmr::map<qualified_name_t, size_t> dep(std::pmr::new_delete_resource());
        for (size_t i = 1; i <= n; ++i) {
            std::string name = "test" + std::to_string(i);
            dep.emplace(qualified_name_t("", std::move(name)), i - 1);
        }

        return dep;
    }
} // namespace

TEST_CASE("aggregate: filter") {
    auto [node, params] = parse("SELECT id, name from test;");
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_alias("id");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    fields.emplace_back(logical_type::FLOAT);
    fields.back().set_alias("dummy");
    auto schema_types = fields;

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), schema_types);
    REQUIRE(filtered.child_types().size() == 2);
    REQUIRE(complex_logical_type::contains(filtered, logical_type::BIGINT));
    REQUIRE(complex_logical_type::contains(filtered, logical_type::STRING_LITERAL));
    REQUIRE_FALSE(complex_logical_type::contains(filtered, logical_type::FLOAT)); // dummy type
}

TEST_CASE("aggregate: constants & aggregations") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> schema_types(resource); // empty schema
    {
        auto [node, params] = parse("SELECT 1, avg(smth) from test;");
        auto filtered = aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                                params.get(),
                                                schema_types);
        REQUIRE(filtered.child_types().size() == 2);
        REQUIRE(complex_logical_type::contains(filtered, logical_type::BIGINT));
        REQUIRE(complex_logical_type::contains(filtered, logical_type::DOUBLE));
    }
    {
        auto [node, params] = parse("SELECT max(smth), count(smth), min(smth), max(smth), 'name' from test;");
        auto filtered = aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                                params.get(),
                                                schema_types);
        REQUIRE(filtered.child_types().size() == 5);
        // The schema is empty here, so MIN / MAX cannot resolve the argument column whose
        // type they answer with and say so with NA. COUNT reads no argument type at all —
        // its kernel output type is fixed (UBIGINT) — so it is typed whatever the schema is.
        REQUIRE(filtered.child_types()[0] == logical_type::NA);      // max(smth)
        REQUIRE(filtered.child_types()[1] == logical_type::UBIGINT); // count(smth)
        REQUIRE(filtered.child_types()[2] == logical_type::NA);      // min(smth)
        REQUIRE(filtered.child_types()[3] == logical_type::NA);      // max(smth)
        REQUIRE(filtered.child_types()[4] == logical_type::STRING_LITERAL);
    }
}

// The projected type of an aggregate has to be the type its executor puts in the chunk. For the
// local engine that executor is the otterbrix compute kernel, and its registry is the authority:
//   count / COUNT(*)  components/compute/kernels/aggregate.cpp:503,508 — output_type::fixed(UBIGINT),
//                     with count_finalize (:401) writing through output.data<uint64_t>()
//   sum, min, max, avg :443, :462, :481, :527 — output_type::computed(same_type_resolver(0)),
//                     i.e. the argument column's own type
// COUNT is the one whose kernel type this computation can adopt for every executor at once: a count
// is a non-negative 64-bit number on the local engine (UBIGINT), on MySQL (BIGINT UNSIGNED, which
// mysql_to_chunk:221 reads as UBIGINT) and on ClickHouse (UInt64), and PostgreSQL's signed int8
// carries the same value in the same width. SUM and AVG have no such single answer — see the
// comment on type_for_call in otterbrix/schema/schema_utils.cpp.
TEST_CASE("aggregate: COUNT is typed as the UBIGINT the engine's count kernel returns") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::INTEGER);
    fields.back().set_alias("x");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");

    SECTION("COUNT(*) and count(column) alike") {
        auto [node, params] = parse("SELECT count(*) AS all_rows, count(x) AS some_rows from test;");
        auto filtered =
            aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);
        REQUIRE(filtered.child_types().size() == 2);
        CHECK(filtered.child_types()[0].alias() == "all_rows");
        CHECK(filtered.child_types()[0].type() == logical_type::UBIGINT);
        CHECK(filtered.child_types()[1].alias() == "some_rows");
        CHECK(filtered.child_types()[1].type() == logical_type::UBIGINT);
    }
    SECTION("a count resolved for HAVING is typed the same way") {
        auto [node, params] = parse("SELECT name, count(x) AS cnt from test group by name having count(x) > 1;");
        auto filtered =
            aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);
        REQUIRE(filtered.child_types().size() == 2);
        CHECK(filtered.child_types()[1].alias() == "cnt");
        CHECK(filtered.child_types()[1].type() == logical_type::UBIGINT);
    }
}

// MIN / MAX answer their argument column's type (same_type_resolver above), so an argument this
// computation cannot resolve to a column leaves the output type unknown. NA is how the schema says
// "no type this computation can name" — the same answer it already gives a projected cast or
// comparison — rather than a BIGINT nothing promised.
TEST_CASE("aggregate: MIN / MAX over an argument outside the schema is NA") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    auto [node, params] = parse("SELECT min(absent) AS lo, max(absent) AS hi from test;");

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);

    REQUIRE(filtered.child_types().size() == 2);
    CHECK(filtered.child_types()[0].type() == logical_type::NA);
    CHECK(filtered.child_types()[1].type() == logical_type::NA);
}

// A remote result column aliased with a source column's name is the aggregate, not that column: its
// prepared type must be the function's, as the backend answers it.
TEST_CASE("aggregate: an aggregate aliased as a source column is typed by its function") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::INTEGER);
    fields.back().set_alias("x");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    auto [node, params] =
        parse("SELECT avg(x) AS x, count(name) AS name, x AS y, min(x) AS x_min, max(name) AS name_max from test;");

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);

    REQUIRE(filtered.child_types().size() == 5);
    REQUIRE(filtered.child_types()[0].alias() == "x");
    REQUIRE(filtered.child_types()[0].type() == logical_type::DOUBLE);
    REQUIRE(filtered.child_types()[1].alias() == "name");
    REQUIRE(filtered.child_types()[1].type() == logical_type::UBIGINT);
    // A plain column under another name keeps the column's type.
    REQUIRE(filtered.child_types()[2].alias() == "y");
    REQUIRE(filtered.child_types()[2].type() == logical_type::INTEGER);
    // MIN / MAX answer their argument column's type.
    REQUIRE(filtered.child_types()[3].alias() == "x_min");
    REQUIRE(filtered.child_types()[3].type() == logical_type::INTEGER);
    REQUIRE(filtered.child_types()[4].alias() == "name_max");
    REQUIRE(filtered.child_types()[4].type() == logical_type::STRING_LITERAL);
}

// MIN / MAX over a source column keep answering that column's type when aliased with its name.
TEST_CASE("aggregate: MIN aliased as its argument column keeps the column's type") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::INTEGER);
    fields.back().set_alias("x");
    auto [node, params] = parse("SELECT min(x) AS x from test;");

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);

    REQUIRE(filtered.child_types().size() == 1);
    REQUIRE(filtered.child_types()[0].alias() == "x");
    REQUIRE(filtered.child_types()[0].type() == logical_type::INTEGER);
}

// The projection is read off the group node, where the transformer puts the whole SELECT list. Two
// of its entries are the engine's own and no column of the answer: the marker naming the grouping
// key, and the hidden __having_* aggregate HAVING resolves against.
TEST_CASE("aggregate: grouping-key markers and hidden HAVING outputs are not columns") {
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_alias("id");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    fields.emplace_back(logical_type::DOUBLE);
    fields.back().set_alias("price");
    auto [node, params] = parse("SELECT id, count(name) AS cnt from test group by id having sum(price) > 1;");

    auto filtered =
        aggregate_filter_schema(static_cast<const logical_plan::node_aggregate_t&>(*node), params.get(), fields);

    REQUIRE(filtered.child_types().size() == 2);
    REQUIRE(filtered.child_types()[0].alias() == "id");
    REQUIRE(filtered.child_types()[0].type() == logical_type::BIGINT);
    REQUIRE(filtered.child_types()[1].alias() == "cnt");
    REQUIRE(filtered.child_types()[1].type() == logical_type::UBIGINT);
}

// A federated sub-query the SQL generator cannot write is lifted out of the
// statement as raw text, and the backend describes its result columns onto that
// stub (ClickhouseManager::describe). The stub IS the input relation of the
// aggregate above it — the transformer's derived-table wrapper, which names no
// table — so the projection is resolved against the described columns. While the
// stub is undescribed there is nothing to resolve against, and the schema stays
// unanswered rather than being invented.
TEST_CASE("aggregate: a described sub-query stub types the projection above it") {
    auto* resource = std::pmr::new_delete_resource();
    auto [node, params] = parse("SELECT id FROM test;");
    auto& agg = static_cast<logical_plan::node_aggregate_t&>(*node);

    std::pmr::vector<otterstax::parser::qualifier_rewrite_t> no_qualifiers(resource);
    auto stub = make_node_schema_raw(resource,
                                     qualified_name_t("ch", "subq", "", "__otterstax_subq_0"),
                                     "SELECT toString(id) AS id FROM ch.db.schema.t",
                                     no_qualifiers);
    agg.append_child(stub);

    SECTION("undescribed: no columns to resolve the projection against") {
        auto computed = compute_otterbrix_schema(agg,
                                                 params.get(),
                                                 cursor::make_cursor(resource),
                                                 std::pmr::map<qualified_name_t, size_t>(resource));
        REQUIRE(computed->is_error());
    }

    SECTION("described: the projection carries the backend's type") {
        std::pmr::vector<complex_logical_type> described(resource);
        described.emplace_back(logical_type::STRING_LITERAL);
        described.back().set_alias("id");
        stub->set_schema(complex_logical_type::create_struct("", described));

        auto computed = compute_otterbrix_schema(agg,
                                                 params.get(),
                                                 cursor::make_cursor(resource),
                                                 std::pmr::map<qualified_name_t, size_t>(resource));
        REQUIRE_FALSE(computed->is_error());
        REQUIRE(computed->type_data().size() == 1);
        const auto& schema = computed->type_data()[0];
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        REQUIRE(schema.child_types()[0].alias() == "id");
        // The base column `id` is a BIGINT in every catalog; the sub-query's is
        // the backend's String, and that is the one the client is told.
        REQUIRE(schema.child_types()[0].type() == logical_type::STRING_LITERAL);
    }
}

// The merged order is a contract with the client: a RowDescription names the
// columns in it and the client decodes the row by those positions. It is the
// order SQL itself would name them in — the left side's columns in their own
// order, then the right side's — and a name already taken is not taken again.
TEST_CASE("merge_schemas: the left side comes first and a name is not repeated") {
    auto* resource = std::pmr::new_delete_resource();

    std::pmr::vector<complex_logical_type> left_fields(resource);
    left_fields.push_back(named_column(logical_type::BIGINT, "id"));
    left_fields.push_back(named_column(logical_type::STRING_LITERAL, "name"));
    auto left = complex_logical_type::create_struct("", left_fields);

    std::pmr::vector<complex_logical_type> right_fields(resource);
    right_fields.push_back(named_column(logical_type::INTEGER, "id"));
    right_fields.push_back(named_column(logical_type::DOUBLE, "weight"));
    auto right = complex_logical_type::create_struct("", right_fields);

    SECTION("left then right") {
        auto merged = merge_schemas(left, right);
        REQUIRE(merged.type() == logical_type::STRUCT);
        CHECK(aliases(merged) == std::vector<std::string>{"id", "name", "weight"});
        // The name was taken by the left side, so it keeps the left side's type.
        CHECK(merged.child_types()[0].type() == logical_type::BIGINT);
    }

    SECTION("the argument order is the answer") {
        auto merged = merge_schemas(right, left);
        CHECK(aliases(merged) == std::vector<std::string>{"id", "weight", "name"});
        CHECK(merged.child_types()[0].type() == logical_type::INTEGER);
    }

    SECTION("a name repeated inside one side is one column") {
        std::pmr::vector<complex_logical_type> twice(resource);
        twice.push_back(named_column(logical_type::BIGINT, "id"));
        twice.push_back(named_column(logical_type::STRING_LITERAL, "tag"));
        twice.push_back(named_column(logical_type::DOUBLE, "id"));
        auto merged = merge_schemas(complex_logical_type::create_struct("", twice), right);
        CHECK(aliases(merged) == std::vector<std::string>{"id", "tag", "weight"});
    }

    SECTION("something that is not a struct has no merge") {
        CHECK(merge_schemas(left, complex_logical_type{logical_type::BIGINT}).type() == logical_type::NA);
        CHECK(merge_schemas(complex_logical_type{logical_type::BIGINT}, right).type() == logical_type::NA);
    }
}

// The same order through the whole join computation: the sides are merged as the
// sides they are, the left child of a join node being the left side of the SQL.
TEST_CASE("join: the left side's columns are named before the right side's") {
    auto [node, params] = parse("SELECT * from test1 cross join test2;");
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> catalog_vec(resource);
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.push_back(named_column(logical_type::BIGINT, "id"));
        fields.push_back(named_column(logical_type::STRING_LITERAL, "name"));
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.push_back(named_column(logical_type::FLOAT, "value"));
        fields.push_back(named_column(logical_type::DOUBLE, "pi"));
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }

    auto dependencies = fill_test(2);
    auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                               params.get(),
                                               cursor::make_cursor(resource, std::move(catalog_vec)),
                                               dependencies);
    REQUIRE_FALSE(joined_cur->is_error());
    REQUIRE(joined_cur->type_data().size() == 1);
    CHECK(aliases(joined_cur->type_data()[0]) == std::vector<std::string>{"id", "name", "value", "pi"});
}

// The shape tests/test_pg_extended_protocol.py describes on the wire: two tables
// sharing every column name are one description of two columns, named in the
// left side's order.
TEST_CASE("join: two sides sharing every column name are one description") {
    auto [node, params] = parse("SELECT * from test1 join test2 on x = y;");
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.push_back(named_column(logical_type::BIGINT, "id"));
    fields.push_back(named_column(logical_type::STRING_LITERAL, "name"));
    auto struct_t = complex_logical_type::create_struct("", fields);
    std::pmr::vector<complex_logical_type> catalog_vec(resource);
    catalog_vec.push_back(struct_t);
    catalog_vec.push_back(struct_t);

    auto dependencies = fill_test(2);
    auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                               params.get(),
                                               cursor::make_cursor(resource, std::move(catalog_vec)),
                                               dependencies);
    REQUIRE_FALSE(joined_cur->is_error());
    REQUIRE(joined_cur->type_data().size() == 1);
    CHECK(aliases(joined_cur->type_data()[0]) == std::vector<std::string>{"id", "name"});
}

// A third side is a join node under the left child, so the recursion has to keep
// the same order: the whole left sub-tree first, then the side joined onto it.
TEST_CASE("join: three sides are merged left to right") {
    auto [node, params] = parse("SELECT * from test1 join test2 on x = y full outer join test3 on y = z;");
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> catalog_vec(resource);
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.push_back(named_column(logical_type::BIGINT, "id"));
        fields.push_back(named_column(logical_type::STRING_LITERAL, "name"));
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.push_back(named_column(logical_type::FLOAT, "value"));
        fields.push_back(named_column(logical_type::DOUBLE, "pi"));
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.push_back(named_column(logical_type::BIGINT, "id"));
        fields.push_back(named_column(logical_type::BOOLEAN, "is_something"));
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }

    auto dependencies = fill_test(3);
    auto joined_cur = compute_otterbrix_schema(static_cast<const logical_plan::node_aggregate_t&>(*node),
                                               params.get(),
                                               cursor::make_cursor(resource, std::move(catalog_vec)),
                                               dependencies);
    REQUIRE_FALSE(joined_cur->is_error());
    REQUIRE(joined_cur->type_data().size() == 1);
    // test3's `id` is the name test1 already took, so it is not named twice.
    CHECK(aliases(joined_cur->type_data()[0]) ==
          std::vector<std::string>{"id", "name", "value", "pi", "is_something"});
}

TEST_CASE("join: simple") {
    auto [node, params] = parse("SELECT * from test1 cross join test2;");
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> fields(resource);
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_alias("id");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    auto struct_t = complex_logical_type::create_struct("", fields);
    std::pmr::vector<complex_logical_type> cursor_types(resource);
    cursor_types.push_back(struct_t);
    cursor_types.push_back(struct_t);
    auto cursor = cursor::make_cursor(resource, std::move(cursor_types));

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
    auto* resource = std::pmr::new_delete_resource();
    std::pmr::vector<complex_logical_type> catalog_vec(resource);
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT);
        fields.back().set_alias("id");
        fields.emplace_back(logical_type::STRING_LITERAL);
        fields.back().set_alias("name");
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::FLOAT);
        fields.back().set_alias("value");
        fields.emplace_back(logical_type::DOUBLE);
        fields.back().set_alias("pi");
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
    }
    {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT);
        fields.back().set_alias("id");
        fields.emplace_back(logical_type::BOOLEAN);
        fields.back().set_alias("is_something");
        catalog_vec.emplace_back(complex_logical_type::create_struct("", fields));
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
