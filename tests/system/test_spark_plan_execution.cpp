// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Spark Connect DataFrame plans (Path B) over the real engine. Each case builds
// the spark::connect::Plan a PySpark client sends, translates it with
// frontend::spark::relation_to_plan and runs it through Scheduler::execute_plan
// the way the ExecutePlan RPC does, then checks the ROWS it answers rather than
// the shape of the plan: a plan the translator shapes wrongly is one the engine
// answers wrongly, refuses, or never reaches.
//
// The tables are created and filled through Scheduler::execute SQL. Every case
// owns its engine (a data dir of its own, removed with the stack) and its own
// database.

#include "scheduler_stack.hpp"

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"

#include <spark/connect/base.pb.h>
#include <spark/connect/expressions.pb.h>
#include <spark/connect/relations.pb.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

using otterstax::test::await_session;
using otterstax::test::run_scheduler_sql;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

namespace {

    namespace sc = ::spark::connect;
    namespace ct = components::types;

    using direction_t = sc::Expression::SortOrder::SortDirection;

    void run_or_fail(const scheduler_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_scheduler_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // <db>.sales: six rows over three regions whose sums and means are exact
    // integers (north 10 + 30 + 50, south 20 + 60, east 40), then `extra_rows`.
    void
    seed_sales(const scheduler_stack& s, session_hash_t& id, const std::string& db, const std::string& extra_rows) {
        run_or_fail(s, id++, "CREATE DATABASE " + db + ";");
        run_or_fail(s, id++, "CREATE TABLE " + db + ".sales (id bigint, region string, amount bigint);");
        run_or_fail(s,
                    id++,
                    "INSERT INTO " + db +
                        ".sales (id, region, amount) VALUES (1, 'north', 10), (2, 'south', 20), (3, 'north', 30), "
                        "(4, 'east', 40), (5, 'south', 60), (6, 'north', 50)" +
                        extra_rows + ";");
    }

    // The amount of each seeded row, by id.
    constexpr int64_t amount_of[] = {0, 10, 20, 30, 40, 60, 50};

    sc::Relation read_table(const std::string& identifier) {
        sc::Relation rel;
        rel.mutable_read()->mutable_named_table()->set_unparsed_identifier(identifier);
        return rel;
    }

    sc::Expression column(const std::string& name) {
        sc::Expression expr;
        expr.mutable_unresolved_attribute()->set_unparsed_identifier(name);
        return expr;
    }

    sc::Expression long_literal(int64_t value) {
        sc::Expression expr;
        expr.mutable_literal()->set_long_(value);
        return expr;
    }

    sc::Expression string_literal(const std::string& value) {
        sc::Expression expr;
        expr.mutable_literal()->set_string(value);
        return expr;
    }

    // `name(args...)`, as PySpark's Column operators and functions send it.
    sc::Expression call(const std::string& name, std::initializer_list<sc::Expression> args) {
        sc::Expression expr;
        auto* fn = expr.mutable_unresolved_function();
        fn->set_function_name(name);
        for (const auto& arg : args) {
            *fn->add_arguments() = arg;
        }
        return expr;
    }

    sc::Expression aliased(const sc::Expression& child, const std::string& name) {
        sc::Expression expr;
        *expr.mutable_alias()->mutable_expr() = child;
        expr.mutable_alias()->add_name(name);
        return expr;
    }

    sc::Expression star() {
        sc::Expression expr;
        expr.mutable_unresolved_star();
        return expr;
    }

    sc::Relation filter(const sc::Relation& input, const sc::Expression& condition) {
        sc::Relation rel;
        *rel.mutable_filter()->mutable_input() = input;
        *rel.mutable_filter()->mutable_condition() = condition;
        return rel;
    }

    sc::Relation project(const sc::Relation& input, std::initializer_list<sc::Expression> expressions) {
        sc::Relation rel;
        *rel.mutable_project()->mutable_input() = input;
        for (const auto& expr : expressions) {
            *rel.mutable_project()->add_expressions() = expr;
        }
        return rel;
    }

    sc::Relation limit(const sc::Relation& input, int32_t n) {
        sc::Relation rel;
        *rel.mutable_limit()->mutable_input() = input;
        rel.mutable_limit()->set_limit(n);
        return rel;
    }

    // groupBy(keys).agg(aggregates); no keys is df.agg() / groupBy().agg().
    sc::Relation aggregate(const sc::Relation& input,
                           std::initializer_list<sc::Expression> keys,
                           std::initializer_list<sc::Expression> aggregates) {
        sc::Relation rel;
        auto* agg = rel.mutable_aggregate();
        *agg->mutable_input() = input;
        agg->set_group_type(sc::Aggregate::GROUP_TYPE_GROUPBY);
        for (const auto& key : keys) {
            *agg->add_grouping_expressions() = key;
        }
        for (const auto& output : aggregates) {
            *agg->add_aggregate_expressions() = output;
        }
        return rel;
    }

    // df.count(), as PySpark sends it: count(lit(1)) with no grouping key.
    sc::Relation count_rows(const sc::Relation& input) {
        return aggregate(input, {}, {call("count", {long_literal(1)})});
    }

    sc::Relation distinct(const sc::Relation& input) {
        sc::Relation rel;
        *rel.mutable_deduplicate()->mutable_input() = input;
        rel.mutable_deduplicate()->set_all_columns_as_keys(true);
        return rel;
    }

    // spark.sql(query) as the leaf of a DataFrame chain.
    sc::Relation sql_leaf(const std::string& query) {
        sc::Relation rel;
        rel.mutable_sql()->set_query(query);
        return rel;
    }

    // orderBy on one column, the NULL placement left to Spark's default.
    sc::Relation sort_by(const sc::Relation& input, const std::string& name, direction_t direction) {
        sc::Relation rel;
        *rel.mutable_sort()->mutable_input() = input;
        auto* order = rel.mutable_sort()->add_order();
        *order->mutable_child() = column(name);
        order->set_direction(direction);
        return rel;
    }

    // relation_to_plan, then Scheduler::execute_plan awaited through the asio
    // bridge: the ExecutePlan RPC's path. A translation refusal comes back as the
    // error it is.
    core::result_wrapper_t<session_payload>
    run_plan(const scheduler_stack& s, session_hash_t id, const sc::Relation& root) {
        sc::Plan plan;
        *plan.mutable_root() = root;
        auto translated = frontend::spark::relation_to_plan(plan, s.resource);
        if (translated.has_error()) {
            return translated.convert_error<session_payload>();
        }
        auto [needs_sched, future] =
            actor_zeta::send(s.scheduler, &Scheduler::execute_plan, id, std::move(translated.value().parsed_data));
        return await_session(std::move(future), std::chrono::milliseconds(10000), s.resource);
    }

    session_payload run_plan_or_fail(const scheduler_stack& s, session_hash_t id, const sc::Relation& root) {
        auto result = run_plan(s, id, root);
        INFO("plan error: " << result.error().what.c_str());
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    }

    // The index of the result column named `name`; a column without a name
    // matches nothing (its alias is read behind has_alias()).
    size_t column_of(const components::vector::data_chunk_t& chunk, std::string_view name) {
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            const auto& type = chunk.data[i].type();
            if (type.has_alias() && type.alias() == name) {
                return i;
            }
        }
        FAIL("the result has no column named " << name);
        return 0;
    }

    // An integral cell as int64: count answers UBIGINT, sum and max keep their
    // argument's BIGINT, avg answers DOUBLE (every seeded mean is exact).
    int64_t as_int64(const ct::logical_value_t& value) {
        REQUIRE_FALSE(value.is_null());
        switch (value.type().type()) {
            case ct::logical_type::BIGINT:
                return value.value<int64_t>();
            case ct::logical_type::UBIGINT:
                return static_cast<int64_t>(value.value<uint64_t>());
            case ct::logical_type::INTEGER:
                return value.value<int32_t>();
            case ct::logical_type::DOUBLE:
                return static_cast<int64_t>(value.value<double>());
            default:
                FAIL("unexpected result type " << static_cast<int>(value.type().type()));
                return 0;
        }
    }

    // The integral column `name`, row by row across every chunk.
    std::pmr::vector<int64_t>
    int_column(const session_payload& payload, std::string_view name, std::pmr::memory_resource* resource) {
        std::pmr::vector<int64_t> values(resource);
        for (const auto& chunk : payload.chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            const auto index = column_of(chunk, name);
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                values.push_back(as_int64(chunk.value(index, row)));
            }
        }
        return values;
    }

    std::pmr::vector<int64_t> sorted(std::pmr::vector<int64_t> values) {
        std::sort(values.begin(), values.end());
        return values;
    }

    std::pmr::vector<int64_t> ints(std::initializer_list<int64_t> values, std::pmr::memory_resource* resource) {
        return std::pmr::vector<int64_t>(values, resource);
    }

} // namespace

TEST_CASE("Spark plan: Read and Project answer every row under the projected names", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_project", [](scheduler_stack s) {
        session_hash_t id = 12100;
        seed_sales(s, id, "spark_project", "");
        const auto sales = read_table("spark_project.sales");

        // spark.table("spark_project.sales")
        auto all = run_plan_or_fail(s, id++, sales);
        CHECK(all.column_count() == 3);
        CHECK(sorted(int_column(all, "id", s.resource)) == ints({1, 2, 3, 4, 5, 6}, s.resource));

        // df.select("id", F.col("amount").alias("amt"))
        auto projected = run_plan_or_fail(s, id++, project(sales, {column("id"), aliased(column("amount"), "amt")}));
        CHECK(projected.column_count() == 2);
        const auto ids = int_column(projected, "id", s.resource);
        const auto amounts = int_column(projected, "amt", s.resource);
        REQUIRE(ids.size() == 6);
        REQUIRE(amounts.size() == 6);
        for (size_t row = 0; row < ids.size(); ++row) {
            INFO("id " << ids[row]);
            REQUIRE(ids[row] >= 1);
            REQUIRE(ids[row] <= 6);
            CHECK(amounts[row] == amount_of[ids[row]]);
        }
    });
}

TEST_CASE("Spark plan: structured filters keep exactly the matching rows", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_filter", [](scheduler_stack s) {
        session_hash_t id = 12200;
        seed_sales(s, id, "spark_filter", "");
        const auto sales = read_table("spark_filter.sales");
        auto* resource = s.resource;

        // df.filter(df.amount > 25)
        auto gt = run_plan_or_fail(s, id++, filter(sales, call(">", {column("amount"), long_literal(25)})));
        CHECK(sorted(int_column(gt, "id", resource)) == ints({3, 4, 5, 6}, resource));

        // df.filter(F.lit(25) < df.amount): the literal first
        auto mirrored = run_plan_or_fail(s, id++, filter(sales, call("<", {long_literal(25), column("amount")})));
        CHECK(sorted(int_column(mirrored, "id", resource)) == ints({3, 4, 5, 6}, resource));

        // df.filter(df.region.isin("north", "east"))
        auto isin = run_plan_or_fail(
            s,
            id++,
            filter(sales, call("in", {column("region"), string_literal("north"), string_literal("east")})));
        CHECK(sorted(int_column(isin, "id", resource)) == ints({1, 3, 4, 6}, resource));

        // df.filter(df.amount.between(20, 40))
        auto between =
            run_plan_or_fail(s,
                             id++,
                             filter(sales, call("between", {column("amount"), long_literal(20), long_literal(40)})));
        CHECK(sorted(int_column(between, "id", resource)) == ints({2, 3, 4}, resource));

        // df.filter(df.amount > 15).filter(df.region == "north"): both conditions hold
        auto stacked = run_plan_or_fail(s,
                                        id++,
                                        filter(filter(sales, call(">", {column("amount"), long_literal(15)})),
                                               call("==", {column("region"), string_literal("north")})));
        CHECK(sorted(int_column(stacked, "id", resource)) == ints({3, 6}, resource));

        // df.select("id", "amount").filter(df.amount > 25): a filter over a select of plain columns
        auto over_select = run_plan_or_fail(
            s,
            id++,
            filter(project(sales, {column("id"), column("amount")}), call(">", {column("amount"), long_literal(25)})));
        CHECK(over_select.column_count() == 2);
        CHECK(sorted(int_column(over_select, "id", resource)) == ints({3, 4, 5, 6}, resource));

        // spark.sql("... WHERE region = 'north'").filter(df.amount > 15): the
        // fragment's constant and the filter's are bound in one parameter node
        sc::Relation sql;
        sql.mutable_sql()->set_query("SELECT * FROM spark_filter.sales WHERE region = 'north'");
        auto over_sql = run_plan_or_fail(s, id++, filter(sql, call(">", {column("amount"), long_literal(15)})));
        CHECK(sorted(int_column(over_sql, "id", resource)) == ints({3, 6}, resource));
    });
}

TEST_CASE("Spark plan: groupBy agg answers one row per group with count sum and avg", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_aggregate", [](scheduler_stack s) {
        session_hash_t id = 12300;
        seed_sales(s, id, "spark_agg", "");

        // df.groupBy("region").agg(F.count("*").alias("n"), F.sum("amount").alias("total"),
        //                          F.avg("amount").alias("mean"), F.max("amount"))
        sc::Relation rel;
        auto* agg = rel.mutable_aggregate();
        *agg->mutable_input() = read_table("spark_agg.sales");
        agg->set_group_type(sc::Aggregate::GROUP_TYPE_GROUPBY);
        *agg->add_grouping_expressions() = column("region");
        *agg->add_aggregate_expressions() = aliased(call("count", {star()}), "n");
        *agg->add_aggregate_expressions() = aliased(call("sum", {column("amount")}), "total");
        *agg->add_aggregate_expressions() = aliased(call("avg", {column("amount")}), "mean");
        *agg->add_aggregate_expressions() = call("max", {column("amount")});

        auto payload = run_plan_or_fail(s, id++, rel);
        REQUIRE(payload.size() == 3);
        REQUIRE(payload.column_count() == 5);

        size_t seen = 0;
        for (const auto& chunk : payload.chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            const auto region = column_of(chunk, "region");
            const auto n = column_of(chunk, "n");
            const auto total = column_of(chunk, "total");
            const auto mean = column_of(chunk, "mean");
            // An aggregate without an alias is named by its function.
            const auto max = column_of(chunk, "max");
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                const auto region_value = chunk.value(region, row);
                const std::string name{region_value.value<std::string_view>()};
                INFO("region " << name);
                const int64_t got[] = {as_int64(chunk.value(n, row)),
                                       as_int64(chunk.value(total, row)),
                                       as_int64(chunk.value(mean, row)),
                                       as_int64(chunk.value(max, row))};
                if (name == "north") {
                    CHECK(got[0] == 3);
                    CHECK(got[1] == 90);
                    CHECK(got[2] == 30);
                    CHECK(got[3] == 50);
                } else if (name == "south") {
                    CHECK(got[0] == 2);
                    CHECK(got[1] == 80);
                    CHECK(got[2] == 40);
                    CHECK(got[3] == 60);
                } else if (name == "east") {
                    CHECK(got[0] == 1);
                    CHECK(got[1] == 40);
                    CHECK(got[2] == 40);
                    CHECK(got[3] == 40);
                } else {
                    FAIL("unexpected region " << name);
                }
                ++seen;
            }
        }
        CHECK(seen == 3);
    });
}

TEST_CASE("Spark plan: avg of an integer column answers DOUBLE and keeps the fraction", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_avg_fraction", [](scheduler_stack s) {
        session_hash_t id = 12350;
        // east becomes 40 and 45, a mean of 42.5; all seven rows average 255 / 7. The
        // engine's avg over BIGINT would answer 42 and 36.
        seed_sales(s, id, "spark_avg", ", (7, 'east', 45)");
        const auto sales = read_table("spark_avg.sales");

        auto mean_of = [](const session_payload& payload, std::string_view region) -> double {
            for (const auto& chunk : payload.chunks) {
                const auto mean = column_of(chunk, "mean");
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (!region.empty() && chunk.value(column_of(chunk, "region"), row).value<std::string_view>() !=
                                               region) {
                        continue;
                    }
                    const auto value = chunk.value(mean, row);
                    REQUIRE(value.type().type() == ct::logical_type::DOUBLE);
                    return value.value<double>();
                }
            }
            FAIL("no mean for region '" << region << "'");
            return 0;
        };

        // df.groupBy("region").agg(F.avg("amount").alias("mean"))
        auto per_region = run_plan_or_fail(
            s,
            id++,
            aggregate(sales, {column("region")}, {aliased(call("avg", {column("amount")}), "mean")}));
        REQUIRE(per_region.size() == 3);
        CHECK(mean_of(per_region, "east") == Catch::Approx(42.5));
        CHECK(mean_of(per_region, "north") == Catch::Approx(30.0));
        CHECK(mean_of(per_region, "south") == Catch::Approx(40.0));

        // df.agg(F.avg("amount").alias("mean"))
        auto overall =
            run_plan_or_fail(s, id++, aggregate(sales, {}, {aliased(call("avg", {column("amount")}), "mean")}));
        REQUIRE(overall.size() == 1);
        CHECK(mean_of(overall, "") == Catch::Approx(255.0 / 7.0));
    });
}

TEST_CASE("Spark plan: orderBy then limit answers the top rows with Spark NULL placement", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_sort_limit", [](scheduler_stack s) {
        session_hash_t id = 12400;
        // A row without an amount: Spark sorts NULLs first ascending and last
        // descending, the opposite of the SQL default.
        seed_sales(s, id, "spark_sort", ", (7, 'west', NULL)");
        const auto sales = read_table("spark_sort.sales");

        // df.orderBy(df.amount.desc()).limit(2)
        auto desc =
            run_plan_or_fail(s,
                             id++,
                             limit(sort_by(sales, "amount", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING), 2));
        CHECK(int_column(desc, "id", s.resource) == ints({5, 6}, s.resource));

        // df.orderBy(df.amount.asc()).limit(2)
        auto asc =
            run_plan_or_fail(s,
                             id++,
                             limit(sort_by(sales, "amount", sc::Expression::SortOrder::SORT_DIRECTION_ASCENDING), 2));
        CHECK(int_column(asc, "id", s.resource) == ints({7, 1}, s.resource));

        // df.orderBy(df.amount.desc()).limit(3).filter(df.amount < 60): the
        // filter must not run before the limit, so it reads a derived table of
        // the top three rows (60, 50, 40) and keeps their order.
        auto top_filtered = run_plan_or_fail(
            s,
            id++,
            filter(limit(sort_by(sales, "amount", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING), 3),
                   call("<", {column("amount"), long_literal(60)})));
        CHECK(int_column(top_filtered, "id", s.resource) == ints({6, 4}, s.resource));
    });
}

TEST_CASE("Spark plan: filter after orderBy and select after limit keep the sorted rows", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_commuting", [](scheduler_stack s) {
        session_hash_t id = 12600;
        seed_sales(s, id, "spark_commute", "");
        const auto by_amount_desc =
            sort_by(read_table("spark_commute.sales"), "amount", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING);

        // df.orderBy(df.amount.desc()).filter(df.amount > 25): the filter runs
        // below the sort and keeps its order.
        auto filtered =
            run_plan_or_fail(s, id++, filter(by_amount_desc, call(">", {column("amount"), long_literal(25)})));
        CHECK(int_column(filtered, "id", s.resource) == ints({5, 6, 4, 3}, s.resource));

        // df.orderBy(df.amount.desc()).limit(2).select("id", F.col("amount").alias("amt")):
        // the top-N rows, projected.
        auto top =
            run_plan_or_fail(s,
                             id++,
                             project(limit(by_amount_desc, 2), {column("id"), aliased(column("amount"), "amt")}));
        CHECK(top.column_count() == 2);
        CHECK(int_column(top, "id", s.resource) == ints({5, 6}, s.resource));
        CHECK(int_column(top, "amt", s.resource) == ints({60, 50}, s.resource));
    });
}

TEST_CASE("Spark plan: crossJoin of two small tables answers every pair", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_cross_join", [](scheduler_stack s) {
        session_hash_t id = 12500;
        run_or_fail(s, id++, "CREATE DATABASE spark_cross;");
        run_or_fail(s, id++, "CREATE TABLE spark_cross.colors (color string);");
        run_or_fail(s, id++, "CREATE TABLE spark_cross.sizes (size_id bigint);");
        run_or_fail(s, id++, "INSERT INTO spark_cross.colors (color) VALUES ('red'), ('blue');");
        run_or_fail(s, id++, "INSERT INTO spark_cross.sizes (size_id) VALUES (1), (2), (3);");

        // colors.crossJoin(sizes)
        sc::Relation rel;
        auto* join = rel.mutable_join();
        *join->mutable_left() = read_table("spark_cross.colors");
        *join->mutable_right() = read_table("spark_cross.sizes");
        join->set_join_type(sc::Join::JOIN_TYPE_CROSS);

        auto payload = run_plan_or_fail(s, id++, rel);
        REQUIRE(payload.size() == 6);

        std::pmr::vector<std::pmr::string> pairs(s.resource);
        for (const auto& chunk : payload.chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            const auto color = column_of(chunk, "color");
            const auto size = column_of(chunk, "size_id");
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                const auto color_value = chunk.value(color, row);
                std::pmr::string pair{color_value.value<std::string_view>(), s.resource};
                pair += ':';
                pair += std::to_string(as_int64(chunk.value(size, row)));
                pairs.push_back(std::move(pair));
            }
        }
        std::sort(pairs.begin(), pairs.end());
        const std::string_view expected[] = {"blue:1", "blue:2", "blue:3", "red:1", "red:2", "red:3"};
        REQUIRE(pairs.size() == 6);
        for (size_t i = 0; i < pairs.size(); ++i) {
            CHECK(std::string_view{pairs[i]} == expected[i]);
        }
    });
}

TEST_CASE("Spark plan: df.count() and groupBy().agg(count(lit(1))) count every row", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_count_literal", [](scheduler_stack s) {
        session_hash_t id = 12700;
        run_or_fail(s, id++, "CREATE DATABASE spark_count;");
        run_or_fail(s, id++, "CREATE TABLE spark_count.items (id bigint, grp string);");
        run_or_fail(s, id++, "INSERT INTO spark_count.items (id, grp) VALUES (1, 'a'), (2, 'a'), (3, 'b');");
        const auto items = read_table("spark_count.items");

        // df.count(): count(lit(1)) over every row, one row answering 3.
        auto counted = run_plan_or_fail(s, id++, count_rows(items));
        CHECK(int_column(counted, "count", s.resource) == ints({3}, s.resource));

        // groupBy().agg(F.count(F.lit(1)).alias("n")): the same count, named.
        auto global = run_plan_or_fail(s, id++, aggregate(items, {}, {aliased(call("count", {long_literal(1)}), "n")}));
        CHECK(int_column(global, "n", s.resource) == ints({3}, s.resource));

        // groupBy("grp").agg(F.count(F.lit(1)).alias("n")): every group counted, not only the first.
        auto grouped =
            run_plan_or_fail(s,
                             id++,
                             aggregate(items, {column("grp")}, {aliased(call("count", {long_literal(1)}), "n")}));
        CHECK(sorted(int_column(grouped, "n", s.resource)) == ints({1, 2}, s.resource));
    });
}

TEST_CASE("Spark plan: an operation on a finished select distinct or aggregate reads a derived table", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_derived", [](scheduler_stack s) {
        session_hash_t id = 12800;
        seed_sales(s, id, "spark_derived", "");
        const auto sales = read_table("spark_derived.sales");
        auto* resource = s.resource;

        // df.select("id", "amount").count()
        auto select_count = run_plan_or_fail(s, id++, count_rows(project(sales, {column("id"), column("amount")})));
        CHECK(int_column(select_count, "count", resource) == ints({6}, resource));

        // df.select("region").distinct().count(): north, south, east
        auto distinct_count = run_plan_or_fail(s, id++, count_rows(distinct(project(sales, {column("region")}))));
        CHECK(int_column(distinct_count, "count", resource) == ints({3}, resource));

        // df.groupBy("region").agg(F.sum("amount").alias("total")).select("total")
        const auto totals = aggregate(sales, {column("region")}, {aliased(call("sum", {column("amount")}), "total")});
        auto selected_totals = run_plan_or_fail(s, id++, project(totals, {column("total")}));
        CHECK(selected_totals.column_count() == 1);
        CHECK(sorted(int_column(selected_totals, "total", resource)) == ints({40, 80, 90}, resource));

        // ....filter(F.col("total") > 50): a filter over the aggregate, as HAVING filters it
        auto large_totals = run_plan_or_fail(s, id++, filter(totals, call(">", {column("total"), long_literal(50)})));
        CHECK(sorted(int_column(large_totals, "total", resource)) == ints({80, 90}, resource));

        // df.select("id", "amount").select("id")
        auto reselected =
            run_plan_or_fail(s, id++, project(project(sales, {column("id"), column("amount")}), {column("id")}));
        CHECK(reselected.column_count() == 1);
        CHECK(sorted(int_column(reselected, "id", resource)) == ints({1, 2, 3, 4, 5, 6}, resource));
    });
}

TEST_CASE("Spark plan: a limit on a limit keeps the smaller window", "[spark-plan]") {
    with_scheduler_stack("/tmp/test_spark_plan_window", [](scheduler_stack s) {
        session_hash_t id = 12900;
        seed_sales(s, id, "spark_window", "");
        const auto by_amount_desc =
            sort_by(read_table("spark_window.sales"), "amount", sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING);

        // df.orderBy(df.amount.desc()).limit(3).first(): first() is limit(1)
        auto first = run_plan_or_fail(s, id++, limit(limit(by_amount_desc, 3), 1));
        CHECK(int_column(first, "id", s.resource) == ints({5}, s.resource));

        // spark.sql("... ORDER BY amount DESC LIMIT 4").limit(2) and .limit(10)
        const auto top_four = sql_leaf("SELECT * FROM spark_window.sales ORDER BY amount DESC LIMIT 4");
        auto top_two = run_plan_or_fail(s, id++, limit(top_four, 2));
        CHECK(int_column(top_two, "id", s.resource) == ints({5, 6}, s.resource));
        auto still_four = run_plan_or_fail(s, id++, limit(top_four, 10));
        CHECK(int_column(still_four, "id", s.resource) == ints({5, 6, 4, 3}, s.resource));
    });
}
