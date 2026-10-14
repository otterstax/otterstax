// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The pre-built-plan entry points through query_session_t: execute_plan of a
// Spark Connect DataFrame plan over the real engine (the scenarios of
// test_spark_plan_execution.cpp), prepare_plan of a remote SELECT described by
// the engine from the federation metadata — no backend round-trip — and never
// executed, execute_plan answering the rows execute
// answers for the same SQL (test_prepare_plan.cpp, over the same recording
// PostgreSQL mock connector), and the refusals both share.

#include "../system/session_stack.hpp"

#include "frontend/spark_connect_server/plan_translator/relation_to_plan.hpp"
#include "integration/postgresql/connection_manager.hpp"

#include <spark/connect/base.pb.h>
#include <spark/connect/expressions.pb.h>
#include <spark/connect/relations.pb.h>

#include <catch2/catch_all.hpp>
#include <libpq-fe.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

using otterstax::session::query_session_t;
using otterstax::test::close_session_statement;
using otterstax::test::execute_session_plan;
using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_plan;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::with_session_stack;

namespace {

    namespace sc = ::spark::connect;
    namespace ct = components::types;

    // ─── Spark DataFrame plans over the real engine ──────────────────────────

    void seed_sales(const session_stack& s, session_hash_t& id, const std::string& db) {
        run_session_or_fail(s, id++, "CREATE DATABASE " + db + ";");
        run_session_or_fail(s, id++, "CREATE TABLE " + db + ".sales (id bigint, region string, amount bigint);");
        run_session_or_fail(s,
                            id++,
                            "INSERT INTO " + db +
                                ".sales (id, region, amount) VALUES (1, 'north', 10), (2, 'south', 20), (3, 'north', "
                                "30), (4, 'east', 40), (5, 'south', 60), (6, 'north', 50);");
    }

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

    ParsedQueryDataPtr translate(const session_stack& s, const sc::Relation& root) {
        sc::Plan plan;
        *plan.mutable_root() = root;
        auto translated = frontend::spark::relation_to_plan(plan, s.resource);
        INFO("translate: " << (translated.has_error() ? translated.error().what.c_str() : "ok"));
        REQUIRE_FALSE(translated.has_error());
        return std::move(translated.value().parsed_data);
    }

    session_payload run_plan_or_fail(const session_stack& s, session_hash_t id, const sc::Relation& root) {
        auto result = execute_session_plan(s, id, translate(s, root));
        INFO("plan error: " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    }

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

    std::vector<int64_t> int_column(const session_payload& payload, std::string_view name) {
        std::vector<int64_t> values;
        for (const auto& chunk : payload.chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            const auto index = column_of(chunk, name);
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                values.push_back(as_int64(chunk.value(index, row)));
            }
        }
        std::sort(values.begin(), values.end());
        return values;
    }

} // namespace

TEST_CASE("session plan: Read Project and Filter answer the matching rows", "[spark-plan]") {
    with_session_stack("/tmp/test_session_plan_project", [](session_stack s) {
        session_hash_t id = 12100;
        seed_sales(s, id, "sess_project");
        const auto sales = read_table("sess_project.sales");

        auto all = run_plan_or_fail(s, id++, sales);
        CHECK(all.column_count() == 3);
        CHECK(int_column(all, "id") == std::vector<int64_t>{1, 2, 3, 4, 5, 6});

        auto projected = run_plan_or_fail(s, id++, project(sales, {column("id"), aliased(column("amount"), "amt")}));
        CHECK(projected.column_count() == 2);
        CHECK(int_column(projected, "amt") == std::vector<int64_t>{10, 20, 30, 40, 50, 60});

        auto gt = run_plan_or_fail(s, id++, filter(sales, call(">", {column("amount"), long_literal(25)})));
        CHECK(int_column(gt, "id") == std::vector<int64_t>{3, 4, 5, 6});
    });
}

TEST_CASE("session plan: groupBy agg and df.count answer per group and overall", "[spark-plan]") {
    with_session_stack("/tmp/test_session_plan_aggregate", [](session_stack s) {
        session_hash_t id = 12300;
        seed_sales(s, id, "sess_agg");
        const auto sales = read_table("sess_agg.sales");

        auto grouped = run_plan_or_fail(s,
                                        id++,
                                        aggregate(sales,
                                                  {column("region")},
                                                  {aliased(call("count", {long_literal(1)}), "n"),
                                                   aliased(call("sum", {column("amount")}), "total")}));
        CHECK(int_column(grouped, "n") == std::vector<int64_t>{1, 2, 3});
        CHECK(int_column(grouped, "total") == std::vector<int64_t>{40, 80, 90});

        auto counted = run_plan_or_fail(s, id++, aggregate(sales, {}, {call("count", {long_literal(1)})}));
        REQUIRE(counted.size() == 1);
        CHECK(as_int64(counted.chunks.front().value(0, 0)) == 6);
    });
}

TEST_CASE("session plan: execute_plan and prepare_plan refuse a missing plan and an extension root") {
    with_session_stack("/tmp/test_session_plan_refusals", [](session_stack s) {
        auto none = execute_session_plan(s, 12400, nullptr);
        REQUIRE(none.has_error());
        REQUIRE(none.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{none.error().what.c_str()} == "execute_plan: no plan to execute");

        auto none_prepared = prepare_session_plan(s, 12401, nullptr);
        REQUIRE(none_prepared.has_error());
        REQUIRE(std::string{none_prepared.error().what.c_str()} == "prepare_plan: no plan to describe");

        GreenplumParser parser(s.resource);
        auto copy = parser.parse("COPY (SELECT 1 AS x) TO '/tmp/test_session_plan_refused.csv' WITH (format = 'csv')");
        INFO("parse: " << (copy.has_error() ? copy.error().what.c_str() : "ok"));
        REQUIRE_FALSE(copy.has_error());
        auto refused = execute_session_plan(s, 12402, std::move(copy.value()));
        REQUIRE(refused.has_error());
        REQUIRE(refused.error().type == core::error_code_t::invalid_parameter);
        REQUIRE(std::string{refused.error().what.c_str()} == "execute_plan: extension statements are not accepted");
    });
}

// ─── A remote SELECT described and executed as a plan (recording PG mock) ────

namespace {

    constexpr const char* kUid = "planpg";
    constexpr std::string_view kSql = "SELECT id, name, price FROM planpg.pgdb.public.items;";

    constexpr Oid kInt4Oid = 23;
    constexpr Oid kFloat8Oid = 701;
    constexpr Oid kVarcharOid = 1043;

    struct item_column {
        const char* name;
        Oid typid;
        ct::logical_type type;
    };

    constexpr std::array<item_column, 3> kColumns{{
        {"id", kInt4Oid, ct::logical_type::INTEGER},
        {"name", kVarcharOid, ct::logical_type::STRING_LITERAL},
        {"price", kFloat8Oid, ct::logical_type::DOUBLE},
    }};

    struct item_row {
        int32_t id;
        std::string name;
        double price;

        bool operator==(const item_row&) const = default;
    };

    const std::array<item_row, 2> kRows{{{1, "widget", 9.5}, {2, "gadget", 19.25}}};

    constexpr std::string_view kProbePrefix = "SELECT * FROM (";

    std::mutex g_mutex;
    std::vector<std::string> g_probe_queries;
    std::vector<std::string> g_data_queries;

    void reset_queries() {
        std::lock_guard guard(g_mutex);
        g_probe_queries.clear();
        g_data_queries.clear();
    }

    size_t probe_count() {
        std::lock_guard guard(g_mutex);
        return g_probe_queries.size();
    }

    size_t data_count() {
        std::lock_guard guard(g_mutex);
        return g_data_queries.size();
    }

    std::unique_ptr<PGresult, decltype(&PQclear)> make_header() {
        std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        std::array<PGresAttDesc, kColumns.size()> attrs{};
        for (std::size_t i = 0; i < kColumns.size(); ++i) {
            attrs[i].name = const_cast<char*>(kColumns[i].name);
            attrs[i].typid = kColumns[i].typid;
            attrs[i].typlen = -1;
            attrs[i].atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        return result;
    }

    class recording_pg_connector final : public pg::IConnector {
    public:
        recording_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        pg::Status status() const noexcept override { return pg::Status::Connected; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(PGresult*)>) override {
            {
                std::lock_guard guard(g_mutex);
                g_data_queries.emplace_back(query);
            }
            std::pmr::vector<ct::complex_logical_type> fields(resource_);
            for (const auto& column : kColumns) {
                fields.emplace_back(column.type, column.name);
            }
            auto chunk = std::make_unique<components::vector::data_chunk_t>(resource_, fields, kRows.size());
            for (std::size_t row = 0; row < kRows.size(); ++row) {
                chunk->set_value(0, row, ct::logical_value_t(resource_, kRows[row].id));
                chunk->set_value(1, row, ct::logical_value_t(resource_, std::string_view{kRows[row].name}));
                chunk->set_value(2, row, ct::logical_value_t(resource_, kRows[row].price));
            }
            chunk->set_cardinality(kRows.size());
            co_return std::move(chunk);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> empty(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                    &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
            }
            if (query.substr(0, kProbePrefix.size()) == kProbePrefix) {
                std::lock_guard guard(g_mutex);
                g_probe_queries.emplace_back(query);
            }
            auto header = make_header();
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(header.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    recording_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<recording_pg_connector>(resource, std::move(params), std::move(alias));
    }

    // Real engine, real parser; the PostgreSQL connection is the recording
    // mock configured under kUid (its table discovered at startup); the stack's
    // default session over it.
    class plan_stack_owner {
    public:
        explicit plan_stack_owner(const std::string& data_dir)
            : owner_(data_dir, options()) {
            reset_queries();
        }

        session_stack stack() const { return owner_.stack(); }

    private:
        static otterstax::test::stack_options_t options() {
            otterstax::test::stack_options_t options;
            options.connectors.postgres = &recording_pg_factory;
            options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = kUid,
                                                                              .host = "localhost",
                                                                              .port = "5432",
                                                                              .username = "user",
                                                                              .password = "pass",
                                                                              .database = "pgdb",
                                                                              .schema = "public",
                                                                              .table = "items"});
            return options;
        }

        session_stack_owner owner_;
    };

    ParsedQueryDataPtr parse_plan(GreenplumParser& parser, std::string_view sql) {
        auto parsed = parser.parse(std::string{sql});
        INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value());
    }

    void require_item_schema(const ct::complex_logical_type& schema) {
        REQUIRE(schema.type() == ct::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == kColumns.size());
        for (std::size_t i = 0; i < kColumns.size(); ++i) {
            const auto& column = schema.child_types()[i];
            INFO("column " << i << " expected " << kColumns[i].name);
            REQUIRE(column.has_alias());
            REQUIRE(column.alias() == kColumns[i].name);
            REQUIRE(column.type() == kColumns[i].type);
        }
    }

    std::vector<item_row> rows_of(const session_payload& payload) {
        std::vector<item_row> rows;
        for (const auto& chunk : payload.chunks) {
            for (std::size_t row = 0; row < chunk.size(); ++row) {
                rows.push_back(item_row{chunk.value(0, row).value<int32_t>(),
                                        std::string{chunk.value(1, row).value<std::string_view>()},
                                        chunk.value(2, row).value<double>()});
            }
        }
        std::sort(rows.begin(), rows.end(), [](const item_row& a, const item_row& b) { return a.id < b.id; });
        return rows;
    }

} // namespace

TEST_CASE("session prepare_plan: a parsed remote SELECT is described and never executed") {
    plan_stack_owner owner("/tmp/test_session_prepare_plan_describe");
    auto s = owner.stack();

    const session_hash_t by_sql = 9900;
    auto reference = prepare_session_sql(s, by_sql, std::string{kSql});
    INFO("prepare_schema: " << (reference.has_error() ? reference.error().what.c_str() : "ok"));
    REQUIRE_FALSE(reference.has_error());
    require_item_schema(reference.value().schema);
    REQUIRE_FALSE(close_session_statement(s, by_sql).has_error());

    reset_queries();
    GreenplumParser parser(s.resource);
    const session_hash_t by_plan = 9901;
    auto prepared = prepare_session_plan(s, by_plan, parse_plan(parser, kSql));
    INFO("prepare_plan: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    REQUIRE(prepared.value().parameter_count == 0);
    require_item_schema(prepared.value().schema);
    REQUIRE(prepared.value().empty());
    REQUIRE(data_count() == 0);
    // Described from federation.remote_columns: the backend is not asked.
    REQUIRE(probe_count() == 0);

    REQUIRE_FALSE(close_session_statement(s, by_plan).has_error());
    auto executed = execute_session_statement(s, by_plan);
    REQUIRE(executed.has_error());
    REQUIRE(executed.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(data_count() == 0);
}

TEST_CASE("session execute_plan: a parsed remote SELECT returns the rows execute returns for its SQL") {
    plan_stack_owner owner("/tmp/test_session_prepare_plan_execute");
    auto s = owner.stack();

    auto by_sql = run_session_sql_payload(s, 9920, std::string{kSql});
    INFO("execute: " << (by_sql.has_error() ? by_sql.error().what.c_str() : "ok"));
    REQUIRE_FALSE(by_sql.has_error());

    GreenplumParser parser(s.resource);
    auto by_plan = execute_session_plan(s, 9921, parse_plan(parser, kSql));
    INFO("execute_plan: " << (by_plan.has_error() ? by_plan.error().what.c_str() : "ok"));
    REQUIRE_FALSE(by_plan.has_error());

    const std::vector<item_row> expected(kRows.begin(), kRows.end());
    REQUIRE(by_sql.value().column_count() == kColumns.size());
    REQUIRE(by_plan.value().column_count() == kColumns.size());
    REQUIRE(rows_of(by_sql.value()) == expected);
    REQUIRE(rows_of(by_plan.value()) == expected);
}

// A DataFrame over a remote table: the Spark translator names the table as the
// SQL path does (four parts keep the alias slot), and the engine resolves it
// through the federation — a bare read through the storage's scan, a filtered
// projection as one pushed-down statement — for execute_plan and prepare_plan.
TEST_CASE("session execute_plan: a DataFrame over a remote table reads it through the federation") {
    plan_stack_owner owner("/tmp/test_session_plan_remote_dataframe");
    auto s = owner.stack();

    auto all = execute_session_plan(s, 9940, translate(s, read_table(std::string{kUid} + ".pgdb.public.items")));
    INFO("read: " << (all.has_error() ? all.error().what.c_str() : "ok"));
    REQUIRE_FALSE(all.has_error());
    const std::vector<item_row> expected(kRows.begin(), kRows.end());
    REQUIRE(rows_of(all.value()) == expected);

    // Three parts read as database.schema.table: the alias in the database slot.
    auto three = execute_session_plan(s, 9941, translate(s, read_table(std::string{kUid} + ".public.items")));
    INFO("three-part read: " << (three.has_error() ? three.error().what.c_str() : "ok"));
    REQUIRE_FALSE(three.has_error());
    REQUIRE(rows_of(three.value()) == expected);

    // df.schema of the same read: described, never executed.
    reset_queries();
    auto described = prepare_session_plan(s, 9942, translate(s, read_table(std::string{kUid} + ".pgdb.public.items")));
    INFO("prepare_plan: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    require_item_schema(described.value().schema);
    REQUIRE(data_count() == 0);
    REQUIRE(probe_count() == 0);
    REQUIRE_FALSE(close_session_statement(s, 9942).has_error());
}
