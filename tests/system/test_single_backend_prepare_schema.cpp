// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "plan_stubs.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"
#include "typed_stacks.hpp"

#include <catch2/catch_all.hpp>
#include <components/logical_plan/node_data.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace otterstax::test;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    template<class B>
    struct facts;

    template<>
    struct facts<ch_backend> {
        static constexpr std::string_view probe_suffix = ") LIMIT 0";
        static constexpr std::string_view events_rendered = "`chdb`.`events`";
        static constexpr std::string_view without_columns = "without a header block";
        static constexpr ch_wire text = ch_wire::String;
        static constexpr ch_wire length = ch_wire::UInt64;
        static constexpr logical_type length_type = logical_type::UBIGINT;
        static constexpr arrow::Type::type length_arrow = arrow::Type::UINT64;
        static uint64_t length_cell(std::size_t row) { return cell::uint64(row); }
    };

    template<>
    struct facts<my_backend> {
        static constexpr std::string_view probe_suffix = ") AS otx_prepare_probe LIMIT 0";
        static constexpr std::string_view events_rendered = "`mydb`.`events`";
        static constexpr std::string_view without_columns = "without column metadata";
        static constexpr my_wire text = my_wire::varchar;
        static constexpr my_wire length = my_wire::bigint;
        static constexpr logical_type length_type = logical_type::BIGINT;
        static constexpr arrow::Type::type length_arrow = arrow::Type::INT64;
        static int64_t length_cell(std::size_t row) { return cell::int64(row); }
    };

    template<>
    struct facts<pg_backend> {
        static constexpr std::string_view probe_suffix = ") AS otx_prepare_probe LIMIT 0";
        static constexpr std::string_view events_rendered = R"("public"."events")";
        static constexpr std::string_view without_columns = "without columns";
        static constexpr pg_wire text = pg_wire::varchar;
        static constexpr pg_wire length = pg_wire::int4;
        static constexpr logical_type length_type = logical_type::INTEGER;
        static constexpr arrow::Type::type length_arrow = arrow::Type::INT32;
        static int32_t length_cell(std::size_t row) { return cell::int32(row); }
    };

    template<class B>
    std::string from(std::string_view select, std::string_view tail = "") {
        return std::string{select} + " FROM " + std::string{B::events} + std::string{tail} + ";";
    }

    template<class B>
    std::string probe_of(std::string statement) {
        while (!statement.empty() && (statement.back() == ';' || statement.back() == ' ')) {
            statement.pop_back();
        }
        return std::string{kProbePrefix} + statement + std::string{facts<B>::probe_suffix};
    }

    template<class B>
    struct subquery {
        static std::string body() { return "SELECT toString(id) AS id FROM " + std::string{B::events}; }
        static std::string sql() { return "SELECT s.id FROM (" + body() + ") s;"; }
        static std::string join() {
            return "SELECT e.name, s.id FROM " + std::string{B::events} + " e JOIN (" + body() +
                   ") s ON s.id = e.name;";
        }
        static std::string rendered() {
            return "SELECT toString(id) AS id FROM " + std::string{facts<B>::events_rendered};
        }
    };

    template<class B>
    std::string data_dir(std::string_view name) {
        return "/tmp/test_single_backend_prepare_" + std::string{B::connection.alias} + "_" + std::string{name};
    }

    void require_single_column(const complex_logical_type& schema, std::string_view alias, logical_type type) {
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        REQUIRE(schema.child_types()[0].has_alias());
        REQUIRE(schema.child_types()[0].alias() == alias);
        REQUIRE(schema.child_types()[0].type() == type);
    }

    template<class B>
    ParsedQueryDataPtr describe_round_trip(actor_stack& stack,
                                           session_hash_t id,
                                           const std::string& sql,
                                           std::string_view alias,
                                           logical_type plan_type,
                                           logical_type described) {
        auto classified = stack.classify(id, stack.parse(sql));
        INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
        REQUIRE_FALSE(classified.has_error());
        auto data = std::move(classified.value());
        REQUIRE(data->backend_type == B::connection.backend);
        {
            const auto& plan_schema = root_stub(*data).schema();
            REQUIRE(plan_schema.child_types().size() == 1);
            REQUIRE(plan_schema.child_types()[0].type() == plan_type);
        }

        auto described_data = stack.describe<B>(id, std::move(data));
        INFO("describe: " << (described_data.has_error() ? described_data.error().what.c_str() : "ok"));
        REQUIRE_FALSE(described_data.has_error());
        data = std::move(described_data.value());
        const auto schema = root_stub(*data).schema();
        require_single_column(schema, alias, described);

        auto executed = stack.execute<B>(id, std::move(data));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        data = std::move(executed.value());
        const auto& raw = root_raw(*data);
        REQUIRE(raw.size() == kDataRows);
        const auto executed_types = raw.data_chunk().types();
        REQUIRE(executed_types.size() == 1);
        REQUIRE(executed_types[0] == schema.child_types()[0]);
        return data;
    }

    void require_prepared_and_streamed(const scheduler_stack& s,
                                       session_hash_t stmt,
                                       const std::string& sql,
                                       std::string_view alias,
                                       logical_type expected,
                                       arrow::Type::type arrow_type) {
        auto prepared = prepare_scheduler_sql(s, stmt, sql);
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        const auto schema = prepared.value().schema;
        require_single_column(schema, alias, expected);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == kDataRows);
        const auto column_types = executed.value().chunks.front().types();
        REQUIRE(column_types.size() == 1);
        REQUIRE(column_types[0] == schema.child_types()[0]);

        auto flight_schema = to_arrow_schema(s.resource, schema);
        INFO("arrow schema: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
        REQUIRE_FALSE(flight_schema.has_error());
        REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow_type);
        for (const auto& chunk : executed.value().chunks) {
            auto batch = chunk_to_record_batch(s.resource, chunk);
            INFO("chunk_to_record_batch: " << (batch.has_error() ? batch.error().what.c_str() : "ok"));
            REQUIRE_FALSE(batch.has_error());
        }
    }

    constexpr std::array<std::pair<std::string_view, logical_type>, 4> kEventsTypes{{
        {"id", logical_type::INTEGER},
        {"score", logical_type::INTEGER},
        {"name", logical_type::STRING_LITERAL},
        {"price", logical_type::DOUBLE},
    }};

    std::vector<pg_backend::column> projection(std::string_view statement) {
        const auto list_begin = statement.find("SELECT ") + 7;
        const auto list = statement.substr(list_begin, statement.find(" FROM ") - list_begin);
        std::vector<std::pair<std::size_t, pg_backend::column>> found;
        for (const auto& column : pg_backend::base_columns()) {
            if (list.find('*') != std::string_view::npos) {
                found.emplace_back(found.size(), column);
            } else if (const auto at = list.find(column.name); at != std::string_view::npos) {
                found.emplace_back(at, column);
            }
        }
        std::sort(found.begin(), found.end(), [](const auto& l, const auto& r) { return l.first < r.first; });
        std::vector<pg_backend::column> columns;
        for (auto& [position, column] : found) {
            columns.push_back(std::move(column));
        }
        return columns;
    }

    void require_prepared_columns(const core::result_wrapper_t<session_payload>& prepared,
                                  const std::vector<std::string_view>& expected,
                                  std::size_t parameter_count = 0) {
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        REQUIRE(prepared.value().parameter_count == parameter_count);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == expected.size());
        for (std::size_t i = 0; i < expected.size(); ++i) {
            const auto& column = schema.child_types()[i];
            const auto want = std::find_if(kEventsTypes.begin(), kEventsTypes.end(), [&](const auto& event) {
                return event.first == expected[i];
            });
            INFO("column " << i << " expected " << expected[i]);
            REQUIRE(want != kEventsTypes.end());
            REQUIRE(column.has_alias());
            REQUIRE(column.alias() == expected[i]);
            REQUIRE(column.type() == want->second);
        }
    }

    const schema_utils::schema_node_t* stub_of_uid(const ParsedQueryData& data, std::string_view uid) {
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if (slot.target.name.unique_identifier != uid) {
                    continue;
                }
                if ((*slot.node)->type() != components::logical_plan::node_type::unused) {
                    return nullptr;
                }
                return static_cast<const schema_utils::schema_node_t*>(slot.node->get());
            }
        }
        return nullptr;
    }

} // namespace

TEST_CASE("single-backend prepare_schema: a column list resolves to the discovered columns in order") {
    stack_owner owner("/tmp/test_single_backend_prepare_columns", {pg_backend::connection});
    mock<pg_backend>::reset(projection);
    auto s = owner.stack();
    session_hash_t id = 9800;

    require_prepared_columns(prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT id, name")), {"id", "name"});
    require_prepared_columns(prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT price, score, id")),
                             {"price", "score", "id"});
}

TEST_CASE("single-backend prepare_schema: SELECT * resolves to every discovered column") {
    stack_owner owner("/tmp/test_single_backend_prepare_star", {pg_backend::connection});
    mock<pg_backend>::reset(projection);
    auto s = owner.stack();
    session_hash_t id = 9820;

    require_prepared_columns(prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT *")),
                             {"id", "score", "name", "price"});
    require_prepared_columns(prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT *", " WHERE price > 10")),
                             {"id", "score", "name", "price"});
}

TEST_CASE("single-backend prepare_schema: WHERE, ORDER BY and LIMIT leave the projected schema") {
    stack_owner owner("/tmp/test_single_backend_prepare_clauses", {pg_backend::connection});
    mock<pg_backend>::reset(projection);
    auto s = owner.stack();
    session_hash_t id = 9840;

    require_prepared_columns(
        prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT name, price", " WHERE price > 10 ORDER BY price")),
        {"name", "price"});
    require_prepared_columns(prepare_scheduler_sql(s, id++, from<pg_backend>("SELECT id", " LIMIT 1")), {"id"});
    const auto clauses = from<pg_backend>("SELECT *", " WHERE score = 10 ORDER BY price DESC LIMIT 5");
    require_prepared_columns(prepare_scheduler_sql(s, id++, clauses), {"id", "score", "name", "price"});
}

TEST_CASE("single-backend prepare_schema: a parameterized SELECT carries its parameter count") {
    stack_owner owner("/tmp/test_single_backend_prepare_param", {pg_backend::connection});
    mock<pg_backend>::reset(projection);
    auto s = owner.stack();

    auto prepared = prepare_scheduler_sql(s, 9860, from<pg_backend>("SELECT id, price", " WHERE id = $1"));
    require_prepared_columns(prepared, {"id", "price"}, /*parameter_count*/ 1);
}

TEST_CASE("single-backend prepare_schema: DoGet streams the backend rows under the prepared schema") {
    stack_owner owner("/tmp/test_single_backend_prepare_stream", {pg_backend::connection});
    mock<pg_backend>::reset(projection);
    auto s = owner.stack();
    const session_hash_t stmt = 9880;

    auto prepared = prepare_scheduler_sql(s, stmt, from<pg_backend>("SELECT id, name, price"));
    require_prepared_columns(prepared, {"id", "name", "price"});

    auto flight_schema = to_arrow_schema(s.resource, prepared.value().schema);
    INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
    REQUIRE_FALSE(flight_schema.has_error());
    REQUIRE(flight_schema.value()->num_fields() == 3);
    REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::INT32);
    REQUIRE(flight_schema.value()->field(1)->type()->id() == arrow::Type::STRING);
    REQUIRE(flight_schema.value()->field(2)->type()->id() == arrow::Type::DOUBLE);

    auto executed = execute_scheduler_statement(s, stmt);
    INFO("execute error: " << executed.error().what.c_str());
    REQUIRE_FALSE(executed.has_error());
    REQUIRE(executed.value().size() == kDataRows);
    REQUIRE(executed.value().column_count() == 3);

    auto batch = chunk_to_record_batch(s.resource, executed.value().chunks.front());
    REQUIRE_FALSE(batch.has_error());
    REQUIRE(batch.value()->schema()->Equals(*flight_schema.value()));
    REQUIRE(batch.value()->num_rows() == static_cast<int64_t>(kDataRows));
    const auto ids = std::static_pointer_cast<arrow::Int32Array>(batch.value()->column(0));
    const auto names = std::static_pointer_cast<arrow::StringArray>(batch.value()->column(1));
    const auto prices = std::static_pointer_cast<arrow::DoubleArray>(batch.value()->column(2));
    for (std::size_t row = 0; row < kDataRows; ++row) {
        const auto at = static_cast<int64_t>(row);
        REQUIRE(ids->Value(at) == cell::int32(row));
        REQUIRE(names->GetString(at) == cell::text(row));
        REQUIRE(prices->Value(at) == cell::float64(row));
    }
}

TEMPLATE_TEST_CASE("backend describe: length(name) AS name is prepared as the function's type on the backend",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using F = facts<TestType>;
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"name", F::length}});

    auto data = describe_round_trip<TestType>(stack,
                                              1,
                                              from<TestType>("SELECT length(name) AS name"),
                                              "name",
                                              logical_type::NA,
                                              F::length_type);
    using cell_type = decltype(F::length_cell(1));
    REQUIRE(root_raw(*data).data_chunk().value(0, 1).template value<cell_type>() == F::length_cell(1));
}

TEMPLATE_TEST_CASE("backend describe: the probe is the executed statement wrapped",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using F = facts<TestType>;
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"name", F::length}});

    describe_round_trip<TestType>(stack,
                                  2,
                                  from<TestType>("SELECT length(name) AS name", " WHERE score > 10"),
                                  "name",
                                  logical_type::NA,
                                  F::length_type);

    const auto probes = mock<TestType>::probe_queries();
    const auto statements = mock<TestType>::data_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(statements.size() == 1);
    REQUIRE(statements.front().back() == ';');
    REQUIRE(probes.front() == probe_of<TestType>(statements.front()));
    REQUIRE(probes.front().find("WHERE") != std::string::npos);
}

TEMPLATE_TEST_CASE("backend describe: a probe the backend refuses is that error and never a plan-derived schema",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"name", facts<TestType>::length}}, TestType::probe_mode::refused);

    auto classified = stack.classify(3, stack.parse(from<TestType>("SELECT length(name) AS name")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe<TestType>(3, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(mock<TestType>::probe_queries().size() == 1);
}

TEMPLATE_TEST_CASE("backend describe: a probe answered without columns is schema_error",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"name", facts<TestType>::length}}, TestType::probe_mode::no_columns);

    auto classified = stack.classify(4, stack.parse(from<TestType>("SELECT length(name) AS name")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe<TestType>(4, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::schema_error);
    REQUIRE(std::string{described.error().what.c_str()}.find(facts<TestType>::without_columns) != std::string::npos);
}

TEMPLATE_TEST_CASE("backend describe: a parameterized statement is unimplemented_yet before the backend is asked",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset();

    auto classified = stack.classify(5, stack.parse(from<TestType>("SELECT id", " WHERE id = $1")));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->otterbrix_params->parameters_count == 1);
    auto described = stack.describe<TestType>(5, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string{described.error().what.c_str()}.find("1 unbound parameter") != std::string::npos);
    REQUIRE(mock<TestType>::probe_queries().empty());
}

TEMPLATE_TEST_CASE("backend describe: a statement without a stub is handed back unchanged and nothing is probed",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset();

    auto classified = stack.classify(6, stack.parse("DELETE FROM " + std::string{TestType::events} + " WHERE id = 1;"));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->backend_type == TestType::connection.backend);
    auto described = stack.describe<TestType>(6, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    const auto& slot = described.value()->otterbrix_params->external_nodes.front().front();
    REQUIRE((*slot.node)->type() == components::logical_plan::node_type::delete_t);
    REQUIRE(mock<TestType>::probe_queries().empty());
}

TEMPLATE_TEST_CASE("backend describe: a raw-SQL sub-query stub is filled from the statement it generates",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using query = subquery<TestType>;
    const auto body = query::body();
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"id", facts<TestType>::text}});

    auto classified = stack.classify(7, stack.parse(query::sql()));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    {
        const auto* stub = raw_stub(*data);
        REQUIRE(stub != nullptr);
        REQUIRE(stub->raw_sql() == std::string_view{body});
        REQUIRE(stub->schema().type() == logical_type::NA);
    }

    auto described = stack.describe<TestType>(7, std::move(data));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    data = std::move(described.value());

    const auto* stub = raw_stub(*data);
    REQUIRE(stub != nullptr);
    REQUIRE(stub->has_raw_sql());
    REQUIRE(stub->raw_sql() == std::string_view{body});
    REQUIRE_FALSE(stub->qualifiers().empty());
    require_single_column(stub->schema(), "id", logical_type::STRING_LITERAL);

    const auto probes = mock<TestType>::probe_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(probes.front() == probe_of<TestType>(query::rendered()));
}

TEMPLATE_TEST_CASE("backend describe: every stub of a statement is probed — raw-SQL and catalog alike",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using query = subquery<TestType>;
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"id", facts<TestType>::text}});

    auto classified = stack.classify(8, stack.parse(query::join()));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(slot_stubs(*classified.value()).size() == 2);

    auto described = stack.describe<TestType>(8, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());

    const auto stubs = slot_stubs(*described.value());
    REQUIRE(stubs.size() == 2);
    for (const auto* stub : stubs) {
        require_single_column(stub->schema(), "id", logical_type::STRING_LITERAL);
    }

    const auto probes = mock<TestType>::probe_queries();
    REQUIRE(probes.size() == 2);
    const auto has_probe = [&probes](const std::string& statement) {
        return std::find(probes.begin(), probes.end(), probe_of<TestType>(statement)) != probes.end();
    };
    REQUIRE(has_probe(query::rendered()));
    REQUIRE(has_probe("SELECT * FROM " + std::string{facts<TestType>::events_rendered}));
}

TEMPLATE_TEST_CASE("backend describe: a refused raw-sub-query probe is the backend's error",
                   "",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {TestType::connection});
    mock<TestType>::reset({{"id", facts<TestType>::text}}, TestType::probe_mode::refused);

    auto classified = stack.classify(9, stack.parse(subquery<TestType>::sql()));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe<TestType>(9, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(mock<TestType>::probe_queries().size() == 1);
}

TEMPLATE_TEST_CASE("backend prepare_schema: length(name) AS name is prepared and streamed as the backend's type",
                   "[describe-routing]",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using F = facts<TestType>;
    stack_owner owner(data_dir<TestType>("func"), {TestType::connection});
    mock<TestType>::reset({{"name", F::length}});
    require_prepared_and_streamed(owner.stack(),
                                  9600,
                                  from<TestType>("SELECT length(name) AS name"),
                                  "name",
                                  F::length_type,
                                  F::length_arrow);
    REQUIRE(mock<TestType>::probe_queries().size() == 1);
    REQUIRE(mock<TestType>::probe_queries().front() == probe_of<TestType>(mock<TestType>::data_queries().front()));
}

TEMPLATE_TEST_CASE("backend prepare_schema: a raw sub-query column is prepared as the backend's type",
                   "[describe-routing]",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    using query = subquery<TestType>;
    stack_owner owner(data_dir<TestType>("subquery"), {TestType::connection});
    mock<TestType>::reset({{"id", facts<TestType>::text}});

    auto prepared = prepare_scheduler_sql(owner.stack(), 9620, query::sql());
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    require_single_column(prepared.value().schema, "id", logical_type::STRING_LITERAL);
    REQUIRE(mock<TestType>::probe_queries().size() == 1);
    REQUIRE(mock<TestType>::probe_queries().front() == probe_of<TestType>(query::rendered()));
}

TEMPLATE_TEST_CASE("backend prepare_schema: a JOIN with a raw sub-query is prepared from both described stubs",
                   "[describe-routing]",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    stack_owner owner(data_dir<TestType>("subquery_join"), {TestType::connection});
    mock<TestType>::reset({{"id", facts<TestType>::text}});

    auto prepared = prepare_scheduler_sql(owner.stack(), 9640, subquery<TestType>::join());
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    require_single_column(prepared.value().schema, "id", logical_type::STRING_LITERAL);
    REQUIRE(mock<TestType>::probe_queries().size() == 2);
}

TEMPLATE_TEST_CASE("backend prepare_schema: a probe the backend refuses fails the prepare with the backend's error",
                   "[describe-routing]",
                   ch_backend,
                   my_backend,
                   pg_backend) {
    stack_owner owner(data_dir<TestType>("refused"), {TestType::connection});
    mock<TestType>::reset({{"name", facts<TestType>::length}}, TestType::probe_mode::refused);
    auto prepared = prepare_scheduler_sql(owner.stack(), 9660, from<TestType>("SELECT length(name) AS name"));
    REQUIRE(prepared.has_error());
    REQUIRE(prepared.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{prepared.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
}

TEST_CASE("ClickhouseManager::describe: COUNT(*) is prepared as the UInt64 ClickHouse answers and not BIGINT") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {ch_backend::connection});
    mock<ch_backend>::reset({{"n", ch_wire::UInt64}});

    auto data = describe_round_trip<ch_backend>(stack,
                                                11,
                                                from<ch_backend>("SELECT COUNT(*) AS n"),
                                                "n",
                                                logical_type::UBIGINT,
                                                logical_type::UBIGINT);
    REQUIRE(root_raw(*data).data_chunk().value(0, 0).value<uint64_t>() == cell::uint64(0));
}

TEST_CASE("ClickhouseManager::describe: score + 1 AS score is prepared as the Int64 ClickHouse widens to") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {ch_backend::connection});
    mock<ch_backend>::reset({{"score", ch_wire::Int64}});

    auto data = describe_round_trip<ch_backend>(stack,
                                                12,
                                                from<ch_backend>("SELECT score + 1 AS score"),
                                                "score",
                                                logical_type::INTEGER,
                                                logical_type::BIGINT);
    REQUIRE(root_raw(*data).data_chunk().value(0, 0).value<int64_t>() == cell::int64(0));
}

TEST_CASE("ClickhouseManager::describe: the probe's header is read under the discovered named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {ch_backend::connection});
    mock<ch_backend>::reset({{"rec", ch_wire::NamedTuple}});

    auto data = describe_round_trip<ch_backend>(stack,
                                                13,
                                                from<ch_backend>("SELECT rec"),
                                                "rec",
                                                logical_type::STRUCT,
                                                logical_type::STRUCT);
    const auto executed_types = root_raw(*data).data_chunk().types();
    const auto& rec = executed_types[0];
    REQUIRE(rec.child_types().size() == 2);
    REQUIRE(rec.child_types()[0].alias() == "a");
    REQUIRE(rec.child_types()[1].alias() == "b");
    REQUIRE(root_raw(*data).data_chunk().value(0, 1).children()[1].is_null());
}

TEST_CASE("ClickhouseManager::describe: a probe answered without any block is schema_error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {ch_backend::connection});
    mock<ch_backend>::reset({{"n", ch_wire::UInt64}}, ch_backend::probe_mode::no_blocks);

    auto classified = stack.classify(14, stack.parse(from<ch_backend>("SELECT COUNT(*) AS n")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe<ch_backend>(14, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::schema_error);
    REQUIRE(std::string{described.error().what.c_str()}.find(facts<ch_backend>::without_columns) != std::string::npos);
}

TEST_CASE("ClickHouse prepare_schema: COUNT(*) AS n is prepared and streamed as the same UBIGINT column",
          "[describe-routing]") {
    stack_owner owner(data_dir<ch_backend>("count"), {ch_backend::connection});
    mock<ch_backend>::reset({{"n", ch_wire::UInt64}});
    require_prepared_and_streamed(owner.stack(),
                                  9900,
                                  from<ch_backend>("SELECT COUNT(*) AS n"),
                                  "n",
                                  logical_type::UBIGINT,
                                  arrow::Type::UINT64);
    REQUIRE(mock<ch_backend>::probe_queries().size() == 1);
    REQUIRE(mock<ch_backend>::probe_queries().front() ==
            probe_of<ch_backend>(mock<ch_backend>::data_queries().front()));
}

TEST_CASE("ClickHouse prepare_schema: score + 1 AS score is prepared and streamed as the same BIGINT column",
          "[describe-routing]") {
    stack_owner owner(data_dir<ch_backend>("arith"), {ch_backend::connection});
    mock<ch_backend>::reset({{"score", ch_wire::Int64}});
    require_prepared_and_streamed(owner.stack(),
                                  9920,
                                  from<ch_backend>("SELECT score + 1 AS score"),
                                  "score",
                                  logical_type::BIGINT,
                                  arrow::Type::INT64);
}

TEST_CASE("MySQLManager::describe: score + 1 AS score is prepared as the BIGINT MySQL widens to") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {my_backend::connection});
    mock<my_backend>::reset({{"score", my_wire::bigint}});

    auto data = describe_round_trip<my_backend>(stack,
                                                21,
                                                from<my_backend>("SELECT score + 1 AS score"),
                                                "score",
                                                logical_type::INTEGER,
                                                logical_type::BIGINT);
    REQUIRE(root_raw(*data).data_chunk().value(0, 0).value<int64_t>() == cell::int64(0));
}

TEST_CASE("MySQL prepare_schema: score + 1 AS score is prepared and streamed as the same BIGINT column",
          "[describe-routing]") {
    stack_owner owner(data_dir<my_backend>("arith"), {my_backend::connection});
    mock<my_backend>::reset({{"score", my_wire::bigint}});
    require_prepared_and_streamed(owner.stack(),
                                  9500,
                                  from<my_backend>("SELECT score + 1 AS score"),
                                  "score",
                                  logical_type::BIGINT,
                                  arrow::Type::INT64);
    REQUIRE(mock<my_backend>::probe_queries().size() == 1);
    REQUIRE(mock<my_backend>::probe_queries().front() ==
            probe_of<my_backend>(mock<my_backend>::data_queries().front()));
}

TEST_CASE("Mixed describe: each backend fills its own stubs and probes nothing else") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    actor_stack stack(&arena, {my_backend::connection, pg_backend::connection});
    mock<my_backend>::reset({{"name", my_wire::bigint}});
    mock<pg_backend>::reset({{"name", pg_wire::varchar}});
    const std::string_view my_uid = my_backend::connection.alias;
    const std::string_view pg_uid = pg_backend::connection.alias;

    const auto join = "SELECT e.name FROM " + std::string{my_backend::events} + " e JOIN " +
                      std::string{pg_backend::events} + " p ON e.name = p.name;";
    auto classified = stack.classify(41, stack.parse(join));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    REQUIRE(data->backend_type == backend_type_t::Mixed);
    REQUIRE(stub_of_uid(*data, my_uid) != nullptr);
    REQUIRE(stub_of_uid(*data, pg_uid) != nullptr);

    auto after_mysql = stack.describe<my_backend>(41, std::move(data));
    INFO("describe mysql: " << (after_mysql.has_error() ? after_mysql.error().what.c_str() : "ok"));
    REQUIRE_FALSE(after_mysql.has_error());
    data = std::move(after_mysql.value());
    require_single_column(stub_of_uid(*data, my_uid)->schema(), "name", logical_type::BIGINT);
    REQUIRE(stub_of_uid(*data, pg_uid)->schema().type() != logical_type::STRING_LITERAL);
    REQUIRE(mock<my_backend>::probe_queries().size() == 1);
    REQUIRE(mock<pg_backend>::probe_queries().empty());

    auto after_pg = stack.describe<pg_backend>(41, std::move(data));
    INFO("describe pg: " << (after_pg.has_error() ? after_pg.error().what.c_str() : "ok"));
    REQUIRE_FALSE(after_pg.has_error());
    data = std::move(after_pg.value());

    require_single_column(stub_of_uid(*data, my_uid)->schema(), "name", logical_type::BIGINT);
    require_single_column(stub_of_uid(*data, pg_uid)->schema(), "name", logical_type::STRING_LITERAL);
    REQUIRE(mock<my_backend>::probe_queries().size() == 1);
    REQUIRE(mock<pg_backend>::probe_queries().size() == 1);
    REQUIRE(mock<my_backend>::probe_queries().front().find(facts<my_backend>::events_rendered) != std::string::npos);
    REQUIRE(mock<pg_backend>::probe_queries().front().find(facts<pg_backend>::events_rendered) != std::string::npos);
}
