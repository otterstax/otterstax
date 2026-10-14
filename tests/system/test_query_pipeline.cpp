// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The query pipeline end to end over the real modules: a query session on the
// sessions pool, the integration backends over mock connectors (their
// tables discovered at startup into the federation metadata), and a real engine
// that reads the remote tables through the federation — each failure site's
// error text, an empty and a multi-chunk result, a JOIN over two servers — plus
// the sequence-root unwrap of the engine describe.

#include "integration/clickhouse/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "session/session_payload.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/parser.hpp"
#include "../mock/pg_db_connector.hpp"
#include "../mock/sql_db_connector.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "utility/logger.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

using otterstax::session::query_session_t;
using otterstax::test::await_session;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::session_stack_owner;
using otterstax::test::stack_options_t;
using otterstax::test::stmt;

namespace {

    using namespace std::chrono_literals;

    stack_options_t mock_options(mysql::connector_factory mysql_factory, otterstax::session::parser_factory_fn parser) {
        stack_options_t options;
        options.parser_factory = parser;
        options.connectors.mysql = mysql_factory;
        options.connections.mysql = otterstax::test::mock_mysql({"1", "2"});
        return options;
    }

    core::result_wrapper_t<session_payload> execute(const session_stack& s, const std::string& sql) {
        auto [ns, fut] = actor_zeta::send(s.session, &query_session_t::execute, stmt(1), sql);
        return await_session(std::move(fut), 10000ms, s.resource);
    }

} // namespace

TEST_CASE("pipeline: a remote SELECT runs through the MySQL backend and the engine") {
    session_stack_owner owner("/tmp/test_pipeline_base", mock_options(&mysql_mock_connector_factory, &make_mock_parser));
    auto r = execute(owner.stack(), "SELECT 1 AS test");
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.size() == 2);
    // The backend's rows, read by the remote table's scan, are what the engine answers.
    const auto& chunk = sp.chunks.front();
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.value(0, 0).value<int32_t>() == 1);
    REQUIRE(chunk.value(1, 1).value<std::string_view>() == "name_1");
}

TEST_CASE("pipeline: a connector failure keeps the connector's message") {
    session_stack_owner owner("/tmp/test_pipeline_connector_error",
                              mock_options(&mysql_mock_connector_factory_throw, &make_mock_parser));
    auto r = execute(owner.stack(), "SELECT 1 AS test");
    REQUIRE(r.has_error());
    // The backend's error reaches the client through the engine, under the
    // engine's execution prefix, its text whole.
    REQUIRE(std::string{r.error().what.c_str()} == "Otterbrix execution failed: MockConnector: exception in runQuery");
}

TEST_CASE("pipeline: an engine failure keeps the engine's code under the execution prefix") {
    session_stack_owner owner("/tmp/test_pipeline_engine_error");
    auto r = execute(owner.stack(), "SELECT * FROM nodb.nowhere;");
    REQUIRE(r.has_error());
    INFO(r.error().what.c_str());
    REQUIRE(std::string{r.error().what.c_str()}.rfind("Otterbrix execution failed: ", 0) == 0);
    REQUIRE((r.error().type == core::error_code_t::database_not_exists ||
             r.error().type == core::error_code_t::table_not_exists));
}

TEST_CASE("pipeline: a parser exception is answered as the parse error") {
    session_stack_owner owner("/tmp/test_pipeline_parser_error",
                              mock_options(&mysql_mock_connector_factory, &make_throwing_mock_parser));
    auto r = execute(owner.stack(), "SELECT 1 AS test");
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
    REQUIRE(r.error().what == "SimpleMockParser: exception in parse");
}

TEST_CASE("pipeline: a JOIN over two servers reads each through the federation and joins in the engine") {
    stack_options_t options;
    options.connections.mysql = otterstax::test::mock_mysql({"campaigns"});
    options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = "products",
                                                                      .host = "localhost",
                                                                      .port = "5432",
                                                                      .username = "user",
                                                                      .password = "pass",
                                                                      .database = "pgdb",
                                                                      .schema = "public"});
    session_stack_owner owner("/tmp/test_pipeline_cross_backend", options);

    // Each mock discovered its table `table` (id, name) and answers rows
    // (1, name_0), (2, name_1).
    auto r = execute(owner.stack(),
                     "SELECT c.id, p.name FROM campaigns.db.\"table\" c JOIN products.pgdb.public.\"table\" p "
                     "ON c.id = p.id;");
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().size() == 2);
    REQUIRE(r.value().column_count() == 2);
}

TEST_CASE("pipeline: an empty backend answer is an empty payload") {
    session_stack_owner owner("/tmp/test_pipeline_empty",
                              mock_options(&mysql_mock_connector_factory_return_empty, &make_mock_parser));
    auto r = execute(owner.stack(), "SELECT 1 AS test");
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().empty());
}

// Regression for the latent >1024-row truncation: the engine's cursor delivers a
// result as a batch of <=1024-row chunks (never one oversized chunk). 2500 rows
// arrive as ceil(2500/1024) = 3 chunks; the whole vector must ride through the
// session into the session_payload without losing rows.
TEST_CASE("pipeline: a multi-chunk result carries all rows through the session") {
    constexpr size_t N = 2500;
    session_stack_owner owner("/tmp/test_pipeline_multichunk");
    const auto s = owner.stack();
    session_hash_t id = 100;
    otterstax::test::seed_people_bulk(s, id, N);

    auto r = run_session_sql_payload(s, id++, "SELECT id, name FROM dmldb.people ORDER BY id;");
    INFO((r.has_error() ? r.error().what.c_str() : ""));
    REQUIRE_FALSE(r.has_error());
    auto sp = std::move(r.value());
    REQUIRE(sp.size() == N);
    REQUIRE(sp.chunks.size() >= 3);
    // Every row carries its own values, in order, across the chunk boundaries —
    // the right total alone would not catch a chunk delivered empty or repeated.
    size_t row = 0;
    for (const auto& chunk : sp.chunks) {
        REQUIRE(chunk.column_count() == 2);
        for (size_t r_idx = 0; r_idx < chunk.size(); ++r_idx, ++row) {
            REQUIRE(chunk.value(0, r_idx).value<int64_t>() == static_cast<int64_t>(row + 1));
            REQUIRE(chunk.value(1, r_idx).value<std::string_view>() == "name_" + std::to_string(row));
        }
    }
    REQUIRE(row == N);
}

// ---------------------------------------------------------------------------
// The engine describe (otterstax::engine::describe) of a parsed statement: the
// parser hands over the bare consumer, and an aggregate root answers its
// columns while any other root answers no schema.
// ---------------------------------------------------------------------------

namespace {

    otterstax::engine::describe_answer_t describe(otterstax::test::test_engine_t& engine,
                                                  std::pmr::map<qualified_name_t, size_t> dependencies,
                                                  ParsedQueryDataPtr data) {
        return engine.drive(otterstax::engine::describe(engine.resource(),
                                                        engine.dispatcher(),
                                                        engine.session(),
                                                        std::move(dependencies),
                                                        std::move(data)));
    }

} // namespace

TEST_CASE("engine describe: a SELECT resolves columns") {
    otterstax::test::test_engine_t engine("/tmp/otterstax_seq_get_schema");
    engine.remove_on_exit();
    auto* resource = engine.resource();

    {
        // Local engine table the describe's dependency probe resolves against.
        auto db_cursor = engine.sql("CREATE DATABASE db1;");
        REQUIRE(db_cursor);
        REQUIRE_FALSE(db_cursor->is_error());
        auto create_cursor = engine.sql("CREATE TABLE db1.campaigns (campaign_id INT, campaign_name STRING, budget DOUBLE);");
        REQUIRE(create_cursor);
        REQUIRE_FALSE(create_cursor->is_error());
    }

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT campaign_name, budget FROM db1.campaigns;");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());

    // The parser hands over the bare consumer.
    const auto& root = data->otterbrix_params->node;
    REQUIRE(root->type() == logical_plan::node_type::aggregate_t);

    std::pmr::map<qualified_name_t, size_t> dependencies(resource);
    dependencies.emplace(root->target(), 0);

    auto result = describe(engine, std::move(dependencies), std::move(data));
    INFO((result.has_error() ? result.error().what.c_str() : ""));
    REQUIRE_FALSE(result.has_error());
    auto [cursor, returned] = std::move(result.value());
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    // Exactly one schema, for the aggregate root.
    REQUIRE(cursor->size() == 1);
    const auto& schema = cursor->type_data()[0];
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    // The schema lists the SELECT-list columns by name, in statement order.
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "campaign_name");
    REQUIRE(schema.child_types()[1].alias() == "budget");
    // The probe resolves the column types, not only their names.
    REQUIRE(schema.child_types()[0].type() == types::logical_type::STRING_LITERAL);
    REQUIRE(schema.child_types()[1].type() == types::logical_type::DOUBLE);
}

TEST_CASE("engine describe: non-aggregate plans keep the empty-schema contract") {
    otterstax::test::test_engine_t engine("/tmp/otterstax_seq_get_schema_empty");
    engine.remove_on_exit();
    auto* resource = engine.resource();
    GreenplumParser parser(resource);

    SECTION("non-aggregate root -> empty schema, no error") {
        auto parsed = parser.parse("CREATE DATABASE describe_only;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t);

        auto result = describe(engine, std::pmr::map<qualified_name_t, size_t>(resource), std::move(data));
        REQUIRE_FALSE(result.has_error());
        auto [cursor, returned] = std::move(result.value());
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
    }
}

TEST_CASE("engine describe: a remote SELECT resolves the declared columns") {
    stack_options_t options;
    options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = "products",
                                                                      .host = "localhost",
                                                                      .port = "5432",
                                                                      .username = "user",
                                                                      .password = "pass",
                                                                      .database = "pgdb",
                                                                      .schema = "public"});
    session_stack_owner owner("/tmp/test_describe_seq_remote", options);
    auto& engine = owner.engine();
    auto* resource = engine.resource();

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.\"table\";");
    REQUIRE_FALSE(parsed.has_error());
    auto data = std::move(parsed.value());
    REQUIRE(data->otterbrix_params->node->type() == logical_plan::node_type::aggregate_t);

    std::pmr::map<qualified_name_t, size_t> dependencies(resource);
    dependencies.emplace(data->otterbrix_params->node->target(), 0);
    auto result = describe(engine, std::move(dependencies), std::move(data));
    INFO((result.has_error() ? result.error().what.c_str() : ""));
    REQUIRE_FALSE(result.has_error());
    auto [cursor, returned] = std::move(result.value());
    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == 1);
    const auto& schema = cursor->type_data()[0];
    REQUIRE(schema.type() == types::logical_type::STRUCT);
    REQUIRE(schema.child_types().size() == 2);
    REQUIRE(schema.child_types()[0].alias() == "id");
    REQUIRE(schema.child_types()[0].type() == types::logical_type::INTEGER);
    REQUIRE(schema.child_types()[1].alias() == "name");
    REQUIRE(schema.child_types()[1].type() == types::logical_type::STRING_LITERAL);
}
