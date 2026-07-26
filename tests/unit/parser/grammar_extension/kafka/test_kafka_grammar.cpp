// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Parser-level tests for the kafka grammar extension (step 3.2): each statement
// must lex+parse into the expected kafka_grammar AST, malformed input must raise
// a parser error, non-kafka SQL must fall through to the core parser, and the
// transform stage must lower the AST into an otterstax::kafka::kafka_node_t.

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

#include <components/logical_plan/node.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/types/types.hpp>

#include "kafka_ast.hpp"
#include "kafka_extension.hpp"
#include "kafka_node.hpp"

using namespace components::sql::parser;
namespace lp = components::logical_plan;

namespace {
    parser_extension_registry_t kafka_registry() {
        parser_extension_registry_t registry;
        REQUIRE_FALSE(registry.add(make_kafka_extension()).has_error());
        return registry;
    }

    const kafka_grammar::kafka_stmt* kafka_stmt(List* tree) {
        return extension_payload<kafka_grammar::kafka_stmt>(tree, "kafka");
    }

    std::vector<const kafka_grammar::column_def*> columns(const kafka_grammar::kafka_stmt* stmt) {
        std::vector<const kafka_grammar::column_def*> out;
        for (const auto* col = stmt->columns; col != nullptr; col = col->next) {
            out.push_back(col);
        }
        return out;
    }

    std::string option_value(const kafka_grammar::kafka_stmt* stmt, std::string_view key) {
        for (const auto* opt = stmt->options; opt != nullptr; opt = opt->next) {
            if (key == opt->key) {
                return std::string(opt->value);
            }
        }
        return {};
    }
} // namespace

TEST_CASE("kafka grammar: CREATE SOURCE") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* stmt = kafka_stmt(
        raw_parser(&arena,
                   "CREATE SOURCE orders (id BIGINT, amount DOUBLE, note VARCHAR) "
                   "WITH (KAFKA_TOPIC='orders_topic', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='localhost:9092')",
                   registry));

    REQUIRE(stmt != nullptr);
    CHECK(stmt->kind == kafka_grammar::stmt_kind::create_source);
    CHECK(stmt->obj == kafka_grammar::object_kind::source);
    CHECK(std::string_view(stmt->name) == "orders");

    auto cols = columns(stmt);
    REQUIRE(cols.size() == 3);
    CHECK(std::string_view(cols[0]->name) == "id");
    CHECK(std::string_view(cols[0]->type) == "BIGINT");
    CHECK(std::string_view(cols[1]->name) == "amount");
    CHECK(std::string_view(cols[2]->name) == "note");

    CHECK(option_value(stmt, "KAFKA_TOPIC") == "orders_topic");
    CHECK(option_value(stmt, "VALUE_FORMAT") == "JSON");
    CHECK(option_value(stmt, "BOOTSTRAP_SERVERS") == "localhost:9092");
}

TEST_CASE("kafka grammar: VARCHAR length is accepted and ignored") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* stmt = kafka_stmt(raw_parser(&arena, "CREATE SOURCE s (name VARCHAR(255)) WITH (KAFKA_TOPIC='t')", registry));
    REQUIRE(stmt != nullptr);
    auto cols = columns(stmt);
    REQUIRE(cols.size() == 1);
    CHECK(std::string_view(cols[0]->type) == "VARCHAR");
}

TEST_CASE("kafka grammar: CREATE STREAM captures the embedded query verbatim") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("with options before AS") {
        auto* stmt = kafka_stmt(raw_parser(&arena,
                                           "CREATE STREAM big_orders WITH (KAFKA_TOPIC='big', VALUE_FORMAT='JSON') "
                                           "AS SELECT id, amount FROM orders WHERE amount > 100;",
                                           registry));

        REQUIRE(stmt != nullptr);
        CHECK(stmt->kind == kafka_grammar::stmt_kind::create_stream);
        CHECK(std::string_view(stmt->name) == "big_orders");
        CHECK(option_value(stmt, "KAFKA_TOPIC") == "big");
        REQUIRE_FALSE(stmt->as_select.empty());
        // The raw tail (sans trailing ';') is handed back for re-parsing untouched.
        CHECK(std::string(stmt->as_select).find("SELECT id, amount FROM orders WHERE amount > 100") !=
              std::string::npos);
        CHECK(std::string(stmt->as_select).find(';') == std::string::npos);
    }

    SECTION("without a WITH clause") {
        auto* stmt = kafka_stmt(raw_parser(&arena, "CREATE STREAM s AS SELECT * FROM orders", registry));
        REQUIRE(stmt != nullptr);
        CHECK(stmt->options == nullptr);
        REQUIRE_FALSE(stmt->as_select.empty());
        CHECK(std::string(stmt->as_select).find("SELECT * FROM orders") != std::string::npos);
    }
}

TEST_CASE("kafka grammar: DROP variants") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("DROP SOURCE") {
        auto* stmt = kafka_stmt(raw_parser(&arena, "DROP SOURCE orders;", registry));
        REQUIRE(stmt != nullptr);
        CHECK(stmt->kind == kafka_grammar::stmt_kind::drop_object);
        CHECK(stmt->obj == kafka_grammar::object_kind::source);
        CHECK(std::string_view(stmt->name) == "orders");
        CHECK_FALSE(stmt->if_exists);
    }

    SECTION("DROP STREAM IF EXISTS") {
        auto* stmt = kafka_stmt(raw_parser(&arena, "DROP STREAM IF EXISTS big_orders", registry));
        REQUIRE(stmt != nullptr);
        CHECK(stmt->obj == kafka_grammar::object_kind::stream);
        CHECK(stmt->if_exists);
    }
}

TEST_CASE("kafka grammar: keywords are case-insensitive") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* stmt = kafka_stmt(raw_parser(&arena, "create source S (a int) with (kafka_topic='t')", registry));
    REQUIRE(stmt != nullptr);
    CHECK(stmt->kind == kafka_grammar::stmt_kind::create_source);
    // option keys keep their source case in the AST; normalization happens in transform
    CHECK(option_value(stmt, "kafka_topic") == "t");
}

TEST_CASE("kafka grammar: non-kafka SQL is not claimed") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("plain SELECT stays core") {
        auto* tree = raw_parser(&arena, "SELECT * FROM t;", registry);
        CHECK(nodeTag(linitial(tree)) == T_SelectStmt);
        CHECK(kafka_stmt(tree) == nullptr);
    }

    SECTION("CREATE TABLE stays core (no collision with our grammar)") {
        auto* tree = raw_parser(&arena, "CREATE TABLE t (a INT)", registry);
        CHECK(kafka_stmt(tree) == nullptr);
    }
}

TEST_CASE("kafka grammar: malformed kafka statements raise a parse error") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    CHECK_THROWS_AS(raw_parser(&arena, "CREATE SOURCE orders", registry), parser_exception_t);
    CHECK_THROWS_AS(raw_parser(&arena, "CREATE STREAM s", registry), parser_exception_t);
    CHECK_THROWS_AS(raw_parser(&arena, "CREATE SOURCE s (a INT)", registry), parser_exception_t); // no WITH
    // unknown column type rejected by the semantic guard in kafka_ext::parse
    CHECK_THROWS_AS(raw_parser(&arena, "CREATE SOURCE s (a FOOBAR) WITH (KAFKA_TOPIC='t')", registry),
                    parser_exception_t);
}

TEST_CASE("kafka grammar: transform lowers to a kafka_node_t") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* node = reinterpret_cast<Node*>(linitial(raw_parser(&arena,
                                                             "CREATE SOURCE orders (id BIGINT, note VARCHAR) "
                                                             "WITH (KAFKA_TOPIC='orders_topic', value_format='JSON')",
                                                             registry)));
    REQUIRE(nodeTag(node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*node);
    REQUIRE_FALSE(result.has_error());

    auto plan = result.node_ptr();
    REQUIRE(plan != nullptr);
    REQUIRE(plan->type() == lp::node_type::unused);

    auto* kn = dynamic_cast<otterstax::kafka::kafka_node_t*>(plan.get());
    REQUIRE(kn != nullptr);
    CHECK(kn->op() == otterstax::kafka::kafka_op::create_source);
    CHECK(kn->name() == "orders");

    REQUIRE(kn->columns().size() == 2);
    CHECK(kn->columns()[0].name == "id");
    CHECK(kn->columns()[0].type.type() == components::types::logical_type::BIGINT);
    CHECK(kn->columns()[0].type.alias() == "id");
    CHECK(kn->columns()[1].type.type() == components::types::logical_type::STRING_LITERAL);

    // WITH keys are upper-cased by transform and reachable via option()
    CHECK(kn->option("KAFKA_TOPIC") == "orders_topic");
    CHECK(kn->option("kafka_topic") == "orders_topic"); // option() is case-insensitive
    CHECK(kn->option("VALUE_FORMAT") == "JSON");
    CHECK_FALSE(kn->option("MISSING").has_value());
}

TEST_CASE("kafka grammar: transform folds identifiers, preserves topic case") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* node = reinterpret_cast<Node*>(linitial(
        raw_parser(&arena, "CREATE SOURCE OrDeRs (Id BIGINT, Amount DOUBLE) WITH (Kafka_Topic='MyTopic')", registry)));
    REQUIRE(nodeTag(node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*node);
    REQUIRE_FALSE(result.has_error());

    auto* kn = dynamic_cast<otterstax::kafka::kafka_node_t*>(result.node_ptr().get());
    REQUIRE(kn != nullptr);
    // object + column identifiers fold to lower
    CHECK(kn->name() == "orders");
    REQUIRE(kn->columns().size() == 2);
    CHECK(kn->columns()[0].name == "id");
    CHECK(kn->columns()[1].name == "amount");
    CHECK(kn->columns()[0].type.alias() == "id");
    // option key resolves case-insensitively; the topic value keeps its case
    CHECK(kn->option("KAFKA_TOPIC") == "MyTopic");
}

TEST_CASE("kafka grammar: transform of a DROP node") {
    auto registry = kafka_registry();
    std::pmr::monotonic_buffer_resource arena;

    auto* node = reinterpret_cast<Node*>(linitial(raw_parser(&arena, "DROP STREAM IF EXISTS s", registry)));
    REQUIRE(nodeTag(node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*node);
    REQUIRE_FALSE(result.has_error());

    auto* kn = dynamic_cast<otterstax::kafka::kafka_node_t*>(result.node_ptr().get());
    REQUIRE(kn != nullptr);
    CHECK(kn->op() == otterstax::kafka::kafka_op::drop_stream);
    CHECK(kn->if_exists());
    CHECK(kn->columns().empty());
}
