// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/backend_type.hpp"
#include "types/otterbrix.hpp"
#include "utility/logger.hpp"

#include "otterbrix/parser/grammar_extension/kafka/kafka_extension.hpp"

#include <integration/cpp/otterbrix.hpp>

#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/transformer/transform_result.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// Which grammar extension claimed the statement. The extension roots
// (external_node_t, kafka_node_t) are both node_type::unused, so the plan
// root's type cannot tell them apart; the
// parser records the claiming extension here from the ExtensionNode envelope
// and the query session routes on it with a static_cast.
enum class extension_kind_t : uint8_t {
    none = 0,
    external = 1, // s3 / file: CREATE EXTERNAL TABLE, COPY (...) TO
    kafka = 2     // kafka DDL
};

struct ParsedQueryData {
    explicit ParsedQueryData(OtterbrixStatementPtr otterbrix_params,
                             components::sql::transform::transform_result&& binder,
                             NodeTag tag);

    components::sql::transform::transform_result& binder();

    OtterbrixStatementPtr otterbrix_params;

    NodeTag tag;

    extension_kind_t extension_kind{extension_kind_t::none};

    // The SQL text the statement was parsed from (GreenplumParser::parse); empty
    // for a plan a frontend built itself. The query session parses it again to
    // run the statement a second time (a plan is single-use).
    std::string sql;

    // A table-level DDL statement's target (CREATE TABLE, CREATE INDEX, DROP
    // TABLE, the indexed table of DROP INDEX) as written, every slot — the plan
    // node keeps only database and table; empty otherwise and for a plan a
    // frontend built itself.
    qualified_name_t ddl_target;

    // An INSERT / UPDATE / DELETE target written with a uid slot
    // (uid.db.schema.table), every slot; empty otherwise. The engine's
    // transformer keeps no uid on a DML target, so the query session runs such a
    // statement on its backend directly (query_session_t::run_remote_dml).
    qualified_name_t dml_target;

    // True while the statement's `$n` hold the placeholders the parser bound to
    // reach its plan (GreenplumParser::parse): the binder reads as fully bound,
    // but no client value has been bound yet. Cleared by the query session once
    // execute_prepared_statement has bound and finalized the real values.
    bool awaiting_bind{false};

private:
    components::sql::transform::transform_result binder_;
};

using ParsedQueryDataPtr = std::unique_ptr<ParsedQueryData>;

class IParser {
public:
    virtual ~IParser() = default;

    virtual core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) = 0;
};

class GreenplumParser : public IParser {
public:
    explicit GreenplumParser(std::pmr::memory_resource* resource);

    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override;

    // Parses `sql` — exactly one SELECT — and returns the transformed plan
    // root, binding every constant into `shared_params`: the caller's
    // parameter node, whose id counter continues, so the fragment's ids never
    // collide with those already in the caller's plan. For a host that builds
    // a plan itself and grafts SQL into it (the Spark Connect translator: a
    // spark.sql() leaf, a filter string). The fragment's table names stay as
    // written; the engine resolves them, remote ones through the federation. A
    // statement that lowers a sub-query is refused: the sub-plan would be
    // dropped with the rest of the fragment's execution plan.
    core::result_wrapper_t<components::logical_plan::node_ptr>
    parse_fragment(const std::string& sql, components::logical_plan::parameter_node_ptr shared_params);

private:
    std::pmr::memory_resource* resource_;
    log_t log_;
    // One shared registry for all DDL parser extensions: s3/file (CREATE EXTERNAL
    // TABLE / COPY ... TO → external_node_t) and kafka (CREATE/DROP SOURCE/STREAM
    // → kafka_node_t). Core SQL is parsed first; only core-rejected statements
    // reach these. Passed to both raw_parser (3-arg) and the transformer.
    components::sql::parser::parser_extension_registry_t registry_;
    // The constructor has no error channel; a failed extension registration is
    // kept here and reported by parse(), so the failure is never silent.
    std::optional<core::error_t> registration_error_;
};

using parser_ptr = std::unique_ptr<IParser>;

inline parser_ptr make_parser(std::pmr::memory_resource* resource) {
    return std::make_unique<GreenplumParser>(resource);
}
