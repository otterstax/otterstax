// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "types/otterbrix.hpp"
#include "utility/logger.hpp"

#include "otterbrix/parser/grammar_extension/kafka/kafka_extension.hpp"

#include <otterbrix/otterbrix.hpp>

#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/transformer/transform_result.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

enum class backend_type_t : uint8_t {
    Unknown = 0,
    MySQL = 1,
    PostgreSQL = 2,
    Mixed = 3,
    Otterbrix = 4, // No external nodes, but should be executed by otterbrix
    ClickHouse = 5
};

// Which grammar extension claimed the statement. The extension roots
// (external_node_t, kafka_node_t) are all node_type::unused, as is the
// schema_node_t stub, so the plan root's type cannot tell them apart; the
// parser records the claiming extension here from the ExtensionNode envelope
// and the Worker routes on it with a static_cast.
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

    backend_type_t backend_type{backend_type_t::Unknown}; // Set by CatalogManager during get_catalog_schema

    // For mixed backend: maps connection UID to its backend type
    std::unordered_map<std::string, backend_type_t> node_backend_types;

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
    // Starts from `seed` (a host's own extensions); the built-in s3/file/kafka
    // extensions are registered on top of it. A name collision between the two
    // sets is not a degraded parser: every parse() then fails with the
    // registration error.
    GreenplumParser(std::pmr::memory_resource* resource, components::sql::parser::parser_extension_registry_t seed);

    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override;

    // Parses `sql` — exactly one SELECT — and returns the transformed plan
    // root, binding every constant into `shared_params`: the caller's
    // parameter node, whose id counter continues, so the fragment's ids never
    // collide with those already in the caller's plan. For a host that builds
    // a plan itself and grafts SQL into it (the Spark Connect translator: a
    // spark.sql() leaf, a filter string). No external-node analysis: `names`
    // receives the full name of every table the statement reads, as parse()
    // collects them, for the caller to resolve the returned nodes against. A
    // statement that lowers a sub-query is refused: the sub-plan would be
    // dropped with the rest of the fragment's execution plan.
    core::result_wrapper_t<components::logical_plan::node_ptr>
    parse_fragment(const std::string& sql,
                   components::logical_plan::parameter_node_ptr shared_params,
                   otterstax::names::name_registry_t& names);

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
