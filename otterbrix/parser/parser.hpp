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

struct ParsedQueryData {
    explicit ParsedQueryData(OtterbrixStatementPtr otterbrix_params,
                             components::sql::transform::transform_result&& binder,
                             NodeTag tag);

    components::sql::transform::transform_result& binder();

    OtterbrixStatementPtr otterbrix_params;

    NodeTag tag;

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

    // Parses `sql` and returns the transformed logical-plan root, appending any
    // constant parameters into `shared_params` (a single shared id-space) via
    // the transformer's execution_plan path. Lighter than parse(): performs no
    // external-node analysis — intended for synthesizing a sub-plan whose
    // fragment (e.g. a WHERE predicate) is re-grafted into an outer plan. The
    // caller owns `shared_params`; its id counter continues, so the appended
    // ids never collide with parameters already in the outer plan.
    virtual core::result_wrapper_t<components::logical_plan::node_ptr>
    parse_fragment(const std::string& sql,
                   components::logical_plan::parameter_node_ptr shared_params) = 0;
};

class GreenplumParser : public IParser {
public:
    explicit GreenplumParser(std::pmr::memory_resource* resource);

    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override;

    core::result_wrapper_t<components::logical_plan::node_ptr>
    parse_fragment(const std::string& sql,
                   components::logical_plan::parameter_node_ptr shared_params) override;

private:
    std::pmr::memory_resource* resource_;
    log_t log_;
    // One shared registry for all DDL parser extensions: s3/file (CREATE EXTERNAL
    // TABLE / COPY ... TO → external_node_t) and kafka (CREATE/DROP SOURCE/STREAM
    // → kafka_node_t). Core SQL is parsed first; only core-rejected statements
    // reach these. Passed to both raw_parser (3-arg) and the transformer.
    components::sql::parser::parser_extension_registry_t registry_;
};

using parser_ptr = std::unique_ptr<IParser>;

inline parser_ptr make_parser(std::pmr::memory_resource* resource) {
    return std::make_unique<GreenplumParser>(resource);
}
