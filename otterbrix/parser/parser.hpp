// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "types/otterbrix.hpp"
#include "utility/logger.hpp"

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
};

class GreenplumParser : public IParser {
public:
    explicit GreenplumParser(std::pmr::memory_resource* resource);

    core::result_wrapper_t<ParsedQueryDataPtr> parse(const std::string& sql) override;

private:
    std::pmr::memory_resource* resource_;
    log_t log_;
    // s3/file DDL extensions (CREATE EXTERNAL TABLE / COPY ... TO). Core SQL is
    // parsed first; only core-rejected statements reach these, whose transform
    // yields an otterstax::external::external_node_t. Passed to both raw_parser
    // (3-arg) and the transformer.
    components::sql::parser::parser_extension_registry_t registry_;
};

using parser_ptr = std::unique_ptr<IParser>;

inline parser_ptr make_parser(std::pmr::memory_resource* resource) {
    return std::make_unique<GreenplumParser>(resource);
}
