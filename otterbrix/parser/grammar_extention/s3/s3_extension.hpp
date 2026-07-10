// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include <memory_resource>
#include <string>

#include <components/sql/parser/extension.hpp>

/*
 * Parser extension for S3-backed external tables. It teaches the engine two
 * statements the core SQL grammar rejects:
 *
 *   CREATE EXTERNAL TABLE <db>.<table> WITH (
 *       s3_alias = '...', location = 's3://bucket/key', format = '...' );
 *
 *   COPY (<select>) TO 's3://bucket/key' WITH ( s3_alias = '...', format = '...' );
 *
 * It claims a statement only when the location is an `s3://` URI (the sibling
 * `file` extension claims local paths). Two stages, wired by make_s3_extension():
 *   - parse     (s3_scan.l / s3_gram.y): raw query  -> ExtensionNode wrapping s3_ext::s3_stmt
 *   - transform (s3_extension.cpp)       : ExtensionNode -> logical_plan
 */
namespace s3_ext {
    components::sql::parser::parse_extension_result_t parse(std::pmr::memory_resource* resource,
                                                            const std::string& query);

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* params);
} // namespace s3_ext

inline components::sql::parser::parser_extension_t make_s3_extension() {
    return components::sql::parser::parser_extension_t{"s3", &s3_ext::parse, &s3_ext::transform};
}
