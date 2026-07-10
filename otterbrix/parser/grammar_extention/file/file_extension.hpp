// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include <memory_resource>
#include <string>

#include <components/sql/parser/extension.hpp>

/*
 * Parser extension for local-file external tables. It teaches the engine two
 * statements the core SQL grammar rejects:
 *
 *   CREATE EXTERNAL TABLE <db>.<table> WITH (
 *       location = '/data/trades.parquet', format = '...' );
 *
 *   COPY (<select>) TO '/data/out.parquet' WITH ( format = '...' );
 *
 * It claims a statement only when the location is NOT an `s3://` URI (the
 * sibling `s3` extension claims s3:// URIs). Two stages, wired by
 * make_file_extension():
 *   - parse     (file_scan.l / file_gram.y): raw query  -> ExtensionNode wrapping file_ext::file_stmt
 *   - transform (file_extension.cpp)        : ExtensionNode -> logical_plan
 */
namespace file_ext {
    components::sql::parser::parse_extension_result_t parse(std::pmr::memory_resource* resource,
                                                            const std::string& query);

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* params);
} // namespace file_ext

inline components::sql::parser::parser_extension_t make_file_extension() {
    return components::sql::parser::parser_extension_t{"file", &file_ext::parse, &file_ext::transform};
}
