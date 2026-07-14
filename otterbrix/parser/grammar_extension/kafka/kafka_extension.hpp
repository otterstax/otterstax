// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <memory_resource>
#include <string>

#include <components/sql/parser/extension.hpp>

namespace kafka_ext {
    components::sql::parser::parse_extension_result_t parse(std::pmr::memory_resource* resource,
                                                            const std::string& query);

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* params);
} // namespace kafka_ext

inline components::sql::parser::parser_extension_t make_kafka_extension() {
    return components::sql::parser::parser_extension_t{"kafka", &kafka_ext::parse, &kafka_ext::transform};
}
