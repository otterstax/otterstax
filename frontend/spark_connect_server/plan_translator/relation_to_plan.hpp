// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>

#include <spark/connect/base.pb.h>
#include <spark/connect/relations.pb.h>

#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"

namespace frontend::spark {

    struct TranslationResult {
        ParsedQueryDataPtr parsed_data;
    };

    core::result_wrapper_t<TranslationResult> relation_to_plan(const ::spark::connect::Plan& plan,
                                                               std::pmr::memory_resource* resource);

    bool contains_window(const ::spark::connect::Relation& rel);

    // A table name as the SQL path reads it, split on the dots outside
    // backticks: `t`, `s.t`, `d.s.t` (a local database) or `c.d.s.t` (the
    // database d of the connection c). A name of more parts names no table:
    // sql_parse_error, never cut down.
    core::result_wrapper_t<qualified_name_t> parse_table_identifier(std::string_view ident,
                                                                    std::pmr::memory_resource* resource);

} // namespace frontend::spark
