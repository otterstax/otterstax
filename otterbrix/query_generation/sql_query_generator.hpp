// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"

namespace sql_gen {

    void generate_values(std::stringstream& stream,
                         const components::vector::data_chunk_t& chunk,
                         backend_type_t backend = backend_type_t::MySQL);

    // Generate table reference string based on backend type
    // MySQL: database.collection
    // PostgreSQL: schema.collection (e.g., public.products)
    std::string table_reference(const collection_full_name_t& name, backend_type_t backend = backend_type_t::MySQL);

    std::string replace_qualifiers(std::string raw_sql,
                                   const std::vector<otterstax::parser::qualifier_rewrite_t>& quals,
                                   backend_type_t backend);

    void generate_query(std::stringstream& stream,
                        const components::logical_plan::node_ptr& node,
                        const components::logical_plan::storage_parameters* parameters,
                        backend_type_t backend = backend_type_t::MySQL);
    std::string generate_query(const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* parameters,
                               backend_type_t backend = backend_type_t::MySQL);

} // namespace sql_gen