// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "host_functions.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "utility/server_version.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_register_udf.hpp>
#include <components/vector/vector_buffer.hpp>

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

namespace otterstax::catalog {
    namespace {
        namespace compute = components::compute;
        using components::types::logical_type;

        core::error_t version_kernel(compute::kernel_context&,
                                     const components::vector::data_chunk_t& in,
                                     components::vector::vector_t& out) {
            auto* heap = static_cast<components::vector::string_vector_buffer_t*>(out.auxiliary().get());
            auto* rows = out.data<std::string_view>();
            for (uint64_t row = 0; row < in.size(); ++row) {
                auto* text = static_cast<char*>(heap->empty_string(version_text.size()));
                std::memcpy(text, version_text.data(), version_text.size());
                rows[row] = std::string_view{text, version_text.size()};
                out.set_null(row, false);
            }
            return core::error_t::no_error();
        }

        // obj_description / col_description: OtterStax keeps no comment on any object, NULL for every row.
        core::error_t no_comment_kernel(compute::kernel_context&,
                                        const components::vector::data_chunk_t& in,
                                        components::vector::vector_t& out) {
            for (uint64_t row = 0; row < in.size(); ++row) {
                out.set_null(row, true);
            }
            return core::error_t::no_error();
        }

        // A host function of `arguments` arguments of any type, answering `result`.
        compute::function_ptr make_function(std::pmr::memory_resource* resource,
                                            std::string_view name,
                                            std::size_t arguments,
                                            logical_type result,
                                            compute::vector_exec_fn kernel,
                                            std::string_view summary) {
            compute::function_doc doc{resource, summary, summary, {}, false};
            auto function = core::pmr::make_polymorphic_unique<compute::vector_function>(resource,
                                                                                        name,
                                                                                        compute::arity::fixed_num(arguments),
                                                                                        doc,
                                                                                        size_t{1},
                                                                                        compute::volatility_t::stable);
            std::pmr::vector<compute::parameter_type> parameters{resource};
            for (std::size_t i = 0; i < arguments; ++i) {
                parameters.push_back(compute::parameter_type::variable(static_cast<compute::parameter_type::variable_id>(i)));
            }
            compute::kernel_signature_t signature(compute::function_type_t::vector,
                                                  std::move(parameters),
                                                  {compute::output_type::fixed(result)});
            [[maybe_unused]] const auto added =
                function->add_kernel(resource, compute::vector_kernel{std::move(signature), kernel});
            assert(!added.contains_error() && "a host function has one kernel slot");
            return function;
        }
    } // namespace

    std::span<const std::string_view> host_function_names() noexcept {
        static constexpr std::string_view names[] = {"version", "obj_description", "col_description"};
        return names;
    }

    compute::function_ptr make_version_function(std::pmr::memory_resource* resource) {
        compute::function_doc doc{resource, "Server version", "VERSION() -> text; the PostgreSQL version spoken", {}, false};
        auto function = core::pmr::make_polymorphic_unique<compute::vector_function>(resource,
                                                                                    "version",
                                                                                    compute::arity::fixed_num(0),
                                                                                    doc,
                                                                                    size_t{1},
                                                                                    compute::volatility_t::stable);
        compute::kernel_signature_t signature(compute::function_type_t::vector,
                                              std::pmr::vector<compute::parameter_type>{resource},
                                              {compute::output_type::fixed(logical_type::TEXT)});
        [[maybe_unused]] const auto added =
            function->add_kernel(resource, compute::vector_kernel{std::move(signature), version_kernel});
        assert(!added.contains_error() && "version() has one kernel slot and no argument");
        return function;
    }

    std::pmr::vector<components::logical_plan::node_ptr> host_function_registrations(std::pmr::memory_resource* resource) {
        OTX_ZONE_N("catalog::host_function_registrations");
        const qualified_name_t local_public{core::dbname_t{std::string{federation::local_database}},
                                           core::schema_t{std::string{host_function_schema}},
                                           core::relname_t{}};
        std::pmr::vector<components::logical_plan::node_ptr> nodes{resource};
        nodes.emplace_back(new components::logical_plan::node_register_udf_t(resource,
                                                                             make_version_function(resource),
                                                                             local_public));
        // PostgreSQL's catalog functions drivers' introspection calls (Django's get_table_list /
        // get_table_description), which the engine does not have yet.
        std::vector<compute::function_ptr> catalog_functions;
        catalog_functions.push_back(make_function(resource,
                                                  "obj_description",
                                                  2,
                                                  logical_type::TEXT,
                                                  no_comment_kernel,
                                                  "obj_description(oid, catalog) -> text: NULL, OtterStax keeps no comments"));
        catalog_functions.push_back(make_function(resource,
                                                  "col_description",
                                                  2,
                                                  logical_type::TEXT,
                                                  no_comment_kernel,
                                                  "col_description(oid, attnum) -> text: NULL, OtterStax keeps no comments"));
        for (auto& function : catalog_functions) {
            nodes.emplace_back(
                new components::logical_plan::node_register_udf_t(resource, std::move(function), local_public));
        }
        return nodes;
    }
} // namespace otterstax::catalog
