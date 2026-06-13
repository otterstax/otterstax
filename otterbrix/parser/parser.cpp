// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parser.hpp"

#include "name_resolution.hpp"
#include "scheduler/schema_utils.hpp"
#include "subquery_extractor.hpp"
#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <cassert>
#include <deque>

using namespace components;

namespace {
    void swap_stubs_into_schema_nodes(std::pmr::memory_resource* resource,
                                      std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes,
                                      const std::vector<otterstax::parser::subquery_stub_t>& stubs) {
        OTX_ZONE_N("otterbrix::swap_stubs_into_schema_nodes");
        if (stubs.empty()) {
            return;
        }

        for (auto& batch : external_nodes) {
            for (auto& entry : batch) {
                auto& node_ref = *entry.node;
                if (node_ref->type() != logical_plan::node_type::aggregate_t) {
                    continue;
                }
                // A stub is an aggregate whose relname is the generated stub id.
                const std::string& relname =
                    static_cast<const logical_plan::node_aggregate_t&>(*node_ref).relname().t;
                if (relname.size() < otterstax::parser::k_stub_prefix.size() ||
                    relname.compare(0,
                                    otterstax::parser::k_stub_prefix.size(),
                                    otterstax::parser::k_stub_prefix) != 0) {
                    continue;
                }

                bool is_outer_aggregate = false;
                for (const auto& child : node_ref->children()) {
                    auto t = child->type();
                    if (t == logical_plan::node_type::select_t ||
                        t == logical_plan::node_type::match_t  ||
                        t == logical_plan::node_type::sort_t   ||
                        t == logical_plan::node_type::limit_t  ||
                        t == logical_plan::node_type::group_t  ||
                        t == logical_plan::node_type::having_t) {
                        is_outer_aggregate = true;
                        break;
                    }
                }

                if (is_outer_aggregate) {
                    continue;
                }
                for (const auto& stub : stubs) {
                    if (stub.stub_id != relname) {
                        continue;
                    }
                    // The resolved target for this slot is the promoted stub
                    // RangeVar (`<uid>.subq.subq.<stub_id>`). Stubs are always
                    // uid-qualified, so every stub slot has a resolved target.
                    const qualified_name_t& name = entry.target.name;
                    auto schema_node =
                        schema_utils::make_node_schema_raw(resource, name, stub.raw_sql, stub.qualifiers);
                    schema_node->set_result_alias(node_ref->result_alias());
                    node_ref = schema_node;
                    break;
                }
            }
        }
    }
} // namespace

static constexpr bool is_valid_external(logical_plan::node_type type) {
    switch (type) {
        case logical_plan::node_type::aggregate_t:
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::data_t: // Questionable
        case logical_plan::node_type::delete_t:
        case logical_plan::node_type::drop_collection_t:
        case logical_plan::node_type::drop_database_t:
        case logical_plan::node_type::drop_index_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
            return true;
        default:
            return false;
    }
}

static constexpr bool is_mutable(logical_plan::node_type type) {
    switch (type) {
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::drop_collection_t:
        case logical_plan::node_type::drop_database_t:
        case logical_plan::node_type::drop_index_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
        case logical_plan::node_type::delete_t:
            return true;
        default:
            return false;
    }
}

// True when the node carries a table reference resolvable through the name
// registry. Nodes with no name at all are local by construction and are
// skipped here BEFORE resolution, so they can never produce a resolution
// error. An aggregate_t with an empty relname is one of the wrapper
// aggregates the transformer builds around JOIN trees / SELECT-without-FROM.
static bool carries_table_reference(const logical_plan::node_t& node) {
    switch (node.type()) {
        case logical_plan::node_type::aggregate_t:
            return !static_cast<const logical_plan::node_aggregate_t&>(node).relname().t.empty();
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
        case logical_plan::node_type::delete_t:
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::drop_collection_t:
        case logical_plan::node_type::drop_index_t:
            // Names live on the catalog_resolve_* sibling(s) inside the
            // wrapping node_sequence_t; node_names() reads them via seq_ctx.
            return true;
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::drop_database_t:
            // No alias is grammatically possible — local by construction.
            return false;
        default:
            return false;
    }
}

// Resolves the secondary table of UPDATE ... FROM / DELETE ... USING: the
// wrapping sequence then carries a SECOND catalog_resolve_table_t. The first
// resolve_table is the DML target (consumed by node_names); the second, when
// present, is the FROM/USING source. Returns an empty name when there is no
// second table.
static core::result_wrapper_t<qualified_name_t>
resolve_dml_from_name(std::pmr::memory_resource* resource,
                      const otterstax::names::name_registry_t& registry,
                      const logical_plan::node_t* seq_ctx) {
    if (seq_ctx == nullptr) {
        return qualified_name_t{};
    }
    size_t resolve_tables_seen = 0;
    for (const auto& child : seq_ctx->children()) {
        if (!child || child->type() != logical_plan::node_type::catalog_resolve_table_t) {
            continue;
        }
        if (++resolve_tables_seen < 2) {
            continue;
        }
        const auto& resolve = static_cast<const logical_plan::node_catalog_resolve_table_t&>(*child);
        return otterstax::names::resolve_table_name(resource, registry, resolve.dbname(), resolve.relname());
    }
    return qualified_name_t{};
}

static core::result_wrapper_t<size_t>
get_external_nodes(std::pmr::memory_resource* resource,
                   const otterstax::names::name_registry_t& registry,
                   logical_plan::node_ptr& node,
                   std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes) {
    OTX_ZONE_N("otterbrix::get_external_nodes");
    struct lookup_node_t {
        logical_plan::node_ptr* ptr;
        logical_plan::node_ptr* parent_ptr;
        size_t batch_index;
    };

    external_nodes.emplace_back();
    size_t size = 0;
    std::deque<lookup_node_t> nodes_lookup;
    nodes_lookup.emplace_back(&node, nullptr, 0);
    while (!nodes_lookup.empty()) {
        auto& n = nodes_lookup.front();
        const auto type = (*n.ptr)->type();
        if (is_valid_external(type) && carries_table_reference(**n.ptr)) {
            // DML roots are node_sequence_t wrappers; the DML node's names
            // live on the sequence's catalog_resolve_table_t children, so
            // pass the parent sequence as resolution context.
            const logical_plan::node_t* seq_ctx =
                (n.parent_ptr != nullptr && (*n.parent_ptr)->type() == logical_plan::node_type::sequence_t)
                    ? n.parent_ptr->get()
                    : nullptr;
            auto resolved = otterstax::names::node_names(**n.ptr, registry, seq_ctx);
            if (resolved.has_error()) {
                return resolved.error();
            }
            qualified_name_t name = std::move(resolved.value());
            // An empty unique_identifier marks a LOCAL (otterbrix) table —
            // not external, not an error.
            if (!name.unique_identifier.empty()) {
                qualified_name_t from_name;
                // The second resolve_table of the wrapping sequence carries the
                // UPDATE...FROM / DELETE...USING source, or the index of a
                // DROP INDEX (its first resolve_table is the indexed table).
                if (type == logical_plan::node_type::update_t || type == logical_plan::node_type::delete_t ||
                    type == logical_plan::node_type::drop_index_t) {
                    auto from_resolved = resolve_dml_from_name(resource, registry, seq_ctx);
                    if (from_resolved.has_error()) {
                        return from_resolved.error();
                    }
                    from_name = std::move(from_resolved.value());
                }
                {
                    // TODO: remove this segment when connection pool will be added
                    // For now uid call can not repeat inside a batch
                    auto it = std::find_if(external_nodes[n.batch_index].begin(),
                                           external_nodes[n.batch_index].end(),
                                           [&name](const auto& entry) {
                                               return entry.target.name.unique_identifier == name.unique_identifier;
                                           });
                    if (it != external_nodes[n.batch_index].end()) {
                        ++n.batch_index;
                        if (external_nodes.size() == n.batch_index) {
                            external_nodes.emplace_back();
                        }
                    }
                }
                external_nodes[n.batch_index].push_back(
                    external_entry_t{n.ptr,
                                     otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                         std::move(name),
                                                                         std::move(from_name)}});
                ++size;
            }
        }
        bool mutable_node = is_mutable(type);
        if (mutable_node) {
            external_nodes.emplace_back();
        }
        for (auto& child : (*n.ptr)->children()) {
            nodes_lookup.emplace_back(&child, n.ptr, n.batch_index + mutable_node);
        }
        nodes_lookup.pop_front();
    }

    if (external_nodes.back().empty()) {
        external_nodes.erase(external_nodes.end() - 1);
    }
    return size;
}

ParsedQueryData::ParsedQueryData(OtterbrixStatementPtr otterbrix_params,
                                 components::sql::transform::transform_result&& binder,
                                 NodeTag tag)
    : otterbrix_params(std::move(otterbrix_params))
    , binder_(std::move(binder))
    , tag(tag) {}

components::sql::transform::transform_result& ParsedQueryData::binder() { return binder_; }

GreenplumParser::GreenplumParser(std::pmr::memory_resource* resource)
    : resource_(resource)
    , log_(get_logger(logger_tag::PARSER)) {
    assert(resource_ != nullptr && "memory resource must not be null");
    assert(log_.is_valid());
}

core::result_wrapper_t<ParsedQueryDataPtr> GreenplumParser::parse(const std::string& sql) {
    OTX_ZONE_N("otterbrix::parse");
    log_->info("parse: starting for: {}", sql.substr(0, 100));
    try {
        tracy_memory_resource arena_mr(resource_, "parser::arena");
        std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);
        sql::transform::transformer transformer(resource_);

        ::Node* reusable_root = nullptr;
        auto extraction = otterstax::parser::prepare_sql(sql, &arena_resource, &reusable_root);
        log_->trace("parse: prepare_sql produced {} stub(s), modified SQL: {}",
                    extraction.stubs.size(),
                    extraction.modified_sql.substr(0, 200));

        ::Node* res = nullptr;
        if (extraction.stubs.empty() && reusable_root) {
            log_->trace("parse: reusing prepare_sql AST (no extraction)");
            res = reusable_root;
        } else {
            log_->trace("parse: calling raw_parser on modified SQL");
            auto* raw = raw_parser(&arena_resource, extraction.modified_sql.c_str());
            if (!raw || list_length(raw) == 0) {
                // raw_parser returns null on a syntax error and an EMPTY list
                // for input with no statements (e.g. "") — linitial on an
                // empty list is undefined behaviour, so guard both here.
                log_->error("parse: raw_parser returned no statements for SQL: {}", sql.substr(0, 100));
                return core::error_t{core::error_code_t::sql_parse_error,
                                     std::pmr::string{"syntax error", resource_}};
            }
            res = reinterpret_cast<::Node*>(linitial(raw));
            otterstax::parser::promote_three_part_qualifiers(res);
        }

        // Collect every RangeVar's full name (uid.db.schema.rel) from the
        // promoted raw AST — including the stub RangeVars
        // (`<uid>.subq.subq.__otterstax_subq_N`) injected by prepare_sql —
        // before the transformer folds names down to (dbname, relname).
        otterstax::names::name_registry_t registry(&arena_resource);
        otterstax::parser::collect_qualified_names(res, registry);

        auto tag = nodeTag(res);
        log_->trace("parse: calling transformer.transform");
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));
        log_->trace("parse: transformer.transform complete");

        if (binder.has_error()) {
            log_->error("parse: transformer error: {}", binder.get_error().what.c_str());
            return binder.get_error();
        }

        auto node = binder.node_ptr();
        if (!node) {
            log_->error("parse: transformer returned null root node — unsupported statement");
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"Unsupported node type", resource_}};
        }

        const size_t param_cnt = binder.parameter_count();
        auto params = binder.params_ptr();

        log_->trace("parse: building ParsedQueryData, param_count={}", param_cnt);
        ParsedQueryDataPtr result = std::make_unique<ParsedQueryData>(
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource_},
                                                 std::move(params),
                                                 std::move(node),
                                                 0,
                                                 param_cnt),
            std::move(binder),
            tag);

        auto external_count = get_external_nodes(resource_,
                                                 registry,
                                                 result->otterbrix_params->node,
                                                 result->otterbrix_params->external_nodes);
        if (external_count.has_error()) {
            log_->error("parse: external node name resolution failed: {}", external_count.error().what.c_str());
            return external_count.error();
        }
        result->otterbrix_params->external_nodes_count = external_count.value();
        log_->info("parse: external_nodes_count={}", result->otterbrix_params->external_nodes_count);

        swap_stubs_into_schema_nodes(resource_, result->otterbrix_params->external_nodes, extraction.stubs);
        log_->trace("parse: swap_stubs_into_schema_nodes complete");

        return result;
    } catch (const std::exception& e) {
        log_->error("parse: caught exception: {}", e.what());
        return core::error_t{core::error_code_t::sql_parse_error, e.what()};
    } catch (...) {
        log_->error("parse: caught unknown exception");
        return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"syntax error", resource_}};
    }
}
