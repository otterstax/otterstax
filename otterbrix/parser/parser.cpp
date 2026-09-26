// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parser.hpp"

#include "grammar_extention/file/file_extension.hpp"
#include "grammar_extention/s3/s3_extension.hpp"
#include "name_resolution.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "subquery_extractor.hpp"
#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <cassert>
#include <deque>
#include <exception>

using namespace components;

namespace {
    void swap_stubs_into_schema_nodes(std::pmr::memory_resource* resource,
                                      std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes,
                                      const std::pmr::vector<otterstax::parser::subquery_stub_t>& stubs) {
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
                    if (std::string_view{stub.stub_id} != relname) {
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

    // raw_parser is the one throwing call of the parse pipeline: both
    // core-grammar and extension syntax errors arrive as parser_exception_t.
    core::result_wrapper_t<::List*> run_raw_parser(std::pmr::memory_resource* arena,
                                                   std::pmr::memory_resource* resource,
                                                   const std::pmr::string& sql,
                                                   const sql::parser::parser_extension_registry_t& registry) {
        try {
            return raw_parser(arena, sql.c_str(), registry);
        } catch (const std::exception& e) {
            return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{e.what(), resource}};
        }
    }

    // The transformer reports its own failures through transform_result; the
    // guard covers the engine helpers it calls that throw.
    core::result_wrapper_t<sql::transform::transform_result>
    run_transform(std::pmr::memory_resource* resource, sql::transform::transformer& transformer, ::Node* root) {
        try {
            return transformer.transform(sql::transform::pg_cell_to_node_cast(root));
        } catch (const std::exception& e) {
            return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{e.what(), resource}};
        }
    }
} // namespace

// Drop kinds are classified by exhaustive switches; -Wswitch is promoted to an
// error for them so a kind the engine adds later fails the build here instead
// of silently landing in a default branch.
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"

// True for the drop kinds whose target may be alias-qualified: a table, or an
// index through its table. The other kinds name engine-local objects only.
static bool drop_targets_table(logical_plan::drop_target_kind kind) {
    switch (kind) {
        case logical_plan::drop_target_kind::collection:
        case logical_plan::drop_target_kind::index:
            return true;
        case logical_plan::drop_target_kind::database:
        case logical_plan::drop_target_kind::type:
        case logical_plan::drop_target_kind::sequence:
        case logical_plan::drop_target_kind::view:
        case logical_plan::drop_target_kind::macro:
            return false;
    }
    assert(false && "drop_target_kind switch above is exhaustive");
    return false;
}

// True for the drop kinds that change what a later batch may observe, so they
// close the current external batch.
static bool drop_is_mutable(logical_plan::drop_target_kind kind) {
    switch (kind) {
        case logical_plan::drop_target_kind::collection:
        case logical_plan::drop_target_kind::database:
        case logical_plan::drop_target_kind::index:
            return true;
        case logical_plan::drop_target_kind::type:
        case logical_plan::drop_target_kind::sequence:
        case logical_plan::drop_target_kind::view:
        case logical_plan::drop_target_kind::macro:
            return false;
    }
    assert(false && "drop_target_kind switch above is exhaustive");
    return false;
}

static bool is_valid_external(const logical_plan::node_t& node) {
    switch (node.type()) {
        case logical_plan::node_type::aggregate_t:
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::data_t: // Questionable
        case logical_plan::node_type::delete_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
            return true;
        case logical_plan::node_type::drop_t:
            return drop_targets_table(static_cast<const logical_plan::node_drop_t&>(node).kind());
        default:
            return false;
    }
}

static bool is_mutable(const logical_plan::node_t& node) {
    switch (node.type()) {
        case logical_plan::node_type::create_collection_t:
        case logical_plan::node_type::create_database_t:
        case logical_plan::node_type::create_index_t:
        case logical_plan::node_type::insert_t:
        case logical_plan::node_type::update_t:
        case logical_plan::node_type::delete_t:
            return true;
        case logical_plan::node_type::drop_t:
            return drop_is_mutable(static_cast<const logical_plan::node_drop_t&>(node).kind());
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
            // The node names its target table itself; node_names() reads it.
            return true;
        case logical_plan::node_type::drop_t:
            return drop_targets_table(static_cast<const logical_plan::node_drop_t&>(node).kind());
        case logical_plan::node_type::create_database_t:
            // No alias is grammatically possible — local by construction.
            return false;
        default:
            return false;
    }
}

#pragma GCC diagnostic pop

// The index of no child at all.
static constexpr size_t k_no_source_child = static_cast<size_t>(-1);

// The FROM / USING source child of an UPDATE / DELETE: the one child that is
// neither the WHERE (match) nor the LIMIT. k_no_source_child when the node
// carries none — or more than one, a shape the generator refuses rather than
// guess which table the statement names. This MUST pick the same child the
// generator's collect_dml_parts picks: the name resolved from it is the name
// the generator writes into the statement.
static size_t dml_source_child(const logical_plan::node_t& node) {
    size_t found = k_no_source_child;
    const auto& children = node.children();
    for (size_t i = 0; i < children.size(); ++i) {
        const auto type = children[i]->type();
        if (type == logical_plan::node_type::match_t || type == logical_plan::node_type::limit_t) {
            continue;
        }
        if (found != k_no_source_child) {
            return k_no_source_child;
        }
        found = i;
    }
    return found;
}

// Only a plain table is pushed down as that source (the generator's rule): an
// aggregate that names a relation and reads nothing under it. A join tree, a
// derived table or a table function stays a sub-plan for the engine to run.
static bool is_plain_table_source(const logical_plan::node_t& node) {
    if (node.type() != logical_plan::node_type::aggregate_t || !node.children().empty()) {
        return false;
    }
    const std::string& relname = static_cast<const logical_plan::node_aggregate_t&>(node).relname().t;
    // A subquery stub is a childless aggregate too, but it names no backend
    // table: swap_stubs_into_schema_nodes still has to replace it with the
    // extracted raw SQL, and it only reaches the stub through a slot of its
    // own. Pushing it down would name `__otterstax_subq_N` as a real table.
    return !relname.empty() &&
           (relname.size() < otterstax::parser::k_stub_prefix.size() ||
            relname.compare(0, otterstax::parser::k_stub_prefix.size(), otterstax::parser::k_stub_prefix) != 0);
}

static core::result_wrapper_t<size_t>
get_external_nodes(std::pmr::memory_resource* resource,
                   const otterstax::names::name_registry_t& registry,
                   logical_plan::node_ptr& node,
                   std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes) {
    OTX_ZONE_N("otterbrix::get_external_nodes");
    struct lookup_node_t {
        logical_plan::node_ptr* ptr;
        size_t batch_index;
    };

    external_nodes.emplace_back();
    size_t size = 0;
    std::pmr::deque<lookup_node_t> nodes_lookup{resource};
    nodes_lookup.emplace_back(&node, 0);
    while (!nodes_lookup.empty()) {
        auto& n = nodes_lookup.front();
        const auto type = (*n.ptr)->type();
        // The child this node's slot already names through from_name, if any:
        // it is generated as part of this statement, never fetched on its own.
        size_t pushed_down_source = k_no_source_child;
        if (is_valid_external(**n.ptr) && carries_table_reference(**n.ptr)) {
            auto resolved = otterstax::names::node_names(**n.ptr, registry);
            if (resolved.has_error()) {
                return resolved.convert_error<size_t>();
            }
            qualified_name_t name = std::move(resolved.value());
            // An empty unique_identifier marks a LOCAL (otterbrix) table —
            // not external, not an error.
            if (!name.unique_identifier.empty()) {
                qualified_name_t from_name;
                // The second name the statement targets. A DROP INDEX names the
                // indexed table (`name`) and, apart from it, the index. An
                // UPDATE / DELETE carries its FROM / USING source as a child
                // sub-plan instead: resolving a plain-table source into
                // from_name is what lets the generator write ONE two-table
                // statement. That child then gets no slot of its own — batches
                // run innermost-first, so a slot there would be fetched and
                // replaced with raw data BEFORE the DML is generated, leaving
                // the statement a source it can no longer name.
                if (type == logical_plan::node_type::drop_t &&
                    static_cast<const logical_plan::node_drop_t&>(**n.ptr).kind() ==
                        logical_plan::drop_target_kind::index) {
                    const auto& drop = static_cast<const logical_plan::node_drop_t&>(**n.ptr);
                    auto index_resolved =
                        otterstax::names::resolve_table_name(resource, registry, drop.dbname(), drop.index_name());
                    if (index_resolved.has_error()) {
                        return index_resolved.convert_error<size_t>();
                    }
                    from_name = std::move(index_resolved.value());
                } else if (type == logical_plan::node_type::update_t || type == logical_plan::node_type::delete_t) {
                    const size_t source = dml_source_child(**n.ptr);
                    if (source != k_no_source_child && is_plain_table_source(*(*n.ptr)->children()[source])) {
                        auto source_resolved = otterstax::names::node_names(*(*n.ptr)->children()[source], registry);
                        if (source_resolved.has_error()) {
                            return source_resolved.convert_error<size_t>();
                        }
                        // A local source resolves to a uid-less name; the
                        // generator reads that as "not on this backend" and
                        // refuses, which is the accurate refusal.
                        from_name = std::move(source_resolved.value());
                        pushed_down_source = source;
                    }
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
        bool mutable_node = is_mutable(**n.ptr);
        if (mutable_node) {
            external_nodes.emplace_back();
        }
        auto& children = (*n.ptr)->children();
        for (size_t i = 0; i < children.size(); ++i) {
            if (i == pushed_down_source) {
                continue;
            }
            nodes_lookup.emplace_back(&children[i], n.batch_index + mutable_node);
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
    : GreenplumParser(resource, components::sql::parser::parser_extension_registry_t{}) {}

GreenplumParser::GreenplumParser(std::pmr::memory_resource* resource,
                                 components::sql::parser::parser_extension_registry_t seed)
    : resource_(resource)
    , log_(get_logger(logger_tag::PARSER))
    , registry_(std::move(seed)) {
    assert(resource_ != nullptr && "memory resource must not be null");
    assert(log_.is_valid());

    // Register the s3/file/kafka DDL extensions once into the shared registry;
    // raw_parser/transformer consult them only for statements the core
    // grammar rejects. A dropped registration is not a no-op: the extension's
    // statements would fail to parse for the life of the process, so the first
    // failure is kept and every parse() reports it.
    auto register_extension = [this](auto extension, const char* name) {
        if (registration_error_) {
            return;
        }
        auto added = registry_.add(std::move(extension));
        if (added.has_error()) {
            log_->error("failed to register {} parser extension: {}", name, added.error().what.c_str());
            registration_error_.emplace(added.error().type, std::pmr::string{added.error().what, resource_});
        }
    };
    register_extension(make_s3_extension(), "s3");
    register_extension(make_file_extension(), "file");
    register_extension(make_kafka_extension(), "kafka");
}

core::result_wrapper_t<ParsedQueryDataPtr> GreenplumParser::parse(const std::string& sql) {
    OTX_ZONE_N("otterbrix::parse");
    log_->info("parse: starting for: {}", std::string_view{sql}.substr(0, 100));
    if (registration_error_) {
        log_->error("parse: parser extensions are not registered: {}", registration_error_->what.c_str());
        return core::error_t{registration_error_->type, std::pmr::string{registration_error_->what, resource_}};
    }

    tracy_memory_resource arena_mr(resource_, "parser::arena");
    std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);

    ::Node* reusable_root = nullptr;
    // The extraction lives on resource_, the AST on the per-parse arena; the
    // stubs are copied onto resource_ again by swap_stubs_into_schema_nodes.
    auto extraction = otterstax::parser::prepare_sql(sql, &arena_resource, resource_, &reusable_root);
    log_->trace("parse: prepare_sql produced {} stub(s), modified SQL: {}",
                extraction.stubs.size(),
                std::string_view{extraction.modified_sql}.substr(0, 200));

    ::Node* res = nullptr;
    if (extraction.stubs.empty() && reusable_root) {
        log_->trace("parse: reusing prepare_sql AST (no extraction)");
        res = reusable_root;
    } else {
        log_->trace("parse: calling raw_parser on modified SQL");
        // 3-arg form: consult the s3/file/kafka extensions for core-rejected DDL.
        auto raw = run_raw_parser(&arena_resource, resource_, extraction.modified_sql, registry_);
        if (raw.has_error()) {
            log_->error("parse: raw_parser failed: {}", raw.error().what.c_str());
            return raw.convert_error<ParsedQueryDataPtr>();
        }
        // raw_parser returns NIL both on a syntax error nobody claimed and for
        // input with no statements (e.g. "" or ";").
        const size_t statement_count = list_length(raw.value());
        if (statement_count == 0) {
            log_->error("parse: raw_parser returned no statements for SQL: {}", std::string_view{sql}.substr(0, 100));
            return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"syntax error", resource_}};
        }
        // One statement per parse(): the pipeline builds exactly one plan, so a
        // trailing statement would otherwise be dropped without a trace.
        if (statement_count > 1) {
            log_->error("parse: {} statements in one query", statement_count);
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"multiple statements in one query are not supported", resource_}};
        }
        res = reinterpret_cast<::Node*>(linitial(raw.value()));
        otterstax::parser::promote_three_part_qualifiers(res);
    }

    auto tag = nodeTag(res);
    extension_kind_t extension_kind = extension_kind_t::none;
    if (tag == T_ExtensionNode) {
        // Only the three built-in extensions produce a root this pipeline can
        // route; a seed extension of the host is not one of them.
        const char* extension_id = reinterpret_cast<::ExtensionNode*>(res)->extension_id;
        const std::string_view id = extension_id == nullptr ? std::string_view{} : std::string_view{extension_id};
        if (id == "s3" || id == "file") {
            extension_kind = extension_kind_t::external;
        } else if (id == "kafka") {
            extension_kind = extension_kind_t::kafka;
        } else {
            log_->error("parse: unknown parser extension '{}'", id);
            std::pmr::string what{"unknown parser extension '", resource_};
            what.append(id);
            what.push_back('\'');
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(what)};
        }
    }
    // The transformer lowers EXPLAIN to the INNER statement's plan and
    // stamps the explain mode on the transform_result's private plan — which
    // this pipeline discards (execute_plan.cpp rebuilds a fresh
    // execution_plan_t). Without this guard "EXPLAIN DELETE ..." would
    // silently EXECUTE the inner statement, so reject up front.
    if (tag == T_ExplainStmt) {
        log_->error("parse: EXPLAIN is not supported by OtterStax");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"EXPLAIN is not supported by OtterStax", resource_}};
    }
    // The transformer lowers only the first object of a DROP list; the rest
    // would be dropped from the plan silently, so reject the list form.
    if (tag == T_DropStmt && list_length(reinterpret_cast<::DropStmt*>(res)->objects) > 1) {
        log_->error("parse: DROP with several objects is not supported by OtterStax");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"DROP of several objects in one statement is not supported", resource_}};
    }

    // Collect every RangeVar's full name (uid.db.schema.rel) from the
    // promoted raw AST — including the stub RangeVars
    // (`<uid>.subq.subq.__otterstax_subq_N`) injected by prepare_sql —
    // before the transformer folds names down to (dbname, relname).
    otterstax::names::name_registry_t registry(&arena_resource);
    otterstax::parser::collect_qualified_names(resource_, res, registry);

    // The transformer stores a CHECK expression and a VIEW body as the verbatim
    // text the user wrote, sliced out of the statement by the byte offsets the
    // grammar recorded while parsing it (transformer/impl/view_body_text.hpp,
    // transformer/utils.cpp slice_check_expression). So it must be handed the
    // exact string `res` was parsed from: `extraction.modified_sql`, which is a
    // copy of `sql` when nothing was extracted and the stub-rewritten statement
    // when a federated sub-query was — passing the original instead would put
    // every offset after a rewrite out by the length of the stub. Given nothing,
    // the engine refuses the statement outright ("the statement text is not
    // available"). The string outlives the transform below.
    sql::transform::transformer transformer(resource_, extraction.modified_sql.c_str(), &registry_);
    log_->trace("parse: calling transformer.transform");
    auto transformed = run_transform(resource_, transformer, res);
    if (transformed.has_error()) {
        log_->error("parse: transformer threw: {}", transformed.error().what.c_str());
        return transformed.convert_error<ParsedQueryDataPtr>();
    }
    auto binder = std::move(transformed.value());
    log_->trace("parse: transformer.transform complete");

    if (binder.has_error()) {
        const auto& error = binder.get_error();
        log_->error("parse: transformer error: {}", error.what.c_str());
        return core::error_t{error.type, std::pmr::string{error.what, resource_}};
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
    result->extension_kind = extension_kind;

    // Take over the catalog lookups the transformer registered (see
    // OtterbrixStatement::catalog_resolves for why they cannot travel with the
    // plan root). finalize() is the only door onto them, and it opens exactly
    // when every parameter is bound: a statement that still carries placeholders
    // is finalized later, by the Worker, once Bind has filled them.
    if (auto& bound = result->binder(); bound.all_bound()) {
        auto finalized = bound.finalize();
        if (finalized.has_error()) {
            log_->error("parse: finalize failed: {}", finalized.error().what.c_str());
            return finalized.convert_error<ParsedQueryDataPtr>();
        }
        result->otterbrix_params->catalog_resolves = finalized.value().catalog_resolves;
    }

    auto external_count = get_external_nodes(resource_,
                                             registry,
                                             result->otterbrix_params->node,
                                             result->otterbrix_params->external_nodes);
    if (external_count.has_error()) {
        log_->error("parse: external node name resolution failed: {}", external_count.error().what.c_str());
        return external_count.convert_error<ParsedQueryDataPtr>();
    }
    result->otterbrix_params->external_nodes_count = external_count.value();
    log_->info("parse: external_nodes_count={}", result->otterbrix_params->external_nodes_count);

    swap_stubs_into_schema_nodes(resource_, result->otterbrix_params->external_nodes, extraction.stubs);
    log_->trace("parse: swap_stubs_into_schema_nodes complete");

    return result;
}

core::result_wrapper_t<logical_plan::node_ptr>
GreenplumParser::parse_fragment(const std::string& sql,
                                logical_plan::parameter_node_ptr shared_params,
                                otterstax::names::name_registry_t& names) {
    OTX_ZONE_N("otterbrix::parse_fragment");
    log_->info("parse_fragment: starting for: {}", std::string_view{sql}.substr(0, 100));
    if (registration_error_) {
        log_->error("parse_fragment: parser extensions are not registered: {}", registration_error_->what.c_str());
        return core::error_t{registration_error_->type, std::pmr::string{registration_error_->what, resource_}};
    }

    tracy_memory_resource arena_mr(resource_, "parser::arena");
    std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);

    // The tree and the transformer read the same text: the transformer slices
    // clause text out of it by the offsets the grammar recorded. It outlives
    // the transform below.
    const std::pmr::string text{sql.data(), sql.size(), resource_};
    auto raw = run_raw_parser(&arena_resource, resource_, text, registry_);
    if (raw.has_error()) {
        log_->error("parse_fragment: raw_parser failed: {}", raw.error().what.c_str());
        return raw.convert_error<logical_plan::node_ptr>();
    }
    const size_t statement_count = list_length(raw.value());
    if (statement_count == 0) {
        log_->error("parse_fragment: raw_parser returned no statements for SQL: {}",
                    std::string_view{sql}.substr(0, 100));
        return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"syntax error", resource_}};
    }
    if (statement_count > 1) {
        log_->error("parse_fragment: {} statements in one fragment", statement_count);
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"multiple statements in one query are not supported", resource_}};
    }
    auto* res = reinterpret_cast<::Node*>(linitial(raw.value()));
    if (nodeTag(res) != T_SelectStmt) {
        log_->error("parse_fragment: the fragment is not a SELECT");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"a SQL fragment must be a single SELECT statement", resource_}};
    }
    otterstax::parser::promote_three_part_qualifiers(res);
    // The full names (uid.db.schema.rel) come off the promoted raw tree, before
    // the transformer folds every table down to (uid, db, rel), as in parse().
    otterstax::parser::collect_qualified_names(resource_, res, names);

    sql::transform::transformer transformer(resource_, text.c_str(), &registry_);
    // The 3-arg execution_plan_t seeds sub_queries with the root it is given;
    // the 2-arg transform appends only the sub-queries it lowers, so the plan
    // starts empty and whatever is in it afterwards is a sub-query.
    logical_plan::execution_plan_t plan(resource_, nullptr, std::move(shared_params));
    plan.sub_queries.clear();
    // run_transform's fence, for the 2-arg transform the fragment needs: the
    // engine helpers the transformer calls may throw.
    core::result_wrapper_t<logical_plan::node_ptr> transformed{nullptr};
    try {
        transformed = transformer.transform(sql::transform::pg_cell_to_node_cast(res), &plan);
    } catch (const std::exception& e) {
        log_->error("parse_fragment: transformer threw: {}", e.what());
        return core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{e.what(), resource_}};
    }
    if (transformed.has_error()) {
        log_->error("parse_fragment: transformer error: {}", transformed.error().what.c_str());
        return core::error_t{transformed.error().type, std::pmr::string{transformed.error().what, resource_}};
    }
    if (!transformed.value()) {
        log_->error("parse_fragment: transformer returned null root node — unsupported statement");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"Unsupported node type", resource_}};
    }
    // A sub-query runs as a plan of its own ahead of the main one; the caller
    // keeps only the root returned here, so it would vanish without a trace.
    if (!plan.sub_queries.empty()) {
        log_->error("parse_fragment: the fragment lowers {} sub-query plan(s)", plan.sub_queries.size());
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"sub-queries are not supported in a SQL fragment", resource_}};
    }
    return transformed.value();
}
