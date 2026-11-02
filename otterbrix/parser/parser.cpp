// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "parser.hpp"

#include "grammar_extention/file/file_extension.hpp"
#include "grammar_extention/s3/s3_extension.hpp"
#include "utility/tracy_memory_resource.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_drop.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

#include <cassert>
#include <string>
#include <vector>

using namespace components;

namespace {

    // The plan of a statement whose `$n` are not bound yet, every `$n` left
    // unbound (finalize_for_describe), for everything read off the plan before
    // the client's Bind. The text was parsed into one statement already; it is
    // parsed again because the statement's own transform stays unfinalized until
    // that Bind (a transform finalizes once).
    core::result_wrapper_t<logical_plan::execution_plan_t>
    describe_plan(std::pmr::memory_resource* resource,
                  const std::pmr::string& text,
                  const sql::parser::parser_extension_registry_t& registry) {
        tracy_memory_resource arena_mr(resource, "parser::arena");
        std::pmr::monotonic_buffer_resource arena_resource(&arena_mr);
        auto raw = raw_parser(&arena_resource, text.c_str(), registry);
        if (raw.has_error()) {
            return core::error_on(resource, raw.error());
        }
        if (list_length(raw.value()) != 1) {
            return core::error_t{core::error_code_t::internal_error,
                                 std::pmr::string{"a parsed statement parsed again into another shape", resource}};
        }
        sql::transform::transformer transformer(resource, text.c_str(), &registry);
        auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw.value())));
        if (binder.has_error()) {
            return core::error_on(resource, binder.get_error());
        }
        return binder.finalize_for_describe();
    }
} // namespace

ParsedQueryData::ParsedQueryData(ExecutionPlanPtr plan,
                                 components::sql::transform::transform_result&& binder,
                                 NodeTag tag)
    : plan(std::move(plan))
    , binder_(std::move(binder))
    , tag(tag) {}

components::sql::transform::transform_result& ParsedQueryData::binder() { return binder_; }

const components::sql::transform::transform_result& ParsedQueryData::binder() const { return binder_; }

GreenplumParser::GreenplumParser(std::pmr::memory_resource* resource)
    : resource_(resource)
    , log_(get_logger(logger_tag::PARSER)) {
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

    // raw_parser answers its error on the arena, which dies with the parse: every
    // error of it is put on resource_ (core::error_on), its position kept.
    // The tree and the transformer read the same text: the transformer slices
    // a CHECK expression and a VIEW body out of it by the byte offsets the
    // grammar recorded. It outlives the transform below.
    const std::pmr::string text{sql.data(), sql.size(), resource_};
    // 3-arg form: consult the s3/file/kafka extensions for core-rejected DDL.
    auto raw = raw_parser(&arena_resource, text.c_str(), registry_);
    if (raw.has_error()) {
        log_->error("parse: raw_parser failed: {}", raw.error().what.c_str());
        return core::error_on(resource_, raw.error());
    }
    // raw_parser returns NIL for input with no statements (e.g. "" or ";").
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
    ::Node* res = reinterpret_cast<::Node*>(linitial(raw.value()));

    auto tag = nodeTag(res);
    extension_kind_t extension_kind = extension_kind_t::none;
    if (tag == T_ExtensionNode) {
        // Only the three built-in extensions produce a root this pipeline can
        // route.
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
    // The transformer lowers EXPLAIN to the INNER statement's plan with the
    // explain mode stamped on it; the session reads such a plan as the inner
    // statement (its kind, its result shape), so EXPLAIN is refused up front.
    if (tag == T_ExplainStmt) {
        log_->error("parse: EXPLAIN is not supported by OtterStax");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"EXPLAIN is not supported by OtterStax", resource_}};
    }
    // A setting of the whole server — ALTER SYSTEM SET / RESET (ALL), ALTER
    // DATABASE ... SET / RESET, ALTER ROLE ... SET / RESET: every other
    // client's sessions would take it, across restarts. A client changes its
    // own session's settings only (SET / SET LOCAL / RESET), so the statement
    // is refused before the engine, whatever it names.
    if (tag == T_AlterSystemStmt || tag == T_AlterDatabaseSetStmt || tag == T_AlterRoleSetStmt) {
        log_->error("parse: a server-wide setting refused");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"server-wide settings are not changed by a client", resource_}};
    }
    // The transformer lowers only the first object of a DROP list; the rest
    // would be dropped from the plan silently, so reject the list form.
    if (tag == T_DropStmt && list_length(reinterpret_cast<::DropStmt*>(res)->objects) > 1) {
        log_->error("parse: DROP with several objects is not supported by OtterStax");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"DROP of several objects in one statement is not supported", resource_}};
    }

    sql::transform::transformer transformer(resource_, text.c_str(), &registry_);
    log_->trace("parse: calling transformer.transform");
    auto binder = transformer.transform(sql::transform::pg_cell_to_node_cast(res));
    log_->trace("parse: transformer.transform complete");

    if (binder.has_error()) {
        const auto& error = binder.get_error();
        log_->error("parse: transformer error: {}", error.what.c_str());
        return core::error_on(resource_, error);
    }

    // The transformer hands the plan out only through finalize(), once. A
    // statement without `$n` is finalized here. One that still names `$n` keeps
    // its transform unfinalized for the client's Bind (the query session binds
    // and finalizes it, query_session_t::parse_bound); until then its plan
    // is the describe plan, every `$n` unbound.
    const size_t param_cnt = binder.parameter_count();
    const bool awaiting_bind = !binder.all_bound();
    auto finalized = awaiting_bind ? describe_plan(resource_, text, registry_) : binder.finalize();
    if (finalized.has_error()) {
        log_->error("parse: finalize failed: {}", finalized.error().what.c_str());
        return finalized.convert_error<ParsedQueryDataPtr>();
    }
    // The last sub-query is the statement; the ones before it are the
    // `IN (SELECT ...)` / `EXISTS (SELECT ...)` / scalar sub-queries the
    // transformer flattened out of it, which the engine runs first and binds
    // into the parameters the statement reads.
    if (!plan_root(finalized.value())) {
        log_->error("parse: transformer returned null root node — unsupported statement");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"Unsupported node type", resource_}};
    }

    log_->trace("parse: building ParsedQueryData, param_count={}", param_cnt);
    ParsedQueryDataPtr result =
        std::make_unique<ParsedQueryData>(std::make_unique<logical_plan::execution_plan_t>(std::move(finalized.value())),
                                          std::move(binder),
                                          tag);
    result->extension_kind = extension_kind;
    result->awaiting_bind = awaiting_bind;
    result->sql = sql;

    return result;
}

core::result_wrapper_t<logical_plan::node_ptr>
GreenplumParser::parse_fragment(const std::string& sql, logical_plan::parameter_node_ptr shared_params) {
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
    auto raw = raw_parser(&arena_resource, text.c_str(), registry_);
    if (raw.has_error()) {
        log_->error("parse_fragment: raw_parser failed: {}", raw.error().what.c_str());
        return core::error_on(resource_, raw.error());
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
    sql::transform::transformer transformer(resource_, text.c_str(), &registry_);
    // The 3-arg execution_plan_t seeds sub_queries with the root it is given;
    // the 2-arg transform appends only the sub-queries it lowers, so the plan
    // starts empty and whatever is in it afterwards is a sub-query.
    logical_plan::execution_plan_t plan(resource_, nullptr, std::move(shared_params));
    plan.sub_queries.clear();
    auto transformed = transformer.transform(sql::transform::pg_cell_to_node_cast(res), &plan);
    if (transformed.has_error()) {
        log_->error("parse_fragment: transformer error: {}", transformed.error().what.c_str());
        return core::error_on(resource_, transformed.error());
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
