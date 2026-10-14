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
#include <exception>
#include <string>
#include <vector>

using namespace components;

namespace {

    // The target of a table-level DDL statement — CREATE TABLE, CREATE INDEX,
    // DROP TABLE — as the statement wrote it, every slot: the transformer keeps
    // only database and table on the plan node (DROP INDEX: the indexed table),
    // and a DDL statement never reaches the engine's host hooks, so a remote table is told from a local
    // one by this name (the query session runs remote DDL on its backend).
    // Empty for every other statement.
    qualified_name_t written_ddl_target(::Node* statement) {
        qualified_name_t name;
        auto from_range_var = [&name](const ::RangeVar* relation) {
            if (relation == nullptr || relation->relname == nullptr) {
                return;
            }
            name.unique_identifier.t = relation->uid != nullptr ? relation->uid : "";
            name.database.t = relation->catalogname != nullptr ? relation->catalogname : "";
            name.schema.t = relation->schemaname != nullptr ? relation->schemaname : "";
            name.collection.t = relation->relname;
        };
        switch (nodeTag(statement)) {
            case T_CreateStmt:
                from_range_var(reinterpret_cast<::CreateStmt*>(statement)->relation);
                break;
            case T_IndexStmt:
                from_range_var(reinterpret_cast<::IndexStmt*>(statement)->relation);
                break;
            case T_DropStmt: {
                auto* drop = reinterpret_cast<::DropStmt*>(statement);
                if ((drop->removeType != OBJECT_TABLE && drop->removeType != OBJECT_INDEX) ||
                    list_length(drop->objects) != 1) {
                    break;
                }
                auto* parts = reinterpret_cast<::List*>(linitial(drop->objects));
                std::vector<std::string> written;
                ::ListCell* cell = nullptr;
                foreach (cell, parts) {
                    written.emplace_back(strVal(lfirst(cell)));
                }
                // DROP INDEX: the leading parts name the indexed table, the last one the index.
                if (drop->removeType == OBJECT_INDEX) {
                    if (written.size() < 2) {
                        break;
                    }
                    written.pop_back();
                }
                // uid.db.schema.table, db.schema.table, db.table, table.
                switch (written.size()) {
                    case 4:
                        name.unique_identifier.t = written[0];
                        name.database.t = written[1];
                        name.schema.t = written[2];
                        name.collection.t = written[3];
                        break;
                    case 3:
                        name.database.t = written[0];
                        name.schema.t = written[1];
                        name.collection.t = written[2];
                        break;
                    case 2:
                        name.database.t = written[0];
                        name.collection.t = written[1];
                        break;
                    case 1:
                        name.collection.t = written[0];
                        break;
                    default:
                        break;
                }
                break;
            }
            default:
                break;
        }
        return name;
    }

    // The target of an INSERT / UPDATE / DELETE written with a uid slot
    // (uid.db.schema.table), every slot; empty for a target written without one.
    // The transformer keeps database, schema and table on the DML node and has no
    // slot for the uid, so the engine cannot resolve such a target through the
    // federation; the query session runs the statement on its backend instead.
    qualified_name_t written_dml_target(::Node* statement) {
        const ::RangeVar* relation = nullptr;
        switch (nodeTag(statement)) {
            case T_InsertStmt:
                relation = reinterpret_cast<::InsertStmt*>(statement)->relation;
                break;
            case T_UpdateStmt:
                relation = reinterpret_cast<::UpdateStmt*>(statement)->relation;
                break;
            case T_DeleteStmt:
                relation = reinterpret_cast<::DeleteStmt*>(statement)->relation;
                break;
            default:
                break;
        }
        if (relation == nullptr || relation->uid == nullptr || relation->relname == nullptr) {
            return {};
        }
        return qualified_name_t{core::uid_t{relation->uid},
                                core::dbname_t{relation->catalogname != nullptr ? relation->catalogname : ""},
                                core::schema_t{relation->schemaname != nullptr ? relation->schemaname : ""},
                                core::relname_t{relation->relname}};
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

    // The tree and the transformer read the same text: the transformer slices
    // a CHECK expression and a VIEW body out of it by the byte offsets the
    // grammar recorded. It outlives the transform below.
    const std::pmr::string text{sql.data(), sql.size(), resource_};
    // 3-arg form: consult the s3/file/kafka extensions for core-rejected DDL.
    auto raw = run_raw_parser(&arena_resource, resource_, text, registry_);
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
    // The transformer lowers EXPLAIN to the INNER statement's plan and
    // stamps the explain mode on the transform_result's private plan — which
    // this pipeline discards (engine_requests.cpp rebuilds a fresh
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

    sql::transform::transformer transformer(resource_, text.c_str(), &registry_);
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

    // The transformer hands the plan out only through finalize(), and finalize()
    // answers only once every `$n` is bound: an early finalize records the
    // failure and breaks every later bind. A statement that still carries
    // placeholders is therefore finalized here over PLACEHOLDER values bound to
    // $1..$N — the ids execute_prepared_statement binds — to reach its root and
    // parameter node. The binder keeps both (the plan it answers shares them),
    // so the real Bind later re-binds and re-finalizes the SAME objects in
    // place: the parameter node, an INSERT's bound row chunk, a deferred LIMIT.
    // A NULL placeholder pins no INSERT column type; anything else takes an
    // integer, the one value a deferred LIMIT / OFFSET accepts.
    const size_t param_cnt = binder.parameter_count();
    const bool awaiting_bind = !binder.all_bound();
    if (awaiting_bind) {
        for (size_t id = 1; id <= param_cnt; ++id) {
            if (tag == T_InsertStmt) {
                binder.bind(id, types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));
            } else {
                binder.bind(id, types::logical_value_t(resource_, std::int64_t{0}));
            }
        }
    }
    auto finalized = binder.finalize();
    if (finalized.has_error()) {
        log_->error("parse: finalize failed: {}", finalized.error().what.c_str());
        return finalized.convert_error<ParsedQueryDataPtr>();
    }
    auto& plan = finalized.value();
    // The last sub-query is the statement; the ones before it are the
    // `IN (SELECT ...)` / `EXISTS (SELECT ...)` / scalar sub-queries the
    // transformer flattened out of it, which the engine runs first and binds
    // into the parameters the statement reads. They travel with the statement
    // (OtterbrixStatement::sub_queries); dropping them would leave those
    // parameters unfilled and the statement answering wrong rows.
    logical_plan::node_ptr node = plan.sub_queries.empty() ? nullptr : plan.sub_queries.back();
    if (!node) {
        log_->error("parse: transformer returned null root node — unsupported statement");
        return core::error_t{core::error_code_t::unimplemented_yet,
                             std::pmr::string{"Unsupported node type", resource_}};
    }
    auto params = plan.parameters;
    // Until Bind the statement's parameter node holds no value, as the binder
    // left it before: the placeholders must not read as bound values to anyone
    // looking before then (the SQL generator refuses a plan with an unbound `$n`).
    // The real finalize writes every value — literals included — back.
    if (awaiting_bind && params) {
        static_cast<void>(params->take_parameters());
    }

    log_->trace("parse: building ParsedQueryData, param_count={}", param_cnt);
    ParsedQueryDataPtr result = std::make_unique<ParsedQueryData>(
        std::make_unique<OtterbrixStatement>(std::move(params), std::move(node), param_cnt),
        std::move(binder),
        tag);
    result->extension_kind = extension_kind;
    result->awaiting_bind = awaiting_bind;
    result->sql = sql;
    result->ddl_target = written_ddl_target(res);
    result->dml_target = written_dml_target(res);
    if (plan.sub_queries.size() > 1) {
        auto& statement = *result->otterbrix_params;
        statement.sub_queries = std::pmr::vector<logical_plan::node_ptr>(plan.sub_queries.begin(),
                                                                         plan.sub_queries.end() - 1,
                                                                         resource_);
        statement.sub_query_results =
            std::pmr::vector<logical_plan::id_result_mapping>(plan.sub_query_results.begin(),
                                                              plan.sub_query_results.end(),
                                                              resource_);
    }

    // Take over the catalog lookups the transformer registered (see
    // OtterbrixStatement::catalog_resolves for why they cannot travel with the
    // plan root). finalize() is the only door onto them; a statement that still
    // carries placeholders takes them over later, by the query session, once Bind has
    // filled them.
    if (!awaiting_bind) {
        result->otterbrix_params->catalog_resolves = plan.catalog_resolves;
    }

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
