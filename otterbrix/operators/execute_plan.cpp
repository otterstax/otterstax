// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "execute_plan.hpp"

#include "otterbrix/operators/schema_probe.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/tracy_profiler.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

using namespace components;

namespace {

    std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
        return s;
    }

    using columns_t = std::pmr::vector<types::complex_logical_type>;

    // Output column types of database.collection, asked of the engine through
    // its synchronous dispatcher. One plan answers whatever the relation is: a
    // VIEW is expanded by the engine into the probe's own aggregate, so the
    // reply already carries the body's columns.
    core::result_wrapper_t<columns_t> probe_relation(otterbrix::base_otterbrix_t* engine,
                                                     std::pmr::memory_resource* resource,
                                                     const std::string& database,
                                                     const std::string& collection) {
        OTX_ZONE_N("otterbrix::probe_relation");
        auto probe = otterstax::schema_probe::make_table_probe(resource, database, collection);
        auto cursor = engine->dispatcher()->execute_plan(otterbrix::session_id_t(), probe.execution_plan(resource));
        auto read = otterstax::schema_probe::read_columns(probe, cursor, resource);
        if (read.has_error()) {
            return read.convert_error<columns_t>();
        }
        return std::move(read.value().columns);
    }

    cursor::cursor_t_ptr error_cursor(std::pmr::memory_resource* resource, const core::error_t& error) {
        return cursor::make_cursor(resource,
                                   core::error_t(error.type, std::pmr::string{error.what.c_str(), resource}));
    }

    // Aggregates over constants. The engine evaluates a node that reads no column once per
    // chunk, not once per row (execution_dag_t::run): over a local table count(1) and sum(1)
    // fold one row of each chunk, and count(1) over no rows crashes it (count_update).

    bool reads_no_column(const expressions::param_storage& operand);

    // A constant, or arithmetic over constants. Any other form is taken to read a column: a
    // field read names its column in key(), not in params().
    bool reads_no_column(const expressions::expression_i& expr) {
        switch (expr.group()) {
            case expressions::expression_group::scalar: {
                const auto& scalar = static_cast<const expressions::scalar_expression_t&>(expr);
                switch (scalar.type()) {
                    case expressions::scalar_type::constant:
                    case expressions::scalar_type::add:
                    case expressions::scalar_type::subtract:
                    case expressions::scalar_type::multiply:
                    case expressions::scalar_type::divide:
                    case expressions::scalar_type::mod:
                    case expressions::scalar_type::unary_minus:
                        return !scalar.params().empty() &&
                               std::all_of(scalar.params().begin(),
                                           scalar.params().end(),
                                           [](const expressions::param_storage& operand) {
                                               return reads_no_column(operand);
                                           });
                    default:
                        return false;
                }
            }
            case expressions::expression_group::cast:
                return reads_no_column(static_cast<const expressions::cast_expression_t&>(expr).child());
            default:
                return false;
        }
    }

    bool reads_no_column(const expressions::param_storage& operand) {
        if (expressions::is_parameter(operand)) {
            return true;
        }
        return expressions::is_expr(operand) && expressions::as_expr(operand) &&
               reads_no_column(*expressions::as_expr(operand));
    }

    // The engine's aggregate functions (compute/kernels/aggregate.cpp). The SQL transformer
    // writes a SELECT-list aggregate as a call to one of them, a HAVING-only one and the Spark
    // translator as an aggregate expression.
    bool is_aggregate_function(const std::string& function) {
        return function == "count" || function == "sum" || function == "avg" || function == "min" ||
               function == "max";
    }

    enum class constant_aggregate_t
    {
        none,
        counts_rows,
        refused
    };

    // count(<non-NULL literal>) counts every row, which is what the parameterless count, the
    // engine's star kernel, does. Any other aggregate over constants is refused.
    constant_aggregate_t classify_aggregate(const std::string& function,
                                            bool distinct,
                                            const std::pmr::vector<expressions::param_storage>& operands,
                                            const logical_plan::parameter_node_t* parameters) {
        if (operands.empty() ||
            !std::all_of(operands.begin(), operands.end(), [](const expressions::param_storage& operand) {
                return reads_no_column(operand);
            })) {
            return constant_aggregate_t::none;
        }
        if (function == "count" && !distinct && operands.size() == 1 && expressions::is_parameter(operands.front()) &&
            parameters != nullptr) {
            const auto& value = parameters->parameter(expressions::as_parameter(operands.front()));
            if (!value.is_null() && value.type().type() != types::logical_type::NA) {
                return constant_aggregate_t::counts_rows;
            }
        }
        return constant_aggregate_t::refused;
    }

    core::error_t refuse_constant_aggregate(const std::string& function, std::pmr::memory_resource* resource) {
        return core::error_t(
            core::error_code_t::unimplemented_yet,
            std::pmr::string{(function + "() over a constant argument is not supported").c_str(), resource});
    }

    core::error_t guard_expression(expressions::expression_i& expr,
                                   const logical_plan::parameter_node_t* parameters,
                                   std::pmr::memory_resource* resource);

    core::error_t guard_operand(expressions::param_storage& operand,
                                const logical_plan::parameter_node_t* parameters,
                                std::pmr::memory_resource* resource) {
        if (expressions::is_expr(operand) && expressions::as_expr(operand)) {
            return guard_expression(*expressions::as_expr(operand), parameters, resource);
        }
        return core::error_t::no_error();
    }

    core::error_t guard_operands(std::pmr::vector<expressions::param_storage>& operands,
                                 const logical_plan::parameter_node_t* parameters,
                                 std::pmr::memory_resource* resource) {
        for (auto& operand : operands) {
            if (auto error = guard_operand(operand, parameters, resource); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t guard_expression(expressions::expression_i& expr,
                                   const logical_plan::parameter_node_t* parameters,
                                   std::pmr::memory_resource* resource) {
        switch (expr.group()) {
            case expressions::expression_group::aggregate: {
                auto& aggregate = static_cast<expressions::aggregate_expression_t&>(expr);
                const std::string function = to_lower(aggregate.function_name());
                const auto kind = classify_aggregate(function, aggregate.is_distinct(), aggregate.params(), parameters);
                if (kind == constant_aggregate_t::refused) {
                    return refuse_constant_aggregate(function, resource);
                }
                if (kind == constant_aggregate_t::counts_rows) {
                    aggregate.params().clear();
                }
                return core::error_t::no_error();
            }
            case expressions::expression_group::scalar:
                return guard_operands(static_cast<expressions::scalar_expression_t&>(expr).params(),
                                      parameters,
                                      resource);
            case expressions::expression_group::function: {
                auto& call = static_cast<expressions::function_expression_t&>(expr);
                const std::string function = to_lower(call.name());
                if (is_aggregate_function(function) && !call.has_star_argument()) {
                    const auto kind = classify_aggregate(function, call.is_distinct(), call.args(), parameters);
                    if (kind == constant_aggregate_t::refused) {
                        return refuse_constant_aggregate(function, resource);
                    }
                    if (kind == constant_aggregate_t::counts_rows) {
                        // COUNT(*) as the transformer writes it: no argument, the star flag.
                        call.args().clear();
                        call.set_star_argument(true);
                        return core::error_t::no_error();
                    }
                }
                return guard_operands(call.args(), parameters, resource);
            }
            case expressions::expression_group::cast:
                return guard_operand(static_cast<expressions::cast_expression_t&>(expr).child(), parameters, resource);
            case expressions::expression_group::sort:
                return guard_operand(static_cast<expressions::sort_expression_t&>(expr).operand(), parameters, resource);
            case expressions::expression_group::compare: {
                auto& compare = static_cast<expressions::compare_expression_t&>(expr);
                if (auto error = guard_operand(compare.left(), parameters, resource); error.contains_error()) {
                    return error;
                }
                if (auto error = guard_operand(compare.right(), parameters, resource); error.contains_error()) {
                    return error;
                }
                for (auto& child : compare.children()) {
                    if (!child) {
                        continue;
                    }
                    if (auto error = guard_expression(*child, parameters, resource); error.contains_error()) {
                        return error;
                    }
                }
                return core::error_t::no_error();
            }
            default:
                return core::error_t::no_error();
        }
    }

    // Every expression of every node, derived tables included.
    core::error_t guard_node(logical_plan::node_t& node,
                             const logical_plan::parameter_node_t* parameters,
                             std::pmr::memory_resource* resource) {
        for (auto& expr : node.expressions()) {
            if (!expr) {
                continue;
            }
            if (auto error = guard_expression(*expr, parameters, resource); error.contains_error()) {
                return error;
            }
        }
        for (auto& child : node.children()) {
            if (!child) {
                continue;
            }
            if (auto error = guard_node(*child, parameters, resource); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

} // namespace

OtterbrixDataManager::OtterbrixDataManager(otterbrix::base_otterbrix_t* engine)
    : otterbrix_(engine) {}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_plan(OtterbrixStatementPtr& otterbrix_params) {
    OTX_ZONE_N("otterbrix::execute_plan");

    // Before the engine sees it: an aggregate over constants would be folded once per chunk.
    // A backend's slice is already data here, so what a backend runs is not touched.
    if (otterbrix_params->node) {
        auto* resource = otterbrix_->dispatcher()->resource();
        if (auto refused = guard_node(*otterbrix_params->node, otterbrix_params->params_node.get(), resource);
            refused.contains_error()) {
            return error_cursor(resource, refused);
        }
    }

    components::logical_plan::execution_plan_t plan(otterbrix_params->node->resource(),
                                                    otterbrix_params->node,
                                                    otterbrix_params->params_node);
    // The plan is built around the statement's CURRENT root — a backend manager
    // may have replaced it with fetched data — so the catalog lookups have to be
    // put back by hand: they are a sibling of the root, not a node under it, and
    // a plan built from (node, params) alone carries none. Copied, not moved:
    // the same statement is planned once for its output schema and executed
    // once, and both plans need them.
    plan.catalog_resolves = otterbrix_params->catalog_resolves;
    return otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(), std::move(plan));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::get_schema(const OtterbrixSchemaParams& otterbrix_params) {
    OTX_ZONE_N("otterbrix::get_schema_impl");
    // The input vector is in dependency-index order (built by OtterbrixManager),
    // so result->type_data()[i] is the STRUCT schema of dependency i — the layout
    // schema_utils::compute_* consumers index into.
    auto* resource = otterbrix_->dispatcher()->resource();
    std::pmr::vector<types::complex_logical_type> table_schemas(resource);
    table_schemas.reserve(otterbrix_params.size());
    for (const auto& [database, collection] : otterbrix_params) {
        if (collection.empty()) {
            // External table or unnamed wrapper aggregate — not probeable
            // locally; keep its positional slot with an empty schema.
            table_schemas.emplace_back(
                types::complex_logical_type::create_struct("",
                                                           std::pmr::vector<types::complex_logical_type>(resource)));
            continue;
        }
        auto columns = probe_relation(otterbrix_, resource, database, collection);
        if (columns.has_error()) {
            return cursor::make_cursor(resource,
                                       core::error_t(columns.error().type,
                                                     std::pmr::string{columns.error().what.c_str(), resource}));
        }
        table_schemas.emplace_back(types::complex_logical_type::create_struct("", columns.value()));
    }
    return cursor::make_cursor(resource, std::move(table_schemas));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::plan_output_schema(OtterbrixStatementPtr& otterbrix_params) {
    OTX_ZONE_N("otterbrix::plan_output_schema");
    auto* resource = otterbrix_->dispatcher()->resource();
    // Plan-only EXPLAIN: the engine resolves and validates the plan and builds
    // the physical tree but scans nothing; validation stamps output_types() on
    // the statement's consumer node. The rendered plan text the cursor carries
    // is not the answer and is dropped.
    logical_plan::execution_plan_t plan{resource, otterbrix_params->node, otterbrix_params->params_node};
    // The statement's catalog lookups, as in execute_plan: validation resolves
    // the same targets the execution will, and a constraint the plan does not
    // name is one the validator cannot see.
    plan.catalog_resolves = otterbrix_params->catalog_resolves;
    plan.explain = logical_plan::explain_type::plan;
    auto cursor = otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(), std::move(plan));
    if (!cursor) {
        return error_cursor(
            resource,
            core::error_t(core::error_code_t::other_error,
                          std::pmr::string{"plan_output_schema: engine returned a null cursor", resource}));
    }
    if (cursor->is_error()) {
        return cursor;
    }

    columns_t columns(resource);
    // A statement is not a relation: it cannot be re-read with LIMIT 0 (a DML
    // would run twice), so this one path still reads the stamp the validator
    // left on the plan's own consumer node. A VIEW needs no branch of its own —
    // the engine splices the body into that node and stamps it there.
    const logical_plan::node_t* consumer = plan_statement_node(otterbrix_params->node.get());
    if (consumer != nullptr && consumer->has_output_types()) {
        columns.assign(consumer->output_types().begin(), consumer->output_types().end());
    } else {
        return error_cursor(
            resource,
            core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"plan_output_schema: the engine stamped no output types for the statement", resource}));
    }

    std::pmr::vector<types::complex_logical_type> schema(resource);
    schema.emplace_back(types::complex_logical_type::create_struct("", columns));
    return cursor::make_cursor(resource, std::move(schema));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_sql(const std::string& query) {
    OTX_ZONE_N("otterbrix::execute_sql");

    return otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(), query);
}

components::cursor::cursor_t_ptr
OtterbrixDataManager::create_collection(const std::string& database,
                                        const std::string& collection,
                                        std::vector<components::table::column_definition_t> columns,
                                        components::catalog::oid_t& out_oid) {
    OTX_ZONE_N("otterbrix::create_collection");

    // Same plan shape as wrapper_dispatcher::create_collection, but built here
    // so we keep the node and can read the planner-stamped table_oid off it.
    // The oid is stamped into the node by value before the engine replies, and
    // the node is ours: this is the create's own channel for it, not a read of
    // the plan's catalog lookups (those the engine owns while it runs).
    out_oid = components::catalog::INVALID_OID;
    auto* resource = otterbrix_->dispatcher()->resource();
    auto create =
        logical_plan::make_node_create_collection(resource, core::relname_t{collection}, std::move(columns), {});
    // create_collection carries only the relname: the database is named on the
    // node and registered as the plan's namespace lookup.
    logical_plan::node_ptr node = sql::transform::name_catalog_target(database, {}, create);
    logical_plan::execution_plan_t plan{resource, node, logical_plan::make_parameter_node(resource)};
    sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, database);
    auto cursor = otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(), std::move(plan));
    if (cursor && !cursor->is_error()) {
        out_oid = create->table_oid();
    }
    return cursor;
}

components::cursor::cursor_t_ptr OtterbrixDataManager::create_database(const std::string& database) {
    OTX_ZONE_N("otterbrix::create_database");

    // CREATE DATABASE goes through execute_sql (the same path as
    // register_external_database) so the transformer applies the catalog-resolve
    // wrapping. An existing database is the desired end state, not a failure.
    auto cur =
        otterbrix_->dispatcher()->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE " + to_lower(database) + ";");
    if (cur && cur->is_error() && cur->get_error().type == core::error_code_t::database_already_exists) {
        return cursor::make_cursor(otterbrix_->dispatcher()->resource());
    }
    return cur;
}

components::cursor::cursor_t_ptr
OtterbrixDataManager::insert_data(const std::string& database,
                                  const std::string& collection,
                                  std::vector<components::table::column_definition_t> columns,
                                  components::vector::data_chunk_t data) {
    OTX_ZONE_N("otterbrix::insert_data");

    const std::string db = to_lower(database);
    const std::string col = to_lower(collection);
    auto* res = otterbrix_->dispatcher()->resource();
    const otterbrix::session_id_t session;

    // A relation that already carries the name is refused before anything is
    // created or inserted. The create's verdict cannot tell: it resolves only the
    // namespace, the engine checks existence against the plan's table resolves,
    // so a create over an existing table is accepted and the insert below would
    // append to it. A free name probes as table_not_exists, or database_not_exists
    // when the database is missing too — the create and insert answer that one.
    auto probe = otterstax::schema_probe::make_table_probe(res, db, col);
    auto probed = otterstax::schema_probe::read_columns(
        probe,
        otterbrix_->dispatcher()->execute_plan(session, probe.execution_plan(res)),
        res);
    if (!probed.has_error()) {
        return error_cursor(
            res,
            core::error_t(core::error_code_t::table_already_exists,
                          std::pmr::string{("insert_data: " + db + "." + col + " already exists").c_str(), res}));
    }
    if (probed.error().type != core::error_code_t::table_not_exists &&
        probed.error().type != core::error_code_t::database_not_exists) {
        return error_cursor(res, probed.error());
    }

    // create_collection carries only the relname; the database is named on the
    // node and registered as the plan's namespace lookup (mirrors create_collection()).
    auto create = logical_plan::make_node_create_collection(res, core::relname_t{col}, std::move(columns), {});
    auto cc_node = sql::transform::name_catalog_target(db, {}, create);
    logical_plan::execution_plan_t create_plan{res, cc_node, logical_plan::make_parameter_node(res)};
    sql::transform::register_catalog_resolve_namespace(res, &create_plan.catalog_resolves, db);
    auto created = otterbrix_->dispatcher()->execute_plan(session, std::move(create_plan));
    // Any other verdict of the engine on the create is the answer.
    if (!created) {
        return cursor::make_cursor(
            res,
            core::error_t(core::error_code_t::other_error,
                          std::pmr::string{("insert_data: null cursor creating " + db + "." + col).c_str(), res}));
    }
    if (created->is_error()) {
        return created;
    }

    return insert_rows(db, col, std::move(data));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::insert_rows(const std::string& database,
                                                                   const std::string& collection,
                                                                   components::vector::data_chunk_t data) {
    OTX_ZONE_N("otterbrix::insert_rows");
    auto* res = otterbrix_->dispatcher()->resource();

    // node_insert is built without a target name; naming it binds the insert to
    // database.collection, and the plan's table lookup carries the outgoing
    // constraints the engine enforces on the rows. The rows go in as a run of
    // chunks within DEFAULT_VECTOR_CAPACITY: a file loader hands over the whole
    // file as one chunk, and the engine takes at most that many rows per chunk.
    auto insert = logical_plan::make_node_insert(res,
                                                 tsl::split_to_capacity(res, std::move(data)),
                                                 std::pmr::vector<components::expressions::key_t>(res));
    auto insert_node = sql::transform::name_catalog_target(database, collection, insert);
    logical_plan::execution_plan_t plan{res, insert_node, logical_plan::make_parameter_node(res)};
    sql::transform::register_catalog_resolve_table(res,
                                                   &plan.catalog_resolves,
                                                   database,
                                                   collection,
                                                   sql::transform::constraint_resolve_kind::outgoing);
    return otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(), std::move(plan));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::delete_rows(const std::string& database,
                                                                   const std::string& collection,
                                                                   const std::string& column,
                                                                   components::types::logical_value_t value) {
    OTX_ZONE_N("otterbrix::delete_rows");
    auto* res = otterbrix_->dispatcher()->resource();

    // The plan shape transform_delete emits for `DELETE ... WHERE column =
    // value`: node_delete over node_match(eq) under the catalog-resolve wrap.
    // The key is on side_t::left — the (single) target table — a hand-built
    // key of undefined side leaves the column unresolved, and the scan feeds
    // the delete a chunk whose types carry no alias.
    auto params = logical_plan::make_parameter_node(res);
    const auto value_param = params->add_parameter(std::move(value));
    expressions::key_t column_key(res, column, expressions::side_t::left);
    auto predicate =
        expressions::make_compare_expression(res, expressions::compare_type::eq, column_key, value_param);
    auto match =
        logical_plan::make_node_match(res, core::dbname_t{database}, core::relname_t{collection}, predicate);
    auto del = logical_plan::make_node_delete(
        res,
        match,
        logical_plan::make_node_limit(res,
                                      core::dbname_t{database},
                                      core::relname_t{collection},
                                      logical_plan::limit_t::unlimit()));
    logical_plan::node_ptr node = sql::transform::name_catalog_target(database, collection, del);
    logical_plan::execution_plan_t plan{res, node, std::move(params)};
    // DELETE is checked against the constraints that REFERENCE the target.
    sql::transform::register_catalog_resolve_table(res,
                                                   &plan.catalog_resolves,
                                                   database,
                                                   collection,
                                                   sql::transform::constraint_resolve_kind::referencing);
    return otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(), std::move(plan));
}

components::cursor::cursor_t_ptr OtterbrixDataManager::describe_collection(const std::string& database,
                                                                           const std::string& collection,
                                                                           components::catalog::oid_t& out_oid) {
    OTX_ZONE_N("otterbrix::describe_collection");
    out_oid = components::catalog::INVALID_OID;
    auto* resource = otterbrix_->dispatcher()->resource();
    const otterbrix::session_id_t session;
    const std::string relation = database + "." + collection;

    // The columns first: a relation the engine does not hold answers with the
    // engine's own code (table_not_exists / database_not_exists), which is what
    // callers match on.
    auto columns = probe_relation(otterbrix_, resource, database, collection);
    if (columns.has_error()) {
        return error_cursor(resource, columns.error());
    }

    // Then the identity of the relation the name resolved to, read out of
    // pg_catalog: the namespace by name, then the relation by name within it.
    // This is what tells a base relation from a VIEW — the column probe cannot,
    // since the engine answers a VIEW with its body's columns — and it is also
    // the only channel for the oid of a relation this process did not create.
    auto namespace_probe = otterstax::schema_probe::make_namespace_probe(resource, database);
    auto namespace_oid = otterstax::schema_probe::read_namespace_oid(
        namespace_probe,
        otterbrix_->dispatcher()->execute_plan(session, namespace_probe.execution_plan(resource)),
        resource);
    if (namespace_oid.has_error()) {
        return error_cursor(resource, namespace_oid.error());
    }
    auto relation_probe = otterstax::schema_probe::make_relation_probe(resource, database, collection);
    auto identity = otterstax::schema_probe::read_relation(
        relation_probe,
        otterbrix_->dispatcher()->execute_plan(session, relation_probe.execution_plan(resource)),
        namespace_oid.value(),
        resource);
    if (identity.has_error()) {
        return error_cursor(resource, identity.error());
    }
    if (identity.value().relkind == components::catalog::relkind::view ||
        identity.value().relkind == components::catalog::relkind::materialized_view) {
        return error_cursor(
            resource,
            core::error_t(core::error_code_t::schema_error,
                          std::pmr::string{("describe_collection: " + relation + " is a VIEW, not a base relation")
                                               .c_str(),
                                           resource}));
    }
    out_oid = identity.value().oid;
    std::pmr::vector<types::complex_logical_type> schema(resource);
    schema.emplace_back(types::complex_logical_type::create_struct("", std::move(columns.value())));
    return cursor::make_cursor(resource, std::move(schema));
}

