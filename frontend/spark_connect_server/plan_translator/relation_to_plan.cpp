// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "relation_to_plan.hpp"

#include "expression_to_plan.hpp"
#include "utility/tracy_profiler.hpp"

#include <spark/connect/expressions.pb.h>

#include <google/protobuf/repeated_field.h>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_union.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/transform_result.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <deque>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace frontend::spark {

    namespace {

        namespace cl = components::logical_plan;
        namespace ce = components::expressions;
        namespace ct = components::types;
        namespace cv = components::vector;
        namespace sc = ::spark::connect;
        namespace cst = components::sql::transform;

        using node_result = core::result_wrapper_t<cl::node_ptr>;
        using expressions_t = google::protobuf::RepeatedPtrField<sc::Expression>;

        node_result make_error(core::error_code_t code, std::string_view what, std::pmr::memory_resource* resource) {
            return node_result{core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
        }

        node_result unsupported(std::string_view what, std::pmr::memory_resource* resource) {
            return make_error(core::error_code_t::unimplemented_yet, what, resource);
        }

        std::pmr::vector<std::pmr::string> split_identifier(std::string_view ident,
                                                            std::pmr::memory_resource* resource) {
            std::pmr::vector<std::pmr::string> parts{resource};
            std::pmr::string current{resource};
            bool in_backtick = false;
            for (char c : ident) {
                if (c == '`') {
                    in_backtick = !in_backtick;
                } else if (c == '.' && !in_backtick) {
                    parts.push_back(std::move(current));
                    current = std::pmr::string{resource};
                } else {
                    current.push_back(c);
                }
            }
            parts.push_back(std::move(current));
            return parts;
        }

        struct table_ref {
            std::string uid;
            std::string dbname;
            std::string schema;
            std::string relname;
        };

        table_ref parse_table_identifier(std::string_view ident, std::pmr::memory_resource* resource) {
            const auto parts = split_identifier(ident, resource);
            table_ref ref;
            switch (parts.size()) {
                case 0:
                    break;
                case 1:
                    ref.relname = std::string(parts[0].begin(), parts[0].end());
                    break;
                case 2:
                    ref.dbname = std::string(parts[0].begin(), parts[0].end());
                    ref.relname = std::string(parts[1].begin(), parts[1].end());
                    break;
                case 3:
                    // `alias.db.table`, read as the SQL path reads three parts
                    // (promote_three_part_qualifiers): the connection alias, the
                    // database, the table, the database standing in for the schema.
                    ref.uid = std::string(parts[0].begin(), parts[0].end());
                    ref.dbname = std::string(parts[1].begin(), parts[1].end());
                    ref.schema = std::string(parts[1].begin(), parts[1].end());
                    ref.relname = std::string(parts[2].begin(), parts[2].end());
                    break;
                default:
                    ref.uid = std::string(parts[0].begin(), parts[0].end());
                    ref.dbname = std::string(parts[1].begin(), parts[1].end());
                    ref.schema = std::string(parts[2].begin(), parts[2].end());
                    ref.relname = std::string(parts[3].begin(), parts[3].end());
                    break;
            }
            return ref;
        }

        struct db_rel_names {
            std::string dbname;
            std::string relname;
        };

        db_rel_names get_names(const cl::node_ptr& node) {
            if (node && node->type() == cl::node_type::aggregate_t) {
                const auto& agg = static_cast<const cl::node_aggregate_t&>(*node);
                return {std::string(agg.dbname().t), std::string(agg.relname().t)};
            }
            return {"", ""};
        }

        cl::node_ptr ensure_aggregate_wrapper(cl::node_ptr node, std::pmr::memory_resource* resource) {
            if (node && node->type() == cl::node_type::aggregate_t) {
                return node;
            }
            auto wrapper =
                cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
            wrapper->append_child(node);
            return wrapper;
        }

        // Depth-first search for the first match_t node in a freshly-parsed fragment:
        // the transformer wraps a WHERE predicate as aggregate -> ... -> match.
        cl::node_ptr find_match_node(const cl::node_ptr& node) {
            if (!node) {
                return nullptr;
            }
            if (node->type() == cl::node_type::match_t) {
                return node;
            }
            for (const auto& child : node->children()) {
                auto found = find_match_node(child);
                if (found) {
                    return found;
                }
            }
            return nullptr;
        }

        cl::join_type map_join_type(sc::Join::JoinType jt) {
            switch (jt) {
                case sc::Join::JOIN_TYPE_INNER:
                case sc::Join::JOIN_TYPE_UNSPECIFIED:
                    return cl::join_type::inner;
                case sc::Join::JOIN_TYPE_FULL_OUTER:
                    return cl::join_type::full;
                case sc::Join::JOIN_TYPE_LEFT_OUTER:
                    return cl::join_type::left;
                case sc::Join::JOIN_TYPE_RIGHT_OUTER:
                    return cl::join_type::right;
                case sc::Join::JOIN_TYPE_CROSS:
                    return cl::join_type::cross;
                default:
                    return cl::join_type::invalid;
            }
        }

        // The schema a Read.NamedTable named for an external table. The aggregate
        // carries only (uid, db, rel), and a PostgreSQL table is mirrored under its
        // schema, so populate_external_nodes puts the schema back into the name.
        struct read_schema_t {
            const cl::node_t* node;
            std::pmr::string schema;
        };

        // What the translation of one relation_to_plan() call shares.
        struct translation_ctx_t {
            explicit translation_ctx_t(std::pmr::memory_resource* resource)
                : resource(resource)
                , params(cl::make_parameter_node(resource))
                , parser(resource)
                , read_schemas(resource) {}

            std::pmr::memory_resource* resource;
            // The plan's one parameter node: every literal gets its id here, the
            // constants of parsed SQL fragments included.
            cl::parameter_node_ptr params;
            // Parses spark.sql() leaves and filter strings (parse_fragment).
            GreenplumParser parser;
            std::pmr::vector<read_schema_t> read_schemas;
        };

        // A DataFrame operation that stacks a clause onto the aggregate below it.
        enum class operation_t
        {
            filter,
            aggregate,
            sort,
            project,
            distinct,
            limit,
        };

        // The clauses already stacked on an aggregate. The validator and the planner
        // keep only the LAST child of each kind and run them in the fixed order
        // scan -> match -> group -> having -> sort -> select -> distinct -> limit,
        // whatever order they were appended in (validate_logical_plan.cpp,
        // create_plan_aggregate.cpp), so an operation is stacked only where that order
        // runs it at the point Spark runs it.
        struct clauses_t {
            cl::node_t* match{nullptr};
            cl::node_t* group{nullptr};
            // The group reduces rows (a grouping key, an aggregate, a HAVING) rather
            // than only projecting them.
            bool aggregation{false};
            // The group only passes columns through: a filter or a sort that runs
            // below it reads the values it would read above it.
            bool plain_columns{false};
            cl::node_t* sort{nullptr};
            cl::node_t* limit{nullptr};
            bool distinct{false};
        };

        clauses_t find_clauses(const cl::node_ptr& node) {
            clauses_t clauses;
            bool having = false;
            for (const auto& child : node->children()) {
                switch (child->type()) {
                    case cl::node_type::match_t:
                        clauses.match = child.get();
                        break;
                    case cl::node_type::group_t:
                        clauses.group = child.get();
                        break;
                    case cl::node_type::having_t:
                        having = true;
                        break;
                    case cl::node_type::sort_t:
                        clauses.sort = child.get();
                        break;
                    case cl::node_type::limit_t:
                        clauses.limit = child.get();
                        break;
                    default:
                        break;
                }
            }
            clauses.distinct = static_cast<const cl::node_aggregate_t&>(*node).is_distinct();
            if (clauses.group == nullptr) {
                return clauses;
            }
            clauses.aggregation = having;
            clauses.plain_columns = !having;
            for (const auto& expr : clauses.group->expressions()) {
                if (expr->group() == ce::expression_group::aggregate) {
                    clauses.aggregation = true;
                }
                if (expr->group() != ce::expression_group::scalar) {
                    clauses.plain_columns = false;
                    continue;
                }
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                if (scalar->type() == ce::scalar_type::group_field) {
                    clauses.aggregation = true;
                }
                if (scalar->type() != ce::scalar_type::star_expand &&
                    (scalar->type() != ce::scalar_type::get_field || !scalar->params().empty())) {
                    clauses.plain_columns = false;
                }
            }
            return clauses;
        }

        // Whether `operation` can be stacked onto the aggregate carrying `clauses`.
        // Filter, groupBy/agg, orderBy, select, distinct and limit run in that order,
        // each once (a second filter is ANDed into the match, a second limit merged
        // into the first). An operation placed ahead of a clause it follows in Spark is
        // stacked only where the two commute: a filter below an orderBy (a filter
        // keeps the order it finds), a filter or an orderBy below a select() of plain
        // columns (they read the same values there) and a row-wise select() below a
        // limit (it keeps the rows and their order; append_group decides row-wise, after
        // translating the select). Anything else reads a derived table (derive).
        bool can_stack(const clauses_t& clauses, operation_t operation) {
            if (operation == operation_t::limit || operation == operation_t::distinct) {
                return clauses.limit == nullptr;
            }
            if ((clauses.limit != nullptr && operation != operation_t::project) || clauses.distinct) {
                return false;
            }
            switch (operation) {
                case operation_t::filter:
                    return clauses.group == nullptr || clauses.plain_columns;
                case operation_t::sort:
                    return clauses.sort == nullptr &&
                           (clauses.group == nullptr || clauses.aggregation || clauses.plain_columns);
                case operation_t::aggregate:
                    return clauses.group == nullptr && clauses.sort == nullptr;
                case operation_t::project:
                    return clauses.group == nullptr;
                default:
                    return false;
            }
        }

        // A derived table: `node` becomes the FROM of a fresh aggregate, which is how
        // the transformer lowers `FROM (SELECT ...)` (transform_select.cpp,
        // T_RangeSubselect): the outer aggregate names no table, the validator takes
        // the inner one's output as its input schema (validate_logical_plan.cpp, the
        // aggregate's data child) and the planner runs the inner plan as its source.
        // A remote read stays the inner aggregate, so populate_external_nodes makes it
        // the external slot: its clauses are pushed to the backend, its rows inlined,
        // and the outer aggregate runs in the engine.
        cl::node_ptr derive(cl::node_ptr node, std::pmr::memory_resource* resource) {
            auto outer =
                cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
            outer->append_child(std::move(node));
            return outer;
        }

        // A select() that renames a column (`col AS x`). The validator names such an
        // output after its source column (the select path of validate_logical_plan.cpp
        // reports the input column's type), while the result carries the new name, so
        // a query derived from it would resolve a name against the wrong column or
        // not at all. An aggregation names its outputs by their keys and is safe.
        bool renames_columns(const clauses_t& clauses) {
            if (clauses.group == nullptr || clauses.aggregation) {
                return false;
            }
            for (const auto& expr : clauses.group->expressions()) {
                if (expr->group() != ce::expression_group::scalar) {
                    continue;
                }
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                if (scalar->type() == ce::scalar_type::get_field && scalar->params().size() == 1 &&
                    ce::is_key(scalar->params().front()) && ce::as_key(scalar->params().front()) != scalar->key()) {
                    return true;
                }
            }
            return false;
        }

        // Whether `expr` reads a column of its input: a literal, or a count(*), reads
        // none. Anything unrecognised counts as reading.
        bool reads_columns(const ce::expression_ptr& expr) {
            const auto reads = [](const ce::param_storage& param) {
                return ce::is_key(param) ||
                       (ce::is_expr(param) && ce::as_expr(param) && reads_columns(ce::as_expr(param)));
            };
            switch (expr->group()) {
                case ce::expression_group::scalar: {
                    const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                    if (scalar->type() == ce::scalar_type::get_field ||
                        scalar->type() == ce::scalar_type::group_field ||
                        scalar->type() == ce::scalar_type::star_expand) {
                        return true;
                    }
                    return std::any_of(scalar->params().begin(), scalar->params().end(), reads);
                }
                case ce::expression_group::aggregate: {
                    const auto& params = static_cast<const ce::aggregate_expression_t*>(expr.get())->params();
                    return std::any_of(params.begin(), params.end(), reads);
                }
                case ce::expression_group::function: {
                    const auto& args = static_cast<const ce::function_expression_t*>(expr.get())->args();
                    return std::any_of(args.begin(), args.end(), reads);
                }
                case ce::expression_group::compare: {
                    const auto* compare = static_cast<const ce::compare_expression_t*>(expr.get());
                    return reads(compare->left()) || reads(compare->right()) ||
                           std::any_of(compare->children().begin(), compare->children().end(), reads_columns);
                }
                default:
                    return true;
            }
        }

        // The refusal of an operation that would read a derived table through a
        // renamed column (renames_columns); it names the operation.
        node_result unsupported_rename(std::string_view operation, std::pmr::memory_resource* resource) {
            std::pmr::string what{"this DataFrame operation chain is not supported: ", resource};
            what.append(operation);
            what.append("() reads the result of a select() that renames a column, which the engine resolves by the "
                        "source column's name there; rename columns in the last select()");
            return node_result{core::error_t{core::error_code_t::unimplemented_yet, std::move(what)}};
        }

        // A limit or an offset stacked on `existing`: two windows in a row are one,
        // rows [o1 + o2, o1 + min(l1 - o2, l2)) of the input, so the merge is exact.
        // `limit` is -1 for an offset, `offset` 0 for a limit.
        cl::limit_t merged_window(const cl::limit_t& existing, int64_t limit, int64_t offset) {
            const bool unlimited = existing.limit() == cl::limit_t::unlimit().limit();
            int64_t remaining = unlimited ? existing.limit() : std::max<int64_t>(existing.limit() - offset, 0);
            if (limit >= 0) {
                remaining = remaining < 0 ? limit : std::min<int64_t>(remaining, limit);
            }
            return cl::limit_t{remaining, existing.offset() + offset};
        }

        // A structured filter condition the engine evaluates as a predicate: a
        // comparison (isNull, isNotNull, isin and between lower to comparisons too),
        // or &, |, ~ over predicates.
        bool is_predicate(const ce::expression_ptr& expr) {
            if (!expr || expr->group() != ce::expression_group::compare) {
                return false;
            }
            const auto* compare = static_cast<const ce::compare_expression_t*>(expr.get());
            if (!compare->is_union()) {
                return true;
            }
            if (compare->children().empty()) {
                return false;
            }
            for (const auto& child : compare->children()) {
                if (!is_predicate(child)) {
                    return false;
                }
            }
            return true;
        }

        bool is_bare_star(const sc::Expression& expr) {
            return expr.expr_type_case() == sc::Expression::kUnresolvedStar &&
                   !expr.unresolved_star().has_unparsed_target();
        }

        // An output computed row by row: no aggregate, and no function call either —
        // whether a function reduces rows is the engine registry's to say, so none is
        // taken to be row-wise.
        bool is_row_wise(const ce::expression_ptr& expr) {
            switch (expr->group()) {
                case ce::expression_group::scalar:
                    for (const auto& param : static_cast<const ce::scalar_expression_t*>(expr.get())->params()) {
                        if (ce::is_expr(param) && !is_row_wise(ce::as_expr(param))) {
                            return false;
                        }
                    }
                    return true;
                case ce::expression_group::compare: {
                    const auto* compare = static_cast<const ce::compare_expression_t*>(expr.get());
                    for (const auto* operand : {&compare->left(), &compare->right()}) {
                        if (ce::is_expr(*operand) && ce::as_expr(*operand) && !is_row_wise(ce::as_expr(*operand))) {
                            return false;
                        }
                    }
                    for (const auto& child : compare->children()) {
                        if (!is_row_wise(child)) {
                            return false;
                        }
                    }
                    return true;
                }
                default:
                    return false;
            }
        }

        // The column a grouping key names: a bare column reference, or the source of
        // one an Alias renamed. nullptr for a computed key.
        const ce::key_t* grouped_column(const cl::expression_ptr& expr) {
            if (expr->group() != ce::expression_group::scalar) {
                return nullptr;
            }
            const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
            if (scalar->type() != ce::scalar_type::get_field) {
                return nullptr;
            }
            if (scalar->params().empty()) {
                return &scalar->key();
            }
            if (scalar->params().size() == 1 && ce::is_key(scalar->params().front())) {
                return &ce::as_key(scalar->params().front());
            }
            return nullptr;
        }

        // A Spark Project or Aggregate in the transformer's shape (transform_select.cpp):
        // the whole output list on a node_group_t — the grouping keys first, as
        // group_field markers, then every output column, each named by its key() —
        // beside an empty node_select_t. The validator dissolves a group with neither
        // a key nor an aggregate into the select, so a plain projection takes the
        // same shape. `grouping` is null for a Project. Where the clauses already on
        // `node` leave no place for the group, it reads a derived table of them.
        node_result append_group(cl::node_ptr node,
                                 const expressions_t* grouping,
                                 const expressions_t& outputs,
                                 operation_t operation,
                                 std::string_view operation_name,
                                 translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            std::pmr::vector<cl::expression_ptr> exprs{resource};
            if (grouping != nullptr) {
                size_t computed_keys = 0;
                for (const auto& expr : *grouping) {
                    auto key = expression_to_plan(expr, ctx.params, resource);
                    if (key.has_error()) {
                        return key.convert_error<cl::node_ptr>();
                    }
                    if (const auto* column = grouped_column(key.value())) {
                        exprs.push_back(ce::make_scalar_expression(resource, ce::scalar_type::group_field, *column));
                        continue;
                    }
                    // A computed key: the marker carries the expression under the name
                    // the transformer gives it; the key's output column below is a
                    // second translation, which the validator matches to it.
                    const std::string marker_name = "__group_key_" + std::to_string(computed_keys++);
                    auto marker = ce::make_scalar_expression(resource,
                                                             ce::scalar_type::group_field,
                                                             ce::key_t{resource, marker_name});
                    marker->append_param(key.value());
                    exprs.push_back(marker);
                }
            }
            // Spark's Aggregate answers its grouping expressions ahead of the
            // aggregates; each is translated anew, so no expression node is shared.
            for (const auto* list : {grouping, &outputs}) {
                if (list == nullptr) {
                    continue;
                }
                for (const auto& expr : *list) {
                    auto output = expression_to_plan(expr, ctx.params, resource);
                    if (output.has_error()) {
                        return output.convert_error<cl::node_ptr>();
                    }
                    exprs.push_back(output.value());
                }
            }
            const auto clauses = find_clauses(node);
            // A select() stacked below a limit keeps the same rows only while it
            // reduces none of them.
            const bool stacks = can_stack(clauses, operation) &&
                                (clauses.limit == nullptr || std::all_of(exprs.begin(), exprs.end(), is_row_wise));
            if (!stacks) {
                if (renames_columns(clauses) && std::any_of(exprs.begin(), exprs.end(), reads_columns)) {
                    return unsupported_rename(operation_name, resource);
                }
                node = derive(std::move(node), resource);
            }
            auto names = get_names(node);
            auto group =
                cl::make_node_group(resource, core::dbname_t{names.dbname}, core::relname_t{names.relname}, exprs);
            node->append_child(group);
            node->append_child(
                cl::make_node_select(resource, core::dbname_t{names.dbname}, core::relname_t{names.relname}));
            return node;
        }

        // The schema of every external table a spark.sql() leaf reads. The transformer
        // folds a table down to (uid, db, rel) on its aggregate, so the schema is
        // resolved back from the names the leaf's own parse tree registered, the way
        // the parser's get_external_nodes resolves a node — an ambiguous name is its
        // error here as well.
        core::error_t record_leaf_schemas(const cl::node_ptr& node,
                                          const otterstax::names::name_registry_t& names,
                                          translation_ctx_t& ctx) {
            if (!node) {
                return core::error_t::no_error();
            }
            if (node->type() == cl::node_type::aggregate_t &&
                !static_cast<const cl::node_aggregate_t&>(*node).uid().t.empty()) {
                auto resolved = otterstax::names::node_names(*node, names);
                if (resolved.has_error()) {
                    return core::error_on(ctx.resource, resolved.error());
                }
                const auto& schema = resolved.value().schema;
                ctx.read_schemas.push_back(
                    read_schema_t{node.get(), std::pmr::string{schema.data(), schema.size(), ctx.resource}});
            }
            for (const auto& child : node->children()) {
                if (auto error = record_leaf_schemas(child, names, ctx); error.contains_error()) {
                    return error;
                }
            }
            return core::error_t::no_error();
        }

        node_result translate_relation(const sc::Relation& rel, translation_ctx_t& ctx);

        node_result translate_read(const sc::Read& read, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            if (read.is_streaming()) {
                return unsupported("streaming reads are not supported", resource);
            }
            switch (read.read_type_case()) {
                case sc::Read::kNamedTable: {
                    auto ref = parse_table_identifier(read.named_table().unparsed_identifier(), resource);
                    cl::node_ptr node;
                    if (ref.uid.empty()) {
                        node =
                            cl::make_node_aggregate(resource, core::dbname_t{ref.dbname}, core::relname_t{ref.relname});
                    } else {
                        node = cl::make_node_aggregate(resource,
                                                       core::uid_t{ref.uid},
                                                       core::dbname_t{ref.dbname},
                                                       core::relname_t{ref.relname});
                        ctx.read_schemas.push_back(
                            read_schema_t{node.get(),
                                          std::pmr::string{ref.schema.data(), ref.schema.size(), resource}});
                    }
                    return node;
                }
                case sc::Read::kDataSource:
                    return unsupported("Spark DataSource reads are not supported", resource);
                default:
                    return unsupported("unset Spark Read type", resource);
            }
        }

        node_result translate_project(const sc::Project& proj, translation_ctx_t& ctx) {
            cl::node_ptr input_node;
            if (proj.has_input()) {
                auto input_res = translate_relation(proj.input(), ctx);
                if (input_res.has_error()) {
                    return input_res;
                }
                input_node = ensure_aggregate_wrapper(input_res.value(), ctx.resource);
            } else {
                input_node = cl::make_node_aggregate(ctx.resource,
                                                     core::dbname_t{std::string{}},
                                                     core::relname_t{std::string{}});
            }

            // A lone `*` projects the input as it is.
            if (proj.expressions_size() == 1 && is_bare_star(proj.expressions(0))) {
                return input_node;
            }
            return append_group(std::move(input_node),
                                nullptr,
                                proj.expressions(),
                                operation_t::project,
                                "select",
                                ctx);
        }

        node_result translate_filter(const sc::Filter& filter, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            auto input_res = translate_relation(filter.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);

            ce::expression_ptr predicate;
            if (filter.condition().expr_type_case() == sc::Expression::kExpressionString) {
                // PySpark's .filter("<sql>") sends the predicate as a raw SQL string
                // (Expression.ExpressionString), not a structured tree. Route it through
                // the real parser, materialising constants into the shared `params` node,
                // so the predicate lands in the executable transformer form
                // compare(key_t, parameter_id_t), the form expression_to_plan builds for
                // a structured comparison as well.
                std::string sql{"SELECT * FROM __otterstax_filter__ WHERE "};
                sql += filter.condition().expression_string().expression();
                // Only the predicate is kept, so the fragment's table names are not needed.
                otterstax::names::name_registry_t fragment_names{resource};
                auto frag = ctx.parser.parse_fragment(sql, ctx.params, fragment_names);
                if (frag.has_error()) {
                    return frag; // same result type (node_ptr) — forward the error as-is
                }
                auto match_node = find_match_node(frag.value());
                if (!match_node || match_node->expressions().empty()) {
                    return make_error(core::error_code_t::unimplemented_yet,
                                      "Spark filter string did not translate to a predicate",
                                      resource);
                }
                predicate = match_node->expressions().front();
            } else {
                auto cond = expression_to_plan(filter.condition(), ctx.params, resource);
                if (cond.has_error()) {
                    return cond.convert_error<cl::node_ptr>();
                }
                if (!is_predicate(cond.value())) {
                    return unsupported("Spark filter condition is not supported: a structured filter is a comparison, "
                                       "isNull, isNotNull, isin or between, combined with &, | and ~",
                                       resource);
                }
                predicate = cond.value();
            }

            auto clauses = find_clauses(input_node);
            if (!can_stack(clauses, operation_t::filter)) {
                // The filter reads what the clauses already there answer — after a
                // groupBy/agg that is a HAVING — so it filters a derived table of them.
                if (renames_columns(clauses)) {
                    return unsupported_rename("filter", resource);
                }
                input_node = derive(std::move(input_node), resource);
                clauses = clauses_t{};
            }

            // Consecutive filters are one WHERE: only the last match child would be
            // read, so the new predicate is ANDed into the existing one.
            if (clauses.match != nullptr) {
                auto both = ce::make_compare_union_expression(resource, ce::compare_type::union_and);
                for (const auto& existing : clauses.match->expressions()) {
                    both->append_child(existing);
                }
                both->append_child(predicate);
                clauses.match->expressions().clear();
                clauses.match->append_expression(both);
                return input_node;
            }

            auto names = get_names(input_node);
            auto match =
                cl::make_node_match(resource, core::dbname_t{names.dbname}, core::relname_t{names.relname}, predicate);
            input_node->append_child(match);
            return input_node;
        }

        node_result translate_sort(const sc::Sort& sort, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            auto input_res = translate_relation(sort.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
            if (const auto clauses = find_clauses(input_node); !can_stack(clauses, operation_t::sort)) {
                // The sort orders what the clauses already there answer — a window,
                // distinct rows, a computed select — so it sorts a derived table of them.
                if (renames_columns(clauses)) {
                    return unsupported_rename("orderBy", resource);
                }
                input_node = derive(std::move(input_node), resource);
            }
            auto names = get_names(input_node);

            std::pmr::vector<cl::expression_ptr> sort_exprs{resource};
            sort_exprs.reserve(sort.order_size());
            for (const auto& so : sort.order()) {
                // A sort key is translated where a standalone SortOrder is: the key
                // stays a plain column and Spark's NULL placement is mapped there.
                sc::Expression order;
                *order.mutable_sort_order() = so;
                auto sort_expr = expression_to_plan(order, ctx.params, resource);
                if (sort_expr.has_error()) {
                    return sort_expr.convert_error<cl::node_ptr>();
                }
                sort_exprs.push_back(sort_expr.value());
            }
            auto sort_node =
                cl::make_node_sort(resource, core::dbname_t{names.dbname}, core::relname_t{names.relname}, sort_exprs);
            input_node->append_child(sort_node);
            return input_node;
        }

        node_result translate_limit(const sc::Limit& limit, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            auto input_res = translate_relation(limit.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
            if (auto* existing = find_clauses(input_node).limit; existing != nullptr) {
                auto* window = static_cast<cl::node_limit_t*>(existing);
                window->set_limit(merged_window(window->limit(), limit.limit(), 0));
                return input_node;
            }
            auto names = get_names(input_node);

            auto limit_node = cl::make_node_limit(resource,
                                                  core::dbname_t{names.dbname},
                                                  core::relname_t{names.relname},
                                                  cl::limit_t{limit.limit()});
            input_node->append_child(limit_node);
            return input_node;
        }

        node_result translate_offset(const sc::Offset& offset, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            auto input_res = translate_relation(offset.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
            if (auto* existing = find_clauses(input_node).limit; existing != nullptr) {
                auto* window = static_cast<cl::node_limit_t*>(existing);
                window->set_limit(merged_window(window->limit(), cl::limit_t::unlimit().limit(), offset.offset()));
                return input_node;
            }
            auto names = get_names(input_node);

            auto unlim = cl::limit_t::unlimit();
            auto limit_node = cl::make_node_limit(resource,
                                                  core::dbname_t{names.dbname},
                                                  core::relname_t{names.relname},
                                                  cl::limit_t{unlim.limit(), offset.offset()});
            input_node->append_child(limit_node);
            return input_node;
        }

        node_result translate_aggregate(const sc::Aggregate& agg, translation_ctx_t& ctx) {
            auto input_res = translate_relation(agg.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto input_node = ensure_aggregate_wrapper(input_res.value(), ctx.resource);

            if (agg.group_type() == sc::Aggregate::GROUP_TYPE_PIVOT ||
                agg.group_type() == sc::Aggregate::GROUP_TYPE_ROLLUP ||
                agg.group_type() == sc::Aggregate::GROUP_TYPE_CUBE ||
                agg.group_type() == sc::Aggregate::GROUP_TYPE_GROUPING_SETS) {
                return unsupported("Spark aggregate group type (rollup/cube/pivot/grouping_sets) is not supported",
                                   ctx.resource);
            }

            return append_group(std::move(input_node),
                                &agg.grouping_expressions(),
                                agg.aggregate_expressions(),
                                operation_t::aggregate,
                                "groupBy/agg",
                                ctx);
        }

        node_result translate_join(const sc::Join& join, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            auto jt = map_join_type(join.join_type());
            if (jt == cl::join_type::invalid) {
                return unsupported("Spark join type (left_anti/left_semi) is not supported", resource);
            }

            auto left_res = translate_relation(join.left(), ctx);
            if (left_res.has_error()) {
                return left_res;
            }
            auto right_res = translate_relation(join.right(), ctx);
            if (right_res.has_error()) {
                return right_res;
            }

            auto join_node =
                cl::make_node_join(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}}, jt);
            join_node->append_child(left_res.value());
            join_node->append_child(right_res.value());

            if (join.has_join_condition()) {
                auto cond = expression_to_plan(join.join_condition(), ctx.params, resource);
                if (cond.has_error()) {
                    return cond.convert_error<cl::node_ptr>();
                }
                join_node->append_expression(cond.value());
            } else if (join.using_columns_size() > 0) {
                // Emit the equi-join predicate in the executable transformer form
                // compare(key_t, key_t) — a key_t implicitly converts to param_storage.
                // A compare(get_field_scalar, get_field_scalar) shape would not be
                // evaluable by the remote SQL generator or the local value-getter.
                // The keys MUST carry a side (left/right): the join value-getter selects
                // the input chunk by key.side(), and the validator rejects an
                // undefined-side key whose bare name occurs in both inputs as ambiguous.
                // The validator stamps key.path() from side+schema afterwards.
                if (join.using_columns_size() == 1) {
                    ce::key_t left_key(resource, std::string_view(join.using_columns(0)), ce::side_t::left);
                    ce::key_t right_key(resource, std::string_view(join.using_columns(0)), ce::side_t::right);
                    join_node->append_expression(
                        ce::make_compare_expression(resource, ce::compare_type::eq, left_key, right_key));
                } else {
                    auto and_expr = ce::make_compare_union_expression(resource, ce::compare_type::union_and);
                    for (const auto& col : join.using_columns()) {
                        ce::key_t left_key(resource, std::string_view(col), ce::side_t::left);
                        ce::key_t right_key(resource, std::string_view(col), ce::side_t::right);
                        and_expr->append_child(
                            ce::make_compare_expression(resource, ce::compare_type::eq, left_key, right_key));
                    }
                    join_node->append_expression(and_expr);
                }
            } else {
                // Neither a condition nor using columns (crossJoin, or join() without
                // `on`): every pair of rows. The planner reads the join's first
                // expression unconditionally, so it carries the match-everything
                // predicate the transformer gives a CROSS JOIN.
                join_node->append_expression(ce::make_compare_expression(resource, ce::compare_type::all_true));
            }

            auto wrapper =
                cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
            wrapper->append_child(join_node);
            return wrapper;
        }

        node_result translate_set_operation(const sc::SetOperation& set_op, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            if (set_op.set_op_type() != sc::SetOperation::SET_OP_TYPE_UNION) {
                return unsupported("Spark set operation (intersect/except) is not supported", resource);
            }
            // A union by name pairs columns by their names, which a plan built without
            // the inputs' schemas cannot know: run positionally it would stack
            // mismatched columns.
            if ((set_op.has_by_name() && set_op.by_name()) ||
                (set_op.has_allow_missing_columns() && set_op.allow_missing_columns())) {
                return unsupported("Spark unionByName() is not supported; use union() with the same column order",
                                   resource);
            }

            auto left_res = translate_relation(set_op.left_input(), ctx);
            if (left_res.has_error()) {
                return left_res;
            }
            auto right_res = translate_relation(set_op.right_input(), ctx);
            if (right_res.has_error()) {
                return right_res;
            }

            bool is_all = set_op.has_is_all() && set_op.is_all();
            auto union_node = cl::make_node_union(resource, left_res.value(), right_res.value(), is_all);

            auto wrapper =
                cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
            wrapper->append_child(union_node);
            return wrapper;
        }

        node_result translate_subquery_alias(const sc::SubqueryAlias& sqa, translation_ctx_t& ctx) {
            auto input_res = translate_relation(sqa.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            auto node = input_res.value();
            node->set_result_alias(sqa.alias());
            return node;
        }

        node_result translate_deduplicate(const sc::Deduplicate& dedup, translation_ctx_t& ctx) {
            auto input_res = translate_relation(dedup.input(), ctx);
            if (input_res.has_error()) {
                return input_res;
            }
            // dropDuplicates(subset) keeps one row per subset key; a DISTINCT over every
            // column keeps more, so it is refused rather than run as one.
            if (dedup.column_names_size() > 0) {
                return unsupported("Spark dropDuplicates() over a subset of columns is not supported", ctx.resource);
            }
            auto node = ensure_aggregate_wrapper(input_res.value(), ctx.resource);
            if (!can_stack(find_clauses(node), operation_t::distinct)) {
                // The distinct rows of a window: a derived table of it. The distinct
                // reads the columns by position, so a rename there is harmless.
                node = derive(std::move(node), ctx.resource);
            }
            static_cast<cl::node_aggregate_t*>(node.get())->set_distinct(true);
            return node;
        }

        node_result translate_range(const sc::Range& range, std::pmr::memory_resource* resource) {
            int64_t start = range.has_start() ? range.start() : 0;
            int64_t end = range.end();
            int64_t step = range.step();

            if (step == 0) {
                return make_error(core::error_code_t::invalid_parameter, "Spark Range step cannot be zero", resource);
            }

            int64_t count = 0;
            if (step > 0) {
                for (int64_t v = start; v < end; v += step) {
                    ++count;
                }
            } else {
                for (int64_t v = start; v > end; v += step) {
                    ++count;
                }
            }

            if (count < 0) {
                count = 0;
            }
            if (static_cast<uint64_t>(count) > 1024) {
                return unsupported("Spark Range exceeds maximum materialization size (1024 rows)", resource);
            }

            // spark.range emits a single LongType column named "id". The name is
            // load-bearing: the engine's plan validator reads col.type.alias() without a
            // has_alias() guard, so an anonymous BIGINT (extension_ == nullptr) makes
            // alias() dereference null -> server SIGSEGV during validate_schema.
            std::pmr::vector<ct::complex_logical_type> types{resource};
            auto id_col = ct::complex_logical_type{ct::logical_type::BIGINT};
            id_col.set_alias("id");
            types.push_back(id_col);
            cv::data_chunk_t chunk(resource, types);

            uint64_t row = 0;
            if (step > 0) {
                for (int64_t v = start; v < end; v += step) {
                    chunk.set_value(0, row, ct::logical_value_t{resource, v});
                    ++row;
                }
            } else {
                for (int64_t v = start; v > end; v += step) {
                    chunk.set_value(0, row, ct::logical_value_t{resource, v});
                    ++row;
                }
            }
            chunk.set_cardinality(row);

            auto data_node = cl::make_node_raw_data(resource, std::move(chunk));
            auto wrapper =
                cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
            wrapper->append_child(data_node);
            return wrapper;
        }

        node_result translate_relation(const sc::Relation& rel, translation_ctx_t& ctx) {
            auto* resource = ctx.resource;
            switch (rel.rel_type_case()) {
                case sc::Relation::kRead:
                    return translate_read(rel.read(), ctx);
                case sc::Relation::kProject:
                    return translate_project(rel.project(), ctx);
                case sc::Relation::kFilter:
                    return translate_filter(rel.filter(), ctx);
                case sc::Relation::kSort:
                    return translate_sort(rel.sort(), ctx);
                case sc::Relation::kLimit:
                    return translate_limit(rel.limit(), ctx);
                case sc::Relation::kOffset:
                    return translate_offset(rel.offset(), ctx);
                case sc::Relation::kAggregate:
                    return translate_aggregate(rel.aggregate(), ctx);
                case sc::Relation::kJoin:
                    return translate_join(rel.join(), ctx);
                case sc::Relation::kSetOp:
                    return translate_set_operation(rel.set_op(), ctx);
                case sc::Relation::kSubqueryAlias:
                    return translate_subquery_alias(rel.subquery_alias(), ctx);
                case sc::Relation::kDeduplicate:
                    return translate_deduplicate(rel.deduplicate(), ctx);
                case sc::Relation::kRange:
                    return translate_range(rel.range(), resource);
                case sc::Relation::kHint: {
                    if (rel.hint().has_input()) {
                        return translate_relation(rel.hint().input(), ctx);
                    }
                    return make_error(core::error_code_t::invalid_parameter,
                                      "Spark Hint has no input relation",
                                      resource);
                }
                case sc::Relation::kSql: {
                    // Route the leaf SQL through parse_fragment so its constants land in
                    // the SHARED `params` node. A bare parse() keeps only the node and
                    // discards the sub-plan's parameter node, so a WHERE/HAVING constant
                    // later surfaces as "value getter: parameter not bound" at execution.
                    // External nodes are rebuilt from the node tree by
                    // populate_external_nodes, so the parser's own external-node analysis
                    // is not needed here — only the schemas the leaf named, which its
                    // nodes no longer carry.
                    otterstax::names::name_registry_t leaf_names{resource};
                    auto leaf = ctx.parser.parse_fragment(rel.sql().query(), ctx.params, leaf_names);
                    if (leaf.has_error()) {
                        return leaf;
                    }
                    if (auto error = record_leaf_schemas(leaf.value(), leaf_names, ctx); error.contains_error()) {
                        return node_result{std::move(error)};
                    }
                    return leaf;
                }
                case sc::Relation::kLocalRelation:
                    return unsupported("Spark LocalRelation is not supported", resource);
                case sc::Relation::kCachedLocalRelation:
                    return unsupported("Spark CachedLocalRelation is not supported", resource);
                case sc::Relation::kCachedRemoteRelation:
                    return unsupported("Spark CachedRemoteRelation is not supported", resource);
                case sc::Relation::kRepartition:
                    if (rel.repartition().has_input()) {
                        return translate_relation(rel.repartition().input(), ctx);
                    }
                    return unsupported("Spark Repartition without input is not supported", resource);
                case sc::Relation::kSample:
                    return unsupported("Spark Sample is not supported", resource);
                case sc::Relation::kToSchema:
                    return unsupported("Spark ToSchema is not supported", resource);
                case sc::Relation::kWithWatermark:
                    return unsupported("Spark WithWatermark (streaming) is not supported", resource);
                case sc::Relation::kMapPartitions:
                case sc::Relation::kGroupMap:
                case sc::Relation::kCoGroupMap:
                case sc::Relation::kApplyInPandasWithState:
                    return unsupported("Spark Python/Pandas map operations are not supported", resource);
                case sc::Relation::kUnpivot:
                    return unsupported("Spark Unpivot is not supported", resource);
                case sc::Relation::kTranspose:
                    return unsupported("Spark Transpose is not supported", resource);
                case sc::Relation::kAsOfJoin:
                    return unsupported("Spark AsOfJoin is not supported", resource);
                case sc::Relation::kLateralJoin:
                    return unsupported("Spark LateralJoin is not supported", resource);
                case sc::Relation::kWithRelations:
                    return unsupported("Spark WithRelations (CTE/DAG) is not supported", resource);
                case sc::Relation::kUnresolvedTableValuedFunction:
                    return unsupported("Spark unresolved table-valued function is not supported", resource);
                case sc::Relation::kCommonInlineUserDefinedTableFunction:
                    return unsupported("Spark Python UDTF is not supported", resource);
                case sc::Relation::kCommonInlineUserDefinedDataSource:
                    return unsupported("Spark Python data source is not supported", resource);
                default:
                    return unsupported("unsupported or unset Spark Relation type", resource);
            }
        }

        // The schema translate_read recorded for `node`, empty when it named none.
        std::string read_schema_of(const std::pmr::vector<read_schema_t>& read_schemas, const cl::node_t* node) {
            for (const auto& entry : read_schemas) {
                if (entry.node == node) {
                    return std::string(entry.schema.begin(), entry.schema.end());
                }
            }
            return std::string{};
        }

        size_t populate_external_nodes(std::pmr::memory_resource* resource,
                                       cl::node_ptr& root,
                                       std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes,
                                       const std::pmr::vector<read_schema_t>& read_schemas) {
            struct lookup_entry {
                cl::node_ptr* ptr;
                size_t batch_index;
            };

            external_nodes.emplace_back();
            size_t count = 0;
            std::pmr::deque<lookup_entry> queue{resource};
            queue.push_back({&root, 0});

            while (!queue.empty()) {
                auto entry = queue.front();
                queue.pop_front();
                auto& node = *entry.ptr;
                if (!node) {
                    continue;
                }

                if (node->type() == cl::node_type::aggregate_t) {
                    const auto& agg = static_cast<const cl::node_aggregate_t&>(*node);
                    const std::string& uid_str = agg.uid().t;
                    if (!uid_str.empty()) {
                        qualified_name_t name{uid_str,
                                              std::string(agg.dbname().t),
                                              read_schema_of(read_schemas, node.get()),
                                              std::string(agg.relname().t)};

                        auto it = std::find_if(external_nodes[entry.batch_index].begin(),
                                               external_nodes[entry.batch_index].end(),
                                               [&name](const external_entry_t& e) {
                                                   return e.target.name.unique_identifier == name.unique_identifier;
                                               });
                        if (it != external_nodes[entry.batch_index].end()) {
                            ++entry.batch_index;
                            if (external_nodes.size() == entry.batch_index) {
                                external_nodes.emplace_back();
                            }
                        }

                        external_nodes[entry.batch_index].push_back(
                            external_entry_t{entry.ptr,
                                             otterstax::names::resolved_target_t{components::catalog::INVALID_OID,
                                                                                 std::move(name),
                                                                                 qualified_name_t{}}});
                        ++count;
                    }
                }

                auto& children_vec = node->children();
                for (size_t i = 0; i < children_vec.size(); ++i) {
                    queue.push_back({&children_vec[i], entry.batch_index});
                }
            }

            if (external_nodes.back().empty()) {
                external_nodes.erase(external_nodes.end() - 1);
            }
            return count;
        }

        bool expr_contains_window(const sc::Expression& expr);

        bool relation_contains_window(const sc::Relation& rel);

        bool expr_contains_window(const sc::Expression& expr) {
            if (expr.expr_type_case() == sc::Expression::kWindow) {
                return true;
            }
            switch (expr.expr_type_case()) {
                case sc::Expression::kUnresolvedFunction: {
                    const auto& fn = expr.unresolved_function();
                    for (const auto& arg : fn.arguments()) {
                        if (expr_contains_window(arg)) {
                            return true;
                        }
                    }
                    break;
                }
                case sc::Expression::kCallFunction: {
                    const auto& fn = expr.call_function();
                    for (const auto& arg : fn.arguments()) {
                        if (expr_contains_window(arg)) {
                            return true;
                        }
                    }
                    break;
                }
                case sc::Expression::kAlias:
                    if (expr_contains_window(expr.alias().expr())) {
                        return true;
                    }
                    break;
                case sc::Expression::kCast:
                    if (expr_contains_window(expr.cast().expr())) {
                        return true;
                    }
                    break;
                case sc::Expression::kSortOrder:
                    if (expr_contains_window(expr.sort_order().child())) {
                        return true;
                    }
                    break;
                case sc::Expression::kCommonInlineUserDefinedFunction: {
                    const auto& udf = expr.common_inline_user_defined_function();
                    for (const auto& arg : udf.arguments()) {
                        if (expr_contains_window(arg)) {
                            return true;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            return false;
        }

        bool relation_contains_window(const sc::Relation& rel) {
            switch (rel.rel_type_case()) {
                case sc::Relation::kProject:
                    for (const auto& e : rel.project().expressions()) {
                        if (expr_contains_window(e)) {
                            return true;
                        }
                    }
                    if (rel.project().has_input()) {
                        return relation_contains_window(rel.project().input());
                    }
                    return false;
                case sc::Relation::kFilter:
                    if (expr_contains_window(rel.filter().condition())) {
                        return true;
                    }
                    return relation_contains_window(rel.filter().input());
                case sc::Relation::kSort:
                    for (const auto& so : rel.sort().order()) {
                        if (expr_contains_window(so.child())) {
                            return true;
                        }
                    }
                    return relation_contains_window(rel.sort().input());
                case sc::Relation::kAggregate:
                    for (const auto& e : rel.aggregate().grouping_expressions()) {
                        if (expr_contains_window(e)) {
                            return true;
                        }
                    }
                    for (const auto& e : rel.aggregate().aggregate_expressions()) {
                        if (expr_contains_window(e)) {
                            return true;
                        }
                    }
                    return relation_contains_window(rel.aggregate().input());
                case sc::Relation::kJoin:
                    if (rel.join().has_join_condition() && expr_contains_window(rel.join().join_condition())) {
                        return true;
                    }
                    return relation_contains_window(rel.join().left()) || relation_contains_window(rel.join().right());
                case sc::Relation::kSetOp:
                    return relation_contains_window(rel.set_op().left_input()) ||
                           relation_contains_window(rel.set_op().right_input());
                case sc::Relation::kLimit:
                    return relation_contains_window(rel.limit().input());
                case sc::Relation::kOffset:
                    return relation_contains_window(rel.offset().input());
                case sc::Relation::kTail:
                    return relation_contains_window(rel.tail().input());
                case sc::Relation::kSubqueryAlias:
                    return relation_contains_window(rel.subquery_alias().input());
                case sc::Relation::kDeduplicate:
                    return relation_contains_window(rel.deduplicate().input());
                case sc::Relation::kHint:
                    if (rel.hint().has_input()) {
                        return relation_contains_window(rel.hint().input());
                    }
                    return false;
                case sc::Relation::kRepartition:
                    if (rel.repartition().has_input()) {
                        return relation_contains_window(rel.repartition().input());
                    }
                    return false;
                case sc::Relation::kRepartitionByExpression:
                    if (rel.repartition_by_expression().has_input()) {
                        return relation_contains_window(rel.repartition_by_expression().input());
                    }
                    return false;
                case sc::Relation::kDrop:
                    if (rel.drop().has_input()) {
                        return relation_contains_window(rel.drop().input());
                    }
                    return false;
                case sc::Relation::kToDf:
                    if (rel.to_df().has_input()) {
                        return relation_contains_window(rel.to_df().input());
                    }
                    return false;
                case sc::Relation::kWithColumnsRenamed:
                    if (rel.with_columns_renamed().has_input()) {
                        return relation_contains_window(rel.with_columns_renamed().input());
                    }
                    return false;
                case sc::Relation::kWithColumns:
                    if (rel.with_columns().has_input()) {
                        return relation_contains_window(rel.with_columns().input());
                    }
                    return false;
                case sc::Relation::kSample:
                    if (rel.sample().has_input()) {
                        return relation_contains_window(rel.sample().input());
                    }
                    return false;
                case sc::Relation::kToSchema:
                    if (rel.to_schema().has_input()) {
                        return relation_contains_window(rel.to_schema().input());
                    }
                    return false;
                case sc::Relation::kShowString:
                    if (rel.show_string().has_input()) {
                        return relation_contains_window(rel.show_string().input());
                    }
                    return false;
                case sc::Relation::kHtmlString:
                    if (rel.html_string().has_input()) {
                        return relation_contains_window(rel.html_string().input());
                    }
                    return false;
                case sc::Relation::kWithWatermark:
                    if (rel.with_watermark().has_input()) {
                        return relation_contains_window(rel.with_watermark().input());
                    }
                    return false;
                case sc::Relation::kCollectMetrics:
                    if (rel.collect_metrics().has_input()) {
                        return relation_contains_window(rel.collect_metrics().input());
                    }
                    return false;
                case sc::Relation::kParse:
                    if (rel.parse().has_input()) {
                        return relation_contains_window(rel.parse().input());
                    }
                    return false;
                case sc::Relation::kFillNa:
                    if (rel.fill_na().has_input()) {
                        return relation_contains_window(rel.fill_na().input());
                    }
                    return false;
                case sc::Relation::kDropNa:
                    if (rel.drop_na().has_input()) {
                        return relation_contains_window(rel.drop_na().input());
                    }
                    return false;
                case sc::Relation::kReplace:
                    if (rel.replace().has_input()) {
                        return relation_contains_window(rel.replace().input());
                    }
                    return false;
                case sc::Relation::kSummary:
                    if (rel.summary().has_input()) {
                        return relation_contains_window(rel.summary().input());
                    }
                    return false;
                case sc::Relation::kCrosstab:
                    if (rel.crosstab().has_input()) {
                        return relation_contains_window(rel.crosstab().input());
                    }
                    return false;
                case sc::Relation::kDescribe:
                    if (rel.describe().has_input()) {
                        return relation_contains_window(rel.describe().input());
                    }
                    return false;
                case sc::Relation::kCov:
                    if (rel.cov().has_input()) {
                        return relation_contains_window(rel.cov().input());
                    }
                    return false;
                case sc::Relation::kCorr:
                    if (rel.corr().has_input()) {
                        return relation_contains_window(rel.corr().input());
                    }
                    return false;
                case sc::Relation::kApproxQuantile:
                    if (rel.approx_quantile().has_input()) {
                        return relation_contains_window(rel.approx_quantile().input());
                    }
                    return false;
                case sc::Relation::kFreqItems:
                    if (rel.freq_items().has_input()) {
                        return relation_contains_window(rel.freq_items().input());
                    }
                    return false;
                case sc::Relation::kSampleBy:
                    if (rel.sample_by().has_input()) {
                        return relation_contains_window(rel.sample_by().input());
                    }
                    return false;
                case sc::Relation::kMapPartitions:
                    if (rel.map_partitions().has_input()) {
                        return relation_contains_window(rel.map_partitions().input());
                    }
                    return false;
                case sc::Relation::kGroupMap:
                    if (rel.group_map().has_input()) {
                        return relation_contains_window(rel.group_map().input());
                    }
                    return false;
                case sc::Relation::kCoGroupMap:
                    if (rel.co_group_map().has_input()) {
                        return relation_contains_window(rel.co_group_map().input());
                    }
                    if (rel.co_group_map().has_other()) {
                        return relation_contains_window(rel.co_group_map().other());
                    }
                    return false;
                case sc::Relation::kApplyInPandasWithState:
                    if (rel.apply_in_pandas_with_state().has_input()) {
                        return relation_contains_window(rel.apply_in_pandas_with_state().input());
                    }
                    return false;
                case sc::Relation::kAsOfJoin:
                    return relation_contains_window(rel.as_of_join().left()) ||
                           relation_contains_window(rel.as_of_join().right());
                case sc::Relation::kLateralJoin:
                    return relation_contains_window(rel.lateral_join().left()) ||
                           relation_contains_window(rel.lateral_join().right());
                case sc::Relation::kUnpivot:
                    if (rel.unpivot().has_input()) {
                        return relation_contains_window(rel.unpivot().input());
                    }
                    return false;
                case sc::Relation::kTranspose:
                    if (rel.transpose().has_input()) {
                        return relation_contains_window(rel.transpose().input());
                    }
                    return false;
                case sc::Relation::kWithRelations:
                    if (rel.with_relations().has_root()) {
                        return relation_contains_window(rel.with_relations().root());
                    }
                    return false;
                default:
                    return false;
            }
        }

    } // namespace

    core::result_wrapper_t<TranslationResult> relation_to_plan(const sc::Plan& plan,
                                                               std::pmr::memory_resource* resource) {
        OTX_ZONE_N("spark::relation_to_plan");
        if (plan.op_type_case() != sc::Plan::kRoot) {
            return make_error(core::error_code_t::unimplemented_yet,
                              "Spark Connect Command execution is not supported in Path B",
                              resource)
                .convert_error<TranslationResult>();
        }
        if (!plan.has_root()) {
            return make_error(core::error_code_t::invalid_parameter, "Spark Plan has no root relation", resource)
                .convert_error<TranslationResult>();
        }

        const auto& root_rel = plan.root();

        if (relation_contains_window(root_rel)) {
            return make_error(core::error_code_t::unimplemented_yet,
                              "Spark window expressions are not supported in Path B",
                              resource)
                .convert_error<TranslationResult>();
        }

        translation_ctx_t ctx{resource};

        auto root_result = translate_relation(root_rel, ctx);
        if (root_result.has_error()) {
            return root_result.convert_error<TranslationResult>();
        }

        auto root_node = ensure_aggregate_wrapper(root_result.value(), resource);

        auto binder = cst::transform_result(resource,
                                            cl::execution_plan_t(resource, root_node, ctx.params),
                                            cst::transform_result::parameter_map_t{resource},
                                            cst::transform_result::insert_map_t{resource},
                                            cst::transform_result::insert_rows_t(resource));

        // parameters_count is the number of unbound `$n` placeholders, as the
        // parser reports it (a positive one makes the describe path refuse the
        // statement). Every literal of a translated plan is bound, so it is 0.
        auto statement =
            std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
                                                 binder.params_ptr(),
                                                 binder.node_ptr(),
                                                 0,
                                                 binder.parameter_count());

        auto ext_count =
            populate_external_nodes(resource, statement->node, statement->external_nodes, ctx.read_schemas);
        statement->external_nodes_count = ext_count;

        auto parsed = std::make_unique<ParsedQueryData>(std::move(statement), std::move(binder), NodeTag::T_SelectStmt);

        return TranslationResult{std::move(parsed)};
    }

    bool contains_window(const sc::Relation& rel) { return relation_contains_window(rel); }

} // namespace frontend::spark
