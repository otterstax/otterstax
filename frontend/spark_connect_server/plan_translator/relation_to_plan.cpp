// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "relation_to_plan.hpp"

#include "expression_to_plan.hpp"

#include <spark/connect/expressions.pb.h>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
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

node_result make_error(core::error_code_t code, std::string_view what, std::pmr::memory_resource* resource) {
    return node_result{core::error_t{code, std::pmr::string{what.data(), what.size(), resource}}};
}

node_result unsupported(std::string_view what, std::pmr::memory_resource* resource) {
    return make_error(core::error_code_t::unimplemented_yet, what, resource);
}

std::pmr::vector<std::pmr::string> split_identifier(std::string_view ident, std::pmr::memory_resource* resource) {
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
            ref.dbname = std::string(parts[0].begin(), parts[0].end());
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
    auto wrapper = cl::make_node_aggregate(resource, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
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

node_result translate_relation(const sc::Relation& rel,
                               cl::parameter_node_ptr params,
                               std::pmr::memory_resource* resource);

node_result translate_read(const sc::Read& read,
                           cl::parameter_node_ptr /*params*/,
                           std::pmr::memory_resource* resource) {
    if (read.is_streaming()) {
        return unsupported("streaming reads are not supported", resource);
    }
    switch (read.read_type_case()) {
        case sc::Read::kNamedTable: {
            auto ref = parse_table_identifier(read.named_table().unparsed_identifier(), resource);
            cl::node_ptr node;
            if (ref.uid.empty()) {
                node = cl::make_node_aggregate(resource,
                                               core::dbname_t{ref.dbname},
                                               core::relname_t{ref.relname});
            } else {
                node = cl::make_node_aggregate(resource,
                                               core::uid_t{ref.uid},
                                               core::dbname_t{ref.dbname},
                                               core::relname_t{ref.relname});
            }
            return node;
        }
        case sc::Read::kDataSource:
            return unsupported("Spark DataSource reads are not supported", resource);
        default:
            return unsupported("unset Spark Read type", resource);
    }
}

node_result translate_project(const sc::Project& proj,
                              cl::parameter_node_ptr params,
                              std::pmr::memory_resource* resource) {
    cl::node_ptr input_node;
    std::string dbname;
    std::string relname;
    if (proj.has_input()) {
        auto input_res = translate_relation(proj.input(), params, resource);
        if (input_res.has_error()) {
            return input_res;
        }
        input_node = ensure_aggregate_wrapper(input_res.value(), resource);
        auto names = get_names(input_node);
        dbname = std::move(names.dbname);
        relname = std::move(names.relname);
    } else {
        input_node = cl::make_node_aggregate(resource,
                                             core::dbname_t{std::string{}},
                                             core::relname_t{std::string{}});
    }

    auto select = cl::make_node_select(resource, core::dbname_t{dbname}, core::relname_t{relname});
    for (const auto& expr : proj.expressions()) {
        auto e = expression_to_plan(expr, params, resource);
        if (e.has_error()) {
            return e.convert_error<cl::node_ptr>();
        }
        select->append_expression(e.value());
    }
    input_node->append_child(select);
    return input_node;
}

node_result translate_filter(const sc::Filter& filter,
                             cl::parameter_node_ptr params,
                             std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(filter.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
    auto names = get_names(input_node);

    ce::expression_ptr predicate;
    if (filter.condition().expr_type_case() == sc::Expression::kExpressionString) {
        // PySpark's .filter("<sql>") sends the predicate as a raw SQL string
        // (Expression.ExpressionString), not a structured tree. Route it through
        // the real parser, materialising constants into the shared `params` node,
        // so the predicate lands in the executable transformer form
        // compare(key_t, parameter_id_t). The structured expression_to_plan path
        // would instead emit a compare(expression, expression) that neither the
        // remote SQL generator nor the local value-getter can evaluate.
        std::string sql{"SELECT * FROM __otterstax_filter__ WHERE "};
        sql += filter.condition().expression_string().expression();
        auto frag = make_parser(resource)->parse_fragment(sql, params);
        if (frag.has_error()) {
            return frag;  // same result type (node_ptr) — forward the error as-is
        }
        auto match_node = find_match_node(frag.value());
        if (!match_node || match_node->expressions().empty()) {
            return make_error(core::error_code_t::unimplemented_yet,
                              "Spark filter string did not translate to a predicate",
                              resource);
        }
        predicate = match_node->expressions().front();
    } else {
        auto cond = expression_to_plan(filter.condition(), params, resource);
        if (cond.has_error()) {
            return cond.convert_error<cl::node_ptr>();
        }
        predicate = cond.value();
    }

    auto match = cl::make_node_match(resource,
                                     core::dbname_t{names.dbname},
                                     core::relname_t{names.relname},
                                     predicate);
    input_node->append_child(match);
    return input_node;
}

node_result translate_sort(const sc::Sort& sort,
                           cl::parameter_node_ptr params,
                           std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(sort.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
    auto names = get_names(input_node);

    std::vector<cl::expression_ptr> sort_exprs;
    sort_exprs.reserve(sort.order_size());
    for (const auto& so : sort.order()) {
        auto child = expression_to_plan(so.child(), params, resource);
        if (child.has_error()) {
            return child.convert_error<cl::node_ptr>();
        }
        ce::sort_order order = ce::sort_order::asc;
        if (so.direction() == sc::Expression::SortOrder::SORT_DIRECTION_DESCENDING) {
            order = ce::sort_order::desc;
        }
        auto& expr = child.value();
        if (expr->group() == ce::expression_group::scalar) {
            auto* scalar = static_cast<ce::scalar_expression_t*>(expr.get());
            if (scalar->type() == ce::scalar_type::get_field) {
                sort_exprs.push_back(ce::make_sort_expression(scalar->key(), order));
                continue;
            }
        }
        return make_error(core::error_code_t::sql_parse_error,
                          "Spark sort key must be a column reference",
                          resource);
    }
    auto sort_node = cl::make_node_sort(resource,
                                        core::dbname_t{names.dbname},
                                        core::relname_t{names.relname},
                                        sort_exprs);
    input_node->append_child(sort_node);
    return input_node;
}

node_result translate_limit(const sc::Limit& limit,
                            cl::parameter_node_ptr params,
                            std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(limit.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
    auto names = get_names(input_node);

    auto limit_node = cl::make_node_limit(resource,
                                          core::dbname_t{names.dbname},
                                          core::relname_t{names.relname},
                                          cl::limit_t{limit.limit()});
    input_node->append_child(limit_node);
    return input_node;
}

node_result translate_offset(const sc::Offset& offset,
                             cl::parameter_node_ptr params,
                             std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(offset.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
    auto names = get_names(input_node);

    auto unlim = cl::limit_t::unlimit();
    auto limit_node = cl::make_node_limit(resource,
                                          core::dbname_t{names.dbname},
                                          core::relname_t{names.relname},
                                          cl::limit_t{unlim.limit(), offset.offset()});
    input_node->append_child(limit_node);
    return input_node;
}

node_result translate_aggregate(const sc::Aggregate& agg,
                                cl::parameter_node_ptr params,
                                std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(agg.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto input_node = ensure_aggregate_wrapper(input_res.value(), resource);
    auto names = get_names(input_node);

    if (agg.group_type() == sc::Aggregate::GROUP_TYPE_PIVOT ||
        agg.group_type() == sc::Aggregate::GROUP_TYPE_ROLLUP ||
        agg.group_type() == sc::Aggregate::GROUP_TYPE_CUBE ||
        agg.group_type() == sc::Aggregate::GROUP_TYPE_GROUPING_SETS) {
        return unsupported("Spark aggregate group type (rollup/cube/pivot/grouping_sets) is not supported",
                           resource);
    }

    if (agg.aggregate_expressions_size() > 0) {
        auto select = cl::make_node_select(resource,
                                           core::dbname_t{names.dbname},
                                           core::relname_t{names.relname});
        for (const auto& expr : agg.aggregate_expressions()) {
            auto e = expression_to_plan(expr, params, resource);
            if (e.has_error()) {
                return e.convert_error<cl::node_ptr>();
            }
            select->append_expression(e.value());
        }
        input_node->append_child(select);
    }

    if (agg.grouping_expressions_size() > 0) {
        auto group = cl::make_node_group(resource,
                                         core::dbname_t{names.dbname},
                                         core::relname_t{names.relname});
        for (const auto& expr : agg.grouping_expressions()) {
            auto e = expression_to_plan(expr, params, resource);
            if (e.has_error()) {
                return e.convert_error<cl::node_ptr>();
            }
            group->append_expression(e.value());
        }
        input_node->append_child(group);
    }

    return input_node;
}

node_result translate_join(const sc::Join& join,
                           cl::parameter_node_ptr params,
                           std::pmr::memory_resource* resource) {
    auto jt = map_join_type(join.join_type());
    if (jt == cl::join_type::invalid) {
        return unsupported("Spark join type (left_anti/left_semi) is not supported", resource);
    }

    auto left_res = translate_relation(join.left(), params, resource);
    if (left_res.has_error()) {
        return left_res;
    }
    auto right_res = translate_relation(join.right(), params, resource);
    if (right_res.has_error()) {
        return right_res;
    }

    auto join_node = cl::make_node_join(resource,
                                        core::dbname_t{std::string{}},
                                        core::relname_t{std::string{}},
                                        jt);
    join_node->append_child(left_res.value());
    join_node->append_child(right_res.value());

    if (join.has_join_condition()) {
        auto cond = expression_to_plan(join.join_condition(), params, resource);
        if (cond.has_error()) {
            return cond.convert_error<cl::node_ptr>();
        }
        join_node->append_expression(cond.value());
    } else if (join.using_columns_size() > 0) {
        // Emit the equi-join predicate in the executable transformer form
        // compare(key_t, key_t) — a key_t implicitly converts to param_storage.
        // The former compare(get_field_scalar, get_field_scalar) shape is not
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
    }

    auto wrapper = cl::make_node_aggregate(resource,
                                           core::dbname_t{std::string{}},
                                           core::relname_t{std::string{}});
    wrapper->append_child(join_node);
    return wrapper;
}

node_result translate_set_operation(const sc::SetOperation& set_op,
                                    cl::parameter_node_ptr params,
                                    std::pmr::memory_resource* resource) {
    if (set_op.set_op_type() != sc::SetOperation::SET_OP_TYPE_UNION) {
        return unsupported("Spark set operation (intersect/except) is not supported", resource);
    }

    auto left_res = translate_relation(set_op.left_input(), params, resource);
    if (left_res.has_error()) {
        return left_res;
    }
    auto right_res = translate_relation(set_op.right_input(), params, resource);
    if (right_res.has_error()) {
        return right_res;
    }

    bool is_all = set_op.has_is_all() && set_op.is_all();
    auto union_node = cl::make_node_union(resource, left_res.value(), right_res.value(), is_all);

    auto wrapper = cl::make_node_aggregate(resource,
                                           core::dbname_t{std::string{}},
                                           core::relname_t{std::string{}});
    wrapper->append_child(union_node);
    return wrapper;
}

node_result translate_subquery_alias(const sc::SubqueryAlias& sqa,
                                     cl::parameter_node_ptr params,
                                     std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(sqa.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto node = input_res.value();
    node->set_result_alias(sqa.alias());
    return node;
}

node_result translate_deduplicate(const sc::Deduplicate& dedup,
                                  cl::parameter_node_ptr params,
                                  std::pmr::memory_resource* resource) {
    auto input_res = translate_relation(dedup.input(), params, resource);
    if (input_res.has_error()) {
        return input_res;
    }
    auto node = input_res.value();
    if (node->type() == cl::node_type::aggregate_t) {
        auto* agg = static_cast<cl::node_aggregate_t*>(node.get());
        agg->set_distinct(true);
    } else {
        auto wrapper = ensure_aggregate_wrapper(node, resource);
        auto* agg = static_cast<cl::node_aggregate_t*>(wrapper.get());
        agg->set_distinct(true);
        return wrapper;
    }
    return node;
}

node_result translate_range(const sc::Range& range,
                            std::pmr::memory_resource* resource) {
    int64_t start = range.has_start() ? range.start() : 0;
    int64_t end = range.end();
    int64_t step = range.step();

    if (step == 0) {
        return make_error(core::error_code_t::invalid_parameter,
                          "Spark Range step cannot be zero",
                          resource);
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

    std::pmr::vector<ct::complex_logical_type> types{resource};
    types.push_back(ct::complex_logical_type{ct::logical_type::BIGINT});
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
    auto wrapper = cl::make_node_aggregate(resource,
                                           core::dbname_t{std::string{}},
                                           core::relname_t{std::string{}});
    wrapper->append_child(data_node);
    return wrapper;
}

node_result translate_relation(const sc::Relation& rel,
                               cl::parameter_node_ptr params,
                               std::pmr::memory_resource* resource) {
    switch (rel.rel_type_case()) {
        case sc::Relation::kRead:
            return translate_read(rel.read(), params, resource);
        case sc::Relation::kProject:
            return translate_project(rel.project(), params, resource);
        case sc::Relation::kFilter:
            return translate_filter(rel.filter(), params, resource);
        case sc::Relation::kSort:
            return translate_sort(rel.sort(), params, resource);
        case sc::Relation::kLimit:
            return translate_limit(rel.limit(), params, resource);
        case sc::Relation::kOffset:
            return translate_offset(rel.offset(), params, resource);
        case sc::Relation::kAggregate:
            return translate_aggregate(rel.aggregate(), params, resource);
        case sc::Relation::kJoin:
            return translate_join(rel.join(), params, resource);
        case sc::Relation::kSetOp:
            return translate_set_operation(rel.set_op(), params, resource);
        case sc::Relation::kSubqueryAlias:
            return translate_subquery_alias(rel.subquery_alias(), params, resource);
        case sc::Relation::kDeduplicate:
            return translate_deduplicate(rel.deduplicate(), params, resource);
        case sc::Relation::kRange:
            return translate_range(rel.range(), resource);
        case sc::Relation::kHint: {
            if (rel.hint().has_input()) {
                return translate_relation(rel.hint().input(), params, resource);
            }
            return make_error(core::error_code_t::invalid_parameter,
                              "Spark Hint has no input relation",
                              resource);
        }
        case sc::Relation::kSql: {
            const auto& sql_rel = rel.sql();
            auto parser = make_parser(resource);
            auto parse_result = parser->parse(sql_rel.query());
            if (parse_result.has_error()) {
                return make_error(core::error_code_t::sql_parse_error,
                                  parse_result.error().what.c_str(), resource);
            }
            auto& parsed = parse_result.value();
            return cl::node_ptr{std::move(parsed->otterbrix_params->node)};
        }
        case sc::Relation::kLocalRelation:
            return unsupported("Spark LocalRelation is not supported", resource);
        case sc::Relation::kCachedLocalRelation:
            return unsupported("Spark CachedLocalRelation is not supported", resource);
        case sc::Relation::kCachedRemoteRelation:
            return unsupported("Spark CachedRemoteRelation is not supported", resource);
        case sc::Relation::kRepartition:
            if (rel.repartition().has_input()) {
                return translate_relation(rel.repartition().input(), params, resource);
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

size_t populate_external_nodes(std::pmr::memory_resource* resource,
                               cl::node_ptr& root,
                               std::pmr::vector<std::pmr::vector<external_entry_t>>& external_nodes) {
    struct lookup_entry {
        cl::node_ptr* ptr;
        size_t batch_index;
    };

    external_nodes.emplace_back();
    size_t count = 0;
    std::deque<lookup_entry> queue;
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
                                      std::string{},
                                      std::string(agg.relname().t)};

                auto it = std::find_if(
                    external_nodes[entry.batch_index].begin(),
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
                                     otterstax::names::resolved_target_t{
                                         components::catalog::INVALID_OID,
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

}  // namespace

core::result_wrapper_t<TranslationResult>
relation_to_plan(const sc::Plan& plan, std::pmr::memory_resource* resource) {
    if (plan.op_type_case() != sc::Plan::kRoot) {
        return make_error(core::error_code_t::unimplemented_yet,
                          "Spark Connect Command execution is not supported in Path B",
                          resource)
            .convert_error<TranslationResult>();
    }
    if (!plan.has_root()) {
        return make_error(core::error_code_t::invalid_parameter,
                          "Spark Plan has no root relation",
                          resource)
            .convert_error<TranslationResult>();
    }

    const auto& root_rel = plan.root();

    if (relation_contains_window(root_rel)) {
        return make_error(core::error_code_t::unimplemented_yet,
                          "Spark window expressions are not supported in Path B",
                          resource)
            .convert_error<TranslationResult>();
    }

    auto params = cl::make_parameter_node(resource);

    auto root_result = translate_relation(root_rel, params, resource);
    if (root_result.has_error()) {
        return root_result.convert_error<TranslationResult>();
    }

    auto root_node = ensure_aggregate_wrapper(root_result.value(), resource);

    // Non-mutating count of materialised parameters (next_id() would advance the
    // shared counter as a side effect of reading it).
    const auto param_count = params->parameters().parameters.size();

    auto binder = cst::transform_result(
        resource,
        cl::execution_plan_t(resource, root_node, params),
        cst::transform_result::parameter_map_t{resource},
        cst::transform_result::insert_map_t{resource},
        cst::transform_result::insert_rows_t(resource));

    auto statement = std::make_unique<OtterbrixStatement>(
        std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
        binder.params_ptr(),
        binder.node_ptr(),
        0,
        param_count);

    auto ext_count = populate_external_nodes(resource, statement->node, statement->external_nodes);
    statement->external_nodes_count = ext_count;

    auto parsed = std::make_unique<ParsedQueryData>(std::move(statement),
                                                    std::move(binder),
                                                    NodeTag::T_SelectStmt);

    return TranslationResult{std::move(parsed)};
}

bool contains_window(const sc::Relation& rel) {
    return relation_contains_window(rel);
}

}  // namespace frontend::spark
