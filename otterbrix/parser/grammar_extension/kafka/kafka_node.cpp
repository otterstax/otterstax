// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_node.hpp"

#include "kafka_ast.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <unordered_map>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components;

namespace {
    std::string to_lower(std::string_view text) {
        std::string out(text);
        std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
        return out;
    }
} // namespace

namespace otterstax::kafka {

    kafka_node_t::kafka_node_t(std::pmr::memory_resource* resource, kafka_op op, std::string name)
        : logical_plan::node_t(resource, logical_plan::node_type::unused)
        , op_(op)
        , name_(std::move(name)) {}

    expressions::hash_t kafka_node_t::hash_impl() const { return 0; }

    std::string kafka_node_t::to_string_impl() const { return "$kafka:" + name_; }

    std::optional<std::string> kafka_node_t::option(std::string_view key) const {
        const auto it = options_.find(to_lower(key));
        if (it == options_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    std::optional<types::complex_logical_type> map_column_type(std::string_view type_name) {
        const std::string lower = to_lower(type_name);

        // Map ksql/SQL type keywords onto otterbrix's canonical (pg-internal) type
        // names, then defer to the core get_logical_type() so the actual
        // logical_type assignment stays the single source of truth (and tracks any
        // new otterbrix types for free). A name that is already canonical (e.g
        // "uuid", "int4") falls through to get_logical_type() unchanged
        static const std::unordered_map<std::string, std::string> sql_alias = {
            {"int", "int4"},
            {"integer", "int4"},
            {"bigint", "int8_t"},
            {"smallint", "int2"},
            {"tinyint", "tinyint"},
            {"double", "float8"},
            {"float", "float4"},
            {"real", "float4"},
            {"boolean", "bool"},
            {"bool", "bool"},
            {"varchar", "string"},
            {"text", "string"},
            {"string", "string"},
        };

        std::string canonical = lower;
        if (const auto it = sql_alias.find(lower); it != sql_alias.end()) {
            canonical = it->second;
        }

        const types::logical_type type = sql::transform::get_logical_type(canonical);
        if (type == types::logical_type::UNKNOWN) {
            return std::nullopt;
        }
        return types::complex_logical_type(type);
    }

    kafka_node_ptr lower_to_node(std::pmr::memory_resource* resource, const kafka_grammar::kafka_stmt& stmt) {
        kafka_op op = kafka_op::create_source;
        switch (stmt.kind) {
            case kafka_grammar::stmt_kind::create_source:
                op = kafka_op::create_source;
                break;
            case kafka_grammar::stmt_kind::create_stream:
                op = kafka_op::create_stream;
                break;
            case kafka_grammar::stmt_kind::drop_object:
                op = stmt.obj == kafka_grammar::object_kind::source ? kafka_op::drop_source
                                                                    : kafka_op::drop_stream;
                break;
        }

        // Unquoted identifiers are case-folded to lower (PostgreSQL convention),
        // so a kafka object resolves the same whether referenced from our DDL or
        // from a plain SELECT/INSERT routed by the core parser
        kafka_node_ptr node{new kafka_node_t(resource, op, to_lower(stmt.name))};

        for (const auto* col = stmt.columns; col != nullptr; col = col->next) {
            auto type = map_column_type(col->type);
            assert(type.has_value() && "column type must be validated by kafka_ext::parse");
            std::string column_name = to_lower(col->name);
            type->set_alias(column_name);
            node->columns().push_back(kafka_column_t{std::move(column_name), std::move(*type)});
        }

        // Option keys folded to lower (case-insensitive lookup via option());
        // values kept verbatim — Kafka topics / bootstrap servers are case-sensitive
        for (const auto* opt = stmt.options; opt != nullptr; opt = opt->next) {
            node->options()[to_lower(opt->key)] = std::string(opt->value);
        }

        if (!stmt.as_select.empty()) {
            node->set_as_select(std::string(stmt.as_select));
        }
        node->set_if_exists(stmt.if_exists);

        return node;
    }

    std::optional<kafka_write_t> kafka_write_target(const logical_plan::node_ptr& root) {
        if (!root) {
            return std::nullopt;
        }
        const logical_plan::node_catalog_resolve_t* resolve = nullptr;
        const logical_plan::node_insert_t* insert = nullptr;
        // Inspect only root + its direct children: the write's target
        // catalog_resolve (kind == table) and the insert_t are siblings under the
        // wrapping sequence_t, while the source's own resolve nodes live deeper
        // (under the insert) — so a SELECT source's tables can't be mistaken for
        // the target
        auto inspect = [&](const logical_plan::node_t* n) {
            if (n->type() == logical_plan::node_type::catalog_resolve_t &&
                static_cast<const logical_plan::node_catalog_resolve_t*>(n)->kind() ==
                    logical_plan::resolve_kind::table) {
                if (!resolve) {
                    resolve = static_cast<const logical_plan::node_catalog_resolve_t*>(n);
                }
            } else if (n->type() == logical_plan::node_type::insert_t) {
                if (!insert) {
                    insert = static_cast<const logical_plan::node_insert_t*>(n);
                }
            }
        };
        inspect(root.get());
        for (const auto& child : root->children()) {
            inspect(child.get());
        }
        if (!insert || !resolve || resolve->dbname() != KAFKA_DATABASE_NAME) {
            return std::nullopt;
        }
        if (insert->children().empty()) {
            return std::nullopt; // an insert always has a source; defensive
        }
        // VALUES store the rows as a node_raw_data (data_t) child; INSERT ... SELECT
        // nests the SELECT's operator tree instead. The former is a one-shot produce,
        // the latter a continuous INSERT INTO query — the caller routes on this
        auto source = insert->children().front();
        const bool source_is_select = source->type() != logical_plan::node_type::data_t;
        return kafka_write_t{resolve->relname(), std::move(source), source_is_select};
    }

    namespace {
        const logical_plan::node_aggregate_t* find_first_aggregate(const logical_plan::node_t* n) {
            if (!n) {
                return nullptr;
            }
            if (auto* agg = dynamic_cast<const logical_plan::node_aggregate_t*>(n)) {
                return agg;
            }
            for (const auto& child : n->children()) {
                if (auto* found = find_first_aggregate(child.get())) {
                    return found;
                }
            }
            return nullptr;
        }
    } // namespace

    namespace {
        // Rebuild a parsed operator with an empty dbname/relname (reusing its
        // expressions) so it chains over a node_raw_data substitution; a parsed
        // operator keeps its source table's relname, which stops it from picking up
        // the raw_data rows. match_t/select_t handled; others pass through
        logical_plan::node_ptr rehome_operator(std::pmr::memory_resource* resource,
                                               const logical_plan::node_ptr& op) {
            if (op->type() == logical_plan::node_type::match_t) {
                auto rehomed = logical_plan::make_node_match(resource, {}, {}, {});
                rehomed->append_expressions(op->expressions());
                return rehomed;
            }
            if (op->type() == logical_plan::node_type::select_t) {
                auto rehomed = logical_plan::make_node_select(resource, {}, {});
                rehomed->append_expressions(op->expressions());
                return rehomed;
            }
            return op;
        }
    } // namespace

    std::optional<kafka_stream_plan_t> kafka_stream_source(std::pmr::memory_resource* resource,
                                                           const logical_plan::node_ptr& root) {
        const logical_plan::node_aggregate_t* agg = find_first_aggregate(root.get());
        if (!agg) {
            return std::nullopt;
        }
        kafka_stream_plan_t out;
        out.source_relname = agg->relname().t;
        out.operators.reserve(agg->children().size());
        for (const auto& child : agg->children()) {
            out.operators.push_back(rehome_operator(resource, child));
        }
        return out;
    }

    const logical_plan::node_aggregate_t* kafka_find_aggregate(const logical_plan::node_ptr& root) {
        return find_first_aggregate(root.get());
    }

} // namespace otterstax::kafka
