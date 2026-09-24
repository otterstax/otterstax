// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

// Schema probe for a relation the engine holds.
//
// Every answer is taken out of the engine's REPLY, never off the plan the probe
// sent: a plan travels into the dispatcher by value and the engine keeps writing
// to its nodes while it runs, so reading what it stamped there would reach into
// another actor's memory. Three questions, one plan each, all answered by the
// cursor:
//
//   make_table_probe(db, rel)        -> read_columns       : the column types
//   make_namespace_probe(db)         -> read_namespace_oid : pg_namespace.oid
//   make_relation_probe(db, rel)     -> read_relation      : pg_class oid + relkind
//
// The columns come from a `LIMIT 0` read, so the answer is the same for an empty
// relation and a full one — the engine types the result from the catalog, not
// from rows. A VIEW needs no second probe: the engine splices the view body into
// the probe's own aggregate and answers with the body's columns (and clears the
// aggregate's name doing so, which is why the subject of an error is snapshotted
// when the probe is built).
//
// pg_catalog answers the identity questions, which `SELECT` cannot: the oid and
// the relkind of the relation a name resolved to. Both lookups bind their literal
// as a plan parameter, so no name is ever spelled into SQL text.
namespace otterstax::schema_probe {

    struct probe_t {
        components::logical_plan::node_ptr root;
        components::logical_plan::parameter_node_ptr params;
        // What the probe asks about, named in its errors. Snapshotted here: the
        // engine clears the aggregate's own name when it splices a VIEW body in.
        std::pmr::string subject;

        components::logical_plan::execution_plan_t execution_plan(std::pmr::memory_resource* resource) const {
            return components::logical_plan::execution_plan_t{resource, root, params};
        }
    };

    // Output column types of a probed relation (alias = column name). A VIEW is
    // indistinguishable here — the engine answers it with its body's columns —
    // so what a name resolved to is asked of `read_relation`'s relkind.
    struct columns_t {
        explicit columns_t(std::pmr::memory_resource* resource)
            : columns(resource) {}

        std::pmr::vector<components::types::complex_logical_type> columns;
    };

    // pg_class identity of the relation a name resolved to.
    struct relation_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        char relkind{components::catalog::relkind::regular};
    };

    namespace detail {

        inline std::pmr::string qualified(std::pmr::memory_resource* resource,
                                          const std::string& database,
                                          const std::string& collection) {
            std::pmr::string name{resource};
            name.append(database.data(), database.size());
            name.push_back('.');
            name.append(collection.data(), collection.size());
            return name;
        }

        inline core::error_t probe_error(std::pmr::memory_resource* resource,
                                         core::error_code_t code,
                                         std::string_view text,
                                         const std::pmr::string& subject) {
            std::pmr::string what{text, resource};
            what.append(subject.data(), subject.size());
            return core::error_t{code, std::move(what)};
        }

        // The cursor of an executed probe, or the reason there is none.
        inline core::error_t probe_reply(std::pmr::memory_resource* resource,
                                         const components::cursor::cursor_t_ptr& cursor,
                                         const std::pmr::string& subject) {
            if (!cursor) {
                return probe_error(resource,
                                   core::error_code_t::other_error,
                                   "schema probe: null cursor for ",
                                   subject);
            }
            if (cursor->is_error()) {
                return core::error_t{cursor->get_error().type,
                                     std::pmr::string{cursor->get_error().what.c_str(), resource}};
            }
            return core::error_t::no_error();
        }

        // `SELECT * FROM pg_catalog.<table> WHERE <column> = <bound literal>`.
        inline probe_t catalog_lookup(std::pmr::memory_resource* resource,
                                      const char* table,
                                      const char* column,
                                      components::types::logical_value_t literal,
                                      std::pmr::string subject) {
            using namespace components::logical_plan;
            const std::string catalog_db{"pg_catalog"};
            const std::string catalog_rel{table};
            auto params = make_parameter_node(resource);
            const auto bound = params->add_parameter(std::move(literal));
            // The key is on side_t::left — the single target table; a key of
            // undefined side leaves the column unresolved.
            auto predicate = components::expressions::make_compare_expression(
                resource,
                components::expressions::compare_type::eq,
                components::expressions::key_t{resource, column, components::expressions::side_t::left},
                bound);
            auto aggregate = make_node_aggregate(resource, core::dbname_t{catalog_db}, core::relname_t{catalog_rel});
            aggregate->append_child(
                make_node_match(resource, core::dbname_t{catalog_db}, core::relname_t{catalog_rel}, predicate));
            return probe_t{std::move(aggregate), std::move(params), std::move(subject)};
        }

    } // namespace detail

    // SELECT * FROM database.collection LIMIT 0 — the engine answers the column
    // types without reading a row.
    inline probe_t make_table_probe(std::pmr::memory_resource* resource,
                                    const std::string& database,
                                    const std::string& collection) {
        using namespace components::logical_plan;
        auto aggregate = make_node_aggregate(resource, core::dbname_t{database}, core::relname_t{collection});
        aggregate->append_child(
            make_node_limit(resource, core::dbname_t{database}, core::relname_t{collection}, limit_t{0}));
        return probe_t{std::move(aggregate),
                       make_parameter_node(resource),
                       detail::qualified(resource, database, collection)};
    }

    // The pg_namespace row of `database` — its oid keys the pg_class lookup.
    inline probe_t make_namespace_probe(std::pmr::memory_resource* resource, const std::string& database) {
        std::pmr::string subject{resource};
        subject.append(database.data(), database.size());
        return detail::catalog_lookup(resource,
                                      "pg_namespace",
                                      "nspname",
                                      components::types::logical_value_t{resource, database},
                                      std::move(subject));
    }

    // The pg_class rows named `collection`, in every namespace; read_relation
    // picks the one of the namespace asked for.
    inline probe_t make_relation_probe(std::pmr::memory_resource* resource,
                                       const std::string& database,
                                       const std::string& collection) {
        return detail::catalog_lookup(resource,
                                      "pg_class",
                                      "relname",
                                      components::types::logical_value_t{resource, collection},
                                      detail::qualified(resource, database, collection));
    }

    // Columns of an executed table probe. A relation without columns answers an
    // empty schema, which is a valid answer and not an error; a relation the
    // engine does not hold carries the engine's own code in the cursor.
    inline core::result_wrapper_t<columns_t> read_columns(const probe_t& probe,
                                                          const components::cursor::cursor_t_ptr& cursor,
                                                          std::pmr::memory_resource* resource) {
        if (auto reply = detail::probe_reply(resource, cursor, probe.subject); reply.contains_error()) {
            return std::move(reply);
        }
        columns_t out{resource};
        out.columns.assign(cursor->type_data().begin(), cursor->type_data().end());
        return out;
    }

    // pg_namespace.oid of an executed namespace probe.
    inline core::result_wrapper_t<components::catalog::oid_t>
    read_namespace_oid(const probe_t& probe,
                       const components::cursor::cursor_t_ptr& cursor,
                       std::pmr::memory_resource* resource) {
        if (auto reply = detail::probe_reply(resource, cursor, probe.subject); reply.contains_error()) {
            return std::move(reply);
        }
        auto oid_column = cursor->column_index("oid");
        if (oid_column.has_error()) {
            return oid_column.convert_error<components::catalog::oid_t>();
        }
        if (cursor->size() == 0) {
            return detail::probe_error(resource,
                                       core::error_code_t::database_not_exists,
                                       "schema probe: pg_namespace has no row for ",
                                       probe.subject);
        }
        return static_cast<components::catalog::oid_t>(cursor->value(oid_column.value(), 0).value<uint32_t>());
    }

    // pg_class oid and relkind of an executed relation probe, taken from the row
    // of `namespace_oid` — a name is unique only within its namespace.
    inline core::result_wrapper_t<relation_t> read_relation(const probe_t& probe,
                                                            const components::cursor::cursor_t_ptr& cursor,
                                                            components::catalog::oid_t namespace_oid,
                                                            std::pmr::memory_resource* resource) {
        if (auto reply = detail::probe_reply(resource, cursor, probe.subject); reply.contains_error()) {
            return std::move(reply);
        }
        auto oid_column = cursor->column_index("oid");
        if (oid_column.has_error()) {
            return oid_column.convert_error<relation_t>();
        }
        auto namespace_column = cursor->column_index("relnamespace");
        if (namespace_column.has_error()) {
            return namespace_column.convert_error<relation_t>();
        }
        auto kind_column = cursor->column_index("relkind");
        if (kind_column.has_error()) {
            return kind_column.convert_error<relation_t>();
        }
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            const auto row_namespace =
                static_cast<components::catalog::oid_t>(cursor->value(namespace_column.value(), row).value<uint32_t>());
            if (row_namespace != namespace_oid) {
                continue;
            }
            const auto kind = cursor->value(kind_column.value(), row).value<std::string_view>();
            if (kind.empty()) {
                return detail::probe_error(resource,
                                           core::error_code_t::schema_error,
                                           "schema probe: pg_class carries no relkind for ",
                                           probe.subject);
            }
            relation_t out;
            out.oid = static_cast<components::catalog::oid_t>(cursor->value(oid_column.value(), row).value<uint32_t>());
            out.relkind = kind.front();
            return out;
        }
        return detail::probe_error(resource,
                                   core::error_code_t::table_not_exists,
                                   "schema probe: pg_class has no row for ",
                                   probe.subject);
    }

} // namespace otterstax::schema_probe
