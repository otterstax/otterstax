// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "name_resolution.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>

#include <cassert>

namespace otterstax::names {

    namespace {

        using components::logical_plan::node_t;
        using components::logical_plan::node_type;

        std::pmr::string make_message(std::pmr::memory_resource* resource,
                                      std::string_view prefix,
                                      std::string_view dbname,
                                      std::string_view relname) {
            std::pmr::string msg{resource};
            msg.reserve(prefix.size() + dbname.size() + relname.size() + 3);
            msg.append(prefix.data(), prefix.size());
            msg.append(" '");
            msg.append(dbname.data(), dbname.size());
            msg.push_back('.');
            msg.append(relname.data(), relname.size());
            msg.push_back('\'');
            return msg;
        }

    } // namespace

    name_registry_t::name_registry_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , entries_(resource)
        , collisions_(resource) {
        assert(resource_ != nullptr && "memory resource must not be null");
    }

    std::pmr::string name_registry_t::make_key_(std::string_view dbname, std::string_view relname) const {
        std::pmr::string key{resource_};
        key.reserve(dbname.size() + relname.size() + 1);
        key.append(dbname.data(), dbname.size());
        key.push_back('\0');
        key.append(relname.data(), relname.size());
        return key;
    }

    void name_registry_t::add(qualified_name_t name) {
        // Mirror the transformer's rangevar_to_qualified_name folding:
        // dbname = (catalogname non-empty ? catalogname : schemaname).
        const std::string& key_db = name.database.empty() ? name.schema : name.database;
        auto key = make_key_(key_db, name.collection);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            entries_.emplace(std::move(key), std::move(name));
            return;
        }
        if (!(it->second == name)) {
            // Keep the FIRST entry; mark the key ambiguous so find() misses.
            collisions_.insert(std::move(key));
        }
    }

    const qualified_name_t* name_registry_t::find(std::string_view dbname, std::string_view relname) const {
        auto key = make_key_(dbname, relname);
        if (collisions_.find(key) != collisions_.end()) {
            return nullptr;
        }
        auto it = entries_.find(key);
        return it == entries_.end() ? nullptr : &it->second;
    }

    bool name_registry_t::collided(std::string_view dbname, std::string_view relname) const {
        return collisions_.find(make_key_(dbname, relname)) != collisions_.end();
    }

    core::result_wrapper_t<qualified_name_t> resolve_table_name(std::pmr::memory_resource* resource,
                                                                const name_registry_t& reg,
                                                                std::string_view dbname,
                                                                std::string_view relname) {
        const auto* full_name = reg.find(dbname, relname);
        if (full_name == nullptr) {
            if (reg.collided(dbname, relname)) {
                return core::error_t{core::error_code_t::ambiguous_name,
                                     make_message(resource, "ambiguous table reference", dbname, relname)};
            }
            return core::error_t{core::error_code_t::table_not_exists,
                                 make_message(resource, "cannot resolve table name", dbname, relname)};
        }
        return *full_name;
    }

    core::result_wrapper_t<qualified_name_t>
    node_names(const node_t& node, const name_registry_t& reg, const node_t* seq_ctx) {
        auto* resource = node.resource();

        std::string_view db;
        std::string_view rel;

        // Shared sibling-resolve: DML + table-level DDL nodes carry no names; the
        // transformer wraps them in a node_sequence_t whose FIRST catalog_resolve
        // (kind==table) sibling carries the target table. Returns an error_t on
        // failure, else sets db/rel and returns nullopt.
        auto resolve_from_sibling = [&]() -> std::optional<core::error_t> {
            if (seq_ctx == nullptr) {
                return core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"DML/DDL node requires its wrapping node_sequence_t context (seq_ctx) "
                                     "to resolve the target table name",
                                     resource}};
            }
            const components::logical_plan::node_catalog_resolve_t* resolve = nullptr;
            for (const auto& child : seq_ctx->children()) {
                if (child && child->type() == node_type::catalog_resolve_t &&
                    static_cast<const components::logical_plan::node_catalog_resolve_t&>(*child).kind() ==
                        components::logical_plan::resolve_kind::table) {
                    resolve = static_cast<const components::logical_plan::node_catalog_resolve_t*>(child.get());
                    break;
                }
            }
            if (resolve == nullptr) {
                return core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"DML/DDL sequence context carries no catalog_resolve (table) sibling",
                                     resource}};
            }
            db = resolve->dbname();
            rel = resolve->relname();
            return std::nullopt;
        };

        // Per-type (dbname, relname) extraction mirroring the engine's
        // enrich per-type switch.
        switch (node.type()) {
            case node_type::aggregate_t: {
                const auto& d = static_cast<const components::logical_plan::node_aggregate_t&>(node);
                db = d.dbname().t; // core::dbname_t — strong typedef over std::string
                rel = d.relname().t;
                break;
            }
            case node_type::match_t: {
                const auto& d = static_cast<const components::logical_plan::node_match_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::group_t: {
                const auto& d = static_cast<const components::logical_plan::node_group_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::sort_t: {
                const auto& d = static_cast<const components::logical_plan::node_sort_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::join_t: {
                const auto& d = static_cast<const components::logical_plan::node_join_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::limit_t: {
                const auto& d = static_cast<const components::logical_plan::node_limit_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::having_t: {
                const auto& d = static_cast<const components::logical_plan::node_having_t&>(node);
                db = d.dbname();
                rel = d.relname();
                break;
            }
            case node_type::create_collection_t: {
                // CREATE TABLE: the node carries only relname; the database
                // name lives on the node_catalog_resolve_namespace_t sibling.
                // An unqualified `CREATE TABLE t` has no such sibling (seq_ctx
                // may even be nullptr) — db stays empty and the registry hit
                // is the local `("", rel)` entry.
                const auto& d = static_cast<const components::logical_plan::node_create_collection_t&>(node);
                rel = d.relname();
                if (seq_ctx != nullptr) {
                    for (const auto& child : seq_ctx->children()) {
                        if (child && child->type() == node_type::catalog_resolve_t &&
                            static_cast<const components::logical_plan::node_catalog_resolve_t&>(*child).kind() ==
                                components::logical_plan::resolve_kind::namespace_) {
                            db = static_cast<const components::logical_plan::node_catalog_resolve_t&>(*child).dbname();
                            break;
                        }
                    }
                }
                break;
            }
            case node_type::create_database_t: {
                // Database-level DDL can never carry a connection alias — the
                // grammar's `database_name` is a single ColId — so these
                // statements are always local. The parser filters them out
                // before resolution (carries_table_reference); reaching this
                // switch is a contract violation.
                return core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"database-level DDL carries no alias-qualifiable name; "
                                     "CREATE DATABASE is local by grammar and must not be resolved",
                                     resource}};
            }
            // DML and table-level DDL nodes carry no names; resolve the target
            // table from the FIRST catalog_resolve (kind==table) sibling via the
            // resolve_from_sibling lambda above. drop_index carries a SECOND
            // resolve_table for the index itself — the table one comes first.
            case node_type::create_index_t:
            case node_type::insert_t:
            case node_type::update_t:
            case node_type::delete_t: {
                if (auto err = resolve_from_sibling()) {
                    return *err;
                }
                break;
            }
            case node_type::drop_t: {
                switch (static_cast<const components::logical_plan::node_drop_t&>(node).kind()) {
                    case components::logical_plan::drop_target_kind::database:
                        // Database-level DDL is always local by grammar; the parser
                        // filters it before resolution — reaching here is a contract violation.
                        return core::error_t{
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"database-level DDL carries no alias-qualifiable name; "
                                             "DROP DATABASE is local by grammar and must not be resolved",
                                             resource}};
                    case components::logical_plan::drop_target_kind::collection:
                    case components::logical_plan::drop_target_kind::index:
                        if (auto err = resolve_from_sibling()) {
                            return *err;
                        }
                        break;
                    default:
                        // type/sequence/view/macro carry no alias-qualifiable table name.
                        return core::error_t{
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"node type carries no table name to resolve", resource}};
                }
                break;
            }
            case node_type::unused: {
                return core::error_t{core::error_code_t::invalid_parameter,
                                     std::pmr::string{"schema_node_t must be resolved by caller", resource}};
            }
            default: {
                return core::error_t{core::error_code_t::invalid_parameter,
                                     std::pmr::string{"node type carries no table name to resolve", resource}};
            }
        }

        return resolve_table_name(resource, reg, db, rel);
    }

} // namespace otterstax::names
