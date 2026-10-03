// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "alias_registry.hpp"

#include "parser.hpp"
#include "subquery_extractor.hpp"
#include "utility/tracy_profiler.hpp"

#include <initializer_list>

namespace otterstax::names {
    namespace {
        std::string spelled(const qualified_name_t& name) {
            std::string text;
            for (const auto* slot : {&name.unique_identifier, &name.database, &name.schema, &name.collection}) {
                if (slot->empty()) {
                    continue;
                }
                if (!text.empty()) {
                    text += '.';
                }
                text += *slot;
            }
            return text;
        }

        const char* backend_name(backend_type_t backend) {
            switch (backend) {
                case backend_type_t::MySQL:
                    return "MySQL";
                case backend_type_t::PostgreSQL:
                    return "PostgreSQL";
                case backend_type_t::ClickHouse:
                    return "ClickHouse";
                default:
                    return "unknown";
            }
        }

        core::error_t refusal(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

        core::error_t no_such_connection(std::pmr::memory_resource* resource, std::string_view uid) {
            return refusal(resource,
                           core::error_code_t::do_not_exists,
                           "no connection is named '" + std::string{uid} + "'");
        }
    } // namespace

    void alias_registry_t::add(std::string alias, alias_t connection) {
        aliases_.insert_or_assign(std::move(alias), std::move(connection));
    }

    const alias_t* alias_registry_t::find(std::string_view alias) const {
        const auto it = aliases_.find(alias);
        return it == aliases_.end() ? nullptr : &it->second;
    }

    core::result_wrapper_t<qualified_name_t> federated_name(std::pmr::memory_resource* resource,
                                                            const alias_registry_t& aliases,
                                                            std::string_view uid,
                                                            std::string_view db,
                                                            std::string_view rel) {
        OTX_ZONE_N("names::federated_name");
        const auto* connection = aliases.find(uid);
        if (connection == nullptr) {
            return no_such_connection(resource, uid);
        }
        if (connection->backend != backend_type_t::PostgreSQL) {
            return qualified_name_t(std::string{uid}, std::string{db}, std::string{}, std::string{rel});
        }
        if (db != connection->database) {
            return refusal(resource,
                           core::error_code_t::invalid_parameter,
                           "table reference '" + std::string{uid} + "." + std::string{db} + "." + std::string{rel} +
                               "' names database '" + std::string{db} + "', but connection '" + std::string{uid} +
                               "' is connected to database '" + connection->database + "'");
        }
        return qualified_name_t(std::string{uid}, std::string{db}, connection->schema, std::string{rel});
    }

    core::result_wrapper_t<qualified_name_t> canonical_table_name(std::pmr::memory_resource* resource,
                                                                  const alias_registry_t& aliases,
                                                                  const qualified_name_t& written) {
        OTX_ZONE_N("names::canonical_table_name");
        if (written.collection.starts_with(otterstax::parser::k_stub_prefix)) {
            return written;
        }

        if (written.unique_identifier.empty()) {
            if (written.schema.empty()) {
                // `rel` or `db.rel` - local table, db should not collide with aliases.
                if (!written.database.empty() && aliases.find(written.database) != nullptr) {
                    return refusal(resource,
                                   core::error_code_t::invalid_parameter,
                                   "table reference '" + spelled(written) + "' names connection '" + written.database +
                                       "' without a database: write " + written.database + ".<database>." +
                                       written.collection);
                }
                return written;
            }
            // `a.b.c` is federated behind a registered alias. For now, local tables have no schema
            // TODO: remove when OttterBrix schema is supported
            if (aliases.find(written.database) == nullptr) {
                return refusal(resource,
                               core::error_code_t::invalid_parameter,
                               "table reference '" + spelled(written) + "' names schema '" + written.schema +
                                   "', but '" + written.database +
                                   "' is a local database and a local table has no schema: write " + written.database +
                                   "." + written.collection);
            }
            return federated_name(resource, aliases, written.database, written.schema, written.collection);
        }

        const auto* connection = aliases.find(written.unique_identifier);
        if (connection == nullptr) {
            return no_such_connection(resource, written.unique_identifier);
        }
        if (connection->backend != backend_type_t::PostgreSQL) {
            if (!written.schema.empty()) {
                return refusal(resource,
                               core::error_code_t::invalid_parameter,
                               "table reference '" + spelled(written) + "' names schema '" + written.schema +
                                   "', but connection '" + written.unique_identifier + "' (" +
                                   backend_name(connection->backend) + ") has no schema level: write " +
                                   written.unique_identifier + "." + written.database + "." + written.collection);
            }
        } else if (written.schema != connection->schema) {
            return refusal(resource,
                           core::error_code_t::unimplemented_yet,
                           "table reference '" + spelled(written) + "' names schema '" + written.schema +
                               "', but connection '" + written.unique_identifier + "' reaches schema '" +
                               connection->schema +
                               "': several schemas behind one connection are not supported, configure a "
                               "connection per schema");
        }
        return federated_name(resource, aliases, written.unique_identifier, written.database, written.collection);
    }
} // namespace otterstax::names
