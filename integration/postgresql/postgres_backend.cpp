// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "postgres_backend.hpp"

#include "utility/tracy_profiler.hpp"

#include <memory>
#include <mutex>
#include <utility>

namespace db {

    template class backend_t<postgres_traits>;

    void pg_metadata_t::set(const std::string& uid, conn::pg_enums_ptr enums) {
        std::unique_lock guard(mutex_);
        enums_.insert_or_assign(uid, std::move(enums));
    }

    conn::pg_enums_ptr pg_metadata_t::find(const std::string& uid) const {
        std::shared_lock guard(mutex_);
        auto it = enums_.find(uid);
        return it == enums_.end() ? nullptr : it->second;
    }

    core::error_t
    postgres_traits::check(const metadata_t& metadata, const std::string& uid, std::pmr::memory_resource* resource) {
        if (metadata.find(uid) == nullptr) {
            return core::error_t(
                core::error_code_t::do_not_exists,
                std::pmr::string{("no ENUM metadata discovered for PostgreSQL connection '" + uid + "'").c_str(),
                                 resource});
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<components::vector::data_chunk_t>
    postgres_traits::slot_chunk(std::pmr::memory_resource* resource,
                                components::logical_plan::node_type,
                                conn::statement_result_t answer,
                                const std::string& uid) {
        if (!answer.rows) {
            return core::error_t(
                core::error_code_t::other_error,
                std::pmr::string{("PostgreSQL backend '" + uid + "' returned no result chunk").c_str(), resource});
        }
        return std::move(*answer.rows);
    }

    namespace {

        // The ENUM types of the connection's database. The oid is cast to bigint so it decodes
        // as BIGINT whatever its value; the names as text.
        actor_zeta::unique_future<core::result_wrapper_t<conn::pg_enums_ptr>>
        fetch_enum_types(PostgresBackend& backend, std::string uid) {
            auto answer = co_await backend.run(
                uid,
                conn::statement_t::rows("SELECT t.oid::bigint, t.typname::text, e.enumlabel::text "
                                        "FROM pg_type t "
                                        "JOIN pg_enum e ON e.enumtypid = t.oid "
                                        "WHERE t.typtype = 'e' "
                                        "ORDER BY t.oid, e.enumsortorder;"));
            if (answer.has_error()) {
                backend.log()->error("fetch_enum_types: query failed for uuid={}: {}", uid, answer.error().what.c_str());
                co_return answer.convert_error<conn::pg_enums_ptr>();
            }
            auto map = std::make_shared<tsl::pg_enum_oid_map>();
            if (answer.value().rows && answer.value().rows->size() > 0) {
                const auto& rows = *answer.value().rows;
                const auto types = rows.types();
                if (rows.column_count() < 3 || types[0].type() != components::types::logical_type::BIGINT) {
                    co_return backend.make_error(core::error_code_t::conversion_failure,
                                                 "pg_enum answered without a bigint oid column");
                }
                auto names = read_text_column(backend.resource(), rows, 1, core::error_code_t::conversion_failure, "pg_enum typname");
                if (names.has_error()) {
                    co_return names.convert_error<conn::pg_enums_ptr>();
                }
                auto labels = read_text_column(backend.resource(), rows, 2, core::error_code_t::conversion_failure, "pg_enum label");
                if (labels.has_error()) {
                    co_return labels.convert_error<conn::pg_enums_ptr>();
                }
                for (uint64_t i = 0; i < rows.size(); ++i) {
                    auto oid = rows.value(0, i);
                    if (oid.is_null()) {
                        co_return backend.make_error(core::error_code_t::conversion_failure,
                                                     "pg_enum row " + std::to_string(i) + " has a NULL oid");
                    }
                    auto& desc = (*map)[static_cast<unsigned int>(oid.value<int64_t>())];
                    if (desc.typname.empty()) {
                        desc.typname = names.value()[i];
                    }
                    desc.values.push_back(std::move(labels.value()[i]));
                }
            }
            backend.log()->info("fetch_enum_types: discovered {} ENUM types for uuid={}", map->size(), uid);
            co_return conn::pg_enums_ptr{std::move(map)};
        }

        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_table_t>>
        probe_table(PostgresBackend& backend, qualified_name_t table, conn::pg_enums_ptr enums) {
            auto probe =
                otterstax::catalog::make_schema_probe_query(backend.resource(), table, backend_type_t::PostgreSQL);
            if (probe.has_error()) {
                co_return probe.convert_error<catalog_ext::discovered_table_t>();
            }
            backend.log()->debug("discover: generated PostgreSQL probe: \"{}\"", probe.value());
            auto answer = co_await backend.run(table.unique_identifier,
                                                          conn::statement_t::schema(std::move(probe.value()), enums));
            if (answer.has_error()) {
                co_return answer.convert_error<catalog_ext::discovered_table_t>();
            }
            // A table may have no columns at all; it is mirrored as the empty STRUCT.
            auto schema = answer.value().schema ? std::move(*answer.value().schema)
                                                : components::types::complex_logical_type::create_struct("", {});
            backend.log()->info("discover: schema discovered for: {}.{}", table.schema, table.collection);
            co_return catalog_ext::discovered_table_t{std::move(table), std::move(schema)};
        }

    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
    postgres_traits::discover(PostgresBackend& backend, qualified_name_t scope) {
        // Run by the catalog only, whose enqueue_impl drives it to completion on the sender's
        // thread, so the zone may span its co_awaits (CLAUDE.md, "Tracy zones in coroutines").
        OTX_ZONE_N("pg::discover");
        const std::string uid = scope.unique_identifier;
        if (scope.schema.empty()) {
            backend.log()->error("discover: no PostgreSQL schema configured for uuid {}", uid);
            co_return backend.make_error(core::error_code_t::missing_field,
                                         "Cannot discover PostgreSQL schema: no schema configured for uuid: " + uid);
        }

        // Refreshed on every discovery of the uid and kept only once the query succeeded;
        // the probes below decode enum columns with it.
        auto enums = co_await fetch_enum_types(backend, uid);
        if (enums.has_error()) {
            co_return enums.convert_error<catalog_ext::discovered_tables_t>();
        }
        backend.metadata().set(uid, enums.value());

        catalog_ext::discovered_tables_t out(backend.resource());
        if (!scope.collection.empty()) {
            auto table = co_await probe_table(backend, scope, enums.value());
            if (table.has_error()) {
                co_return table.convert_error<catalog_ext::discovered_tables_t>();
            }
            out.push_back(std::move(table.value()));
            co_return std::move(out);
        }

        // Whole-schema discovery via information_schema.
        auto list = otterstax::catalog::make_list_tables_query(backend.resource(), backend_type_t::PostgreSQL, scope.schema);
        auto listed = co_await backend.run(uid, conn::statement_t::rows(std::string(std::string_view(list))));
        if (listed.has_error()) {
            co_return listed.convert_error<catalog_ext::discovered_tables_t>();
        }
        std::vector<std::string> names;
        if (listed.value().rows) {
            auto read = read_text_column(backend.resource(),
                                         *listed.value().rows,
                                         0,
                                         core::error_code_t::missing_field,
                                         "Failed to read table names from PostgreSQL");
            if (read.has_error()) {
                co_return read.convert_error<catalog_ext::discovered_tables_t>();
            }
            names = std::move(read.value());
        }
        backend.log()->info("discover: found {} tables in schema {}", names.size(), scope.schema);

        std::pmr::vector<std::pmr::string> failed(backend.resource());
        for (const auto& name : names) {
            qualified_name_t table = scope;
            table.collection = name;
            auto probed = co_await probe_table(backend, table, enums.value());
            if (probed.has_error()) {
                backend.log()->error("discover: failed to fetch schema for {}.{}: {}",
                                     scope.schema,
                                     name,
                                     probed.error().what.c_str());
                failed.emplace_back((scope.schema + "." + name).c_str());
                continue;
            }
            out.push_back(std::move(probed.value()));
        }
        if (!failed.empty()) {
            co_return otterstax::catalog::make_discovery_error(backend.resource(), failed);
        }
        co_return std::move(out);
    }

} // namespace db
