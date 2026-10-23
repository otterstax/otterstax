// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "mysql_backend.hpp"

#include "utility/tracy_profiler.hpp"

#include <utility>

namespace db {

    template class backend_t<mysql_traits>;

    core::result_wrapper_t<components::vector::data_chunk_t>
    mysql_traits::slot_chunk(std::pmr::memory_resource* resource,
                             components::logical_plan::node_type,
                             conn::statement_result_t answer,
                             const std::string& uid) {
        if (!answer.rows) {
            return core::error_t(
                core::error_code_t::other_error,
                std::pmr::string{("MySQL backend '" + uid + "' returned no result chunk").c_str(), resource});
        }
        return std::move(*answer.rows);
    }

    namespace {

        // Read by the very table execute types its rows with (mysql_to_struct), so a mirrored
        // column's type is the executed column's type by construction; a type that table has
        // no arm for is the same conversion_failure execute answers for it.
        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_table_t>>
        probe_table(MySQLBackend& backend, qualified_name_t table) {
            auto probe = otterstax::catalog::make_schema_probe_query(backend.resource(), table, backend_type_t::MySQL);
            if (probe.has_error()) {
                co_return probe.convert_error<catalog_ext::discovered_table_t>();
            }
            backend.log()->debug("discover: generated MySQL probe: \"{}\"", probe.value());
            auto answer = co_await backend.run(table.unique_identifier,
                                                          conn::statement_t::schema(std::move(probe.value())));
            if (answer.has_error()) {
                co_return answer.convert_error<catalog_ext::discovered_table_t>();
            }
            auto schema = answer.value().schema ? std::move(*answer.value().schema)
                                                : components::types::complex_logical_type::create_struct("", {});
            backend.log()->info("discover: schema discovered for: {}", table.to_string());
            co_return catalog_ext::discovered_table_t{std::move(table), std::move(schema)};
        }

    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
    mysql_traits::discover(MySQLBackend& backend, qualified_name_t scope) {
        // Run by the catalog only, whose enqueue_impl drives it to completion on the sender's
        // thread, so the zone may span its co_awaits (CLAUDE.md, "Tracy zones in coroutines").
        OTX_ZONE_N("mysql::discover");
        const std::string uid = scope.unique_identifier;
        catalog_ext::discovered_tables_t out(backend.resource());

        if (!scope.collection.empty()) {
            auto table = co_await probe_table(backend, scope);
            if (table.has_error()) {
                co_return table.convert_error<catalog_ext::discovered_tables_t>();
            }
            out.push_back(std::move(table.value()));
            co_return std::move(out);
        }

        // Whole-database discovery via information_schema.
        if (scope.database.empty()) {
            backend.log()->error("discover: no MySQL database configured for uuid {}", uid);
            co_return backend.make_error(core::error_code_t::missing_field,
                                         "Cannot discover MySQL schema: no database configured for uuid: " + uid);
        }
        auto list = otterstax::catalog::make_list_tables_query(backend.resource(), backend_type_t::MySQL, scope.database);
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
                                         "Failed to read table names from MySQL");
            if (read.has_error()) {
                co_return read.convert_error<catalog_ext::discovered_tables_t>();
            }
            names = std::move(read.value());
        }
        backend.log()->info("discover: found {} tables in MySQL database {}", names.size(), scope.database);

        std::pmr::vector<std::pmr::string> failed(backend.resource());
        for (const auto& name : names) {
            auto probed = co_await probe_table(backend, qualified_name_t(uid, scope.database, "", name));
            if (probed.has_error()) {
                backend.log()->error("discover: failed to fetch schema for {}.{}: {}",
                                     scope.database,
                                     name,
                                     probed.error().what.c_str());
                failed.emplace_back((scope.database + "." + name).c_str());
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
