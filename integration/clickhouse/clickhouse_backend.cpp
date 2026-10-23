// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "clickhouse_backend.hpp"

#include "otterbrix/translators/input/affected_rows_carrier.hpp"
#include "utility/tracy_profiler.hpp"

#include <memory>
#include <mutex>
#include <utility>

namespace db {

    template class backend_t<clickhouse_traits>;

    void ch_metadata_t::set(const std::string& uid, const std::string& table, conn::named_types_ptr types) {
        std::unique_lock guard(mutex_);
        tables_[uid].insert_or_assign(table, std::move(types));
    }

    conn::named_types_ptr ch_metadata_t::find(const std::string& uid, const std::string& table) const {
        std::shared_lock guard(mutex_);
        auto u = tables_.find(uid);
        if (u == tables_.end()) {
            return nullptr;
        }
        auto t = u->second.find(table);
        return t == u->second.end() ? nullptr : t->second;
    }

    conn::statement_t clickhouse_traits::slot_statement(components::logical_plan::node_type slot,
                                                        std::string sql,
                                                        const metadata_t& metadata,
                                                        const std::string& uid,
                                                        const std::string& table) {
        using components::logical_plan::node_type;
        switch (slot) {
            case node_type::insert_t:
            case node_type::update_t:
            case node_type::delete_t:
                return conn::statement_t::affected_rows(std::move(sql));
            default:
                return conn::statement_t::rows(std::move(sql), metadata.find(uid, table));
        }
    }

    core::result_wrapper_t<components::vector::data_chunk_t>
    clickhouse_traits::slot_chunk(std::pmr::memory_resource* resource,
                                  components::logical_plan::node_type slot,
                                  conn::statement_result_t answer,
                                  const std::string& uid) {
        using components::logical_plan::node_type;
        switch (slot) {
            case node_type::insert_t:
                return tsl::make_affected_rows_carrier(resource, answer.affected_rows);
            case node_type::update_t:
            case node_type::delete_t:
                return tsl::make_affected_rows_carrier(resource, 0);
            default:
                if (!answer.rows) {
                    return core::error_t(
                        core::error_code_t::other_error,
                        std::pmr::string{("ClickHouse backend '" + uid + "' returned no result chunk").c_str(), resource});
                }
                return std::move(*answer.rows);
        }
    }

    conn::statement_t clickhouse_traits::probe_statement(std::string probe,
                                                         const metadata_t& metadata,
                                                         const std::string& uid,
                                                         const std::string& table) {
        return conn::statement_t::schema(std::move(probe), metadata.find(uid, table));
    }

    namespace {

        // The system.columns answer for one table, decoded: column → type string.
        actor_zeta::unique_future<core::result_wrapper_t<conn::named_types_ptr>>
        fetch_named_types(ClickhouseBackend& backend, std::string uid, std::string database, std::string table) {
            auto query = otterstax::catalog::make_named_types_query(backend.resource(), database, table);
            auto answer = co_await backend.run(uid, conn::statement_t::rows(std::string(std::string_view(query))));
            if (answer.has_error()) {
                backend.log()->error("fetch_named_types failed for {}.{}: {}", database, table, answer.error().what.c_str());
                co_return answer.convert_error<conn::named_types_ptr>();
            }
            auto fresh = std::make_shared<conn::named_types_t>();
            if (answer.value().rows) {
                const auto& columns = *answer.value().rows;
                const std::string what = "system.columns of " + database + "." + table;
                auto names = read_text_column(backend.resource(), columns, 0, core::error_code_t::schema_error, what);
                if (names.has_error()) {
                    co_return names.convert_error<conn::named_types_ptr>();
                }
                auto types = read_text_column(backend.resource(), columns, 1, core::error_code_t::schema_error, what);
                if (types.has_error()) {
                    co_return types.convert_error<conn::named_types_ptr>();
                }
                for (std::size_t i = 0; i < names.value().size(); ++i) {
                    fresh->insert_or_assign(std::move(names.value()[i]), std::move(types.value()[i]));
                }
            }
            backend.log()->debug("fetch_named_types: {} column type(s) for {}.{}.{}", fresh->size(), uid, database, table);
            co_return conn::named_types_ptr{std::move(fresh)};
        }

        // Named types first, then the probe, which answers the header block (columns, no
        // rows); an answer without a block that has columns did not describe the table, and
        // registering a column-less table would hide that until the first query against it.
        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_table_t>>
        probe_table(ClickhouseBackend& backend, std::string uid, std::string database, std::string table) {
            auto named = co_await fetch_named_types(backend, uid, database, table);
            if (named.has_error()) {
                co_return named.convert_error<catalog_ext::discovered_table_t>();
            }
            backend.metadata().set(uid, table, named.value());

            qualified_name_t name(uid, database, "", table);
            auto probe = otterstax::catalog::make_schema_probe_query(backend.resource(), name, backend_type_t::ClickHouse);
            if (probe.has_error()) {
                co_return probe.convert_error<catalog_ext::discovered_table_t>();
            }
            auto answer = co_await backend.run(uid, conn::statement_t::schema(std::move(probe.value()), named.value()));
            if (answer.has_error()) {
                co_return answer.convert_error<catalog_ext::discovered_table_t>();
            }
            if (!answer.value().schema) {
                co_return backend.make_error(core::error_code_t::schema_error,
                                             "ClickHouse schema probe returned no columns for " + database + "." + table);
            }
            backend.log()->info("discover: schema discovered for: {}.{}", database, table);
            co_return catalog_ext::discovered_table_t{std::move(name), std::move(*answer.value().schema)};
        }

    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
    clickhouse_traits::discover(ClickhouseBackend& backend, qualified_name_t scope) {
        // Run by the catalog only, whose enqueue_impl drives it to completion on the sender's
        // thread, so the zone may span its co_awaits (CLAUDE.md, "Tracy zones in coroutines").
        OTX_ZONE_N("ch::discover");
        const std::string uid = scope.unique_identifier;
        const std::string database = scope.database;
        if (database.empty()) {
            backend.log()->error("discover: no ClickHouse database configured for uuid {}", uid);
            co_return backend.make_error(core::error_code_t::missing_field,
                                         "Cannot discover ClickHouse schema: no database configured for uuid: " + uid);
        }

        catalog_ext::discovered_tables_t out(backend.resource());
        if (!scope.collection.empty()) {
            auto table = co_await probe_table(backend, uid, database, scope.collection);
            if (table.has_error()) {
                co_return table.convert_error<catalog_ext::discovered_tables_t>();
            }
            out.push_back(std::move(table.value()));
            co_return std::move(out);
        }

        // Whole-database discovery via system.tables.
        auto list = otterstax::catalog::make_list_tables_query(backend.resource(), backend_type_t::ClickHouse, database);
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
                                         "Failed to read table names from ClickHouse");
            if (read.has_error()) {
                co_return read.convert_error<catalog_ext::discovered_tables_t>();
            }
            names = std::move(read.value());
        }
        backend.log()->info("discover: found {} tables in ClickHouse database {}", names.size(), database);

        // One table at a time: discovery runs once, at startup.
        std::pmr::vector<std::pmr::string> failed(backend.resource());
        for (const auto& table : names) {
            auto probed = co_await probe_table(backend, uid, database, table);
            if (probed.has_error()) {
                backend.log()->error("discover: failed to fetch schema for {}.{}: {}",
                                     database,
                                     table,
                                     probed.error().what.c_str());
                failed.emplace_back((database + "." + table).c_str());
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
