// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <clickhouse/columns/string.h>

#include <optional>

using namespace db;

namespace {

    using tsl::make_error;

} // namespace

ClickhouseManager::ClickhouseManager(std::pmr::memory_resource* res,
                                     ch::ConnectorManager* connector_manager,
                                     statement_executor_t::pool_t* statements)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::CH_CONNECTION_MANAGER))
    , statements_(statements) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    // The wrapped connector manager's io pool is started here, once: it is the
    // only place that knows the pool is about to receive queries.
    connector_manager_->start();
}

// The connector manager first refuses new statements and aborts the running ones
// (shutdown), so statements_ (the last data member), which waits for the
// statements still queued, waits a bounded time.
ClickhouseManager::~ClickhouseManager() { connector_manager_->shutdown(); }

std::vector<std::optional<std::string>>
ClickhouseManager::declared_types_for(const otterstax::federation::scan_request_t& request) const {
    std::vector<std::optional<std::string>> declared(request.columns.size());
    std::shared_lock guard(named_types_mutex_);
    auto u_it = named_types_.find(request.uid);
    if (u_it == named_types_.end()) {
        return declared;
    }
    for (std::size_t i = 0; i < request.columns.size(); ++i) {
        const auto& origin = request.columns[i];
        if (!origin) {
            continue;
        }
        auto d_it = u_it->second.find(origin->table.database);
        if (d_it == u_it->second.end()) {
            continue;
        }
        auto t_it = d_it->second.find(origin->table.table);
        if (t_it == d_it->second.end()) {
            continue;
        }
        if (auto c_it = t_it->second.find(origin->column); c_it != t_it->second.end()) {
            declared[i] = c_it->second;
        }
    }
    return declared;
}

core::result_wrapper_t<ClickhouseManager::named_types_t>
ClickhouseManager::fetch_named_types(const std::string& uuid, const std::string& database, const std::string& table) {
    OTX_ZONE_N("ClickhouseManager::fetch_named_types");
    auto query = otterstax::catalog::make_named_types_query(resource(), database, table);

    // Filled on the io thread, read here after get(): the handler only sees
    // this frame, never the backend's cache. A block without rows describes no
    // column; a block with rows that is not the (name, type) pair
    // system.columns answers with is a malformed reply, not an empty one.
    named_types_t fresh;
    auto handler = [this, &fresh](const ch::select_result_t& columns) -> otterstax::asio_error_t {
        for (const auto& block : columns.blocks) {
            if (block.GetRowCount() == 0) {
                continue;
            }
            if (block.GetColumnCount() < 2) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "system.columns answered with " + std::to_string(block.GetColumnCount()) +
                                      " column(s) instead of (name, type)");
            }
            auto name_col = block[0]->As<clickhouse::ColumnString>();
            auto type_col = block[1]->As<clickhouse::ColumnString>();
            if (!name_col || !type_col) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "system.columns answered with non-string name/type columns");
            }
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                fresh.insert_or_assign(std::string(name_col->At(i)), std::string(type_col->At(i)));
            }
        }
        return otterstax::asio_error_t{};
    };

    if (auto err = connector_manager_->executeQuery(uuid, query, handler).get(); err.contains_error()) {
        log_->error("fetch_named_types failed for {}.{}: {}", database, table, err.what.c_str());
        return std::move(err);
    }
    log_->debug("fetch_named_types: {} column type(s) for {}.{}.{}", fresh.size(), uuid, database, table);
    return std::move(fresh);
}

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
core::result_wrapper_t<otterstax::federation::discovered_tables_t> ClickhouseManager::discover(const qualified_name_t& scope) {
    OTX_ZONE_N("ClickhouseManager::discover");
    // One discovery at a time: each publishes the metadata it read (the cache below) only after its
    // queries, so two running at once could leave the older reading in place of the newer one.
    std::lock_guard guard(mutex_);
    const std::string& uuid = scope.unique_identifier.t;
    if (scope.database.t.empty()) {
        log_->error("discover: no ClickHouse database configured for uuid {}", uuid);
        return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover ClickHouse schema: no database configured for uuid: " + uuid);
    }
    const std::string& database = scope.database.t;

    otterstax::federation::discovered_tables_t out(resource());
    // Named types first, then the `WHERE 1 = 0` probe, which answers with the
    // header block (columns, no rows); no block or a block without columns
    // means the probe did not describe the table, and registering a
    // column-less table would hide that until the first query against it.
    auto probe = [this, &out, &uuid, &database](const std::string& table_name) -> core::error_t {
        auto fetched = fetch_named_types(uuid, database, table_name);
        if (fetched.has_error()) {
            return core::error_on(resource(), fetched.error());
        }
        named_types_t overrides = std::move(fetched.value());
        {
            std::unique_lock named_types_guard(named_types_mutex_);
            named_types_[uuid][database].insert_or_assign(table_name, overrides);
        }

        qualified_name_t table(core::uid_t{uuid}, core::dbname_t{database}, core::schema_t{}, core::relname_t{table_name});
        auto schema_handler = [this, table, overrides, &out](const ch::select_result_t& probe_result)
            -> otterstax::asio_error_t {
            const auto& schema_blocks = probe_result.blocks;
            if (schema_blocks.empty() || schema_blocks[0].GetColumnCount() == 0) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "ClickHouse schema probe returned no columns for " + table.database.t + "." +
                                      table.collection.t);
            }
            auto schema_struct = tsl::ch_to_struct(resource(), schema_blocks[0], overrides);
            out.push_back(otterstax::federation::discovered_table_t{table, std::move(schema_struct)});
            log_->info("discover: schema discovered for: {}.{}", table.database.t, table.collection.t);
            return otterstax::asio_error_t{};
        };
        auto probe_query = otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::ClickHouse);
        if (probe_query.has_error()) {
            return core::error_on(resource(), probe_query.error());
        }
        log_->debug("discover: generated ClickHouse probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.t.empty()) {
        if (auto err = probe(scope.collection.t); err.contains_error()) {
            return std::move(err);
        }
        return std::move(out);
    }

    // Whole-database discovery via system.tables.
    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::ClickHouse, database);
    log_->debug("discover: querying ClickHouse tables: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [this, &table_names](const ch::select_result_t& listed) -> otterstax::asio_error_t {
        for (const auto& block : listed.blocks) {
            if (block.GetRowCount() == 0) {
                continue;
            }
            auto name_col = block[0]->As<clickhouse::ColumnString>();
            if (!name_col) {
                return make_error(resource(),
                                  core::error_code_t::missing_field,
                                  "Failed to read table names from ClickHouse");
            }
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                auto view = name_col->At(i);
                table_names.emplace_back(view.data(), view.size());
            }
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        return std::move(err);
    }
    log_->info("discover: found {} tables in ClickHouse database {}", table_names.size(), database);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        std::string table_name{tn.c_str(), tn.size()};
        if (auto err = probe(table_name); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}", database, table_name, err.what.c_str());
            failed_tables.emplace_back((database + "." + table_name).c_str());
        }
    }
    if (!failed_tables.empty()) {
        return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    return std::move(out);
}

core::error_t ClickhouseManager::execute_ddl(const std::string& uid, const std::string& sql) {
    OTX_ZONE_N("ClickhouseManager::execute_ddl");
    log_->debug("execute_ddl on {}: \"{}\"", uid, sql);
    auto handler = [](const ch::select_result_t&) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
    return connector_manager_->executeQuery(uid, sql, handler).get();
}

actor_zeta::unique_future<core::error_t>
ClickhouseManager::scan(std::pmr::memory_resource* resource,
                        otterstax::federation::scan_request_t request,
                        std::shared_ptr<otterstax::federation::scan_rows_t> rows) {
    OTX_ZONE_N("ClickhouseManager::scan");
    actor_zeta::promise<core::error_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), rows = std::move(rows), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("ClickhouseManager::scan::run");
        promise.set_value(answer_of(
            [&]() -> core::error_t {
                // Per result column, the system.columns type of the table column it reads
                // as it is (of whichever table of the statement): the declared type
                // ch_to_chunk reads a Tuple / Bool / LowCardinality column as.
                const auto overrides = declared_types_for(request);
                std::optional<core::error_t> conversion;
                auto handler = [this, &conversion, &overrides](const ch::select_result_t& result)
                    -> std::unique_ptr<components::vector::data_chunk_t> {
                    auto converted = tsl::ch_to_chunk(resource_, result.blocks, overrides);
                    if (converted.has_error()) {
                        conversion.emplace(core::error_on(resource_, converted.error()));
                        return nullptr;
                    }
                    return std::make_unique<components::vector::data_chunk_t>(std::move(converted.value()));
                };
                log_->debug("scan on {}: \"{}\"", request.uid, request.sql);
                auto outcome = connector_manager_->executeQuery(request.uid, request.sql, handler).get();
                if (outcome.has_error()) {
                    return core::error_on(resource_, outcome.error());
                }
                if (!outcome.value()) {
                    return conversion ? std::move(*conversion)
                                      : make_error(resource_,
                                                   core::error_code_t::other_error,
                                                   "ClickHouse backend '" + request.uid +
                                                       "' returned no result chunk");
                }
                rows->chunks.push_back(std::move(*outcome.value()));
                return core::error_t::no_error();
            },
            [this, &request](std::string_view what) {
                return make_error(resource_,
                                  core::error_code_t::io_error,
                                  "ClickHouse backend '" + request.uid + "' read failed: " + std::string{what});
            }));
    });
    return future;
}

actor_zeta::unique_future<otterstax::federation::write_result_t>
ClickhouseManager::write(std::pmr::memory_resource* resource, otterstax::federation::write_request_t request) {
    OTX_ZONE_N("ClickhouseManager::write");
    actor_zeta::promise<otterstax::federation::write_result_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("ClickhouseManager::write::run");
        promise.set_value(answer_of(
            [&]() -> otterstax::federation::write_result_t {
                // No ClickHouse block carries an affected-row count: written_rows (the sum of the
                // Progress packets) is an INSERT's count, and a lightweight DELETE / ALTER TABLE ...
                // UPDATE is a mutation that reports nothing — 0, whatever the server wrote.
                const bool insert = request.kind == otterstax::federation::write_kind_t::insert;
                auto handler = [insert](const ch::select_result_t& result) -> int64_t {
                    return insert ? static_cast<int64_t>(result.written_rows) : int64_t{0};
                };
                log_->debug("write on {}: \"{}\"", request.uid, request.sql);
                auto outcome = connector_manager_->executeQuery(request.uid, request.sql, handler).get();
                if (outcome.has_error()) {
                    return otterstax::federation::write_result_t{core::error_on(resource_, outcome.error())};
                }
                return otterstax::federation::write_result_t{static_cast<std::uint64_t>(outcome.value())};
            },
            [this, &request](std::string_view what) {
                return otterstax::federation::write_result_t{
                    make_error(resource_,
                               core::error_code_t::io_error,
                               "ClickHouse backend '" + request.uid + "' write failed: " + std::string{what})};
            }));
    });
    return future;
}
