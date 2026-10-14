// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <charconv>
#include <optional>
#include <string_view>

using namespace db;

namespace {

    // pg_type.oid of bytea.
    constexpr Oid bytea_oid = 17;

    using tsl::make_error;

} // namespace

PostgressManager::PostgressManager(std::pmr::memory_resource* res,
                                   pg::ConnectorManager* connector_manager,
                                   statement_executor_t::pool_t* statements)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::PG_CONNECTION_MANAGER))
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
PostgressManager::~PostgressManager() { connector_manager_->shutdown(); }

std::optional<tsl::pg_enum_oid_map> PostgressManager::enums_of(const std::string& uid) const {
    std::shared_lock guard(enums_mutex_);
    auto it = enums_.find(uid);
    if (it == enums_.end()) {
        return std::nullopt;
    }
    return it->second;
}

core::result_wrapper_t<tsl::pg_enum_oid_map> PostgressManager::fetch_enum_types(const std::string& uuid) {
    OTX_ZONE_N("PostgressManager::fetch_enum_types");
    const std::string query =
        "SELECT t.oid, t.typname, e.enumlabel "
        "FROM pg_type t "
        "JOIN pg_enum e ON e.enumtypid = t.oid "
        "WHERE t.typtype = 'e' "
        "ORDER BY t.oid, e.enumsortorder;";

    // Filled on the io thread, read here after get(): the handler only sees
    // this frame, never the backend's cache.
    tsl::pg_enum_oid_map map;
    auto handler = [this, &map](PGresult* result) -> otterstax::asio_error_t {
        if (result == nullptr) {
            return make_error(resource(), core::error_code_t::io_error, "pg_enum query produced no result");
        }
        const int nrows = PQntuples(result);
        for (int i = 0; i < nrows; ++i) {
            const char* oid_str = PQgetvalue(result, i, 0);
            const char* typname = PQgetvalue(result, i, 1);
            const char* enumlabel = PQgetvalue(result, i, 2);
            if (oid_str == nullptr || typname == nullptr || enumlabel == nullptr) {
                return make_error(resource(),
                                  core::error_code_t::conversion_failure,
                                  "pg_enum row " + std::to_string(i) + " has a NULL oid, typname or label");
            }
            unsigned int oid = 0;
            const std::string_view oid_text{oid_str};
            auto [end, ec] = std::from_chars(oid_text.data(), oid_text.data() + oid_text.size(), oid);
            if (ec != std::errc{} || end != oid_text.data() + oid_text.size()) {
                return make_error(resource(),
                                  core::error_code_t::conversion_failure,
                                  "pg_enum row " + std::to_string(i) + " has a non-numeric oid '" + oid_str + "'");
            }
            auto& desc = map[oid];
            if (desc.typname.empty()) {
                desc.typname = typname;
            }
            desc.values.emplace_back(enumlabel);
        }
        return otterstax::asio_error_t{};
    };

    if (auto err = connector_manager_->executeQuery(uuid, query, handler).get(); err.contains_error()) {
        log_->error("fetch_enum_types: query failed for uuid={}: {}", uuid, err.what.c_str());
        return std::move(err);
    }
    log_->info("fetch_enum_types: discovered {} ENUM types for uuid={}", map.size(), uuid);
    return std::move(map);
}

core::result_wrapper_t<std::unordered_map<std::string, std::vector<std::string>>>
PostgressManager::fetch_primary_keys(const std::string& uuid, const std::string& schema, const std::string& table) {
    OTX_ZONE_N("PostgressManager::fetch_primary_keys");
    const auto query = otterstax::catalog::make_primary_keys_query(resource(), schema, table);
    // Filled on the io thread, read here after get().
    std::unordered_map<std::string, std::vector<std::string>> keys;
    auto handler = [this, &keys](PGresult* result) -> otterstax::asio_error_t {
        if (result == nullptr) {
            return make_error(resource(), core::error_code_t::io_error, "the primary key query produced no result");
        }
        const int rows = PQntuples(result);
        if (rows != 0 && PQnfields(result) != 2) {
            return make_error(resource(),
                              core::error_code_t::schema_error,
                              "the primary key query answered " + std::to_string(PQnfields(result)) +
                                  " columns instead of (relname, attname)");
        }
        for (int i = 0; i < rows; ++i) {
            // pg_class.relname and pg_attribute.attname are NOT NULL.
            assert(!PQgetisnull(result, i, 0) && !PQgetisnull(result, i, 1));
            keys[PQgetvalue(result, i, 0)].emplace_back(PQgetvalue(result, i, 1));
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, query, handler).get(); err.contains_error()) {
        log_->error("fetch_primary_keys: query failed for uuid={}: {}", uuid, err.what.c_str());
        return std::move(err);
    }
    return std::move(keys);
}

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
core::result_wrapper_t<otterstax::federation::discovered_tables_t> PostgressManager::discover(const qualified_name_t& scope) {
    OTX_ZONE_N("PostgressManager::discover");
    // One discovery at a time: each publishes the metadata it read (the cache below) only after its
    // queries, so two running at once could leave the older reading in place of the newer one.
    std::lock_guard guard(mutex_);
    const std::string& uuid = scope.unique_identifier.t;
    if (scope.schema.t.empty()) {
        log_->error("discover: no PostgreSQL schema configured for uuid {}", uuid);
        return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover PostgreSQL schema: no schema configured for uuid: " + uuid);
    }

    // The ENUM map is refreshed on every discovery of the uid and cached only
    // once the query succeeded; the probes below decode enum columns with it.
    auto enums = fetch_enum_types(uuid);
    if (enums.has_error()) {
        return core::error_on(resource(), enums.error());
    }
    const tsl::pg_enum_oid_map pg_enum_oids = std::move(enums.value());
    {
        std::unique_lock enums_guard(enums_mutex_);
        enums_.insert_or_assign(uuid, pg_enum_oids);
    }

    // The primary keys of the scope, one query: a row the engine picked is
    // changed on the backend by its key.
    auto fetched_keys = fetch_primary_keys(uuid, scope.schema.t, scope.collection.t);
    if (fetched_keys.has_error()) {
        return core::error_on(resource(), fetched_keys.error());
    }
    const auto primary_keys = std::move(fetched_keys.value());

    otterstax::federation::discovered_tables_t out(resource());
    auto probe = [this, &out, &uuid, &pg_enum_oids, &primary_keys](const qualified_name_t& table) -> core::error_t {
        const auto key = primary_keys.find(table.collection.t);
        auto schema_handler = [this,
                               table,
                               pg_enum_oids,
                               primary_key = key == primary_keys.end() ? std::vector<std::string>{} : key->second,
                               &out](PGresult* schema_result) -> otterstax::asio_error_t {
            auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
            // A key with a bytea column names no row the federation can write back
            // (the engine holds its raw bytes as a STRING_LITERAL, which no character
            // literal names; a NUL byte ends the query text), and goes whole: the table
            // then has no key. pg_to_chunk reads bytea by its type oid.
            std::vector<std::string> binary_columns;
            for (int column = 0; column < PQnfields(schema_result); ++column) {
                if (PQftype(schema_result, column) == bytea_oid) {
                    binary_columns.emplace_back(PQfname(schema_result, column));
                }
            }
            auto usable_key = primary_key;
            const bool inexact_key =
                std::any_of(usable_key.begin(), usable_key.end(), [&binary_columns](const std::string& column) {
                    return std::find(binary_columns.begin(), binary_columns.end(), column) != binary_columns.end();
                });
            if (inexact_key) {
                usable_key.clear();
                log_->info("discover: {}.{} has a bytea primary key column: its rows are not changed one by one",
                           table.schema.t,
                           table.collection.t);
            }
            out.push_back(otterstax::federation::discovered_table_t{table, std::move(schema_struct), std::move(usable_key)});
            log_->info("discover: schema discovered for: {}.{}", table.schema.t, table.collection.t);
            return otterstax::asio_error_t{};
        };
        auto probe_query =
            otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::PostgreSQL);
        if (probe_query.has_error()) {
            return core::error_on(resource(), probe_query.error());
        }
        log_->debug("discover: generated PostgreSQL probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.t.empty()) {
        if (auto err = probe(scope); err.contains_error()) {
            return std::move(err);
        }
        return std::move(out);
    }

    // Whole-schema discovery via information_schema.
    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::PostgreSQL, scope.schema.t);
    log_->debug("discover: querying information_schema: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [&table_names](PGresult* result) -> otterstax::asio_error_t {
        const int num_tables = PQntuples(result);
        for (int i = 0; i < num_tables; ++i) {
            table_names.emplace_back(PQgetvalue(result, i, 0));
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        return std::move(err);
    }
    log_->info("discover: found {} tables in schema {}", table_names.size(), scope.schema.t);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        qualified_name_t table = scope;
        table.collection = std::string{tn.c_str(), tn.size()};
        if (auto err = probe(table); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}",
                        table.schema.t,
                        table.collection.t,
                        err.what.c_str());
            failed_tables.emplace_back((table.schema.t + "." + table.collection.t).c_str());
        }
    }
    if (!failed_tables.empty()) {
        return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    return std::move(out);
}

core::error_t PostgressManager::execute_ddl(const std::string& uid, const std::string& sql) {
    OTX_ZONE_N("PostgressManager::execute_ddl");
    log_->debug("execute_ddl on {}: \"{}\"", uid, sql);
    auto handler = [](PGresult*) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
    return connector_manager_->executeQuery(uid, sql, handler).get();
}

actor_zeta::unique_future<core::error_t>
PostgressManager::scan(std::pmr::memory_resource* resource,
                       otterstax::federation::scan_request_t request,
                       std::shared_ptr<otterstax::federation::scan_rows_t> rows) {
    OTX_ZONE_N("PostgressManager::scan");
    actor_zeta::promise<core::error_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), rows = std::move(rows), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("PostgressManager::scan::run");
        promise.set_value(answer_of(
            [&]() -> core::error_t {
                // The ENUM labels the discovery of the uid read: pg_to_chunk decodes
                // an enum column by its type oid. A uid without them never went
                // through discovery.
                auto enums = enums_of(request.uid);
                if (!enums) {
                    return make_error(resource_,
                                      core::error_code_t::do_not_exists,
                                      "no PostgreSQL ENUM metadata for connection '" + request.uid +
                                          "': it was never discovered");
                }
                std::optional<core::error_t> conversion;
                auto handler = [this, &conversion, &enums](PGresult* result)
                    -> std::unique_ptr<components::vector::data_chunk_t> {
                    auto converted = tsl::pg_to_chunk(resource_, result, *enums);
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
                                                   "PostgreSQL backend '" + request.uid +
                                                       "' returned no result chunk");
                }
                rows->chunks.push_back(std::move(*outcome.value()));
                return core::error_t::no_error();
            },
            [this, &request](std::string_view what) {
                return make_error(resource_,
                                  core::error_code_t::io_error,
                                  "PostgreSQL backend '" + request.uid + "' read failed: " + std::string{what});
            }));
    });
    return future;
}

actor_zeta::unique_future<otterstax::federation::write_result_t>
PostgressManager::write(std::pmr::memory_resource* resource, otterstax::federation::write_request_t request) {
    OTX_ZONE_N("PostgressManager::write");
    actor_zeta::promise<otterstax::federation::write_result_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("PostgressManager::write::run");
        promise.set_value(answer_of(
            [&]() -> otterstax::federation::write_result_t {
                // The command tag's count (INSERT 0 n / UPDATE n / DELETE n).
                auto handler = [](PGresult* result) -> int64_t {
                    const char* tuples = result == nullptr ? nullptr : PQcmdTuples(result);
                    if (tuples == nullptr || *tuples == '\0') {
                        return 0;
                    }
                    int64_t count = 0;
                    const std::string_view text{tuples};
                    std::from_chars(text.data(), text.data() + text.size(), count);
                    return count;
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
                               "PostgreSQL backend '" + request.uid + "' write failed: " + std::string{what})};
            }));
    });
    return future;
}
