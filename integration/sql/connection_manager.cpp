// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <charconv>
#include <optional>

using namespace db;

namespace {

    using tsl::make_error;

} // namespace

MySQLManager::MySQLManager(std::pmr::memory_resource* res,
                           mysql::ConnectorManager* connector_manager,
                           statement_executor_t::pool_t* statements)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::SQL_CONNECTION_MANAGER))
    , statements_(statements) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    log_->info("MySQLManager initialized successfully");
    // The wrapped connector manager's io pool is started here, once: it is the
    // only place that knows the pool is about to receive queries.
    connector_manager_->start();
}

std::optional<std::uint64_t> db::mysql_update_rows_matched(std::string_view info) {
    OTX_ZONE_N("mysql_update_rows_matched");
    // The numbers of the message, in order. ER_UPDATE_INFO is a printf format with three %ld and
    // no positional arguments, so every translation keeps them in that order; no translation
    // spells a digit in its words.
    std::uint64_t numbers[3] = {0, 0, 0};
    std::size_t count = 0;
    const char* at = info.data();
    const char* last = info.data() + info.size();
    while (at != last) {
        if (*at < '0' || *at > '9') {
            ++at;
            continue;
        }
        std::uint64_t value = 0;
        auto [end, ec] = std::from_chars(at, last, value);
        if (ec != std::errc{} || count == 3) {
            return std::nullopt;
        }
        numbers[count++] = value;
        at = end;
    }
    if (count != 3) {
        return std::nullopt;
    }
    return numbers[0];
}

// The connector manager first refuses new statements and aborts the running ones
// (shutdown), so statements_ (the last member), which waits for every statement
// still queued, waits a bounded time: each finishes at once with an error, and
// fulfils its operator's promise, before the connector manager it uses can go.
MySQLManager::~MySQLManager() { connector_manager_->shutdown(); }

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
core::result_wrapper_t<otterstax::federation::discovered_tables_t> MySQLManager::discover(const qualified_name_t& scope) {
    OTX_ZONE_N("MySQLManager::discover");
    const std::string& uuid = scope.unique_identifier.t;
    otterstax::federation::discovered_tables_t out(resource());

    // The generated literals double a backslash, which a server under
    // NO_BACKSLASH_ESCAPES would read as two characters: such a connection is
    // refused rather than written to with the wrong text.
    {
        bool no_backslash_escapes = false;
        auto mode_handler = [&no_backslash_escapes](const boost::mysql::results& result) -> otterstax::asio_error_t {
            for (auto row : result.rows()) {
                if (row.size() != 0 && row.at(0).is_string()) {
                    no_backslash_escapes = row.at(0).as_string().find("NO_BACKSLASH_ESCAPES") != std::string_view::npos;
                }
            }
            return otterstax::asio_error_t{};
        };
        if (auto err = connector_manager_->executeQuery(uuid, "SELECT @@SESSION.sql_mode", mode_handler).get();
            err.contains_error()) {
            return std::move(err);
        }
        if (no_backslash_escapes) {
            return make_error(resource(),
                                 core::error_code_t::unimplemented_yet,
                                 "MySQL connection '" + uuid +
                                     "' runs with sql_mode NO_BACKSLASH_ESCAPES: OtterStax escapes a backslash in "
                                     "the string literals it sends");
        }
    }

    auto probe = [this, &out, &uuid](const qualified_name_t& table) -> core::error_t {
        auto schema_handler = [this, table, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
            // Read by the very table a read types its rows with, so a recorded
            // column's type is the read column's type by construction. A
            // column type that table has no arm for is the same
            // conversion_failure a read answers for it, naming the column —
            // never a column recorded without a type.
            auto schema_struct = tsl::mysql_to_struct(resource(), result);
            if (schema_struct.has_error()) {
                return make_error(resource(), schema_struct.error().type, schema_struct.error().what.c_str());
            }
            // The primary key: the probe's column definitions carry PRI_KEY_FLAG
            // on every column of it (table order; the row-change statement
            // compares every key column, so their order is immaterial). A key
            // with a column whose value the federation's row-change sink cannot
            // write back as a literal MySQL compares exactly with the stored one
            // names no row, and goes whole (a part of it could name several
            // rows): the table then has no key and its rows are not changed one
            // by one (a statement the pushdown rule sends whole is unaffected).
            // Such a column is a binary one (BINARY / VARBINARY / BLOB /
            // GEOMETRY: the engine holds its raw bytes as a STRING_LITERAL, which
            // no character literal names) or a DECIMAL the engine holds as its
            // text (wider than 38 digits: MySQL compares it with a string literal
            // as DOUBLE, so `k = '…'` could match another row or none).
            std::vector<std::string> primary_key;
            std::vector<std::string> inexact_columns;
            const auto& fields = schema_struct.value().child_types();
            std::size_t index = 0;
            for (const auto& column : result.meta()) {
                if (column.is_primary_key()) {
                    primary_key.emplace_back(column.column_name());
                }
                const auto type = column.type();
                const bool decimal_as_text = type == boost::mysql::column_type::decimal && index < fields.size() &&
                                             fields[index].type() != components::types::logical_type::DECIMAL;
                if (type == boost::mysql::column_type::binary || type == boost::mysql::column_type::varbinary ||
                    type == boost::mysql::column_type::blob || type == boost::mysql::column_type::geometry ||
                    decimal_as_text) {
                    inexact_columns.emplace_back(column.column_name());
                }
                ++index;
            }
            const bool inexact_key =
                std::any_of(primary_key.begin(), primary_key.end(), [&inexact_columns](const std::string& column) {
                    return std::find(inexact_columns.begin(), inexact_columns.end(), column) != inexact_columns.end();
                });
            if (inexact_key) {
                primary_key.clear();
                log_->info("discover: {}.{} has a binary or wide DECIMAL primary key column: its rows are not changed "
                           "one by one",
                           table.database.t,
                           table.collection.t);
            }
            out.push_back(
                otterstax::federation::discovered_table_t{table, std::move(schema_struct.value()), std::move(primary_key)});
            log_->info("discover: schema discovered for: {}.{}", table.database.t, table.collection.t);
            return otterstax::asio_error_t{};
        };
        auto probe_query = otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::MySQL);
        if (probe_query.has_error()) {
            return core::error_on(resource(), probe_query.error());
        }
        log_->debug("discover: generated MySQL probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.t.empty()) {
        if (auto err = probe(scope); err.contains_error()) {
            return std::move(err);
        }
        return std::move(out);
    }

    // Whole-database discovery via information_schema.
    if (scope.database.t.empty()) {
        log_->error("discover: no MySQL database configured for uuid {}", uuid);
        return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover MySQL schema: no database configured for uuid: " + uuid);
    }

    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::MySQL, scope.database.t);
    log_->debug("discover: querying information_schema: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [&table_names](const boost::mysql::results& result) -> otterstax::asio_error_t {
        for (auto row : result.rows()) {
            auto view = row.at(0).as_string();
            table_names.emplace_back(view.data(), view.size());
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        return std::move(err);
    }
    log_->info("discover: found {} tables in MySQL database {}", table_names.size(), scope.database.t);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        std::string table_name{tn.c_str(), tn.size()};
        qualified_name_t table(scope.unique_identifier, scope.database, core::schema_t{}, core::relname_t{table_name});
        if (auto err = probe(table); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}",
                        scope.database.t,
                        table_name,
                        err.what.c_str());
            failed_tables.emplace_back((scope.database.t + "." + table_name).c_str());
        }
    }
    if (!failed_tables.empty()) {
        return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    return std::move(out);
}

core::error_t MySQLManager::execute_ddl(const std::string& uid, const std::string& sql) {
    OTX_ZONE_N("MySQLManager::execute_ddl");
    log_->debug("execute_ddl on {}: \"{}\"", uid, sql);
    auto handler = [](const boost::mysql::results&) -> otterstax::asio_error_t { return otterstax::asio_error_t{}; };
    return connector_manager_->executeQuery(uid, sql, handler).get();
}

actor_zeta::unique_future<core::error_t>
MySQLManager::scan(std::pmr::memory_resource* resource,
                   otterstax::federation::scan_request_t request,
                   std::shared_ptr<otterstax::federation::scan_rows_t> rows) {
    OTX_ZONE_N("MySQLManager::scan");
    actor_zeta::promise<core::error_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), rows = std::move(rows), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("MySQLManager::scan::run");
        promise.set_value(answer_of(
            [&]() -> core::error_t {
                std::optional<core::error_t> conversion;
                auto handler = [this, &conversion](const boost::mysql::results& result)
                    -> std::unique_ptr<components::vector::data_chunk_t> {
                    auto converted = tsl::mysql_to_chunk(resource_, result);
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
                                                   "MySQL backend '" + request.uid + "' returned no result chunk");
                }
                rows->chunks.push_back(std::move(*outcome.value()));
                return core::error_t::no_error();
            },
            [this, &request](std::string_view what) {
                return make_error(resource_,
                                  core::error_code_t::io_error,
                                  "MySQL backend '" + request.uid + "' read failed: " + std::string{what});
            }));
    });
    return future;
}

actor_zeta::unique_future<otterstax::federation::write_result_t>
MySQLManager::write(std::pmr::memory_resource* resource, otterstax::federation::write_request_t request) {
    OTX_ZONE_N("MySQLManager::write");
    actor_zeta::promise<otterstax::federation::write_result_t> promise(resource);
    auto future = promise.get_future();
    statements_.post([this, request = std::move(request), promise = std::move(promise)]() mutable {
        OTX_ZONE_N("MySQLManager::write::run");
        promise.set_value(answer_of(
            [&]() -> otterstax::federation::write_result_t {
                // An UPDATE answers the rows it matched (mysql_update_rows_matched), an INSERT /
                // DELETE its affected rows.
                const bool update = request.kind == otterstax::federation::write_kind_t::update;
                bool info_missing = false;
                auto handler = [update, &info_missing](const boost::mysql::results& result) -> int64_t {
                    if (!update) {
                        return static_cast<int64_t>(result.affected_rows());
                    }
                    const auto matched = mysql_update_rows_matched(result.info());
                    if (!matched) {
                        info_missing = true;
                        return 0;
                    }
                    return static_cast<int64_t>(*matched);
                };
                log_->debug("write on {}: \"{}\"", request.uid, request.sql);
                auto outcome = connector_manager_->executeQuery(request.uid, request.sql, handler).get();
                if (outcome.has_error()) {
                    return otterstax::federation::write_result_t{core::error_on(resource_, outcome.error())};
                }
                if (info_missing) {
                    return otterstax::federation::write_result_t{
                        make_error(resource_,
                                   core::error_code_t::conversion_failure,
                                   "MySQL backend '" + request.uid + "' answered an UPDATE without its matched rows")};
                }
                return otterstax::federation::write_result_t{static_cast<std::uint64_t>(outcome.value())};
            },
            [this, &request](std::string_view what) {
                return otterstax::federation::write_result_t{
                    make_error(resource_,
                               core::error_code_t::io_error,
                               "MySQL backend '" + request.uid + "' write failed: " + std::string{what})};
            }));
    });
    return future;
}
