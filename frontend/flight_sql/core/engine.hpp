// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The engine contract for Flight SQL. The only boundary between the protocol
// and the backend (the Scheduler adapter, a demo engine, ...). Changes must
// stay backward compatible.
//
// The layer is deliberately self-contained (std only): it must not see engine
// types, so it carries its own data shapes where the project already has
// engine-side twins — core::TableInfo vs utility/table_info.hpp (an adapter
// maps between them), core::like_match vs the catalog's matcher. Do not
// include engine headers from core/ or ipc/.

#include <ipc/array.hpp>
#include <ipc/ipc_reader.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace flight::core {

// Execution error: maps to INVALID_ARGUMENT + message text.
struct EngineError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct QueryResult {
    ipc::SchemaPtr schema;
    std::vector<ipc::RecordBatch> batches;
};

struct TableInfo {
    std::string catalog;   // may be empty (NULL in the result)
    std::string db_schema; // may be empty (NULL in the result)
    std::string name;
    std::string type = "TABLE";
    ipc::SchemaPtr schema;
};

struct EngineMetadata {
    std::vector<std::string> catalogs;
    std::vector<std::string> db_schemas; // for CommandGetDbSchemas (simplified: not bound to the catalog filter)
    std::vector<TableInfo> tables;
    std::vector<std::string> table_types;
};

// A prepared query: text + the result and parameter schemas.
struct Prepared {
    std::string query;
    ipc::SchemaPtr dataset_schema;   // empty for update
    ipc::SchemaPtr parameter_schema; // one field per '?'

    // Opaque engine-side resource handle (e.g. the Worker's prepared-statement
    // session). The engine fills it in prepare(); the protocol layer never
    // interprets it, it only carries it back to the engine. Empty when the
    // engine holds no per-handle resource.
    std::string engine_handle;
};

// Bound parameters: rows of values (a row = a vector with one entry per parameter).
using BoundParams = std::vector<std::vector<ipc::Value>>;

class IEngine {
  public:
    virtual ~IEngine() = default;

    // SELECT-like queries: text -> arrow result.
    virtual QueryResult execute(const std::string& query) = 0;

    // UPDATE/INSERT/DDL by text; returns the number of affected rows.
    virtual std::int64_t execute_update(const std::string& query) = 0;

    // Prepared statements
    virtual Prepared prepare(const std::string& query) = 0;
    virtual QueryResult execute_prepared(const Prepared& prepared, const BoundParams& params) = 0;
    virtual std::int64_t execute_update_prepared(const Prepared& prepared,
                                                 const BoundParams& params) = 0;

    // Release the engine resource behind a prepared statement (one that will
    // never be executed). FlightSqlCore calls it on ClosePreparedStatement;
    // the default is a no-op.
    virtual void close_prepared(const Prepared& /*prepared*/) {}

    virtual EngineMetadata metadata() = 0;

    // Human-readable dialect name (for SqlInfo).
    virtual std::string dialect_name() const = 0;
};

// SQL-LIKE matcher (% — any sequence, _ — one character).
// No escaping; a ""/NULL pattern does not filter.
[[nodiscard]] bool like_match(std::string_view text, std::string_view pattern);

} // namespace flight::core
