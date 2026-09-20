// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL core: the protocol/engine exchange shapes, the ticket manager
// and the FlightInfo/FlightData assembly. There is no engine interface on
// purpose — the server has exactly one engine, and FlightSqlCore owns it
// (a flight::engine::SchedulerEngine, held through a forward declaration).

#include "auth.hpp"

#include "utility/table_info.hpp"

// The engine's postgres-derived headers #define ERROR (a pg error code,
// pg_type_definitions.h), which collides with FlightSql's generated
// SetSessionOptionsResult.ErrorValue enum member — the same clash class as
// the parser's DAY/SECOND the catalog already undefs for arrow. The guard is
// prepended to the GENERATED headers themselves (patch_pb_undef.cmake), so
// no TU's include order decides whether the protocol compiles.
#include <Flight.grpc.pb.h>
#include <ipc/array.hpp>
#include <ipc/ipc_reader.hpp>
#include <ipc/ipc_writer.hpp>

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace flight::engine {
    class SchedulerEngine; // the one engine this server has
}

namespace flight::core {

namespace fp = arrow::flight::protocol;

// Execution error: maps to INVALID_ARGUMENT + message text.
struct EngineError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct QueryResult {
    ipc::SchemaPtr schema; // always VALID, possibly empty (no result set)
    std::vector<ipc::RecordBatch> batches;
};

// The tables the metadata commands answer with: the PROJECT's table_info
// (qualified name + engine schema), not a protocol-side twin — the schema is
// converted to IPC where the metadata batches are built (commands.cpp).
struct EngineMetadata {
    std::vector<std::string> catalogs;
    std::vector<std::string> db_schemas; // for CommandGetDbSchemas (simplified: not bound to the catalog filter)
    std::vector<table_info> tables;
    std::vector<std::string> table_types;
};

// A prepared query: text + the result and parameter schemas.
struct Prepared {
    std::string query;
    ipc::SchemaPtr dataset_schema;   // empty for update
    ipc::SchemaPtr parameter_schema; // one field per '?'

    // Opaque engine-side resource handle (the Worker's prepared-statement
    // session). The engine fills it in prepare(); the protocol layer never
    // interprets it, it only carries it back to the engine. Empty when the
    // engine holds no per-handle resource.
    std::string engine_handle;
};

// Bound parameters: rows of values (a row = a vector with one entry per parameter).
using BoundParams = std::vector<std::vector<ipc::Value>>;

// SQL-LIKE matcher (% — any sequence, _ — one character).
// No escaping; a ""/NULL pattern does not filter.
[[nodiscard]] bool like_match(std::string_view text, std::string_view pattern);

// A materialized query result (fully in memory for now).
struct CachedResult {
    ipc::SchemaPtr schema;
    std::vector<std::uint8_t> schema_message;                 // bare flatbuffer Message(Schema)
    std::vector<ipc::RecordBatchMessage> batch_messages;      // ready for DoGet
    std::int64_t total_rows = 0;
};

// Prepared statement state: template + bound parameters.
struct PreparedStatementState {
    Prepared prepared;
    BoundParams bound;
};

class FlightSqlCore {
  public:
    FlightSqlCore(AuthService auth, std::unique_ptr<engine::SchedulerEngine> engine);
    ~FlightSqlCore(); // the engine is held through a forward declaration

    AuthService& auth() { return auth_; }
    engine::SchedulerEngine& engine() { return *engine_; }

    // Prepared statements
    grpc::Status create_prepared(const std::string& query, std::string* handle_out);
    grpc::Status close_prepared(const std::string& handle);
    grpc::Status prepared_lookup(const std::string& handle,
                                 PreparedStatementState const** out) const;
    grpc::Status prepared_bind(const std::string& handle, BoundParams rows);

    // Materialize a result and register a ticket for it.
    std::string register_result(ipc::SchemaPtr schema, std::vector<ipc::RecordBatch> batches);

    // Build FlightInfo for a ticket (NOT_FOUND when the ticket is unknown).
    grpc::Status make_flight_info(const fp::FlightDescriptor& descriptor, const std::string& ticket,
                                  fp::FlightInfo* out) const;
    grpc::Status make_schema_result(const std::string& ticket, fp::SchemaResult* out) const;

    // The ticket's data for DoGet.
    grpc::Status lookup(const std::string& ticket, CachedResult const** out) const;

  private:
    AuthService auth_;
    std::unique_ptr<engine::SchedulerEngine> engine_;
    mutable std::mutex mutex_;
    // A ticket lives from GetFlightInfo to DoGet (seconds), but the server is
    // long-lived: an unbounded list of materialized results leaks. Keep at
    // most kMaxCachedResults; the oldest one is evicted (FIFO: a DoGet arrives
    // right after its GetFlightInfo, so re-reading an evicted ticket is rare
    // and answers NOT_FOUND).
    static constexpr std::size_t kMaxCachedResults = 1024;
    std::unordered_map<std::string, CachedResult> results_;
    std::deque<std::string> result_order_;
    std::unordered_map<std::string, PreparedStatementState> prepared_;
    std::atomic<std::uint64_t> ticket_counter_{0};
    std::atomic<std::uint64_t> prepared_counter_{0};
};

} // namespace flight::core
