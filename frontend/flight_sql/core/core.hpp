// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL core: the protocol/engine exchange shapes, the ticket manager
// and the FlightInfo/FlightData assembly. The data plane rides the PROJECT's
// arrow core — the same types and serializers the file translators use
// (tsl::chunk_to_record_batch in, arrow::ipc payloads out) — with no
// wire-protocol layer of our own. There is no engine interface on purpose —
// the server has exactly one engine, and FlightSqlCore owns it (a
// flight::engine::SchedulerEngine, held through a forward declaration).

#include "auth.hpp"

#include "utility/table_info.hpp"

#include <arrow/api.h>
#include <arrow/ipc/writer.h>

// The engine's postgres-derived headers #define ERROR (a pg error code,
// pg_type_definitions.h), which collides with FlightSql's generated
// SetSessionOptionsResult.ErrorValue enum member — the same clash class as
// the parser's DAY/SECOND the catalog already undefs for arrow. The guard is
// prepended to the GENERATED headers themselves (patch_pb_undef.cmake), so
// no TU's include order decides whether the protocol compiles.
#undef ERROR
#include <Flight.grpc.pb.h>
#undef ERROR

#include <atomic>
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
    // Always a VALID (possibly empty) schema; batches carry the rows.
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

// The tables the metadata commands answer with: the PROJECT's table_info
// (qualified name + engine schema); the arrow schema of a table is built
// where the metadata batches are assembled (commands.cpp).
struct EngineMetadata {
    std::vector<std::string> catalogs;
    std::vector<std::string> db_schemas; // for CommandGetDbSchemas (simplified: not bound to the catalog filter)
    std::pmr::vector<table_info> tables;
    std::vector<std::string> table_types;
};

// A prepared query: text + the result and parameter schemas.
struct Prepared {
    std::string query;
    std::shared_ptr<arrow::Schema> dataset_schema;   // null for update
    std::shared_ptr<arrow::Schema> parameter_schema; // one field per '?'

    // Opaque engine-side resource handle (the Worker's prepared-statement
    // session). The engine fills it in prepare(); the protocol layer never
    // interprets it, it only carries it back to the engine. Empty when the
    // engine holds no per-handle resource.
    std::string engine_handle;
};

// Bound parameters: the DoPut batches as the client sent them (the engine
// adapter reads rows out of them through the project's tsl::arrow_to_chunk).
using BoundParams = std::vector<std::shared_ptr<arrow::RecordBatch>>;

// SQL-LIKE matcher (% — any sequence, _ — one character).
// No escaping; a ""/NULL pattern does not filter.
[[nodiscard]] bool like_match(std::string_view text, std::string_view pattern);

// One DoGet-ready batch message: the bare flatbuffer header (data_header) and
// the aligned body buffers (data_body), as arrow::ipc produced them.
struct BatchPayload {
    std::string metadata; // bare Message(header=RecordBatch)
    std::string body;
};

// A materialized query result, already serialized for the wire.
struct CachedResult {
    std::string schema_ipc;     // encapsulated schema (FlightInfo.schema / SchemaResult)
    std::string schema_message; // the same schema as a BARE message: DoGet's first FlightData
    std::vector<BatchPayload> batches; // ready for DoGet (bare headers, like IpcPayload.metadata)
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

    // Serialize a result and register a ticket for it.
    std::string register_result(const std::shared_ptr<arrow::Schema>& schema,
                                std::vector<std::shared_ptr<arrow::RecordBatch>> batches);

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
