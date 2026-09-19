// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL core: ticket manager and FlightInfo/FlightData assembly.

#include "auth.hpp"
#include "engine.hpp"

#include <Flight.grpc.pb.h>
#include <ipc/array.hpp>
#include <ipc/ipc_writer.hpp>

#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace flight::core {

namespace fp = arrow::flight::protocol;

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
    FlightSqlCore(AuthService auth, std::unique_ptr<IEngine> engine);

    AuthService& auth() { return auth_; }
    IEngine& engine() { return *engine_; }

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
    std::unique_ptr<IEngine> engine_;
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
