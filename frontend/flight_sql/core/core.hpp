// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL core: the protocol/engine exchange shapes, the ticket manager
// and the FlightInfo/FlightData assembly. The data plane rides the PROJECT's
// arrow core — the same types and serializers the file translators use
// (tsl::chunk_to_record_batch in, arrow::ipc payloads out) — with no
// wire-protocol layer of our own. There is no engine interface on purpose —
// the server has exactly one engine, and FlightSqlCore owns it (a
// flight::engine::SessionEngine, held through a forward declaration).

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

// After arrow and the protocol: the engine's parser headers it pulls in #define
// names (DAY, SECOND, ERROR) that arrow's and the protocol's headers declare.
#include "session/query_session.hpp"

#include <atomic>
#include <compare>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace flight::engine {
    class SessionEngine; // the one engine this server has
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

// One GetDbSchemas row: a schema under the catalog (database) that holds it.
struct DbSchema {
    std::string catalog;
    std::string schema;

    friend bool operator==(const DbSchema&, const DbSchema&) = default;
    friend auto operator<=>(const DbSchema&, const DbSchema&) = default;
};

// The tables the metadata commands answer with: the PROJECT's table_info
// (qualified name + engine schema); the arrow schema of a table is built
// where the metadata batches are assembled (commands.cpp). The catalogs and
// the schemas are derived from the tables: each once, in the order the
// Flight SQL spec asks for (catalog_name, then db_schema_name).
struct EngineMetadata {
    std::vector<std::string> catalogs;  // distinct, sorted
    std::vector<DbSchema> db_schemas;   // distinct (catalog, schema), sorted
    std::pmr::vector<table_info> tables;
    std::vector<std::string> table_types;
};

// A prepared query: text + the result and parameter schemas.
struct Prepared {
    std::string query;
    std::shared_ptr<arrow::Schema> dataset_schema;   // null for update
    std::shared_ptr<arrow::Schema> parameter_schema; // one field per '?'
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

// A materialized query result, already serialized for the wire. One owner at
// a time: the RPC that materialized it, the ticket cache, then the DoGet that
// took it out.
struct CachedResult {
    std::string schema_ipc;     // encapsulated schema (FlightInfo.schema / SchemaResult)
    std::string schema_message; // the same schema as a BARE message: DoGet's first FlightData
    std::vector<BatchPayload> batches; // ready for DoGet (bare headers, like IpcPayload.metadata)
    std::int64_t total_rows = 0;
    std::size_t bytes = 0; // every serialized byte above: what the ticket cache accounts for
};
using CachedResultPtr = std::unique_ptr<const CachedResult>;

// Serialize a result for the wire (EngineError when arrow refuses it).
[[nodiscard]] CachedResultPtr materialize(const std::shared_ptr<arrow::Schema>& schema,
                                          const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

// The FlightInfo GetFlightInfo / PollFlightInfo answer for `result`: built
// before the result goes to the cache, its one endpoint's ticket set once
// register_result answered it (set_flight_ticket).
void make_flight_info(const fp::FlightDescriptor& descriptor, const CachedResult& result, fp::FlightInfo* out);
void set_flight_ticket(std::string ticket, fp::FlightInfo* out);

// Prepared statement state: template + bound parameters. A lookup copies it
// out under the core's lock (the schemas and batches inside are arrow's shared
// buffers), so an execution reads the state it took whatever a bind stores
// meanwhile.
struct PreparedStatementState {
    Prepared prepared;
    BoundParams bound;
};

// The bounds of the results that wait for their DoGet (see FlightSqlCore).
struct ResultCacheLimits {
    std::size_t results;
    std::size_t bytes;
};
inline constexpr ResultCacheLimits kDefaultResultCacheLimits{1024, std::size_t{256} << 20};

class FlightSqlCore {
  public:
    FlightSqlCore(AuthService auth, std::unique_ptr<engine::SessionEngine> engine,
                  ResultCacheLimits limits = kDefaultResultCacheLimits);
    ~FlightSqlCore(); // the engine is held through a forward declaration

    AuthService& auth() { return auth_; }
    engine::SessionEngine& engine() { return *engine_; }

    // Tickets and prepared-statement handles are kIdBytes from
    // frontend::secure_random_bytes (hex, behind their prefix) — never a
    // counter another client could guess — and belong to the client that
    // obtained them (`owner`: AuthService::client_of, empty for an anonymous
    // service). Presented by another client they are NOT_FOUND, exactly as an
    // unknown one, and left untouched. No secure bytes: UNAVAILABLE.
    static constexpr std::size_t kIdBytes = 16; // 128 bits

    // Prepared statements. Each entry is the only owner of the statement's
    // query session (opened by create_prepared, closed — under the lock — by
    // close_prepared or the core's end). A lookup hands out a copy of the
    // state, so a bind racing an execution never changes what it reads.
    grpc::Status create_prepared(const std::string& query, const std::string& owner, std::string* handle_out);
    grpc::Status close_prepared(const std::string& handle, const std::string& owner);
    grpc::Status
    prepared_lookup(const std::string& handle, const std::string& owner, PreparedStatementState* out) const;
    grpc::Status prepared_bind(const std::string& handle, const std::string& owner, BoundParams rows);
    // An execution of a prepared statement on its session, with the parameters
    // last bound. Every step of it reaches the session under the lock — it only
    // queues a statement — and awaits the answer outside, so a close racing it
    // closes the session: a statement the session runs goes on to its end, and
    // an execution the close cut short answers NOT_FOUND, as an unknown handle.
    // INVALID_ARGUMENT for what the engine refuses, INTERNAL for any other
    // failure.
    grpc::Status execute_prepared(const std::string& handle, const std::string& owner, QueryResult* out);
    grpc::Status execute_update_prepared(const std::string& handle, const std::string& owner, std::int64_t* out);
    // One step of such an execution: `use` is called with the statement's
    // query session under the lock — it only queues a statement, whose answer
    // is awaited outside — and answers true; false, `use` not called, once the
    // statement was closed.
    template<typename Use>
    bool use_prepared(const std::string& handle, Use&& use) {
        std::lock_guard lock(mutex_);
        const auto it = prepared_.find(handle);
        if (it == prepared_.end()) {
            return false;
        }
        use(*it->second.session);
        return true;
    }

    // Keep a result for the DoGet that redeems its ticket (the cache owns it
    // from here); answers the ticket.
    grpc::Status register_result(CachedResultPtr result, const std::string& owner, std::string* ticket_out);
    // DoGet: the ticket's result, taken out of the cache — Flight tickets are
    // single use (Flight.proto, `Ticket`), so a result is held from
    // GetFlightInfo to its DoGet and no longer, and the DoGet is its only
    // owner. NOT_FOUND for a ticket unknown, already redeemed, evicted or
    // another client's (which stays cached).
    grpc::Status take_result(const std::string& ticket, const std::string& owner, CachedResultPtr* out);

    // What the cache holds (tests, diagnostics).
    [[nodiscard]] std::size_t cached_results() const;
    [[nodiscard]] std::size_t cached_bytes() const;

  private:
    AuthService auth_;
    std::unique_ptr<engine::SessionEngine> engine_;
    mutable std::mutex mutex_;
    // Results whose DoGet has not come — normally one per GetFlightInfo in
    // flight, for a few milliseconds. A client that never redeems its ticket
    // (it read only FlightInfo, or went away) would leave its result here for
    // the life of the server, so the cache is bounded by count AND by bytes:
    // past either bound the oldest results are evicted (FIFO), the one just
    // registered never — a single result larger than the byte bound stays
    // until it is redeemed or pushed out by the next one. A DoGet owns the
    // result it took out, so an eviction never frees what a DoGet streams.
    struct CacheEntry {
        CachedResultPtr result;
        std::string owner;                      // the client the ticket was issued to
        std::list<std::string>::iterator order; // its place in result_order_
    };
    struct PreparedEntry {
        std::string owner; // the client that created the statement
        PreparedStatementState state;
        otterstax::session::session_ptr session; // the statement's query session: closed with the entry
    };
    // Takes the entry out of the cache; answers its result.
    CachedResultPtr drop_locked(std::unordered_map<std::string, CacheEntry>::iterator it);
    // `run` (one of the engine's prepared executions) over the statement's
    // state, its steps reaching the session through use_prepared; the statuses
    // above.
    template<typename Run>
    grpc::Status run_prepared(const std::string& handle, const std::string& owner, Run run);

    ResultCacheLimits limits_;
    std::unordered_map<std::string, CacheEntry> results_;
    std::list<std::string> result_order_; // the cached tickets, oldest first
    std::size_t cached_bytes_ = 0;
    std::unordered_map<std::string, PreparedEntry> prepared_;
};

} // namespace flight::core
