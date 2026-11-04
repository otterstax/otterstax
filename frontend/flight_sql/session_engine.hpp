// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The engine side of the Flight SQL server: query-session execute /
// prepare_schema / execute_prepared_statement wrapped as the one engine
// FlightSqlCore owns. Every conversion goes through the project's own
// translators (to_arrow_schema / chunk_to_record_batch / arrow_to_chunk) — the
// same ones the file paths use; there is no flight-specific data-plane code.
//
// Session scope: a Flight SQL RPC that runs a statement by text runs it on a
// query session of its own, which the RPC owns and closes when it ends
// (autocommit: a statement that would leave a transaction open — BEGIN — is
// rolled back and refused, by text and prepared alike). A prepared statement
// keeps ONE query session from CreatePreparedStatement to
// ClosePreparedStatement, owned by its entry in FlightSqlCore: its executions —
// in whatever RPC, on whatever gRPC thread — run on that session, one at a
// time, each step reaching it under the core's lock
// (FlightSqlCore::use_prepared).
//
// The engine is called from inside an asio-grpc coroutine, and every session
// call is awaited with frontend/common/asio_future_bridge.hpp's
// await_future_blocking — the same bridge the wire frontends use. The gRPC
// server runs its GrpcContext on N threads (see server.cpp), so one blocked
// query does not stall the others.

#include "core/core.hpp"

#include "session/session_factory.hpp"
#include "session/session_payload.hpp"

#include <actor-zeta.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace flight::engine {

    class SessionEngine final {
    public:
        SessionEngine(otterstax::session::session_factory_t& sessions, std::pmr::memory_resource* resource);

        // SELECT-like queries and DML by text, each on a session of its own.
        core::QueryResult execute(const std::string& query);
        std::int64_t execute_update(const std::string& query);

        // Prepared statements. prepare() opens the statement's session, prepares
        // the text there — ONE entry of the session (core::Prepared::statement)
        // — and hands the session to the caller (FlightSqlCore keeps it with
        // the handle until ClosePreparedStatement, which closes the session and
        // its entry). Every execution runs that entry on that session — the one
        // `statements` keeps under `handle`, reached step by step
        // (FlightSqlCore::use_prepared) — with its own parameters (the entry is
        // reusable: query_session_t::execute_prepared_statement parses the text
        // again and binds them; the pattern the MySQL and PG frontends follow).
        // A step that finds the session closed throws core::EngineError.
        std::pair<core::Prepared, otterstax::session::session_ptr> prepare(const std::string& query);
        core::QueryResult execute_prepared(core::FlightSqlCore& statements,
                                           const std::string& handle,
                                           const core::Prepared& prepared,
                                           const core::BoundParams& params);
        std::int64_t execute_update_prepared(core::FlightSqlCore& statements,
                                             const std::string& handle,
                                             const core::Prepared& prepared,
                                             const core::BoundParams& params);

        // Metadata (GetCatalogs / GetDbSchemas / GetTables), read from the
        // engine's catalog (pg_catalog.pg_database / pg_namespace / pg_class)
        // through a query session of its own; no backend is asked. A catalog
        // is a database as SQL writes it: `d` for a local one, `c.d` for the
        // database d of the connection c; a schema is a schema of it. Hidden:
        // pg_catalog, information_schema and the database `federation`. The
        // filters run in the engine: `catalog` (when given) is an exact match,
        // the patterns are LIKE patterns (empty: no filter). Sorted by
        // catalog, schema, table; a table's columns (include_schema) are the
        // engine's describe of `SELECT * FROM` it. Throw core::EngineError.
        std::vector<std::string> catalogs();
        std::vector<core::DbSchema> db_schemas(const std::optional<std::string>& catalog,
                                               const std::string& schema_pattern);
        std::pmr::vector<table_info> tables(const std::optional<std::string>& catalog,
                                            const std::string& schema_pattern,
                                            const std::string& table_pattern,
                                            bool include_schema);

        std::string dialect_name() const;

    private:
        using query_session_t = otterstax::session::query_session_t;

        // `session` reaches the session a step runs on: called with a callable
        // over a query_session_t&, it calls it under the owner's lock (an RPC's
        // own session_ptr, or FlightSqlCore::use_prepared) and answers true;
        // false once the owner closed the session.
        //
        // One call on that session: `send` queues it there (actor_zeta::send,
        // under the owner's lock) and answers its future, awaited outside the
        // reach. Throws core::EngineError: the session's error, under `stage`,
        // or the session closed.
        template<typename Session, typename Send>
        session_payload call(const Session& session, Send send, const char* stage);
        // execute + await; throws core::EngineError.
        template<typename Session>
        session_payload run_query(const Session& session, const std::string& sql);
        // prepare_schema under a fresh statement id + await; throws.
        template<typename Session>
        std::pair<otterstax::session::statement_id_t, session_payload> prepare_fresh(const Session& session,
                                                                                     const std::string& sql);
        // execute_prepared_statement of the statement's entry with one parameter row.
        template<typename Session>
        session_payload run_prepared(const Session& session, const core::Prepared& prepared, const core::BoundParams& params);
        // The DoPut batches -> engine chunks (tsl::arrow_to_chunk), one per
        // non-empty batch: column i is `$<i+1>`, one row is one parameter set.
        std::pmr::vector<components::vector::data_chunk_t> to_param_chunks(const core::BoundParams& params);
        // A statement that left the session inside an explicit transaction
        // (BEGIN): rolled back and refused (throws). No Flight SQL scope
        // outlives what it runs, so the client could never end it.
        template<typename Session>
        void refuse_open_transaction(const Session& session);
        // Best-effort close_statement (idempotent in the session).
        template<typename Session>
        void close_quietly(const Session& session, otterstax::session::statement_id_t id);

        otterstax::session::session_factory_t& sessions_;
        std::pmr::memory_resource* resource_;
    };

} // namespace flight::engine
