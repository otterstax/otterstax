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

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <utility>

namespace components::types {
    class logical_value_t;
}

namespace flight::engine {

    class SessionEngine final {
    public:
        SessionEngine(otterstax::session::session_factory_t& sessions, std::pmr::memory_resource* resource);

        // SELECT-like queries and DML by text, each on a session of its own.
        core::QueryResult execute(const std::string& query);
        std::int64_t execute_update(const std::string& query);

        // Prepared statements. prepare() opens the statement's session, prepares
        // the text there and hands the session to the caller (FlightSqlCore
        // keeps it with the handle until ClosePreparedStatement). A session's
        // stored statement is single-use, so every execution re-prepares the
        // text on that session — the one `statements` keeps under `handle`,
        // reached step by step (FlightSqlCore::use_prepared) — and binds the
        // parameters there (the pattern the MySQL and PG frontends follow). A
        // step that finds the session closed throws core::EngineError.
        std::pair<core::Prepared, otterstax::session::session_ptr> prepare(const std::string& query);
        core::QueryResult execute_prepared(core::FlightSqlCore& statements,
                                           const std::string& handle,
                                           const core::Prepared& prepared,
                                           const core::BoundParams& params);
        std::int64_t execute_update_prepared(core::FlightSqlCore& statements,
                                             const std::string& handle,
                                             const core::Prepared& prepared,
                                             const core::BoundParams& params);

        // Metadata (Catalogs / DbSchemas / Tables / TableTypes): the engine's own
        // tables (pg_catalog) and the remote tables the federation knows
        // (federation.remote_tables), read through a query session of its own
        // and listed by catalog::list_tables.
        core::EngineMetadata metadata();

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
        // Re-prepare + execute_prepared_statement with one parameter row.
        template<typename Session>
        session_payload run_prepared(const Session& session, const std::string& sql, const core::BoundParams& params);
        // The DoPut batches -> engine parameter rows (tsl::arrow_to_chunk).
        std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>
        to_param_rows(const core::BoundParams& params);
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
