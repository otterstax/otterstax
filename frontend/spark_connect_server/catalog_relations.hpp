// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Answers Spark Connect Catalog relations (spark.catalog.*) from the ENGINE's
// catalog.
//
// Spark's Catalog API (spark.catalog.listDatabases(), listTables(),
// tableExists(), ...) arrives as `Relation.catalog`. Unlike regular relations
// it builds no logical plan of its own: the databases and tables are read from
// the engine's pg_catalog.pg_namespace / pg_catalog.pg_class and the remote
// tables from federation.remote_tables, with SQL through the Spark session's
// query session, and a table's columns come from preparing
// `SELECT * FROM <table>` (then closing that statement). Both local tables and
// the remote tables the federation knows are listed. The rows are laid out
// exactly as PySpark 3.5–4.2 reads them — by position — so a list answer is a
// session_payload with one row per entry; an existence check is a single
// boolean, currentCatalog/currentDatabase a single string. What Spark sees of
// the engine's catalog, and each row layout, is in catalog_rows.hpp.
// Supported: currentCatalog, listCatalogs, currentDatabase, listDatabases,
// getDatabase, databaseExists, listTables, getTable, tableExists, listColumns;
// every other operation answers core::error_t{unimplemented_yet}. Nothing
// throws.

#include "session/query_session.hpp"
#include "session/session_payload.hpp"
#include "spark_sessions.hpp"
#include "utility/session.hpp"

#include <core/result_wrapper.hpp>

#include <agrpc/grpc_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <memory_resource>
#include <string>

// Forward declaration keeps protobuf out of the header; the .cpp includes
// spark/connect/catalog.pb.h for the full ::spark::connect::Catalog definition.
namespace spark::connect {
    class Catalog;
} // namespace spark::connect

namespace frontend::spark {

    // Each request of this operation reaches the query session of the Spark
    // session `session_id` in `sessions` as a step (`begin`: under the
    // registry's lock, the answer awaited outside, the step finished however
    // the wait ends; a session closed meanwhile is a connection_closed error);
    // every request runs there under `statement` (the session runs them one at
    // a time, so the prepared statement a column lookup stores cannot collide
    // with another). Runs on the calling handler's GrpcExecutor.
    boost::asio::awaitable<core::result_wrapper_t<session_payload>, agrpc::GrpcExecutor>
    handle_catalog_relation(const ::spark::connect::Catalog& catalog,
                            idle_session_registry_t<otterstax::session::session_ptr>& sessions,
                            const std::string& session_id,
                            otterstax::session::statement_id_t statement,
                            std::pmr::memory_resource* resource);

} // namespace frontend::spark
