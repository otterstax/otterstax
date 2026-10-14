// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL command parsing (the Any in FlightDescriptor.cmd / the DoPut
// descriptor) and result assembly (including the fixed metadata schemas).

#include <core/core.hpp>

#include <memory>

namespace arrow::flight::protocol::sql {
    class CommandGetDbSchemas;
    class CommandGetTables;
}

namespace flight::core {

// Execute the descriptor's command: a SELECT or a metadata request.
// (engine is core.engine() — passed explicitly to keep the call sites explicit.)
// result_out receives the materialized result, not yet registered: GetFlightInfo
// registers it for its DoGet, GetSchema answers its schema and keeps nothing.
// `client` is the caller (AuthService::client_of): a prepared statement it
// names must be its own.
grpc::Status execute_descriptor(FlightSqlCore& core, engine::SessionEngine& engine,
                                const arrow::flight::protocol::FlightDescriptor& descriptor,
                                const std::string& client, CachedResultPtr* result_out);

// The GetCatalogs / GetDbSchemas / GetTables answers over one metadata
// listing: every catalog once, every (catalog, schema) pair once under its
// own catalog, every table under its catalog and schema, in the listing's
// order (catalog_name, then db_schema_name, then table_name). GetDbSchemas and
// GetTables keep the rows of `catalog` (exact match) when the command carries
// it, and those whose schema matches db_schema_filter_pattern (LIKE); GetTables
// also filters on table_name_filter_pattern (LIKE) and table_types. Throw
// EngineError.
std::shared_ptr<arrow::RecordBatch> catalogs_batch(const EngineMetadata& md);
std::shared_ptr<arrow::RecordBatch> db_schemas_batch(const arrow::flight::protocol::sql::CommandGetDbSchemas& cmd,
                                                     const EngineMetadata& md);
std::shared_ptr<arrow::RecordBatch> tables_batch(const arrow::flight::protocol::sql::CommandGetTables& cmd,
                                                 const EngineMetadata& md);

// Parse the DoPut command (the Any from the first FlightData).
enum class DoPutKind { Update, PreparedUpdate, PreparedBind };
struct DoPutCommand {
    DoPutKind kind = DoPutKind::Update;
    std::string query;  // Update
    std::string handle; // Prepared*
};
grpc::Status do_put_command(const arrow::flight::protocol::FlightDescriptor& descriptor,
                            DoPutCommand* out);

} // namespace flight::core
