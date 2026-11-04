// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL command parsing (the Any in FlightDescriptor.cmd / the DoPut
// descriptor) and result assembly (including the fixed metadata schemas).

#include <core/core.hpp>

#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

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

// The GetCatalogs / GetDbSchemas / GetTables answers over what
// engine::SessionEngine listed (filtered and sorted there): one row per
// catalog, per (catalog, schema), per table — its arrow schema serialized when
// `include_schema`, NULL otherwise. Throw EngineError.
std::shared_ptr<arrow::RecordBatch> catalogs_batch(const std::vector<std::string>& catalogs);
std::shared_ptr<arrow::RecordBatch> db_schemas_batch(const std::vector<DbSchema>& schemas);
std::shared_ptr<arrow::RecordBatch> tables_batch(const std::pmr::vector<table_info>& tables, bool include_schema);

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
