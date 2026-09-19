// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Engine payload (schema + data chunks) -> Flight SQL IPC model.
//
// This is the chunk_to_record_batch of the old Arrow-based FlightSQL
// frontend, retargeted at the in-house Arrow IPC: the same column mapping
// (the logical type decides DECIMAL; the scale rides in the type), the same
// name-based column-to-field matching (the n-th chunk column named X feeds
// the n-th schema field named X), the same refusals (nested types, unnamed
// columns) — reported as core::EngineError instead of an Arrow Status.

#include "core/engine.hpp"
#include "ipc/array.hpp"
#include "ipc/types.hpp"
#include "scheduler/session_data.hpp"

#include <cstddef>
#include <vector>

namespace flight::conv {

    // The IPC schema of a payload's result schema (a STRUCT whose children are
    // the columns). A schema that is not a STRUCT is an empty schema — the
    // payload carries no result set. Throws core::EngineError on a column with
    // no IPC mapping.
    ipc::SchemaPtr schema_to_ipc(const components::types::complex_logical_type& schema);

    // One IPC batch per non-empty chunk. Every schema field must be fed from
    // exactly one chunk column, matched by name; a chunk column the schema does
    // not name is skipped (the schema is the contract handed to the client).
    // Throws core::EngineError. `schema` is what schema_to_ipc answered for
    // the same payload.
    std::vector<ipc::RecordBatch> chunks_to_ipc(const session_payload& payload, const ipc::SchemaPtr& schema);

    // The parameter schema handed out at CreatePreparedStatement: one nullable
    // int64 field per parameter, named "$1".."$N" (the model the reference
    // drivers bind against — arrow-go refuses to coerce its typed values into
    // a utf8 field). Parameter types are not resolved at prepare time (the
    // engine's binder holds them until Bind), so the adapter re-types each
    // bound value the way a text protocol frontend types its literals (see
    // scheduler_engine).
    ipc::SchemaPtr parameter_ipc_schema(std::size_t parameter_count);

} // namespace flight::conv
