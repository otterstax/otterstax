// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/catalog/settings.hpp>
#include <components/sql/parser/nodes/nodes.h>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

namespace otterstax::session {

    // The transaction statement a statement is, read off its plan root
    // (node_transaction_t::op()): its CommandComplete tag.
    enum class transaction_command_t : std::uint8_t
    {
        none,
        begin,
        commit,
        rollback,
        savepoint,
        release,
        rollback_to,
    };

} // namespace otterstax::session

// What a query session answers a frontend: the result of one statement.
//
// Cursor results arrive as a batch of <=DEFAULT_VECTOR_CAPACITY chunks
// (components::cursor::cursor_t::chunks(), never combined into one). The payload
// carries them as a vector and exposes row-level accessors that span chunks.
// Producers hand over >=1 chunk (possibly empty) — an empty result is one empty
// chunk — but `chunks` is public and mutable, so the accessors are defined on
// an emptied batch as well instead of reading front() blindly.
//
// A statement that answers rows has columns; one that answers no rows (DDL, a
// DML without RETURNING, CREATE EXTERNAL TABLE, COPY ... TO, Kafka DDL) has none,
// and `affected_rows` is the count it reports — the rows an INSERT / UPDATE /
// DELETE wrote (the engine's cursor->affected_rows(), or the backend's count of a
// DML that ran entirely there), 0 for everything else. Every frontend tells a
// resultset from an affected count by the column count alone.
//
// `transaction` names the transaction statement an executed BEGIN / COMMIT /
// ROLLBACK / SAVEPOINT / RELEASE / ROLLBACK TO was (its CommandComplete tag) —
// `rollback` for a COMMIT that ended a failed block, which the engine answers
// with success and rolls back; `none` for every other statement and for a
// prepare. The block the connection is in after the statement is the engine's
// (query_session_t::transaction_status()).
//
// A prepare answers `schema` (a STRUCT of the result columns, NA for a
// statement without rows) and `parameter_types`, the engine's type of each
// `$n` (cursor_t::parameter_types(), `$<i+1>` at i). A statement run under a
// window (query_session_t::execute_window / fetch_window) answers `portal`, the
// engine's portal while more rows may follow (`has_more`), 0 once it ended.
struct session_payload {
    components::types::complex_logical_type schema;
    std::pmr::vector<components::vector::data_chunk_t> chunks;
    size_t parameter_count;
    NodeTag tag;
    size_t affected_rows{0};
    otterstax::session::transaction_command_t transaction{otterstax::session::transaction_command_t::none};
    std::pmr::vector<components::types::complex_logical_type> parameter_types;
    std::uint64_t portal{0};
    bool has_more{false};
    // What an executed statement that succeeded did to the engine session's
    // settings, for a frontend that follows them as the engine keeps them
    // (components::catalog::session_settings_t; the PostgreSQL frontend's
    // ParameterStatus of TimeZone / search_path): every assignment of a SET /
    // RESET / RESET ALL of the session or SET LOCAL — its value as the engine
    // canonicalises it, nullopt for the default —, inside a block too; and
    // the savepoint a SAVEPOINT / RELEASE / ROLLBACK TO named. The block's end
    // is the frontend's to follow (transaction, the session's status). Empty
    // for every other statement, a prepare and a failure.
    struct setting_change_t {
        components::catalog::setting_id id;
        std::optional<std::string> value;
        bool local{false};
    };
    std::vector<setting_change_t> setting_changes;
    std::optional<std::string> savepoint;

    explicit session_payload(std::pmr::memory_resource* resource)
        : schema()
        , chunks(resource)
        , parameter_count(0)
        , tag(NodeTag::T_Null)
        , parameter_types(resource) {
        chunks.emplace_back(resource, std::pmr::vector<components::types::complex_logical_type>{resource}, 0);
    }

    session_payload(components::types::complex_logical_type schema,
                    std::pmr::vector<components::vector::data_chunk_t>&& chunks,
                    size_t parameter_count,
                    NodeTag tag,
                    size_t affected_rows = 0)
        : schema(std::move(schema))
        , chunks(std::move(chunks))
        , parameter_count(parameter_count)
        , tag(tag)
        , affected_rows(affected_rows)
        , parameter_types(this->chunks.get_allocator()) {
        // `this->`, not `chunks`: the parameter shadows the member here and has
        // already been moved from. The producer contract is one empty chunk for
        // an empty result, never no chunks.
        assert(!this->chunks.empty() && "session_payload: batch must hold at least one chunk");
    }

    // Rows of the result, across every chunk.
    size_t size() const {
        size_t total = 0;
        for (const auto& c : chunks) {
            total += c.size();
        }
        return total;
    }
    size_t column_count() const { return chunks.empty() ? 0 : chunks.front().column_count(); }
    bool empty() const { return size() == 0; }
    // What a frontend reports as the statement's row count: the rows of a
    // resultset, the affected count of a statement without columns.
    size_t reported_rows() const { return column_count() > 0 ? size() : affected_rows; }
};
