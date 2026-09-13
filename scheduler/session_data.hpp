// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/sql/parser/nodes/nodes.h>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cassert>
#include <memory_resource>
#include <vector>

// Cursor results arrive as a batch of <=DEFAULT_VECTOR_CAPACITY chunks
// (components::cursor::cursor_t::chunks(), never combined into one). The payload
// carries them as a vector and exposes row-level accessors that span chunks.
// Producers hand over >=1 chunk (possibly empty) — an empty result is one empty
// chunk — but `chunks` is public and mutable, so the accessors are defined on
// an emptied batch as well instead of reading front() blindly.
struct session_payload {
    components::types::complex_logical_type schema;
    std::pmr::vector<components::vector::data_chunk_t> chunks;
    size_t parameter_count;
    NodeTag tag;

    explicit session_payload(std::pmr::memory_resource* resource)
        : schema()
        , chunks(resource)
        , parameter_count(0)
        , tag(NodeTag::T_Null) {
        chunks.emplace_back(resource, std::pmr::vector<components::types::complex_logical_type>{resource}, 0);
    }

    session_payload(components::types::complex_logical_type schema,
                    std::pmr::vector<components::vector::data_chunk_t>&& chunks,
                    size_t parameter_count,
                    NodeTag tag)
        : schema(std::move(schema))
        , chunks(std::move(chunks))
        , parameter_count(parameter_count)
        , tag(tag) {
        // `this->`, not `chunks`: the parameter shadows the member here and has
        // already been moved from. The producer contract is one empty chunk for
        // an empty result, never no chunks.
        assert(!this->chunks.empty() && "session_payload: batch must hold at least one chunk");
    }

    size_t size() const {
        size_t total = 0;
        for (const auto& c : chunks) {
            total += c.size();
        }
        return total;
    }
    size_t column_count() const { return chunks.empty() ? 0 : chunks.front().column_count(); }
    bool empty() const { return size() == 0; }
};
