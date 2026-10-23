// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/translators/input/pg_enum_types.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>

namespace conn {

    // Column name → ClickHouse type string (system.columns): the overrides
    // tsl::ch_to_chunk / ch_to_struct take for a table's named Tuple columns.
    using named_types_t = std::unordered_map<std::string, std::string>;
    using named_types_ptr = std::shared_ptr<const named_types_t>;

    // The ENUM types of one PostgreSQL connection (tsl::pg_to_chunk / pg_to_struct).
    using pg_enums_ptr = std::shared_ptr<const tsl::pg_enum_oid_map>;

    // What a driver needs to decode a statement's answer beyond the answer itself.
    // Discovery fills it at startup and nothing changes it afterwards, so a statement
    // carries a pointer, never a copy. MySQL needs none.
    using decode_hints_t = std::variant<std::monostate, named_types_ptr, pg_enums_ptr>;

    enum class result_kind
    {
        rows,          // the result set as one data_chunk_t (a DML's count carrier where the
                       // backend's translator builds one from the answer)
        schema,        // the result's columns only, as a STRUCT (discovery and prepare probes)
        affected_rows, // a DML: the count only
    };

    // One statement for one backend connection. Every field is owned: the statement
    // crosses from the actor that builds it to the pool thread that runs it, and
    // nothing in it refers back to the caller.
    //
    // `repeatable` is separate from `kind`: kind says how the answer is decoded, repeatable
    // whether the driver may send the statement a second time after a stale connection lost
    // its answer. MySQL and PostgreSQL decode a DML as `rows` (their translators build the
    // count carrier), so the kind alone cannot tell a read from a write.
    struct statement_t {
        std::string sql;
        result_kind kind{result_kind::rows};
        decode_hints_t hints{};
        std::chrono::milliseconds timeout{0}; // 0: the driver's own statement deadline
        bool repeatable{true};                // false: a DML or DDL, whose lost answer may hide an applied change

        static statement_t rows(std::string sql, decode_hints_t hints = {}) {
            return {std::move(sql), result_kind::rows, std::move(hints), {}, true};
        }
        static statement_t schema(std::string sql, decode_hints_t hints = {}) {
            return {std::move(sql), result_kind::schema, std::move(hints), {}, true};
        }
        static statement_t affected_rows(std::string sql) {
            return {std::move(sql), result_kind::affected_rows, {}, {}, false};
        }

        // The same statement, never repeated by the driver.
        statement_t once() && {
            repeatable = false;
            return std::move(*this);
        }
    };

    // A statement's answer, already decoded. Which member is set follows the
    // statement's kind; a `schema` statement answered without columns leaves `schema`
    // empty and the caller words the error (discovery and prepare say different things).
    struct statement_result_t {
        std::optional<components::vector::data_chunk_t> rows;
        std::optional<components::types::complex_logical_type> schema;
        uint64_t affected_rows{0};
    };

    using statement_outcome_t = core::result_wrapper_t<statement_result_t>;

} // namespace conn
