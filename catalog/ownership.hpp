// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

// What user DDL / DML must leave alone. A client's CREATE / DROP DATABASE never reaches here: it is a
// schema of local, or refused (session's plan::place_namespace_ddl), so no engine database of a
// connection or of OtterStax is a client's to create or drop. Matching is ASCII case-insensitive: an
// unquoted identifier reaches the engine lower-cased.
namespace otterstax::catalog {
    // A relation of `federation` (OtterStax's tables, Kafka's bookkeeping) written — `target`'s
    // database is `federation` — and the schema of the Kafka objects, local.kafka, dropped
    // (`drops_schema`; a client's DROP SCHEMA names a schema of local, an empty database counting as
    // local). invalid_parameter `<what>
    // is owned by OtterStax ...` (the message on `resource`), no_error otherwise. A relation name
    // the session's place completes into `federation` is not seen.
    [[nodiscard]] core::error_t
    check_write_ownership(std::pmr::memory_resource* resource, const qualified_name_t& target, bool drops_schema);
} // namespace otterstax::catalog
