// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>

// The database names user DDL must leave alone. CREATE DATABASE / DROP DATABASE
// are engine-local by grammar, but three kinds of name belong to OtterStax:
//
//  - a connection uid of the federation registry: an engine database of that
//    name would capture the connection's remote names before the federation is
//    asked (the engine resolves a local `uid.table` / `uid.schema.table` first);
//  - `kafka`, the Kafka object database (KafkaManager);
//  - `federation`, the metadata database (otterbrix/federation/metadata.hpp).
//
// Matching is ASCII case-insensitive: an unquoted identifier reaches the engine
// lower-cased while a uid keeps its configured spelling.
namespace otterstax::catalog {

    // invalid_parameter `database '<name>' is owned by connection '<owner>'` for
    // an owned name (the owner: the uid as config.yaml spells it, "kafka" or
    // "federation"; the message lives on `resource`), no_error otherwise.
    core::error_t check_database_ownership(std::pmr::memory_resource* resource, std::string_view name);

} // namespace otterstax::catalog
