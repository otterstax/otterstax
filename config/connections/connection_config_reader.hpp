// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connection_config.hpp"
#include "otterbrix/federation/metadata.hpp"

#include <core/result_wrapper.hpp>

#include <yaml-cpp/yaml.h>

#include <array>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace config {

// The connection alias no `connections:` entry may take, compared case-insensitively:
// it is the engine database that holds the remote-table metadata (federation.remote_tables /
// remote_columns). An alias is the first qualifier of a federated name,
// so a connection under it would shadow that database. (A header-only constant: config
// links nothing of the federation.)
inline constexpr std::string_view reserved_connection_alias = otterstax::federation::metadata_database;

// The engine database of the Kafka objects (KafkaManager; the same name as
// otterstax::kafka::KAFKA_DATABASE_NAME, which config cannot include — a static_assert
// in backends/backends.cpp pins the two together). A connection under it would capture
// `kafka.<object>`.
inline constexpr std::string_view reserved_kafka_alias = "kafka";

// Every alias no `connections:` entry may take (any case, any section), with what
// the name is.
inline constexpr std::array<std::pair<std::string_view, std::string_view>, 2> reserved_connection_aliases{{
    {reserved_connection_alias, "the engine database of the remote-table metadata"},
    {reserved_kafka_alias, "the engine database of the Kafka objects"},
}};

// Parse the `connections:` subtree of the single server config file (config.yaml)
// into a ConnectionsConfig. Sections (mysql / postgresql / clickhouse / s3) are
// each optional and default to an empty vector; a null/missing node yields an
// empty config, so the server still starts with no remote backends registered.
// Every entry is validated before the config is returned: the first incomplete
// entry, malformed port or field that is not a scalar is an invalid_parameter
// whose message names the section and the entry. Then the aliases: a backend
// alias (mysql / postgresql / clickhouse) equal to another under the
// federation's case folding, or an s3 alias equal to another s3 alias, is an
// invalid_parameter naming both entries. `resource` owns the error message.
core::result_wrapper_t<ConnectionsConfig> parse_connections(const YAML::Node& connections_node,
                                                            std::pmr::memory_resource* resource);

// Per-descriptor validation. Returns a human-readable message naming the first
// missing required field, the reserved alias or the malformed port, or
// std::nullopt if the entry is complete enough to register. Parsing itself is lenient (missing keys become
// empty strings); these checks are what parse_connections turns into its
// invalid_parameter before any connector is touched.
//   mysql/clickhouse : alias, host, username, database (port optional — defaults)
//   postgresql       : alias, host, username, database (port/schema default)
//   s3               : alias, access_key, secret_key
// In every section the alias must not be one of `reserved_connection_aliases`
// (`federation`, `kafka`) in any case.
// A non-empty backend port must be a decimal integer in 1..65535 — the same
// rule the connectors apply when opening the backend. mysql/postgresql/clickhouse
// take an optional integer `pool_size` (default 1) that must be >= 1; a value
// that is not an integer is rejected at parse time like a non-scalar field.
std::optional<std::string> validation_error(const MysqlConnectionDesc& c);
std::optional<std::string> validation_error(const PgConnectionDesc& c);
std::optional<std::string> validation_error(const ChConnectionDesc& c);
std::optional<std::string> validation_error(const S3CredentialDesc& c);

}  // namespace config
