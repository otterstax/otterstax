// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connection_config.hpp"

#include <yaml-cpp/yaml.h>

#include <optional>
#include <string>

namespace config {

// Parse the `connections:` subtree of the single server config file (config.yaml)
// into a ConnectionsConfig. Sections (mysql / postgresql / clickhouse / s3) are
// each optional and default to an empty vector; a null/missing node yields an
// empty config, so the server still starts with no remote backends registered.
ConnectionsConfig parse_connections(const YAML::Node& connections_node);

// Required-field validation for a single descriptor. Returns a human-readable
// message naming the first missing required field, or std::nullopt if the entry
// is complete enough to register. Parsing itself is lenient (missing keys become
// empty strings); these functions are what the registrar uses to reject an
// incomplete entry before attempting to open it.
//   mysql/clickhouse : alias, host, username, database (port optional — defaults)
//   postgresql       : alias, host, username, database (port/schema default)
//   s3               : alias, access_key, secret_key
std::optional<std::string> validation_error(const MysqlConnectionDesc& c);
std::optional<std::string> validation_error(const PgConnectionDesc& c);
std::optional<std::string> validation_error(const ChConnectionDesc& c);
std::optional<std::string> validation_error(const S3CredentialDesc& c);

}  // namespace config
