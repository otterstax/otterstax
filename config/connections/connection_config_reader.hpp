// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connection_config.hpp"

#include <core/result_wrapper.hpp>

#include <yaml-cpp/yaml.h>

#include <memory_resource>
#include <optional>
#include <string>

namespace config {

// Parse the `connections:` subtree of the single server config file (config.yaml)
// into a ConnectionsConfig. Sections (mysql / postgresql / clickhouse / s3) are
// each optional and default to an empty vector; a null/missing node yields an
// empty config, so the server still starts with no remote backends registered.
// Every entry is validated before the config is returned: the first incomplete
// entry, malformed port or field that is not a scalar is an invalid_parameter
// whose message names the section and the entry. `resource` owns the error
// message.
core::result_wrapper_t<ConnectionsConfig> parse_connections(const YAML::Node& connections_node,
                                                            std::pmr::memory_resource* resource);

// Per-descriptor validation. Returns a human-readable message naming the first
// missing required field or the malformed port, or std::nullopt if the entry is
// complete enough to register. Parsing itself is lenient (missing keys become
// empty strings); these checks are what parse_connections turns into its
// invalid_parameter before any connector is touched.
//   mysql/clickhouse : alias, host, username, database (port optional — defaults)
//   postgresql       : alias, host, username, database (port/schema default)
//   s3               : alias, access_key, secret_key
// A non-empty backend port must be a decimal integer in 1..65535 — the same
// rule the connectors apply when opening the backend.
std::optional<std::string> validation_error(const MysqlConnectionDesc& c);
std::optional<std::string> validation_error(const PgConnectionDesc& c);
std::optional<std::string> validation_error(const ChConnectionDesc& c);
std::optional<std::string> validation_error(const S3CredentialDesc& c);

}  // namespace config
