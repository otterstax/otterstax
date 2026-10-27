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
// whose message names the section and the entry, and so is a backend alias
// (mysql / postgresql / clickhouse) that is not an unquoted SQL identifier
// ([a-z_][a-z0-9_]*) or that another backend entry already uses. `resource`
// owns the error message.
core::result_wrapper_t<ConnectionsConfig> parse_connections(const YAML::Node& connections_node,
                                                            std::pmr::memory_resource* resource);

// Parses a `pool:` block; `field` names it in error messages ("pool" or "service.file_transfers").
// Missing node -> defaults. Unknown/non-integer setting, min_size < 1, max_size < min_size,
// idle_timeout_ms/queue_timeout_ms <= 0, check_after_idle_ms/connect_backoff_ms < 0 -> invalid_parameter.
core::result_wrapper_t<PoolDesc> parse_pool(const YAML::Node& node,
                                            const std::string& field,
                                            std::pmr::memory_resource* resource);

// Per-descriptor validation: the first missing required field or malformed port, else nullopt.
// Required: alias/host/username/database (mysql, postgresql, clickhouse); alias/access_key/secret_key (s3).
// Optional port must be decimal 1..65535; statement_timeout_ms/connect_timeout_ms must be positive.
std::optional<std::string> validation_error(const MysqlConnectionDesc& c);
std::optional<std::string> validation_error(const PgConnectionDesc& c);
std::optional<std::string> validation_error(const ChConnectionDesc& c);
std::optional<std::string> validation_error(const S3CredentialDesc& c);

}  // namespace config
