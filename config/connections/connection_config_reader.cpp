// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_config_reader.hpp"

#include <stdexcept>

namespace config {

namespace {

std::string get_str(const YAML::Node& node, const char* key, const std::string& fallback = "") {
    return node[key] ? node[key].as<std::string>() : fallback;
}

// Returns an error naming the first empty required field, or nullopt. `fields`
// pairs each required field name with its value.
std::optional<std::string> first_missing(
    std::initializer_list<std::pair<const char*, const std::string*>> fields) {
    for (const auto& [name, value] : fields) {
        if (value->empty()) {
            return std::string("missing required field '") + name + "'";
        }
    }
    return std::nullopt;
}

// Fail fast: an incomplete connection must abort startup, not be silently
// skipped — a half-configured backend is a config bug the operator has to fix.
template <typename Desc>
void validate_or_throw(const Desc& d, const char* kind) {
    if (auto err = validation_error(d)) {
        throw std::runtime_error(std::string("invalid ") + kind + " connection (alias='" +
                                 d.alias + "'): " + *err);
    }
}

}  // namespace

ConnectionsConfig parse_connections(const YAML::Node& connections_node) {
    ConnectionsConfig result;

    if (!connections_node || !connections_node.IsMap()) {
        return result;
    }

    if (connections_node["mysql"] && connections_node["mysql"].IsSequence()) {
        for (const auto& node : connections_node["mysql"]) {
            result.mysql.push_back(MysqlConnectionDesc{
                .alias = get_str(node, "alias"),
                .host = get_str(node, "host"),
                .port = get_str(node, "port"),
                .username = get_str(node, "username"),
                .password = get_str(node, "password"),
                .database = get_str(node, "database"),
                .table = get_str(node, "table"),
            });
        }
    }

    if (connections_node["postgresql"] && connections_node["postgresql"].IsSequence()) {
        for (const auto& node : connections_node["postgresql"]) {
            result.postgresql.push_back(PgConnectionDesc{
                .alias = get_str(node, "alias"),
                .host = get_str(node, "host"),
                .port = get_str(node, "port"),
                .username = get_str(node, "username"),
                .password = get_str(node, "password"),
                .database = get_str(node, "database"),
                .schema = get_str(node, "schema", "public"),
                .table = get_str(node, "table"),
            });
        }
    }

    if (connections_node["clickhouse"] && connections_node["clickhouse"].IsSequence()) {
        for (const auto& node : connections_node["clickhouse"]) {
            result.clickhouse.push_back(ChConnectionDesc{
                .alias = get_str(node, "alias"),
                .host = get_str(node, "host"),
                .port = get_str(node, "port"),
                .username = get_str(node, "username"),
                .password = get_str(node, "password"),
                .database = get_str(node, "database"),
                .table = get_str(node, "table"),
            });
        }
    }

    if (connections_node["s3"] && connections_node["s3"].IsSequence()) {
        for (const auto& node : connections_node["s3"]) {
            result.s3.push_back(S3CredentialDesc{
                .alias = get_str(node, "alias"),
                .access_key = get_str(node, "access_key"),
                .secret_key = get_str(node, "secret_key"),
                .region = get_str(node, "region"),
                .session_token = get_str(node, "session_token"),
                .endpoint = get_str(node, "endpoint"),
            });
        }
    }

    // Validate every parsed entry up front: any incomplete connection throws
    // here, so the caller (ConfigReader::load → main) aborts startup rather than
    // coming up with a broken backend.
    for (const auto& c : result.mysql) validate_or_throw(c, "mysql");
    for (const auto& c : result.postgresql) validate_or_throw(c, "postgresql");
    for (const auto& c : result.clickhouse) validate_or_throw(c, "clickhouse");
    for (const auto& c : result.s3) validate_or_throw(c, "s3");

    return result;
}

// port is intentionally not required: an empty port makes the connector fall
// back to the driver's default (see the addConnection implementations).
std::optional<std::string> validation_error(const MysqlConnectionDesc& c) {
    return first_missing({{"alias", &c.alias},
                          {"host", &c.host},
                          {"username", &c.username},
                          {"database", &c.database}});
}

std::optional<std::string> validation_error(const PgConnectionDesc& c) {
    return first_missing({{"alias", &c.alias},
                          {"host", &c.host},
                          {"username", &c.username},
                          {"database", &c.database}});
}

std::optional<std::string> validation_error(const ChConnectionDesc& c) {
    return first_missing({{"alias", &c.alias},
                          {"host", &c.host},
                          {"username", &c.username},
                          {"database", &c.database}});
}

std::optional<std::string> validation_error(const S3CredentialDesc& c) {
    return first_missing({{"alias", &c.alias},
                          {"access_key", &c.access_key},
                          {"secret_key", &c.secret_key}});
}

}  // namespace config
