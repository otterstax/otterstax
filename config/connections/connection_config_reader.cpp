// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_config_reader.hpp"

#include "utility/parse_port.hpp"
#include "yaml_scalar.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>

namespace config {

namespace {

// An invalid entry is reported as an invalid_parameter whose message names the
// section and the entry, so the operator can find it in the file.
core::error_t invalid_connection(const char* kind, const std::string& entry, const std::string& what,
                                 std::pmr::memory_resource* resource) {
    return core::error_t(core::error_code_t::invalid_parameter,
                         std::pmr::string{(std::string("invalid ") + kind + " connection (" + entry + "): " + what).c_str(),
                                          resource});
}

// Reads the scalar fields of one connection entry. A missing key yields the
// fallback; a key whose value is not a scalar is recorded as the entry's error
// and every later read yields an empty string, so the first bad field is the
// one reported. An entry that is a scalar is that error before any key is read
// (yaml-cpp throws on a subscript of a scalar).
class entry_reader_t {
public:
    entry_reader_t(const YAML::Node& node, const char* kind, std::size_t index, std::pmr::memory_resource* resource)
        : node_(node)
        , kind_(kind)
        , index_(index)
        , resource_(resource) {
        if (node_.IsScalar()) {
            error_ = invalid_connection(kind_, "entry #" + std::to_string(index_ + 1), "expected a mapping of keys",
                                        resource_);
        }
    }

    std::string str(const char* key, const std::string& fallback = "") {
        if (error_.contains_error() || !node_[key]) {
            return fallback;
        }
        auto value = yaml_scalar<std::string>(node_[key], key, resource_);
        if (value.has_error()) {
            error_ = invalid_connection(kind_, "entry #" + std::to_string(index_ + 1),
                                        std::string{value.error().what.data(), value.error().what.size()}, resource_);
            return "";
        }
        return std::move(value.value());
    }

    // An optional integer field: a missing key yields the fallback, a value that
    // is not an integer is the entry's error (reported like a non-scalar field).
    int integer(const char* key, int fallback) {
        if (error_.contains_error() || !node_[key]) {
            return fallback;
        }
        auto value = yaml_scalar<int>(node_[key], key, resource_);
        if (value.has_error()) {
            error_ = invalid_connection(kind_, "entry #" + std::to_string(index_ + 1),
                                        std::string{value.error().what.data(), value.error().what.size()}, resource_);
            return fallback;
        }
        return value.value();
    }

    core::error_t take_error() { return std::move(error_); }

private:
    const YAML::Node& node_;
    const char* kind_;
    std::size_t index_;
    std::pmr::memory_resource* resource_;
    core::error_t error_{core::error_t::no_error()};
};

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

// The folding the federation looks a uid up by (otterstax::federation::
// fold_identifier, otterbrix/federation/metadata.cpp: ASCII lower case): two
// aliases equal under it name one connection. Spelled here because config links
// nothing of the federation.
std::string fold_alias(const std::string& alias) {
    std::string out{alias};
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

// The names of the engine databases OtterStax keeps for itself (any case): no
// connection may take one (reserved_connection_aliases).
std::optional<std::string> reserved_alias_error(const std::string& alias) {
    for (const auto& [name, what] : reserved_connection_aliases) {
        if (fold_alias(alias) == fold_alias(std::string{name})) {
            return "alias '" + alias + "' is reserved: '" + std::string{name} + "' is " + std::string{what};
        }
    }
    return std::nullopt;
}

// An empty port means "driver default"; anything else must satisfy the same
// rule the connectors apply when they open the backend (decimal, 1..65535), so
// a bad port is rejected here at startup instead of at the first open.
std::optional<std::string> port_error(const std::string& port) {
    if (port.empty()) {
        return std::nullopt;
    }
    auto parsed = otterstax::parse_port(port, std::pmr::new_delete_resource());
    if (parsed.has_error()) {
        return std::string{parsed.error().what.data(), parsed.error().what.size()};
    }
    return std::nullopt;
}

// Required fields first, then the reserved alias, then the port: the first
// failing check names the error, so an incomplete entry is reported as such even
// if its port is bad.
std::optional<std::string> backend_validation_error(
    std::initializer_list<std::pair<const char*, const std::string*>> required,
    const std::string& alias,
    const std::string& port) {
    if (auto missing = first_missing(required)) {
        return missing;
    }
    if (auto reserved = reserved_alias_error(alias)) {
        return reserved;
    }
    return port_error(port);
}

// pool_size is optional (default 1); present, it must be at least 1.
std::optional<std::string> pool_size_error(int pool_size) {
    if (pool_size < 1) {
        return "pool_size must be an integer >= 1 (got " + std::to_string(pool_size) + ")";
    }
    return std::nullopt;
}

// Fail fast: an incomplete connection must abort startup, not be silently
// skipped — a half-configured backend is a config bug the operator has to fix.
template <typename Desc>
core::error_t validate(const Desc& d, const char* kind, std::pmr::memory_resource* resource) {
    if (auto err = validation_error(d)) {
        return invalid_connection(kind, "alias='" + d.alias + "'", *err, resource);
    }
    return core::error_t::no_error();
}

template <typename Desc>
core::error_t validate_all(const std::vector<Desc>& descs, const char* kind, std::pmr::memory_resource* resource) {
    for (const auto& c : descs) {
        if (auto err = validate(c, kind, resource); err.contains_error()) {
            return err;
        }
    }
    return core::error_t::no_error();
}

// A backend alias is the connection uid every federated name starts with and
// the federation resolves it case-insensitively, so the mysql / postgresql /
// clickhouse aliases share one namespace under that folding: a second entry
// under an alias already taken would leave one of the two backends unreachable
// (and its remote tables recorded under the other's uid). s3 aliases are
// another namespace — the `s3_alias` option of CREATE EXTERNAL TABLE / COPY ...
// TO, looked up exactly (conn::s3::ConnectorManager), where a second entry would
// silently replace the first.
core::error_t duplicate_alias_error(const ConnectionsConfig& config, std::pmr::memory_resource* resource) {
    std::unordered_map<std::string, std::string> backends; // folded alias -> "section alias='...'"
    auto take = [&](const char* kind, const std::string& alias) -> core::error_t {
        auto [it, inserted] = backends.emplace(fold_alias(alias), std::string{kind} + " alias='" + alias + "'");
        if (inserted) {
            return core::error_t::no_error();
        }
        return invalid_connection(kind,
                                  "alias='" + alias + "'",
                                  "alias is already taken by the " + it->second +
                                      " entry (backend aliases are compared case-insensitively across mysql, "
                                      "postgresql and clickhouse)",
                                  resource);
    };
    for (const auto& c : config.mysql) {
        if (auto err = take("mysql", c.alias); err.contains_error()) {
            return err;
        }
    }
    for (const auto& c : config.postgresql) {
        if (auto err = take("postgresql", c.alias); err.contains_error()) {
            return err;
        }
    }
    for (const auto& c : config.clickhouse) {
        if (auto err = take("clickhouse", c.alias); err.contains_error()) {
            return err;
        }
    }
    std::unordered_map<std::string, std::size_t> s3;
    for (std::size_t i = 0; i < config.s3.size(); ++i) {
        if (!s3.emplace(config.s3[i].alias, i).second) {
            return invalid_connection("s3",
                                      "alias='" + config.s3[i].alias + "'",
                                      "alias is already taken by another s3 entry",
                                      resource);
        }
    }
    return core::error_t::no_error();
}

}  // namespace

core::result_wrapper_t<ConnectionsConfig> parse_connections(const YAML::Node& connections_node,
                                                            std::pmr::memory_resource* resource) {
    ConnectionsConfig result;

    if (!connections_node || !connections_node.IsMap()) {
        return result;
    }

    if (connections_node["mysql"] && connections_node["mysql"].IsSequence()) {
        std::size_t index = 0;
        for (const auto& node : connections_node["mysql"]) {
            entry_reader_t entry{node, "mysql", index++, resource};
            MysqlConnectionDesc desc{
                .alias = entry.str("alias"),
                .host = entry.str("host"),
                .port = entry.str("port"),
                .username = entry.str("username"),
                .password = entry.str("password"),
                .database = entry.str("database"),
                .table = entry.str("table"),
                .pool_size = entry.integer("pool_size", 1),
            };
            if (auto err = entry.take_error(); err.contains_error()) {
                return err;
            }
            result.mysql.push_back(std::move(desc));
        }
    }

    if (connections_node["postgresql"] && connections_node["postgresql"].IsSequence()) {
        std::size_t index = 0;
        for (const auto& node : connections_node["postgresql"]) {
            entry_reader_t entry{node, "postgresql", index++, resource};
            PgConnectionDesc desc{
                .alias = entry.str("alias"),
                .host = entry.str("host"),
                .port = entry.str("port"),
                .username = entry.str("username"),
                .password = entry.str("password"),
                .database = entry.str("database"),
                .schema = entry.str("schema", "public"),
                .table = entry.str("table"),
                .pool_size = entry.integer("pool_size", 1),
            };
            if (auto err = entry.take_error(); err.contains_error()) {
                return err;
            }
            result.postgresql.push_back(std::move(desc));
        }
    }

    if (connections_node["clickhouse"] && connections_node["clickhouse"].IsSequence()) {
        std::size_t index = 0;
        for (const auto& node : connections_node["clickhouse"]) {
            entry_reader_t entry{node, "clickhouse", index++, resource};
            ChConnectionDesc desc{
                .alias = entry.str("alias"),
                .host = entry.str("host"),
                .port = entry.str("port"),
                .username = entry.str("username"),
                .password = entry.str("password"),
                .database = entry.str("database"),
                .table = entry.str("table"),
                .pool_size = entry.integer("pool_size", 1),
            };
            if (auto err = entry.take_error(); err.contains_error()) {
                return err;
            }
            result.clickhouse.push_back(std::move(desc));
        }
    }

    if (connections_node["s3"] && connections_node["s3"].IsSequence()) {
        std::size_t index = 0;
        for (const auto& node : connections_node["s3"]) {
            entry_reader_t entry{node, "s3", index++, resource};
            S3CredentialDesc desc{
                .alias = entry.str("alias"),
                .access_key = entry.str("access_key"),
                .secret_key = entry.str("secret_key"),
                .region = entry.str("region"),
                .session_token = entry.str("session_token"),
                .endpoint = entry.str("endpoint"),
            };
            if (auto err = entry.take_error(); err.contains_error()) {
                return err;
            }
            result.s3.push_back(std::move(desc));
        }
    }

    // Validate every parsed entry up front: the first incomplete connection is
    // the result, so the caller (load_config → main) aborts startup
    // rather than coming up with a broken backend.
    if (auto err = validate_all(result.mysql, "mysql", resource); err.contains_error()) {
        return err;
    }
    if (auto err = validate_all(result.postgresql, "postgresql", resource); err.contains_error()) {
        return err;
    }
    if (auto err = validate_all(result.clickhouse, "clickhouse", resource); err.contains_error()) {
        return err;
    }
    if (auto err = validate_all(result.s3, "s3", resource); err.contains_error()) {
        return err;
    }
    if (auto err = duplicate_alias_error(result, resource); err.contains_error()) {
        return err;
    }

    return result;
}

// port is intentionally not required: an empty port makes the connector fall
// back to the driver's default (see the addConnection implementations). A
// non-empty port must be a valid TCP port.
std::optional<std::string> validation_error(const MysqlConnectionDesc& c) {
    if (auto err = backend_validation_error({{"alias", &c.alias},
                                             {"host", &c.host},
                                             {"username", &c.username},
                                             {"database", &c.database}},
                                            c.alias,
                                            c.port)) {
        return err;
    }
    return pool_size_error(c.pool_size);
}

std::optional<std::string> validation_error(const PgConnectionDesc& c) {
    if (auto err = backend_validation_error({{"alias", &c.alias},
                                             {"host", &c.host},
                                             {"username", &c.username},
                                             {"database", &c.database}},
                                            c.alias,
                                            c.port)) {
        return err;
    }
    return pool_size_error(c.pool_size);
}

std::optional<std::string> validation_error(const ChConnectionDesc& c) {
    if (auto err = backend_validation_error({{"alias", &c.alias},
                                             {"host", &c.host},
                                             {"username", &c.username},
                                             {"database", &c.database}},
                                            c.alias,
                                            c.port)) {
        return err;
    }
    return pool_size_error(c.pool_size);
}

std::optional<std::string> validation_error(const S3CredentialDesc& c) {
    if (auto missing = first_missing({{"alias", &c.alias},
                                      {"access_key", &c.access_key},
                                      {"secret_key", &c.secret_key}})) {
        return missing;
    }
    return reserved_alias_error(c.alias);
}

}  // namespace config
