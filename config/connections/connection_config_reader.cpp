// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_config_reader.hpp"

#include "utility/parse_port.hpp"
#include "yaml_scalar.hpp"

#include <cstddef>
#include <memory_resource>
#include <string>
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
// one reported.
class entry_reader_t {
public:
    entry_reader_t(const YAML::Node& node, const char* kind, std::size_t index, std::pmr::memory_resource* resource)
        : node_(node)
        , kind_(kind)
        , index_(index)
        , resource_(resource) {}

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

// Required fields first, then the port: the first failing check names the
// error, so an incomplete entry is reported as such even if its port is bad.
std::optional<std::string> backend_validation_error(
    std::initializer_list<std::pair<const char*, const std::string*>> required, const std::string& port) {
    if (auto missing = first_missing(required)) {
        return missing;
    }
    return port_error(port);
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
    // the result, so the caller (ConfigReader::load → main) aborts startup
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

    return result;
}

// port is intentionally not required: an empty port makes the connector fall
// back to the driver's default (see the addConnection implementations). A
// non-empty port must be a valid TCP port.
std::optional<std::string> validation_error(const MysqlConnectionDesc& c) {
    return backend_validation_error({{"alias", &c.alias},
                                     {"host", &c.host},
                                     {"username", &c.username},
                                     {"database", &c.database}},
                                    c.port);
}

std::optional<std::string> validation_error(const PgConnectionDesc& c) {
    return backend_validation_error({{"alias", &c.alias},
                                     {"host", &c.host},
                                     {"username", &c.username},
                                     {"database", &c.database}},
                                    c.port);
}

std::optional<std::string> validation_error(const ChConnectionDesc& c) {
    return backend_validation_error({{"alias", &c.alias},
                                     {"host", &c.host},
                                     {"username", &c.username},
                                     {"database", &c.database}},
                                    c.port);
}

std::optional<std::string> validation_error(const S3CredentialDesc& c) {
    return first_missing({{"alias", &c.alias},
                          {"access_key", &c.access_key},
                          {"secret_key", &c.secret_key}});
}

}  // namespace config
