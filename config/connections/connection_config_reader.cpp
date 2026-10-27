// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_config_reader.hpp"

#include "utility/parse_port.hpp"
#include "yaml_scalar.hpp"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace config {
    namespace {
        // An invalid entry is reported as an invalid_parameter whose message names the
        // section and the entry, so the operator can find it in the file.
        core::error_t invalid_connection(const char* kind,
                                         const std::string& entry,
                                         const std::string& what,
                                         std::pmr::memory_resource* resource) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{(std::string("invalid ") + kind + " connection (" + entry + "): " + what).c_str(),
                                 resource});
        }

        // Reads the scalar fields of one connection entry. A missing key yields the
        // fallback; a key whose value is not a scalar is recorded as the entry's error
        // and every later read yields an empty string, so the first bad field is the
        // one reported.
        class entry_reader_t {
        public:
            entry_reader_t(const YAML::Node& node,
                           const char* kind,
                           std::size_t index,
                           std::pmr::memory_resource* resource)
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
                    fail(value.error());
                    return "";
                }
                return std::move(value.value());
            }

            int integer(const char* key, int fallback) {
                if (error_.contains_error() || !node_[key]) {
                    return fallback;
                }
                auto value = yaml_scalar<int>(node_[key], key, resource_);
                if (value.has_error()) {
                    fail(value.error());
                    return fallback;
                }
                return value.value();
            }

            // The entry's optional `pool:` block; the defaults when it has none.
            PoolDesc pool() {
                if (error_.contains_error()) {
                    return {};
                }
                auto value = parse_pool(node_["pool"], "pool", resource_);
                if (value.has_error()) {
                    fail(value.error());
                    return {};
                }
                return value.value();
            }

            core::error_t take_error() { return std::move(error_); }

        private:
            void fail(const core::error_t& error) {
                error_ = invalid_connection(kind_,
                                            "entry #" + std::to_string(index_ + 1),
                                            std::string{error.what.data(), error.what.size()},
                                            resource_);
            }

            const YAML::Node& node_;
            const char* kind_;
            std::size_t index_;
            std::pmr::memory_resource* resource_;
            core::error_t error_{core::error_t::no_error()};
        };

        // Returns an error naming the first empty required field, or nullopt. `fields`
        // pairs each required field name with its value.
        std::optional<std::string>
        first_missing(std::initializer_list<std::pair<const char*, const std::string*>> fields) {
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
        std::optional<std::string>
        backend_validation_error(std::initializer_list<std::pair<const char*, const std::string*>> required,
                                 const std::string& port) {
            if (auto missing = first_missing(required)) {
                return missing;
            }
            return port_error(port);
        }

        std::optional<std::string> positive_error(const char* field, int value) {
            if (value <= 0) {
                return std::string("'") + field + "' must be positive, got " + std::to_string(value);
            }
            return std::nullopt;
        }

        // Fail fast: an incomplete connection must abort startup, not be silently
        // skipped — a half-configured backend is a config bug the operator has to fix.
        template<typename Desc>
        core::error_t validate(const Desc& d, const char* kind, std::pmr::memory_resource* resource) {
            if (auto err = validation_error(d)) {
                return invalid_connection(kind, "alias='" + d.alias + "'", *err, resource);
            }
            return core::error_t::no_error();
        }

        template<typename Desc>
        core::error_t
        validate_all(const std::vector<Desc>& descs, const char* kind, std::pmr::memory_resource* resource) {
            for (const auto& c : descs) {
                if (auto err = validate(c, kind, resource); err.contains_error()) {
                    return err;
                }
            }
            return core::error_t::no_error();
        }

        bool is_unquoted_identifier(const std::string& alias) {
            const auto lower_or_underscore = [](char c) { return (c >= 'a' && c <= 'z') || c == '_'; };
            const auto digit = [](char c) { return c >= '0' && c <= '9'; };
            return !alias.empty() && lower_or_underscore(alias.front()) &&
                   std::all_of(alias.begin(), alias.end(), [&](char c) { return lower_or_underscore(c) || digit(c); });
        }

        core::error_t validate_backend_aliases(const ConnectionsConfig& config, std::pmr::memory_resource* resource) {
            std::map<std::string_view, const char*> seen;
            const auto check = [&](const auto& descs, const char* kind) {
                for (const auto& desc : descs) {
                    const std::string entry = "alias='" + desc.alias + "'";
                    if (!is_unquoted_identifier(desc.alias)) {
                        return invalid_connection(
                            kind,
                            entry,
                            "an alias is written unquoted in SQL: lower-case letters, digits and '_', "
                            "not starting with a digit",
                            resource);
                    }
                    if (auto [first, inserted] = seen.emplace(desc.alias, kind); !inserted) {
                        return invalid_connection(kind,
                                                  entry,
                                                  std::string("the alias already names a ") + first->second +
                                                      " connection",
                                                  resource);
                    }
                }
                return core::error_t::no_error();
            };
            if (auto err = check(config.mysql, "mysql"); err.contains_error()) {
                return err;
            }
            if (auto err = check(config.postgresql, "postgresql"); err.contains_error()) {
                return err;
            }
            return check(config.clickhouse, "clickhouse");
        }
    } // namespace

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
                    .statement_timeout_ms = entry.integer("statement_timeout_ms", 90000),
                    .pool = entry.pool(),
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
                    .statement_timeout_ms = entry.integer("statement_timeout_ms", 90000),
                    .pool = entry.pool(),
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
                    .pool = entry.pool(),
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
                    .connect_timeout_ms = entry.integer("connect_timeout_ms", 1000),
                    .pool = entry.pool(),
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
        if (auto err = validate_backend_aliases(result, resource); err.contains_error()) {
            return err;
        }

        return result;
    }

    core::result_wrapper_t<PoolDesc>
    parse_pool(const YAML::Node& node, const std::string& field, std::pmr::memory_resource* resource) {
        PoolDesc pool;
        if (!node) {
            return pool;
        }
        const auto invalid = [&](const std::string& what) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{("field '" + field + "': " + what).c_str(), resource});
        };
        if (!node.IsMap()) {
            return invalid("expected a map of pool settings");
        }

        int min_size = static_cast<int>(pool.min_size);
        int max_size = static_cast<int>(pool.max_size);
        const std::pair<const char*, int*> settings[] = {
            {"min_size", &min_size},
            {"max_size", &max_size},
            {"idle_timeout_ms", &pool.idle_timeout_ms},
            {"check_after_idle_ms", &pool.check_after_idle_ms},
            {"queue_timeout_ms", &pool.queue_timeout_ms},
            {"connect_backoff_ms", &pool.connect_backoff_ms},
        };
        // A misspelt setting would otherwise leave its default in force unnoticed.
        for (const auto& kv : node) {
            auto key = yaml_scalar<std::string>(kv.first, field, resource);
            if (key.has_error()) {
                return key.convert_error<PoolDesc>();
            }
            const bool known = std::any_of(std::begin(settings), std::end(settings), [&](const auto& setting) {
                return key.value() == setting.first;
            });
            if (!known) {
                return invalid("unknown pool setting '" + key.value() + "'");
            }
        }
        for (const auto& [key, out] : settings) {
            if (!node[key]) {
                continue;
            }
            auto value = yaml_scalar<int>(node[key], field + "." + key, resource);
            if (value.has_error()) {
                return value.convert_error<PoolDesc>();
            }
            *out = value.value();
        }

        // min_size >= 1: startup discovery needs a connection, and the pool keeps it.
        if (min_size < 1) {
            return invalid("'min_size' must be at least 1, got " + std::to_string(min_size));
        }
        if (max_size < min_size) {
            return invalid("'max_size' (" + std::to_string(max_size) + ") must not be below 'min_size' (" +
                           std::to_string(min_size) + ")");
        }
        if (auto err = positive_error("idle_timeout_ms", pool.idle_timeout_ms)) {
            return invalid(*err);
        }
        if (auto err = positive_error("queue_timeout_ms", pool.queue_timeout_ms)) {
            return invalid(*err);
        }
        if (pool.check_after_idle_ms < 0) {
            return invalid("'check_after_idle_ms' must not be negative");
        }
        if (pool.connect_backoff_ms < 0) {
            return invalid("'connect_backoff_ms' must not be negative");
        }
        pool.min_size = static_cast<std::size_t>(min_size);
        pool.max_size = static_cast<std::size_t>(max_size);
        return pool;
    }

    // port is intentionally not required: an empty port makes the driver fall
    // back to the backend's default port. A non-empty port must be a valid TCP
    // port.
    std::optional<std::string> validation_error(const MysqlConnectionDesc& c) {
        if (auto err = backend_validation_error(
                {{"alias", &c.alias}, {"host", &c.host}, {"username", &c.username}, {"database", &c.database}},
                c.port)) {
            return err;
        }
        return positive_error("statement_timeout_ms", c.statement_timeout_ms);
    }

    std::optional<std::string> validation_error(const PgConnectionDesc& c) {
        if (auto err = backend_validation_error(
                {{"alias", &c.alias}, {"host", &c.host}, {"username", &c.username}, {"database", &c.database}},
                c.port)) {
            return err;
        }
        return positive_error("statement_timeout_ms", c.statement_timeout_ms);
    }

    std::optional<std::string> validation_error(const ChConnectionDesc& c) {
        return backend_validation_error(
            {{"alias", &c.alias}, {"host", &c.host}, {"username", &c.username}, {"database", &c.database}},
            c.port);
    }

    std::optional<std::string> validation_error(const S3CredentialDesc& c) {
        if (auto err = first_missing({{"alias", &c.alias}, {"access_key", &c.access_key}, {"secret_key", &c.secret_key}})) {
            return err;
        }
        return positive_error("connect_timeout_ms", c.connect_timeout_ms);
    }
} // namespace config
