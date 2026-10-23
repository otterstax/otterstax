// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "config/config.hpp"
#include "config/connections/connection_config_reader.hpp"

#include <catch2/catch_all.hpp>
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace {
    std::pmr::memory_resource* resource() { return std::pmr::new_delete_resource(); }

    // Parse a YAML string and return its top-level `connections` node.
    YAML::Node connections_node(const std::string& yaml) { return YAML::Load(yaml)["connections"]; }

    // parse_connections on a node that must be accepted; the descriptors it produced.
    config::ConnectionsConfig parse_ok(const YAML::Node& node) {
        auto parsed = config::parse_connections(node, resource());
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value());
    }

    // parse_connections on a node that must be rejected; the error it produced.
    core::error_t parse_error(const YAML::Node& node) {
        auto parsed = config::parse_connections(node, resource());
        REQUIRE(parsed.has_error());
        return parsed.error();
    }

    // ConfigReader::load on a file that must be accepted; the whole ServiceConfig.
    config::ServiceConfig load_ok(const std::string& path) {
        config::ConfigReader reader{resource()};
        auto loaded = reader.load(path);
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

    // ConfigReader::load on a file that must be rejected; the error it produced.
    core::error_t load_error(const std::string& path) {
        config::ConfigReader reader{resource()};
        auto loaded = reader.load(path);
        REQUIRE(loaded.has_error());
        return loaded.error();
    }

    bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
    }

    // Write `content` to a unique temp file and return its path (used for the
    // whole-file ConfigReader tests).
    std::string write_temp(const std::string& content) {
        static std::atomic<int> counter{0};
        auto path = std::filesystem::temp_directory_path() / ("otterstax_cfg_" + std::to_string(counter++) + ".yaml");
        std::ofstream out(path);
        out << content;
        out.close();
        return path.string();
    }
} // namespace

// ── parse_connections (the `connections:` subtree) ───────────────────────────

TEST_CASE("parse_connections: full config with every section") {
    const std::string yaml = R"(
connections:
  mysql:
    - alias: m1
      host: mariadb1
      port: "3306"
      username: user1
      password: pass1
      database: db1
      table: t1
    - alias: m2
      host: mariadb2
      port: "3307"
      username: user2
      password: pass2
      database: db2
      table: ""
  postgresql:
    - alias: pg1
      host: postgres1
      port: "5432"
      username: pguser
      password: pgpass
      database: pgdb
      schema: myschema
      table: ""
  clickhouse:
    - alias: ch1
      host: clickhouse1
      port: "9000"
      username: chuser
      password: chpass
      database: chdb
      table: orders
  s3:
    - alias: rustfs1
      access_key: ak
      secret_key: sk
      region: us-east-1
      session_token: tok
      endpoint: rustfs:9000
)";

    auto cfg = parse_ok(connections_node(yaml));

    REQUIRE(cfg.mysql.size() == 2);
    REQUIRE(cfg.postgresql.size() == 1);
    REQUIRE(cfg.clickhouse.size() == 1);
    REQUIRE(cfg.s3.size() == 1);

    SECTION("mysql fields") {
        const auto& m1 = cfg.mysql[0];
        CHECK(m1.alias == "m1");
        CHECK(m1.host == "mariadb1");
        CHECK(m1.port == "3306");
        CHECK(m1.username == "user1");
        CHECK(m1.password == "pass1");
        CHECK(m1.database == "db1");
        CHECK(m1.table == "t1");
        CHECK(cfg.mysql[1].alias == "m2");
        CHECK(cfg.mysql[1].table.empty());
    }

    SECTION("postgresql fields incl. schema") {
        const auto& pg = cfg.postgresql[0];
        CHECK(pg.alias == "pg1");
        CHECK(pg.host == "postgres1");
        CHECK(pg.port == "5432");
        CHECK(pg.username == "pguser");
        CHECK(pg.password == "pgpass");
        CHECK(pg.database == "pgdb");
        CHECK(pg.schema == "myschema");
    }

    SECTION("clickhouse fields") {
        const auto& ch = cfg.clickhouse[0];
        CHECK(ch.alias == "ch1");
        CHECK(ch.host == "clickhouse1");
        CHECK(ch.port == "9000");
        CHECK(ch.database == "chdb");
        CHECK(ch.table == "orders");
    }

    SECTION("s3 fields") {
        const auto& s3 = cfg.s3[0];
        CHECK(s3.alias == "rustfs1");
        CHECK(s3.access_key == "ak");
        CHECK(s3.secret_key == "sk");
        CHECK(s3.region == "us-east-1");
        CHECK(s3.session_token == "tok");
        CHECK(s3.endpoint == "rustfs:9000");
    }
}

TEST_CASE("parse_connections: null/missing node yields an empty config") {
    YAML::Node absent; // null node
    auto cfg = parse_ok(absent);
    CHECK(cfg.mysql.empty());
    CHECK(cfg.postgresql.empty());
    CHECK(cfg.clickhouse.empty());
    CHECK(cfg.s3.empty());

    // A document without a `connections:` key -> node is null too.
    auto cfg2 = parse_ok(connections_node("mysql:\n  port: 8816\n"));
    CHECK(cfg2.mysql.empty());
}

TEST_CASE("parse_connections: missing sections default to empty vectors") {
    const std::string yaml = R"(
connections:
  mysql:
    - alias: only
      host: h
      port: "3306"
      username: u
      password: p
      database: d
      table: ""
)";
    auto cfg = parse_ok(connections_node(yaml));
    CHECK(cfg.mysql.size() == 1);
    CHECK(cfg.postgresql.empty());
    CHECK(cfg.clickhouse.empty());
    CHECK(cfg.s3.empty());
}

TEST_CASE("parse_connections: postgresql schema defaults to 'public' when omitted") {
    const std::string yaml = R"(
connections:
  postgresql:
    - alias: pg
      host: h
      port: "5432"
      username: u
      password: p
      database: d
      table: ""
)";
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.postgresql.size() == 1);
    CHECK(cfg.postgresql[0].schema == "public");
}

TEST_CASE("parse_connections: optional s3 fields default to empty") {
    const std::string yaml = R"(
connections:
  s3:
    - alias: aws
      access_key: ak
      secret_key: sk
)";
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.s3.size() == 1);
    const auto& s3 = cfg.s3[0];
    CHECK(s3.alias == "aws");
    CHECK(s3.access_key == "ak");
    CHECK(s3.secret_key == "sk");
    CHECK(s3.region.empty());
    CHECK(s3.session_token.empty());
    CHECK(s3.endpoint.empty());
}

TEST_CASE("parse_connections: multiple entries per section preserve order") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: a, host: h1, port: "1", username: u, password: p, database: d, table: "" }
    - { alias: b, host: h2, port: "2", username: u, password: p, database: d, table: "" }
    - { alias: c, host: h3, port: "3", username: u, password: p, database: d, table: "" }
)";
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.mysql.size() == 3);
    CHECK(cfg.mysql[0].alias == "a");
    CHECK(cfg.mysql[1].alias == "b");
    CHECK(cfg.mysql[2].alias == "c");
}

TEST_CASE("parse_connections: a backend alias is reachable from SQL and names one connection", "[names]") {
    struct row_t {
        const char* name;
        std::string entries;
        bool accepted;
    };
    const std::string my = R"({ host: h, port: "3306", username: u, password: p, database: d, table: "" })";
    const auto entry = [](const char* alias, const std::string& fields) {
        return "    - { alias: " + std::string{alias} + ", " + fields.substr(2);
    };
    const std::string pg = R"({ host: h, port: "5432", username: u, password: p, database: d, schema: s, table: "" })";
    const std::string ch = R"({ host: h, port: "9000", username: u, password: p, database: d, table: "" })";
    const std::vector<row_t> rows{
        {"distinct lower-case aliases across sections",
         "  mysql:\n" + entry("shop", my) + "\n  postgresql:\n" + entry("pg_2", pg) + "\n  clickhouse:\n" +
             entry("ch", ch) + "\n",
         true},
        {"same alias in two sections",
         "  mysql:\n" + entry("shop", my) + "\n  postgresql:\n" + entry("shop", pg) + "\n",
         false},
        {"same alias twice in one section", "  clickhouse:\n" + entry("ch", ch) + "\n" + entry("ch", ch) + "\n", false},
        {"alias with an upper-case letter", "  postgresql:\n" + entry("Shop", pg) + "\n", false},
        {"alias with a dot", "  postgresql:\n" + entry("pg.main", pg) + "\n", false},
        {"alias with a dash", "  mysql:\n" + entry("my-shop", my) + "\n", false},
        {"alias starting with a digit", "  clickhouse:\n" + entry("1ch", ch) + "\n", false},
        {"alias starting with an underscore", "  mysql:\n" + entry("_shop2", my) + "\n", true},
    };
    for (const auto& row : rows) {
        DYNAMIC_SECTION(row.name) {
            const std::string yaml = "connections:\n" + row.entries;
            INFO(yaml);
            auto parsed = config::parse_connections(connections_node(yaml), resource());
            if (row.accepted) {
                REQUIRE_FALSE(parsed.has_error());
            } else {
                REQUIRE(parsed.has_error());
                CHECK(parsed.error().type == core::error_code_t::invalid_parameter);
            }
        }
    }
}

TEST_CASE("parse_connections: an incomplete connection aborts parsing (fail-fast)") {
    // A backend entry missing a required field (host) is an invalid_parameter —
    // a broken connection is a config error, not something to silently skip.
    const std::string missing_host = R"(
connections:
  mysql:
    - { alias: a, port: "3306", username: u, password: p, database: d, table: "" }
)";
    CHECK(parse_error(connections_node(missing_host)).type == core::error_code_t::invalid_parameter);

    const std::string missing_db = R"(
connections:
  clickhouse:
    - { alias: a, host: h, port: "9000", username: u, password: p, table: "" }
)";
    CHECK(parse_error(connections_node(missing_db)).type == core::error_code_t::invalid_parameter);

    const std::string s3_no_secret = R"(
connections:
  s3:
    - { alias: a, access_key: k }
)";
    CHECK(parse_error(connections_node(s3_no_secret)).type == core::error_code_t::invalid_parameter);
}

TEST_CASE("parse_connections: a field that is not a scalar is an error naming the field") {
    // yaml-cpp refuses to read a sequence as a string; that refusal is a value,
    // not an exception leaving the parser.
    const std::string host_is_a_list = R"(
connections:
  mysql:
    - { alias: a, host: [h1, h2], port: "3306", username: u, password: p, database: d, table: "" }
)";
    auto err = parse_error(connections_node(host_is_a_list));
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "mysql"));
    CHECK(mentions(err, "host"));
}

TEST_CASE("parse_connections: an empty table field is allowed") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: a, host: h, username: u, password: p, database: d, table: "" }
)";
    // table is optional; a complete entry with an empty table must parse fine
    // (and port omitted entirely is fine too — it defaults in the connector).
    REQUIRE_FALSE(config::parse_connections(connections_node(yaml), resource()).has_error());
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.mysql.size() == 1);
    CHECK(cfg.mysql[0].table.empty());
    CHECK(cfg.mysql[0].port.empty());
}

// ── validation_error (required-field checks) ─────────────────────────────────

TEST_CASE("validation_error: complete descriptors pass") {
    config::MysqlConnectionDesc m{.alias = "a", .host = "h", .port = "1", .username = "u", .database = "d"};
    CHECK_FALSE(config::validation_error(m).has_value());

    config::PgConnectionDesc pg{.alias = "a", .host = "h", .port = "1", .username = "u", .database = "d"};
    CHECK_FALSE(config::validation_error(pg).has_value());

    config::ChConnectionDesc ch{.alias = "a", .host = "h", .port = "1", .username = "u", .database = "d"};
    CHECK_FALSE(config::validation_error(ch).has_value());

    config::S3CredentialDesc s3{.alias = "a", .access_key = "k", .secret_key = "s"};
    CHECK_FALSE(config::validation_error(s3).has_value());
}

TEST_CASE("validation_error: port is optional for backends") {
    config::MysqlConnectionDesc m{.alias = "a", .host = "h", .port = "", .username = "u", .database = "d"};
    CHECK_FALSE(config::validation_error(m).has_value());
}

TEST_CASE("validation_error: reports the first missing required field") {
    config::MysqlConnectionDesc m{.alias = "", .host = "h", .username = "u", .database = "d"};
    auto err = config::validation_error(m);
    REQUIRE(err.has_value());
    CHECK(err->find("alias") != std::string::npos);

    config::MysqlConnectionDesc m2{.alias = "a", .host = "", .username = "u", .database = "d"};
    auto err2 = config::validation_error(m2);
    REQUIRE(err2.has_value());
    CHECK(err2->find("host") != std::string::npos);

    config::MysqlConnectionDesc m3{.alias = "a", .host = "h", .username = "u", .database = ""};
    auto err3 = config::validation_error(m3);
    REQUIRE(err3.has_value());
    CHECK(err3->find("database") != std::string::npos);
}

TEST_CASE("validation_error: a non-empty backend port must be a decimal integer in 1..65535") {
    const auto with_port = [](const std::string& port) {
        return config::MysqlConnectionDesc{.alias = "a", .host = "h", .port = port, .username = "u", .database = "d"};
    };

    SECTION("valid ports pass") {
        for (const std::string port : {"1", "3306", "65535"}) {
            INFO("port=" << port);
            CHECK_FALSE(config::validation_error(with_port(port)).has_value());
        }
    }

    SECTION("malformed and out-of-range ports are reported, naming the port") {
        for (const std::string port : {"0", "65536", "-1", "abc", "33o6", "3306 ", " 3306", "+3306", "1e3", "0x1"}) {
            INFO("port=" << port);
            auto err = config::validation_error(with_port(port));
            REQUIRE(err.has_value());
            CHECK(err->find("port") != std::string::npos);
            CHECK(err->find(port) != std::string::npos);
        }
    }

    SECTION("the same rule applies to postgresql and clickhouse") {
        config::PgConnectionDesc pg{.alias = "a", .host = "h", .port = "65536", .username = "u", .database = "d"};
        auto pg_err = config::validation_error(pg);
        REQUIRE(pg_err.has_value());
        CHECK(pg_err->find("port") != std::string::npos);

        config::ChConnectionDesc ch{.alias = "a", .host = "h", .port = "nine", .username = "u", .database = "d"};
        auto ch_err = config::validation_error(ch);
        REQUIRE(ch_err.has_value());
        CHECK(ch_err->find("port") != std::string::npos);
    }

    SECTION("a missing required field is reported before a bad port") {
        config::MysqlConnectionDesc m{.alias = "a", .host = "", .port = "abc", .username = "u", .database = "d"};
        auto err = config::validation_error(m);
        REQUIRE(err.has_value());
        CHECK(err->find("host") != std::string::npos);
    }
}

TEST_CASE("parse_connections: a malformed backend port aborts parsing (fail-fast)") {
    const std::string not_a_number = R"(
connections:
  mysql:
    - { alias: a, host: h, port: "abc", username: u, password: p, database: d, table: "" }
)";
    CHECK(parse_error(connections_node(not_a_number)).type == core::error_code_t::invalid_parameter);

    const std::string out_of_range = R"(
connections:
  postgresql:
    - { alias: pg, host: h, port: "70000", username: u, password: p, database: d, table: "" }
)";
    CHECK(parse_error(connections_node(out_of_range)).type == core::error_code_t::invalid_parameter);

    const std::string zero = R"(
connections:
  clickhouse:
    - { alias: ch, host: h, port: "0", username: u, password: p, database: d, table: "" }
)";
    CHECK(parse_error(connections_node(zero)).type == core::error_code_t::invalid_parameter);

    // An unquoted YAML integer is read back as its decimal text and stays valid.
    const std::string unquoted = R"(
connections:
  mysql:
    - { alias: a, host: h, port: 3306, username: u, password: p, database: d, table: "" }
)";
    auto cfg = parse_ok(connections_node(unquoted));
    REQUIRE(cfg.mysql.size() == 1);
    CHECK(cfg.mysql[0].port == "3306");
}

TEST_CASE("validation_error: s3 requires access_key and secret_key") {
    config::S3CredentialDesc no_key{.alias = "a", .access_key = "", .secret_key = "s"};
    auto err = config::validation_error(no_key);
    REQUIRE(err.has_value());
    CHECK(err->find("access_key") != std::string::npos);

    config::S3CredentialDesc no_secret{.alias = "a", .access_key = "k", .secret_key = ""};
    auto err2 = config::validation_error(no_secret);
    REQUIRE(err2.has_value());
    CHECK(err2->find("secret_key") != std::string::npos);
}

// ── ConfigReader (whole config.yaml: service settings + connections) ─────────

TEST_CASE("ConfigReader: parses service ports, retry and the embedded connections") {
    const std::string yaml = R"(
service:
  flight_sql:
    host: "127.0.0.1"
    port: 9815
  mysql:
    port: 9816
  postgres:
    port: 9817
  connection_retry:
    max_attempts: 7
    delay_ms: 500
connections:
  mysql:
    - alias: m1
      host: h
      port: "3306"
      username: u
      password: p
      database: d
      table: ""
  s3:
    - alias: s3a
      access_key: k
      secret_key: s
      endpoint: rustfs:9000
)";
    auto cfg = load_ok(write_temp(yaml));

    CHECK(cfg.flight_sql.host == "127.0.0.1");
    CHECK(cfg.flight_sql.port == 9815);
    CHECK(cfg.mysql.port == 9816);
    CHECK(cfg.postgres.port == 9817);

    CHECK(cfg.connection_retry.max_attempts == 7);
    CHECK(cfg.connection_retry.delay_ms == 500);

    REQUIRE(cfg.connections.mysql.size() == 1);
    CHECK(cfg.connections.mysql[0].alias == "m1");
    REQUIRE(cfg.connections.s3.size() == 1);
    CHECK(cfg.connections.s3[0].alias == "s3a");
    CHECK(cfg.connections.s3[0].endpoint == "rustfs:9000");
    CHECK(cfg.connections.postgresql.empty());
    CHECK(cfg.connections.clickhouse.empty());
}

TEST_CASE("ConfigReader: defaults apply and connections are empty when file is missing") {
    auto cfg = load_ok("/nonexistent/config.yaml");

    CHECK(cfg.flight_sql.port == 8815);
    CHECK(cfg.mysql.port == 8816);
    CHECK(cfg.postgres.port == 8817);
    CHECK(cfg.connection_retry.max_attempts == 1);
    CHECK(cfg.connection_retry.delay_ms == 1000);
    CHECK(cfg.connections.mysql.empty());
    CHECK(cfg.connections.postgresql.empty());
    CHECK(cfg.connections.clickhouse.empty());
    CHECK(cfg.connections.s3.empty());
}

TEST_CASE("ConfigReader: retry defaults apply when the section is omitted") {
    const std::string yaml = R"(
service:
  mysql:
    port: 8816
)";
    auto cfg = load_ok(write_temp(yaml));
    CHECK(cfg.mysql.port == 8816);
    CHECK(cfg.connection_retry.max_attempts == 1);
    CHECK(cfg.connection_retry.delay_ms == 1000);
}

TEST_CASE("ConfigReader: a config with no connections section yields empty connections") {
    const std::string yaml = R"(
service:
  mysql:
    port: 8816
)";
    auto cfg = load_ok(write_temp(yaml));
    CHECK(cfg.mysql.port == 8816);
    CHECK(cfg.connections.mysql.empty());
    CHECK(cfg.connections.s3.empty());
}

TEST_CASE("ConfigReader: malformed YAML is an error") {
    const std::string yaml = "service: [ {port: }"; // unbalanced flow mapping
    auto err = load_error(write_temp(yaml));
    CHECK(err.type == core::error_code_t::conversion_failure);
    CHECK(mentions(err, "configuration file"));
}

TEST_CASE("ConfigReader: a service port that is not an integer is an error naming the field") {
    // yaml-cpp refuses to read the text as an int; that refusal is a value, not
    // an exception leaving the reader.
    const std::string yaml = R"(
service:
  mysql:
    port: abc
)";
    auto err = load_error(write_temp(yaml));
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "service.mysql.port"));
}

TEST_CASE("ConfigReader: aborts (returns an error) when a backend port is malformed") {
    // A port the connector could never open must stop the server at config
    // load, not surface as a failed open (or worse) at registration time.
    const std::string yaml = R"(
service:
  mysql:
    port: 8816
connections:
  mysql:
    - alias: m1
      host: h
      port: "33o6"
      username: u
      password: p
      database: d
      table: ""
)";
    auto err = load_error(write_temp(yaml));
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "port"));
}

TEST_CASE("ConfigReader: aborts (returns an error) when a connection is invalid") {
    // A well-formed file with an incomplete backend must fail to load, so the
    // server never starts with a broken connection.
    const std::string yaml = R"(
service:
  mysql:
    port: 8816
connections:
  postgresql:
    - alias: pg
      port: "5432"
      username: u
      password: p
      database: d
)";
    CHECK(load_error(write_temp(yaml)).type == core::error_code_t::invalid_parameter);
}

// ── ConfigReader: service.spark_connect (the Spark Connect gRPC server) ──────

TEST_CASE("ConfigReader: parses the spark_connect host and port") {
    const std::string yaml = R"(
service:
  flight_sql:
    host: "127.0.0.1"
    port: 9815
  mysql:
    port: 9816
  postgres:
    port: 9817
  spark_connect:
    host: "127.0.0.2"
    port: 16002
  connection_retry:
    max_attempts: 7
    delay_ms: 500
connections:
  mysql:
    - alias: m1
      host: h
      port: "3306"
      username: u
      password: p
      database: d
      table: ""
)";
    auto cfg = load_ok(write_temp(yaml));

    CHECK(cfg.spark_connect.host == "127.0.0.2");
    CHECK(cfg.spark_connect.port == 16002);

    // The neighbouring service settings and the connections are read as before.
    CHECK(cfg.flight_sql.port == 9815);
    CHECK(cfg.mysql.port == 9816);
    CHECK(cfg.postgres.port == 9817);
    CHECK(cfg.connection_retry.max_attempts == 7);
    REQUIRE(cfg.connections.mysql.size() == 1);
    CHECK(cfg.connections.mysql[0].alias == "m1");
}

TEST_CASE("ConfigReader: spark_connect defaults to 0.0.0.0:15002 when its node is absent") {
    SECTION("a service node without spark_connect") {
        const std::string yaml = R"(
service:
  mysql:
    port: 8816
)";
        auto cfg = load_ok(write_temp(yaml));
        CHECK(cfg.spark_connect.host == "0.0.0.0");
        CHECK(cfg.spark_connect.port == 15002);
    }

    SECTION("no config file at all") {
        auto cfg = load_ok("/nonexistent/config.yaml");
        CHECK(cfg.spark_connect.host == "0.0.0.0");
        CHECK(cfg.spark_connect.port == 15002);
    }
}

TEST_CASE("ConfigReader: a spark_connect port that is not an integer is an error naming the field") {
    // Read through yaml_scalar like every service value: the refusal is a
    // value, not an exception leaving the reader.
    const std::string yaml = R"(
service:
  spark_connect:
    host: "0.0.0.0"
    port: abc
)";
    auto err = load_error(write_temp(yaml));
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "service.spark_connect.port"));
}

// ── the pool: block, statement and connect timeouts, service.file_transfers ──

TEST_CASE("parse_connections: an entry without a pool block gets the default pool and timeouts") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: my, host: h, username: u, database: d }
  postgresql:
    - { alias: pg, host: h, username: u, database: d }
  clickhouse:
    - { alias: ch, host: h, username: u, database: d }
  s3:
    - { alias: lake, access_key: ak, secret_key: sk }
)";
    auto cfg = parse_ok(connections_node(yaml));
    const config::PoolDesc defaults;
    for (const auto* pool : {&cfg.mysql.front().pool,
                             &cfg.postgresql.front().pool,
                             &cfg.clickhouse.front().pool,
                             &cfg.s3.front().pool}) {
        CHECK(pool->min_size == defaults.min_size);
        CHECK(pool->max_size == defaults.max_size);
        CHECK(pool->idle_timeout_ms == defaults.idle_timeout_ms);
        CHECK(pool->check_after_idle_ms == defaults.check_after_idle_ms);
        CHECK(pool->queue_timeout_ms == defaults.queue_timeout_ms);
        CHECK(pool->connect_backoff_ms == defaults.connect_backoff_ms);
    }
    CHECK(defaults.min_size == 1);
    CHECK(defaults.max_size == 4);
    CHECK(cfg.mysql.front().statement_timeout_ms == 90000);
    CHECK(cfg.postgresql.front().statement_timeout_ms == 90000);
    CHECK(cfg.s3.front().connect_timeout_ms == 1000);
}

TEST_CASE("parse_connections: the pool block and the timeouts are read per entry") {
    const std::string yaml = R"(
connections:
  mysql:
    - alias: my
      host: h
      username: u
      database: d
      statement_timeout_ms: 5000
      pool: { min_size: 2, max_size: 8, idle_timeout_ms: 1000, check_after_idle_ms: 0,
              queue_timeout_ms: 2000, connect_backoff_ms: 0 }
  postgresql:
    - { alias: pg, host: h, username: u, database: d, statement_timeout_ms: 7000, pool: { max_size: 2 } }
  clickhouse:
    - { alias: ch, host: h, username: u, database: d, pool: { min_size: 3, max_size: 3 } }
  s3:
    - { alias: lake, access_key: ak, secret_key: sk, connect_timeout_ms: 250, pool: { max_size: 6 } }
)";
    auto cfg = parse_ok(connections_node(yaml));
    const auto& my = cfg.mysql.front();
    CHECK(my.statement_timeout_ms == 5000);
    CHECK(my.pool.min_size == 2);
    CHECK(my.pool.max_size == 8);
    CHECK(my.pool.idle_timeout_ms == 1000);
    CHECK(my.pool.check_after_idle_ms == 0);
    CHECK(my.pool.queue_timeout_ms == 2000);
    CHECK(my.pool.connect_backoff_ms == 0);

    CHECK(cfg.postgresql.front().statement_timeout_ms == 7000);
    CHECK(cfg.postgresql.front().pool.min_size == 1);
    CHECK(cfg.postgresql.front().pool.max_size == 2);
    CHECK(cfg.clickhouse.front().pool.min_size == 3);
    CHECK(cfg.clickhouse.front().pool.max_size == 3);
    CHECK(cfg.s3.front().connect_timeout_ms == 250);
    CHECK(cfg.s3.front().pool.max_size == 6);
}

TEST_CASE("parse_connections: an invalid pool block aborts parsing naming the setting") {
    struct case_t {
        const char* pool;
        const char* names;
    };
    const case_t cases[] = {
        {"{ min_size: 0 }", "min_size"},
        {"{ min_size: 5, max_size: 4 }", "max_size"},
        {"{ max_size: 0 }", "max_size"},
        {"{ idle_timeout_ms: 0 }", "idle_timeout_ms"},
        {"{ queue_timeout_ms: -1 }", "queue_timeout_ms"},
        {"{ check_after_idle_ms: -1 }", "check_after_idle_ms"},
        {"{ connect_backoff_ms: -5 }", "connect_backoff_ms"},
        {"{ max_szie: 8 }", "max_szie"},
        {"{ min_size: lots }", "min_size"},
        {"[1, 2]", "pool"},
    };
    for (const auto& c : cases) {
        INFO("pool: " << c.pool);
        const std::string yaml =
            std::string("connections:\n  clickhouse:\n    - { alias: ch, host: h, username: u, database: d, pool: ") +
            c.pool + " }\n";
        auto err = parse_error(connections_node(yaml));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, c.names));
        CHECK(mentions(err, "clickhouse"));
    }
}

TEST_CASE("parse_connections: a non-positive statement or connect timeout aborts parsing") {
    SECTION("mysql statement_timeout_ms") {
        auto err = parse_error(connections_node(R"(
connections:
  mysql:
    - { alias: my, host: h, username: u, database: d, statement_timeout_ms: 0 }
)"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "statement_timeout_ms"));
    }
    SECTION("postgresql statement_timeout_ms") {
        auto err = parse_error(connections_node(R"(
connections:
  postgresql:
    - { alias: pg, host: h, username: u, database: d, statement_timeout_ms: -1 }
)"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "statement_timeout_ms"));
    }
    SECTION("s3 connect_timeout_ms") {
        auto err = parse_error(connections_node(R"(
connections:
  s3:
    - { alias: lake, access_key: ak, secret_key: sk, connect_timeout_ms: 0 }
)"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "connect_timeout_ms"));
    }
}

TEST_CASE("ConfigReader: service.file_transfers sizes the local-file transfer pool") {
    SECTION("read when present") {
        auto cfg = load_ok(write_temp(R"(
service:
  file_transfers: { min_size: 2, max_size: 6 }
)"));
        CHECK(cfg.file_transfers.min_size == 2);
        CHECK(cfg.file_transfers.max_size == 6);
    }
    SECTION("the default pool when absent") {
        auto cfg = load_ok("/nonexistent/config.yaml");
        CHECK(cfg.file_transfers.min_size == 1);
        CHECK(cfg.file_transfers.max_size == 4);
    }
    SECTION("an invalid block is an error naming the field") {
        auto err = load_error(write_temp(R"(
service:
  file_transfers: { min_size: 3, max_size: 1 }
)"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "service.file_transfers"));
    }
}
