// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "config/connections/connection_config_reader.hpp"
#include "config/config.hpp"

#include <catch2/catch.hpp>
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

// Parse a YAML string and return its top-level `connections` node.
YAML::Node connections_node(const std::string& yaml) {
    return YAML::Load(yaml)["connections"];
}

// Write `content` to a unique temp file and return its path (used for the
// whole-file ConfigReader tests).
std::string write_temp(const std::string& content) {
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("otterstax_cfg_" + std::to_string(counter++) + ".yaml");
    std::ofstream out(path);
    out << content;
    out.close();
    return path.string();
}

}  // namespace

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
    - alias: minio1
      access_key: ak
      secret_key: sk
      region: us-east-1
      session_token: tok
      endpoint: minio:9000
)";

    auto cfg = config::parse_connections(connections_node(yaml));

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
        CHECK(s3.alias == "minio1");
        CHECK(s3.access_key == "ak");
        CHECK(s3.secret_key == "sk");
        CHECK(s3.region == "us-east-1");
        CHECK(s3.session_token == "tok");
        CHECK(s3.endpoint == "minio:9000");
    }
}

TEST_CASE("parse_connections: null/missing node yields an empty config") {
    YAML::Node absent;  // null node
    auto cfg = config::parse_connections(absent);
    CHECK(cfg.mysql.empty());
    CHECK(cfg.postgresql.empty());
    CHECK(cfg.clickhouse.empty());
    CHECK(cfg.s3.empty());

    // A document without a `connections:` key -> node is null too.
    auto cfg2 = config::parse_connections(connections_node("mysql:\n  port: 8816\n"));
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
    auto cfg = config::parse_connections(connections_node(yaml));
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
    auto cfg = config::parse_connections(connections_node(yaml));
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
    auto cfg = config::parse_connections(connections_node(yaml));
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
    auto cfg = config::parse_connections(connections_node(yaml));
    REQUIRE(cfg.mysql.size() == 3);
    CHECK(cfg.mysql[0].alias == "a");
    CHECK(cfg.mysql[1].alias == "b");
    CHECK(cfg.mysql[2].alias == "c");
}

TEST_CASE("parse_connections: an incomplete connection aborts parsing (fail-fast)") {
    // A backend entry missing a required field (host) must throw — a broken
    // connection is a config error, not something to silently skip.
    const std::string missing_host = R"(
connections:
  mysql:
    - { alias: a, port: "3306", username: u, password: p, database: d, table: "" }
)";
    CHECK_THROWS_AS(config::parse_connections(connections_node(missing_host)), std::runtime_error);

    const std::string missing_db = R"(
connections:
  clickhouse:
    - { alias: a, host: h, port: "9000", username: u, password: p, table: "" }
)";
    CHECK_THROWS_AS(config::parse_connections(connections_node(missing_db)), std::runtime_error);

    const std::string s3_no_secret = R"(
connections:
  s3:
    - { alias: a, access_key: k }
)";
    CHECK_THROWS_AS(config::parse_connections(connections_node(s3_no_secret)), std::runtime_error);
}

TEST_CASE("parse_connections: an empty table field is allowed") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: a, host: h, username: u, password: p, database: d, table: "" }
)";
    // table is optional; a complete entry with an empty table must parse fine
    // (and port omitted entirely is fine too — it defaults in the connector).
    REQUIRE_NOTHROW(config::parse_connections(connections_node(yaml)));
    auto cfg = config::parse_connections(connections_node(yaml));
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
      endpoint: minio:9000
)";
    config::ConfigReader reader;
    auto cfg = reader.load(write_temp(yaml));

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
    CHECK(cfg.connections.s3[0].endpoint == "minio:9000");
    CHECK(cfg.connections.postgresql.empty());
    CHECK(cfg.connections.clickhouse.empty());
}

TEST_CASE("ConfigReader: defaults apply and connections are empty when file is missing") {
    config::ConfigReader reader;
    auto cfg = reader.load("/nonexistent/config.yaml");

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
    auto cfg = config::ConfigReader{}.load(write_temp(yaml));
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
    auto cfg = config::ConfigReader{}.load(write_temp(yaml));
    CHECK(cfg.mysql.port == 8816);
    CHECK(cfg.connections.mysql.empty());
    CHECK(cfg.connections.s3.empty());
}

TEST_CASE("ConfigReader: malformed YAML throws") {
    const std::string yaml = "service: [ {port: }";  // unbalanced flow mapping
    config::ConfigReader reader;
    CHECK_THROWS_AS(reader.load(write_temp(yaml)), std::runtime_error);
}

TEST_CASE("ConfigReader: aborts (throws) when a connection is invalid") {
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
    config::ConfigReader reader;
    CHECK_THROWS_AS(reader.load(write_temp(yaml)), std::runtime_error);
}
