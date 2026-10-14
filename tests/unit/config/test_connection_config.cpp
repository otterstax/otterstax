// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "config/connections/connection_config_reader.hpp"
#include "config/config.hpp"

#include <catch2/catch_all.hpp>
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

namespace {

std::pmr::memory_resource* resource() {
    return std::pmr::new_delete_resource();
}

// Parse a YAML string and return its top-level `connections` node.
YAML::Node connections_node(const std::string& yaml) {
    return YAML::Load(yaml)["connections"];
}

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

// load_config on a file that must be accepted; the whole ServiceConfig.
config::ServiceConfig load_ok(const std::string& path) {
    auto loaded = config::load_config(resource(), path);
    REQUIRE_FALSE(loaded.has_error());
    return std::move(loaded.value());
}

// load_config on a file that must be rejected; the error it produced.
core::error_t load_error(const std::string& path) {
    auto loaded = config::load_config(resource(), path);
    REQUIRE(loaded.has_error());
    return loaded.error();
}

bool mentions(const core::error_t& error, std::string_view text) {
    return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
}

// Write `content` to a unique temp file and return its path (used for the
// whole-file load_config tests).
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

// ── pool_size (mysql / postgresql) ───────────────────────────────────────────

TEST_CASE("parse_connections: pool_size defaults to 1 when omitted") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: m, host: h, username: u, database: d }
  postgresql:
    - { alias: p, host: h, username: u, database: d }
)";
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.mysql.size() == 1);
    REQUIRE(cfg.postgresql.size() == 1);
    CHECK(cfg.mysql[0].pool_size == 1);
    CHECK(cfg.postgresql[0].pool_size == 1);
}

TEST_CASE("parse_connections: pool_size is read for mysql and postgresql entries") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: m1, host: h, username: u, database: d, pool_size: 4 }
    - { alias: m2, host: h, username: u, database: d, pool_size: "2" }
  postgresql:
    - { alias: p, host: h, username: u, database: d, schema: s, pool_size: 8 }
)";
    auto cfg = parse_ok(connections_node(yaml));
    CHECK(cfg.mysql[0].pool_size == 4);
    CHECK(cfg.mysql[1].pool_size == 2);
    CHECK(cfg.postgresql[0].pool_size == 8);
    CHECK(cfg.postgresql[0].schema == "s");
}

TEST_CASE("parse_connections: a pool_size below 1 is invalid_parameter naming the entry") {
    for (const char* value : {"0", "-1"}) {
        INFO("pool_size " << value);
        for (const char* section : {"mysql", "postgresql"}) {
            INFO("section " << section);
            const std::string yaml = std::string("connections:\n  ") + section +
                                     ":\n    - { alias: bad, host: h, username: u, database: d, pool_size: " + value +
                                     " }\n";
            auto err = parse_error(connections_node(yaml));
            CHECK(err.type == core::error_code_t::invalid_parameter);
            CHECK(mentions(err, section));
            CHECK(mentions(err, "alias='bad'"));
            CHECK(mentions(err, "pool_size"));
        }
    }
}

TEST_CASE("parse_connections: a pool_size that is not an integer is invalid_parameter naming the field") {
    for (const char* value : {"abc", "1.5", "[1, 2]", "{ n: 1 }", "99999999999"}) {
        INFO("pool_size " << value);
        const std::string yaml = std::string("connections:\n  mysql:\n") +
                                 "    - { alias: bad, host: h, username: u, database: d, pool_size: " + value + " }\n";
        auto err = parse_error(connections_node(yaml));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "mysql"));
        CHECK(mentions(err, "entry #1"));
        CHECK(mentions(err, "pool_size"));
    }
}

TEST_CASE("validation_error: pool_size must be at least 1") {
    config::MysqlConnectionDesc m{.alias = "a", .host = "h", .username = "u", .database = "d", .pool_size = 0};
    auto err = config::validation_error(m);
    REQUIRE(err.has_value());
    CHECK(err->find("pool_size") != std::string::npos);

    config::PgConnectionDesc pg{.alias = "a", .host = "h", .username = "u", .database = "d", .pool_size = -3};
    auto err2 = config::validation_error(pg);
    REQUIRE(err2.has_value());
    CHECK(err2->find("pool_size") != std::string::npos);

    // A missing required field is still reported first.
    config::MysqlConnectionDesc incomplete{.alias = "a", .host = "", .username = "u", .database = "d", .pool_size = 0};
    auto err3 = config::validation_error(incomplete);
    REQUIRE(err3.has_value());
    CHECK(err3->find("host") != std::string::npos);

    config::PgConnectionDesc ok{.alias = "a", .host = "h", .username = "u", .database = "d", .pool_size = 16};
    CHECK_FALSE(config::validation_error(ok).has_value());
}

TEST_CASE("load_config: pool_size reaches the embedded connection descriptors") {
    auto path = write_temp(R"(
connections:
  mysql:
    - { alias: m, host: h, port: "3306", username: u, password: p, database: d, table: "", pool_size: 3 }
  postgresql:
    - { alias: pg, host: h, port: "5432", username: u, password: p, database: d, schema: s, table: "" }
)");
    auto cfg = load_ok(path);
    REQUIRE(cfg.connections.mysql.size() == 1);
    REQUIRE(cfg.connections.postgresql.size() == 1);
    CHECK(cfg.connections.mysql[0].pool_size == 3);
    CHECK(cfg.connections.postgresql[0].pool_size == 1);
    std::filesystem::remove(path);
}

// ── the reserved alias `federation` ─────────────────────────────────────────

TEST_CASE("parse_connections: the alias federation is refused in every section and any case") {
    const std::string backend_fields = ", host: h, username: u, database: d }\n";
    const std::string s3_fields = ", access_key: k, secret_key: s }\n";
    for (const char* alias : {"federation", "Federation", "FEDERATION", "fEdErAtIoN"}) {
        INFO("alias " << alias);
        for (const char* section : {"mysql", "postgresql", "clickhouse", "s3"}) {
            INFO("section " << section);
            const std::string fields = std::string_view{section} == "s3" ? s3_fields : backend_fields;
            const std::string yaml =
                std::string("connections:\n  ") + section + ":\n    - { alias: " + alias + fields;
            auto err = parse_error(connections_node(yaml));
            CHECK(err.type == core::error_code_t::invalid_parameter);
            CHECK(mentions(err, std::string("invalid ") + section + " connection"));
            CHECK(mentions(err, std::string("alias='") + alias + "'"));
            CHECK(mentions(err, "reserved"));
        }
    }
}

// `kafka` is the engine database of the Kafka objects: a connection under it
// would capture kafka.<object>.
TEST_CASE("parse_connections: the alias kafka is refused in every section and any case") {
    const std::string backend_fields = ", host: h, username: u, database: d }\n";
    const std::string s3_fields = ", access_key: k, secret_key: s }\n";
    for (const char* alias : {"kafka", "Kafka", "KAFKA"}) {
        INFO("alias " << alias);
        for (const char* section : {"mysql", "postgresql", "clickhouse", "s3"}) {
            INFO("section " << section);
            const std::string fields = std::string_view{section} == "s3" ? s3_fields : backend_fields;
            const std::string yaml =
                std::string("connections:\n  ") + section + ":\n    - { alias: " + alias + fields;
            auto err = parse_error(connections_node(yaml));
            CHECK(err.type == core::error_code_t::invalid_parameter);
            CHECK(mentions(err, std::string("alias='") + alias + "'"));
            CHECK(mentions(err, "reserved"));
            CHECK(mentions(err, "Kafka"));
        }
    }
    // A name that only contains it is a name of its own.
    auto cfg = parse_ok(connections_node("connections:\n  mysql:\n    - { alias: kafka2, host: h, username: u, database: d }\n"));
    REQUIRE(cfg.mysql.size() == 1);
    CHECK(cfg.mysql[0].alias == "kafka2");

    config::PgConnectionDesc pg{.alias = "Kafka", .host = "h", .username = "u", .database = "d"};
    auto err = config::validation_error(pg);
    REQUIRE(err.has_value());
    CHECK(err->find("reserved") != std::string::npos);
}

TEST_CASE("parse_connections: the reserved alias is refused after valid entries before it") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: m, host: h, username: u, database: d }
  postgresql:
    - { alias: p, host: h, username: u, database: d }
    - { alias: Federation, host: h, username: u, database: d }
)";
    auto err = parse_error(connections_node(yaml));
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "postgresql"));
    CHECK(mentions(err, "alias='Federation'"));
    CHECK(mentions(err, "reserved"));
}

TEST_CASE("parse_connections: an alias that only contains federation is accepted") {
    const std::string yaml = R"(
connections:
  mysql:
    - { alias: federation2, host: h, username: u, database: d }
  postgresql:
    - { alias: my_federation, host: h, username: u, database: d }
  clickhouse:
    - { alias: federatio, host: h, username: u, database: d }
  s3:
    - { alias: federations, access_key: k, secret_key: s }
)";
    auto cfg = parse_ok(connections_node(yaml));
    REQUIRE(cfg.mysql.size() == 1);
    CHECK(cfg.mysql[0].alias == "federation2");
    CHECK(cfg.postgresql[0].alias == "my_federation");
    CHECK(cfg.clickhouse[0].alias == "federatio");
    CHECK(cfg.s3[0].alias == "federations");
}

TEST_CASE("validation_error: the reserved alias is reported after the missing fields") {
    config::MysqlConnectionDesc m{.alias = "Federation", .host = "h", .username = "u", .database = "d"};
    auto err = config::validation_error(m);
    REQUIRE(err.has_value());
    CHECK(err->find("reserved") != std::string::npos);

    config::S3CredentialDesc s3{.alias = "FEDERATION", .access_key = "k", .secret_key = "s"};
    auto err2 = config::validation_error(s3);
    REQUIRE(err2.has_value());
    CHECK(err2->find("reserved") != std::string::npos);

    // An incomplete entry is reported as incomplete first.
    config::ChConnectionDesc incomplete{.alias = "federation", .host = "", .username = "u", .database = "d"};
    auto err3 = config::validation_error(incomplete);
    REQUIRE(err3.has_value());
    CHECK(err3->find("host") != std::string::npos);

    // The reserved alias outranks a bad port.
    config::PgConnectionDesc bad_port{.alias = "federation", .host = "h", .port = "abc", .username = "u", .database = "d"};
    auto err4 = config::validation_error(bad_port);
    REQUIRE(err4.has_value());
    CHECK(err4->find("reserved") != std::string::npos);
}

TEST_CASE("load_config: a connection under the reserved alias aborts the load") {
    auto path = write_temp(R"(
service:
  mysql: { port: 8816 }
connections:
  clickhouse:
    - { alias: Federation, host: h, port: "9000", username: u, password: p, database: d, table: "" }
)");
    auto err = load_error(path);
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "clickhouse"));
    CHECK(mentions(err, "alias='Federation'"));
    CHECK(mentions(err, "reserved"));
    std::filesystem::remove(path);
}

// ── load_config (whole config.yaml: service settings + connections) ─────────

TEST_CASE("load_config: parses service ports, retry and the embedded connections") {
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
    CHECK(cfg.connections.s3[0].endpoint == "minio:9000");
    CHECK(cfg.connections.postgresql.empty());
    CHECK(cfg.connections.clickhouse.empty());
}

TEST_CASE("load_config: defaults apply and connections are empty when file is missing") {
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

TEST_CASE("load_config: retry defaults apply when the section is omitted") {
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

TEST_CASE("load_config: a config with no connections section yields empty connections") {
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

TEST_CASE("load_config: malformed YAML is an error") {
    const std::string yaml = "service: [ {port: }";  // unbalanced flow mapping
    auto err = load_error(write_temp(yaml));
    CHECK(err.type == core::error_code_t::conversion_failure);
    CHECK(mentions(err, "configuration file"));
}

TEST_CASE("load_config: a service port that is not an integer is an error naming the field") {
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

TEST_CASE("load_config: a service port outside 1..65535 is an error naming the field") {
    // Read as an integer in range: 70000 would otherwise be cut to 16 bits and
    // listen on another port (4464).
    for (const char* section : {"flight_sql", "mysql", "postgres", "spark_connect"}) {
        INFO(section);
        auto err = load_error(write_temp(std::string{"service:\n  "} + section + ":\n    port: 70000\n"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, std::string{"service."} + section + ".port"));
        CHECK(mentions(err, "out of range"));
    }
    auto zero = load_error(write_temp("service:\n  mysql:\n    port: 0\n"));
    CHECK(zero.type == core::error_code_t::invalid_parameter);
}

TEST_CASE("load_config: a section that is not a mapping is an error naming it") {
    // yaml-cpp throws on a subscript of a scalar; every section is checked to be
    // a mapping before a key of it is read, so the throw never happens.
    const std::pair<const char*, const char*> cases[] = {
        {"service: 5\n", "'service'"},
        {"service: [a, b]\n", "'service'"},
        {"service:\n  mysql: 8816\n", "service.mysql"},
        {"service:\n  postgres: 8817\n", "service.postgres"},
        {"service:\n  flight_sql: 8815\n", "service.flight_sql"},
        {"service:\n  spark_connect: 15002\n", "service.spark_connect"},
        {"service:\n  connection_retry: 3\n", "service.connection_retry"},
        {"service:\n  mysql: [8816]\n", "service.mysql"},
        {"just a scalar\n", "(document)"},
    };
    for (const auto& [yaml, field] : cases) {
        INFO(yaml);
        auto err = load_error(write_temp(yaml));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, field));
        CHECK(mentions(err, "expected a mapping"));
    }
    // An empty file is the defaults, as before.
    auto cfg = load_ok(write_temp(""));
    CHECK(cfg.mysql.port == 8816);
}

TEST_CASE("parse_connections: an entry that is a scalar is an error naming the entry") {
    for (const char* section : {"mysql", "postgresql", "clickhouse", "s3"}) {
        INFO(section);
        auto err = parse_error(connections_node(std::string{"connections:\n  "} + section + ":\n    - just_a_name\n"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, section));
        CHECK(mentions(err, "entry #1"));
        CHECK(mentions(err, "expected a mapping"));
    }
}

TEST_CASE("load_config: aborts (returns an error) when a backend port is malformed") {
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

TEST_CASE("load_config: aborts (returns an error) when a connection is invalid") {
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

// ── load_config: service.spark_connect (the Spark Connect gRPC server) ──────

TEST_CASE("load_config: parses the spark_connect host and port") {
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

TEST_CASE("load_config: spark_connect defaults to 0.0.0.0:15002 when its node is absent") {
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

TEST_CASE("load_config: a spark_connect port that is not an integer is an error naming the field") {
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

// ── aliases: one connection per name ──────────────────────────────────────────

// A backend alias is the connection uid a federated name starts with, and the
// federation resolves it case-insensitively: a second backend under the same
// alias (in any section, in any case) would leave one of the two unreachable
// and record its tables under the other's uid. The uid keeps the backend it was
// registered with because a config that would rebind it never loads.
TEST_CASE("parse_connections: a backend alias taken twice is refused across sections and case") {
    struct case_t {
        const char* first_section;
        const char* first_alias;
        const char* second_section;
        const char* second_alias;
    };
    for (const auto& c : {case_t{"mysql", "shop", "mysql", "shop"},
                          case_t{"mysql", "shop", "postgresql", "shop"},
                          case_t{"postgresql", "Shop", "clickhouse", "SHOP"},
                          case_t{"mysql", "shop", "clickhouse", "sHoP"}}) {
        INFO(c.first_section << " " << c.first_alias << " / " << c.second_section << " " << c.second_alias);
        const std::string entry = ", host: h, username: u, database: d }\n";
        std::string yaml = "connections:\n";
        if (std::string_view{c.first_section} == c.second_section) {
            yaml += std::string("  ") + c.first_section + ":\n    - { alias: " + c.first_alias + entry +
                    "    - { alias: " + c.second_alias + entry;
        } else {
            yaml += std::string("  ") + c.first_section + ":\n    - { alias: " + c.first_alias + entry + "  " +
                    c.second_section + ":\n    - { alias: " + c.second_alias + entry;
        }
        auto err = parse_error(connections_node(yaml));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, std::string("alias='") + c.second_alias + "'"));
        CHECK(mentions(err, "already taken"));
        CHECK(mentions(err, std::string(c.first_section) + " alias='" + c.first_alias + "'"));
    }
}

TEST_CASE("parse_connections: an s3 alias is its own namespace and taken once") {
    SECTION("an s3 alias equal to a backend alias is accepted") {
        const std::string yaml = R"(
connections:
  mysql:
    - { alias: lake, host: h, username: u, database: d }
  s3:
    - { alias: lake, access_key: k, secret_key: s }
)";
        auto cfg = parse_ok(connections_node(yaml));
        CHECK(cfg.mysql.size() == 1);
        CHECK(cfg.s3.size() == 1);
    }
    SECTION("two s3 entries under one alias are refused") {
        const std::string yaml = R"(
connections:
  s3:
    - { alias: lake, access_key: k, secret_key: s }
    - { alias: lake, access_key: k2, secret_key: s2 }
)";
        auto err = parse_error(connections_node(yaml));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "s3"));
        CHECK(mentions(err, "alias='lake'"));
        CHECK(mentions(err, "already taken"));
    }
    SECTION("s3 aliases differing in case are two aliases (looked up exactly)") {
        const std::string yaml = R"(
connections:
  s3:
    - { alias: lake, access_key: k, secret_key: s }
    - { alias: LAKE, access_key: k2, secret_key: s2 }
)";
        CHECK(parse_ok(connections_node(yaml)).s3.size() == 2);
    }
}

TEST_CASE("load_config: a backend alias taken twice aborts the load") {
    auto path = write_temp(R"(
connections:
  mysql:
    - { alias: shop, host: h, port: "3306", username: u, password: p, database: d, table: "" }
  postgresql:
    - { alias: SHOP, host: h, port: "5432", username: u, password: p, database: d, table: "" }
)");
    auto err = load_error(path);
    CHECK(err.type == core::error_code_t::invalid_parameter);
    CHECK(mentions(err, "alias='SHOP'"));
    CHECK(mentions(err, "already taken"));
    std::filesystem::remove(path);
}

// ── load_config: service.spark_connect.session_timeout_s ────────────────────

TEST_CASE("load_config: the spark_connect session timeout defaults to Spark's 60 minutes") {
    auto cfg = load_ok("/nonexistent/config.yaml");
    CHECK(cfg.spark_connect.session_timeout_s == 3600);
}

TEST_CASE("load_config: the spark_connect session timeout is read and range-checked") {
    SECTION("a value") {
        auto cfg = load_ok(write_temp("service:\n  spark_connect: { session_timeout_s: 120 }\n"));
        CHECK(cfg.spark_connect.session_timeout_s == 120);
        CHECK(cfg.spark_connect.port == 15002);
    }
    SECTION("zero keeps sessions until ReleaseSession") {
        auto cfg = load_ok(write_temp("service:\n  spark_connect: { session_timeout_s: 0 }\n"));
        CHECK(cfg.spark_connect.session_timeout_s == 0);
    }
    for (const char* bad : {"-1", "abc", "1.5", "99999999999"}) {
        INFO(bad);
        auto err = load_error(write_temp(std::string("service:\n  spark_connect: { session_timeout_s: ") + bad + " }\n"));
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(err, "service.spark_connect.session_timeout_s"));
    }
}
