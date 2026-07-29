// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string>
#include <vector>

namespace config {

// Plain-data descriptors for the connection config file. These carry no
// connector dependencies; the ComponentManager converts them into the
// connector-specific parameter structs when it registers connections at
// startup.

struct MysqlConnectionDesc {
    std::string alias;
    std::string host;
    std::string port;
    std::string username;
    std::string password;
    std::string database;
    std::string table;
};

struct PgConnectionDesc {
    std::string alias;
    std::string host;
    std::string port;
    std::string username;
    std::string password;
    std::string database;
    std::string schema{"public"};  // PostgreSQL schema, defaults to "public"
    std::string table;
};

struct ChConnectionDesc {
    std::string alias;
    std::string host;
    std::string port;
    std::string username;
    std::string password;
    std::string database;
    std::string table;
};

struct S3CredentialDesc {
    std::string alias;
    std::string access_key;
    std::string secret_key;
    std::string region;         // optional
    std::string session_token;  // optional — for IAM roles
    std::string endpoint;       // optional — for MinIO / custom S3-compatible endpoints
};

// Aggregate parsed from the connection config file: the single source of truth
// for every remote backend and s3 alias known to the server.
struct ConnectionsConfig {
    std::vector<MysqlConnectionDesc> mysql;
    std::vector<PgConnectionDesc> postgresql;
    std::vector<ChConnectionDesc> clickhouse;
    std::vector<S3CredentialDesc> s3;
};

// Startup retry policy for opening remote backend connections. A slow backend
// (e.g. a DB still booting) makes the eager connect in addConnection throw; the
// registrar retries up to max_attempts, sleeping delay_ms between tries.
// max_attempts <= 1 means a single attempt (no retry). Applies to
// mysql/postgresql/clickhouse — s3 aliases are only stored, not opened.
struct ConnectionRetryConfig {
    int max_attempts = 1;
    int delay_ms = 1000;
};

}  // namespace config
