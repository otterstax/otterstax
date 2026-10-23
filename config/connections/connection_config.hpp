// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace config {

// Plain-data descriptors for the connection config file. These carry no
// connector dependencies; the ComponentManager converts them into the
// connector-specific parameter structs when it registers connections at
// startup.

// The optional `pool:` block of a connection (and `service.file_transfers`):
// how many connections — or, for s3 and local files, concurrent transfers — one
// alias may hold, and when they open, close and give up. Converted into
// conn::pool_config_t at registration. Validated at parse time:
// 1 <= min_size <= max_size, every duration positive except check_after_idle_ms
// and connect_backoff_ms, which may be 0.
struct PoolDesc {
    std::size_t min_size = 1;        // opened at startup, never closed
    std::size_t max_size = 4;        // hard cap on connections to this backend
    int idle_timeout_ms = 60000;     // a connection above min_size idle this long closes
    int check_after_idle_ms = 30000; // a connection idle this long is pinged before use
    int queue_timeout_ms = 30000;    // a statement waiting longer than this fails
    int connect_backoff_ms = 1000;   // wait after a failed connect before the next one
};

struct MysqlConnectionDesc {
    std::string alias;
    std::string host;
    std::string port;
    std::string username;
    std::string password;
    std::string database;
    std::string table;
    int statement_timeout_ms = 90000;  // total time one statement may take, client side
    PoolDesc pool;
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
    int statement_timeout_ms = 90000;  // total time one statement may take, client side
    PoolDesc pool;
};

struct ChConnectionDesc {
    std::string alias;
    std::string host;
    std::string port;
    std::string username;
    std::string password;
    std::string database;
    std::string table;
    PoolDesc pool;  // ClickHouse keeps its 10 s socket bound instead of a statement deadline
};

struct S3CredentialDesc {
    std::string alias;
    std::string access_key;
    std::string secret_key;
    std::string region;         // optional
    std::string session_token;  // optional — for IAM roles
    std::string endpoint;       // optional — for RustFS / custom S3-compatible endpoints
    int connect_timeout_ms = 1000;  // S3Options::connect_timeout
    PoolDesc pool;                  // concurrent transfers for this alias
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
// (e.g. a DB still booting) fails the pool's first min_size connects; the
// registrar retries up to max_attempts, sleeping delay_ms between tries.
// max_attempts <= 1 means a single attempt (no retry). Applies to
// mysql/postgresql/clickhouse — s3 aliases are only stored, not opened.
struct ConnectionRetryConfig {
    int max_attempts = 1;
    int delay_ms = 1000;
};

}  // namespace config
