// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// ComponentManager brought up and torn down without a single query, frontend or
// database connection — the shape main.cpp leaves behind when startup aborts
// after the engine exists — with only an s3 alias and the local-file transfer
// pool registered, which open no connection either. Teardown must be clean on a data dir the engine has
// never seen (first start: WAL/disk/catalog bootstrap files are created and
// closed within one constructor/destructor pair) and must stay clean when the
// same process does it again.

#include "component_manager/component_manager.hpp"
#include "otterbrix/config.hpp"

#include <arrow/filesystem/s3fs.h>

#include <catch2/catch_all.hpp>

#include <filesystem>

TEST_CASE("ComponentManager: constructed and destroyed with no database connection on a fresh data dir three times over") {
    const auto dir = std::filesystem::temp_directory_path() / "otterstax_component_manager_lifecycle";

    for (int attempt = 0; attempt < 3; ++attempt) {
        std::filesystem::remove_all(dir);
        REQUIRE_FALSE(std::filesystem::exists(dir));
        {
            config::ConnectionsConfig connections;
            // An endpoint that refuses every connection: registering the alias
            // builds its filesystem and sends nothing.
            connections.s3.push_back(config::S3CredentialDesc{.alias = "lifecycle",
                                                              .access_key = "ak",
                                                              .secret_key = "sk",
                                                              .region = "us-east-1",
                                                              .endpoint = "127.0.0.1:1"});
            ComponentManager cmanager(make_create_config(dir), connections);
            REQUIRE(cmanager.getResource() != nullptr);
            REQUIRE(cmanager.scheduler_address());
            REQUIRE(cmanager.catalog_address());
            auto registered = cmanager.register_connections(connections, config::ConnectionRetryConfig{});
            INFO("register_connections: " << registered.what.c_str());
            REQUIRE_FALSE(registered.contains_error());
            // Building the alias's s3 driver initialises Arrow's S3 subsystem.
            // Arrow requires the process to FinalizeS3 before exit or it may
            // segfault during static teardown; the server does that through
            // conn::s3::subsystem_finalizer_t in main.cpp, this binary through
            // the listener in tests/system/main.cpp.
            REQUIRE(arrow::fs::IsS3Initialized());
        }
        REQUIRE(std::filesystem::exists(dir));
    }

    std::filesystem::remove_all(dir);
}
