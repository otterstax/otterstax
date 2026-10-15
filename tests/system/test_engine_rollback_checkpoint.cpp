// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "tests/temporary_directory.hpp"
#include <services/disk/manager_disk.hpp>
#include <catch2/catch_all.hpp>
#include <fstream>

namespace {
    std::filesystem::path user_sidecar(const std::filesystem::path& root) {
        const auto system = std::to_string(static_cast<unsigned>(services::disk::manager_disk_t::system_dir_oid()));
        std::vector<std::filesystem::path> found;
        for (const auto& ns : std::filesystem::directory_iterator(root)) {
            if (!ns.is_directory() || ns.path().filename().string() == system) continue;
            for (const auto& table : std::filesystem::directory_iterator(ns.path())) {
                auto path = table.path() / "table.otbx.wal_id";
                if (table.is_directory() && std::filesystem::exists(path)) found.push_back(path);
            }
        }
        REQUIRE(found.size() == 1);
        return found.front();
    }

    uint64_t wal_id(const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        uint64_t id = 0;
        input.read(reinterpret_cast<char*>(&id), sizeof(id));
        REQUIRE(input.good());
        return id;
    }
}

TEST_CASE("Engine non-tail rollback advances durable checkpoints without losing concurrent rows", "[rollback-checkpoint]") {
    otterstax::test::temporary_directory directory{"rollback-checkpoint"};
    auto config = make_create_config(directory.path);
    config.wal.auto_checkpoint_threshold_bytes = 0;
    {
        auto engine = db::make_otterbrix_engine(config);
        const otterbrix::session_id_t aborted, committed, reader;
        const auto run = [&](auto session, const std::string& sql) {
            auto result = engine->dispatcher()->execute_sql(session, sql);
            REQUIRE(result);
            INFO(sql);
            INFO((result->is_error() ? result->get_error().what.c_str() : "success"));
            REQUIRE_FALSE(result->is_error());
            return result;
        };
        run(committed, "CREATE DATABASE transactions;");
        run(committed, "CREATE TABLE transactions.items (id BIGINT);");
        run(committed, "INSERT INTO transactions.items (id) VALUES (1);");
        run(committed, "CHECKPOINT;");
        const auto sidecar = user_sidecar(config.disk.path);
        auto durable_id = wal_id(sidecar);

        run(aborted, "BEGIN;");
        std::string batch = "INSERT INTO transactions.items (id) VALUES ";
        for (int i = 2; i <= 1026; ++i) {
            if (i != 2) batch += ',';
            batch += '(' + std::to_string(i) + ')';
        }
        run(aborted, batch);
        run(committed, "INSERT INTO transactions.items (id) VALUES (2000);");
        run(reader, "BEGIN;");
        REQUIRE(run(reader, "SELECT id FROM transactions.items;")->size() == 2);
        run(aborted, "ROLLBACK;");
        run(committed, "CHECKPOINT;");
        CHECK(wal_id(sidecar) > durable_id);
        durable_id = wal_id(sidecar);
        REQUIRE(run(reader, "SELECT id FROM transactions.items;")->size() == 2);
        run(reader, "ROLLBACK;");

        // The same abort must handle both an interleaved range and a tail range.
        run(aborted, "BEGIN;");
        run(aborted, "INSERT INTO transactions.items (id) VALUES (3000);");
        run(committed, "INSERT INTO transactions.items (id) VALUES (4000);");
        run(aborted, "INSERT INTO transactions.items (id) VALUES (5000);");
        run(aborted, "ROLLBACK;");
        run(committed, "CHECKPOINT;");
        CHECK(wal_id(sidecar) > durable_id);
        auto rows = run(committed, "SELECT id FROM transactions.items ORDER BY id;");
        REQUIRE(rows->size() == 3);
        CHECK(rows->value(0, 0).value<int64_t>() == 1);
        CHECK(rows->value(0, 1).value<int64_t>() == 2000);
        CHECK(rows->value(0, 2).value<int64_t>() == 4000);
    }
    auto reopened = db::make_otterbrix_engine(config);
    auto rows = reopened->dispatcher()->execute_sql(otterbrix::session_id_t{},
                                                    "SELECT id FROM transactions.items ORDER BY id;");
    REQUIRE(rows);
    REQUIRE_FALSE(rows->is_error());
    REQUIRE(rows->size() == 3);
    CHECK(rows->value(0, 0).value<int64_t>() == 1);
    CHECK(rows->value(0, 1).value<int64_t>() == 2000);
    CHECK(rows->value(0, 2).value<int64_t>() == 4000);
}
