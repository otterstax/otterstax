// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Error paths of the S3 transfers that need no object store, each a typed core::error_t the
// Worker forwards unchanged; and the abort contract: opening an output stream sends nothing,
// so an export that fails before any data costs no request and publishes nothing.

#include "connectors/common/transfer.hpp"
#include "connectors/s3/driver.hpp"
#include "scheduler_stack.hpp"
#include "utility/session.hpp"

#include "../mock/drive.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <chrono>
#include <memory>
#include <memory_resource>
#include <string>
#include <utility>

namespace {

    using Catch::Matchers::ContainsSubstring;
    using otterstax::test::drive;

    constexpr std::chrono::milliseconds kConnectTimeout{1000};

    conn::s3::connect_params valid_params(std::pmr::memory_resource* resource, const std::string& alias) {
        conn::s3::connect_params params{
            .region = std::pmr::string{resource},
            .access_key = std::pmr::string{resource},
            .secret_key = std::pmr::string{resource},
            .session_token = std::pmr::string{resource},
            .endpoint = std::pmr::string{resource},
            .alias = std::pmr::string{resource},
        };
        params.alias = alias;
        params.access_key = "ak";
        params.secret_key = "sk";
        params.region = "us-east-1";
        // A custom endpoint that refuses every connection keeps the client from
        // resolving a real AWS host, and makes any request that does go out fail.
        params.endpoint = "127.0.0.1:1";
        return params;
    }

    // The alias's transfer pool over a driver for valid_params.
    std::unique_ptr<conn::transfer_pools_t> pools_with(std::pmr::memory_resource* resource, const std::string& alias) {
        auto driver = conn::s3::driver_t::make(resource, valid_params(resource, alias), kConnectTimeout);
        INFO("make: " << (driver.has_error() ? driver.error().what.c_str() : "ok"));
        REQUIRE_FALSE(driver.has_error());
        auto pools = std::make_unique<conn::transfer_pools_t>(resource);
        REQUIRE_FALSE(pools->add_s3(alias, std::move(driver.value()), {}).contains_error());
        return pools;
    }

    // A result with one typed column and no rows: what an export writes.
    components::cursor::cursor_t_ptr empty_result(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::INTEGER, "id");
        return components::cursor::make_cursor(resource, std::move(types));
    }

} // namespace

TEST_CASE("s3 driver: make rejects an empty alias or empty keys as invalid_parameter") {
    auto* resource = std::pmr::new_delete_resource();

    SECTION("empty alias") {
        auto r = conn::s3::driver_t::make(resource, valid_params(resource, ""), kConnectTimeout);
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("alias"));
    }

    SECTION("empty access_key") {
        auto params = valid_params(resource, "rustfs");
        params.access_key.clear();
        auto r = conn::s3::driver_t::make(resource, params, kConnectTimeout);
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("rustfs"));
    }

    SECTION("empty secret_key") {
        auto params = valid_params(resource, "rustfs");
        params.secret_key.clear();
        auto r = conn::s3::driver_t::make(resource, params, kConnectTimeout);
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
    }

    SECTION("a second registration of an alias is invalid_parameter") {
        auto pools = pools_with(resource, "rustfs");
        auto again = conn::s3::driver_t::make(resource, valid_params(resource, "rustfs"), kConnectTimeout);
        REQUIRE_FALSE(again.has_error());
        auto added = pools->add_s3("rustfs", std::move(again.value()), {});
        REQUIRE(added.contains_error());
        CHECK(added.type == core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("s3 transfers: load / export on an unknown alias fail with do_not_exists") {
    auto* resource = std::pmr::new_delete_resource();
    conn::transfer_pools_t pools(resource);

    auto input = drive(resource, pools.s3("open_input", "nobody", conn::load_t{"bucket/key.parquet", ""}));
    REQUIRE(input.has_error());
    CHECK(input.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{input.error().what.c_str()}, ContainsSubstring("nobody"));
    CHECK_THAT(std::string{input.error().what.c_str()}, ContainsSubstring("open_input"));

    auto output = drive(resource,
                        pools.s3("open_output",
                                 "nobody",
                                 conn::export_t{"bucket/key.csv", conn::file::FileFormat::CSV, empty_result(resource)}));
    REQUIRE(output.has_error());
    CHECK(output.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{output.error().what.c_str()}, ContainsSubstring("open_output"));
}

TEST_CASE("s3 driver: a path Arrow refuses is io_error before any request") {
    auto* resource = std::pmr::new_delete_resource();
    auto pools = pools_with(resource, "rustfs");

    SECTION("load of a bucket-only path (not an object)") {
        auto r = drive(resource, pools->s3("open_input", "rustfs", conn::load_t{"bucket-without-key", "csv"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("open_input"));
    }

    SECTION("export to a bucket-only path (not an object)") {
        auto r = drive(
            resource,
            pools->s3("open_output",
                      "rustfs",
                      conn::export_t{"bucket-without-key", conn::file::FileFormat::CSV, empty_result(resource)}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("open_output"));
    }
}

TEST_CASE("s3 driver: an export that fails before any write sends nothing") {
    // The endpoint refuses every connection. The export opens its stream, the write refuses the
    // format and the stream is aborted: had the open or the abort gone to the network, the answer
    // would be the endpoint's io_error. The multipart upload is created on the first write only.
    auto* resource = std::pmr::new_delete_resource();
    auto pools = pools_with(resource, "minio");

    auto r = drive(resource,
                   pools->s3("open_output",
                             "minio",
                             conn::export_t{"bucket/out.csv", conn::file::FileFormat::Unknown, empty_result(resource)}));
    REQUIRE(r.has_error());
    CHECK(r.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("s3 statements: the Worker forwards the transfer's error code instead of flattening it") {
    otterstax::test::with_scheduler_stack("/tmp/test_s3_errors_worker_otb", [](const otterstax::test::scheduler_stack& s) {
        session_hash_t id = 9800;

        SECTION("CREATE EXTERNAL TABLE on an unknown alias is do_not_exists and names the alias") {
            auto r = otterstax::test::run_scheduler_sql_payload(
                s,
                id++,
                "CREATE EXTERNAL TABLE db.tbl WITH (s3_alias = 'nobody', location = 's3://bucket/key.parquet', "
                "format = 'parquet');");
            REQUIRE(r.has_error());
            CHECK(r.error().type == core::error_code_t::do_not_exists);
            CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("nobody"));
            CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("CREATE EXTERNAL TABLE"));
        }

        SECTION("COPY ... TO an unknown alias is do_not_exists before the query runs") {
            auto r = otterstax::test::run_scheduler_sql_payload(
                s,
                id++,
                "COPY (SELECT * FROM nodb.notable) TO 's3://bucket/out.csv' WITH (s3_alias = 'nobody', format = 'csv');");
            REQUIRE(r.has_error());
            CHECK(r.error().type == core::error_code_t::do_not_exists);
            CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("open_output"));
        }

        SECTION("COPY ... TO an extension-less key without a format is invalid_parameter") {
            auto r = otterstax::test::run_scheduler_sql_payload(
                s,
                id++,
                "COPY (SELECT 1) TO 's3://bucket/out' WITH (s3_alias = 'nobody');");
            REQUIRE(r.has_error());
            CHECK(r.error().type == core::error_code_t::invalid_parameter);
        }
    });
}
