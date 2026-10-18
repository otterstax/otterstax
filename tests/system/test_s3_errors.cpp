// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Error paths of the S3 actors that need no object store, each a typed core::error_t that
// S3Manager forwards unchanged; and the abort contract: opening an output stream sends nothing,
// so an upload aborted before any data costs no request and publishes nothing.

#include "connectors/s3/manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "utility/session.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/io/interfaces.h>

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>
#include <utility>

namespace {

    using Catch::Matchers::ContainsSubstring;

    // Both S3 actors run their handlers to completion inside enqueue_impl, so a
    // future returned by send() is settled by the time send() returns.
    template<typename Sent>
    auto settled(Sent&& sent) {
        REQUIRE(sent.second.is_ready());
        return std::move(sent.second).take_ready();
    }

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
        // A custom endpoint keeps the client from resolving a real AWS host.
        params.endpoint = "127.0.0.1:1";
        return params;
    }

} // namespace

TEST_CASE("s3 connector: add_credentials rejects an empty alias or empty keys as invalid_parameter") {
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    const auto id = session_id().hash();

    SECTION("empty alias") {
        auto params = valid_params(resource, "");
        auto r = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("alias"));
    }

    SECTION("empty access_key") {
        auto params = valid_params(resource, "rustfs");
        params.access_key.clear();
        auto r = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("rustfs"));
    }

    SECTION("empty secret_key") {
        auto params = valid_params(resource, "rustfs");
        params.secret_key.clear();
        auto r = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
    }

    SECTION("a rejected alias is not stored") {
        auto params = valid_params(resource, "rustfs");
        params.secret_key.clear();
        auto rejected = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(rejected.has_error());

        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::open_output,
                                          id,
                                          std::string{"rustfs"},
                                          std::string{"bucket/key.csv"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::do_not_exists);
    }

    SECTION("a complete credential set is stored and can be re-registered") {
        auto first = settled(actor_zeta::send(s3->address(),
                                              &conn::s3::ConnectorManager::add_credentials,
                                              id,
                                              valid_params(resource, "rustfs")));
        REQUIRE_FALSE(first.has_error());
        CHECK(first.value());
        // Builds and caches the alias's filesystem.
        auto before = settled(actor_zeta::send(s3->address(),
                                               &conn::s3::ConnectorManager::open_input,
                                               id,
                                               std::string{"rustfs"},
                                               std::string{"bucket-without-key"}));
        REQUIRE(before.has_error());
        CHECK(before.error().type == core::error_code_t::io_error);

        auto again = settled(actor_zeta::send(s3->address(),
                                              &conn::s3::ConnectorManager::add_credentials,
                                              id,
                                              valid_params(resource, "rustfs")));
        REQUIRE_FALSE(again.has_error());
        CHECK(again.value());
        // The re-registration dropped the cached filesystem; the next call builds a
        // fresh one from the new set and answers the same verdict.
        auto after = settled(actor_zeta::send(s3->address(),
                                              &conn::s3::ConnectorManager::open_input,
                                              id,
                                              std::string{"rustfs"},
                                              std::string{"bucket-without-key"}));
        REQUIRE(after.has_error());
        CHECK(after.error().type == core::error_code_t::io_error);
    }
}

TEST_CASE("s3 connector: list / open_input / open_output on an unknown alias fail with do_not_exists") {
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    const auto id = session_id().hash();

    auto listed = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::list,
                                           id,
                                           std::string{"nobody"},
                                           std::string{"bucket/prefix"}));
    REQUIRE(listed.has_error());
    CHECK(listed.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{listed.error().what.c_str()}, ContainsSubstring("nobody"));

    auto input = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::open_input,
                                          id,
                                          std::string{"nobody"},
                                          std::string{"bucket/key.parquet"}));
    REQUIRE(input.has_error());
    CHECK(input.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{input.error().what.c_str()}, ContainsSubstring("nobody"));
    CHECK_THAT(std::string{input.error().what.c_str()}, ContainsSubstring("open_input"));

    auto output = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::open_output,
                                           id,
                                           std::string{"nobody"},
                                           std::string{"bucket/key.csv"}));
    REQUIRE(output.has_error());
    CHECK(output.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{output.error().what.c_str()}, ContainsSubstring("open_output"));
}

TEST_CASE("s3 connector: a path Arrow refuses is io_error before any request") {
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    const auto id = session_id().hash();

    auto stored = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::add_credentials,
                                           id,
                                           valid_params(resource, "rustfs")));
    REQUIRE_FALSE(stored.has_error());

    SECTION("open_input of a bucket-only path (not an object)") {
        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::open_input,
                                          id,
                                          std::string{"rustfs"},
                                          std::string{"bucket-without-key"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("open_input"));
    }

    SECTION("open_output of a bucket-only path (not an object)") {
        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::open_output,
                                          id,
                                          std::string{"rustfs"},
                                          std::string{"bucket-without-key"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("open_output"));
    }
}

TEST_CASE("s3 connector: an output stream aborted before any write sends nothing") {
    // The endpoint refuses every connection, so open + abort both succeeding proves neither went
    // to the network: the multipart upload is created on the first write only.
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    const auto id = session_id().hash();

    auto stored = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::add_credentials,
                                           id,
                                           valid_params(resource, "minio")));
    REQUIRE_FALSE(stored.has_error());

    auto opened = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::open_output,
                                           id,
                                           std::string{"minio"},
                                           std::string{"bucket/out.csv"}));
    REQUIRE_FALSE(opened.has_error());
    REQUIRE(opened.value() != nullptr);
    auto aborted = opened.value()->Abort();
    INFO("Abort: " << aborted.ToString());
    CHECK(aborted.ok());
    CHECK(opened.value()->closed());
}

TEST_CASE("S3Manager: forwards the connector's error code instead of flattening it to other_error") {
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    // No file manager is reachable: every case below fails before the
    // integration actor would forward to it.
    auto mgr = actor_zeta::spawn<db::S3Manager>(resource, s3->address(), actor_zeta::address_t::empty_address());
    const auto id = session_id().hash();

    SECTION("download on an unknown alias is do_not_exists and names the alias") {
        auto r = settled(actor_zeta::send(mgr->address(),
                                          &db::S3Manager::download,
                                          id,
                                          std::string{"nobody"},
                                          std::string{"bucket/key.parquet"},
                                          std::string{"db"},
                                          std::string{"tbl"},
                                          std::string{}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::do_not_exists);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("nobody"));
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("S3Manager::download"));
    }

    SECTION("ls on an unknown alias is do_not_exists") {
        auto r = settled(actor_zeta::send(mgr->address(),
                                          &db::S3Manager::ls,
                                          id,
                                          std::string{"nobody"},
                                          std::string{"bucket/prefix"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::do_not_exists);
    }

    SECTION("upload with a null statement is invalid_parameter") {
        auto r = settled(actor_zeta::send(mgr->address(),
                                          &db::S3Manager::upload,
                                          id,
                                          std::string{"nobody"},
                                          std::string{"bucket/out.csv"},
                                          OtterbrixStatementPtr{},
                                          std::string{}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
    }
}
