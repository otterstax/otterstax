// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Error paths of the two S3 actors that need no object store: a rejected
// credential set, an alias nobody registered, an object path Arrow refuses
// before any request goes out, and a local file that is not there. Every
// verdict must come back as a typed core::error_t — the connector names the
// cause (invalid_parameter / do_not_exists / io_error) and db::S3Manager
// forwards that code unchanged rather than flattening it to other_error.

#include "connectors/s3/manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <filesystem>
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

    std::size_t entries_in(const std::string& dir) {
        std::size_t count = 0;
        std::error_code ec;
        for (auto it = std::filesystem::directory_iterator(dir, ec); !ec && it != std::filesystem::directory_iterator();
             it.increment(ec)) {
            ++count;
        }
        return count;
    }

    struct download_dir {
        std::string path;
        explicit download_dir(const char* name)
            : path((std::filesystem::temp_directory_path() / name).string()) {
            std::filesystem::remove_all(path);
        }
        ~download_dir() { std::filesystem::remove_all(path); }
    };

} // namespace

TEST_CASE("s3 connector: add_credentials rejects an empty alias or empty keys as invalid_parameter") {
    download_dir dir{"otterstax_s3_errors_add"};
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource, dir.path);
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
        auto params = valid_params(resource, "minio");
        params.access_key.clear();
        auto r = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("minio"));
    }

    SECTION("empty secret_key") {
        auto params = valid_params(resource, "minio");
        params.secret_key.clear();
        auto r = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
    }

    SECTION("a rejected alias is not stored") {
        auto params = valid_params(resource, "minio");
        params.secret_key.clear();
        auto rejected = settled(
            actor_zeta::send(s3->address(), &conn::s3::ConnectorManager::add_credentials, id, std::move(params)));
        REQUIRE(rejected.has_error());

        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::upload,
                                          id,
                                          std::string{"minio"},
                                          std::string{"bucket/key.csv"},
                                          std::string{"/nonexistent/file.csv"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::do_not_exists);
    }

    SECTION("a complete credential set is stored and can be re-registered") {
        auto first = settled(actor_zeta::send(s3->address(),
                                              &conn::s3::ConnectorManager::add_credentials,
                                              id,
                                              valid_params(resource, "minio")));
        REQUIRE_FALSE(first.has_error());
        CHECK(first.value());
        auto again = settled(actor_zeta::send(s3->address(),
                                              &conn::s3::ConnectorManager::add_credentials,
                                              id,
                                              valid_params(resource, "minio")));
        REQUIRE_FALSE(again.has_error());
        CHECK(again.value());
    }
}

TEST_CASE("s3 connector: list / download / upload on an unknown alias fail with do_not_exists and touch nothing") {
    download_dir dir{"otterstax_s3_errors_unknown"};
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource, dir.path);
    const auto id = session_id().hash();

    auto listed = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::list,
                                           id,
                                           std::string{"nobody"},
                                           std::string{"bucket/prefix"}));
    REQUIRE(listed.has_error());
    CHECK(listed.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{listed.error().what.c_str()}, ContainsSubstring("nobody"));

    auto downloaded = settled(actor_zeta::send(s3->address(),
                                               &conn::s3::ConnectorManager::download,
                                               id,
                                               std::string{"nobody"},
                                               std::string{"bucket/key.parquet"}));
    REQUIRE(downloaded.has_error());
    CHECK(downloaded.error().type == core::error_code_t::do_not_exists);
    CHECK_THAT(std::string{downloaded.error().what.c_str()}, ContainsSubstring("nobody"));

    auto uploaded = settled(actor_zeta::send(s3->address(),
                                             &conn::s3::ConnectorManager::upload,
                                             id,
                                             std::string{"nobody"},
                                             std::string{"bucket/key.csv"},
                                             std::string{"/nonexistent/file.csv"}));
    REQUIRE(uploaded.has_error());
    CHECK(uploaded.error().type == core::error_code_t::do_not_exists);

    CHECK(entries_in(dir.path) == 0);
}

TEST_CASE("s3 connector: a transfer Arrow refuses is io_error and leaves no file in the download directory") {
    download_dir dir{"otterstax_s3_errors_io"};
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource, dir.path);
    const auto id = session_id().hash();

    auto stored = settled(actor_zeta::send(s3->address(),
                                           &conn::s3::ConnectorManager::add_credentials,
                                           id,
                                           valid_params(resource, "minio")));
    REQUIRE_FALSE(stored.has_error());

    SECTION("download of a bucket-only path (not an object) is rejected before any request") {
        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::download,
                                          id,
                                          std::string{"minio"},
                                          std::string{"bucket-without-key"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("download"));
        CHECK(entries_in(dir.path) == 0);
    }

    SECTION("upload of a missing local file is rejected before any request") {
        const std::string missing = (std::filesystem::path(dir.path) / "does_not_exist.csv").string();
        auto r = settled(actor_zeta::send(s3->address(),
                                          &conn::s3::ConnectorManager::upload,
                                          id,
                                          std::string{"minio"},
                                          std::string{"bucket/key.csv"},
                                          missing));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::io_error);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("upload"));
    }
}

TEST_CASE("S3Manager: forwards the connector's error code instead of flattening it to other_error") {
    download_dir dir{"otterstax_s3_errors_mgr"};
    auto* resource = std::pmr::new_delete_resource();
    auto s3 = actor_zeta::spawn<conn::s3::ConnectorManager>(resource, dir.path);
    // No file manager is reachable: every case below fails before the
    // integration actor would forward to it.
    auto mgr = actor_zeta::spawn<db::S3Manager>(resource,
                                                s3->address(),
                                                actor_zeta::address_t::empty_address(),
                                                dir.path);
    const auto id = session_id().hash();

    SECTION("download on an unknown alias is do_not_exists and names the alias") {
        auto r = settled(actor_zeta::send(mgr->address(),
                                          &db::S3Manager::download,
                                          id,
                                          std::string{"nobody"},
                                          std::string{"bucket/key.parquet"},
                                          std::string{"db"},
                                          std::string{"tbl"}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::do_not_exists);
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("nobody"));
        CHECK_THAT(std::string{r.error().what.c_str()}, ContainsSubstring("S3Manager::download"));
        CHECK(entries_in(dir.path) == 0);
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
                                          OtterbrixStatementPtr{}));
        REQUIRE(r.has_error());
        CHECK(r.error().type == core::error_code_t::invalid_parameter);
    }
}
