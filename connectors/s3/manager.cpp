// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <ctime>
#include <filesystem>
#include <thread>

namespace conn::s3 {

namespace {

// ── helpers ──────────────────────────────────────────────────────────────────

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>>
make_s3fs(const connect_params& creds) {
    OTX_ZONE_N("s3::make_s3fs");
    auto opts = arrow::fs::S3Options::FromAccessKey(std::string{std::string_view{creds.access_key}},
                                                    std::string{std::string_view{creds.secret_key}},
                                                    std::string{std::string_view{creds.session_token}});

    if (!creds.region.empty())
        opts.region = std::string_view{creds.region};

    if (!creds.endpoint.empty()) {
        opts.endpoint_override = std::string_view{creds.endpoint};
        // Disable virtual addressing for custom endpoints (e.g. LocalStack)
        opts.scheme = "http";
    }

    return arrow::fs::S3FileSystem::Make(opts);
}

// Stream all bytes from src_fs:src_path → dst_fs:dst_path.
// Used by both download (S3→local) and upload (local→S3). The source is opened
// first, so a missing source never creates an empty destination.
arrow::Status cp_impl(arrow::fs::FileSystem& src_fs,
                      const std::string&     src_path,
                      arrow::fs::FileSystem& dst_fs,
                      const std::string&     dst_path) {
    OTX_ZONE_N("s3::cp_impl");
    constexpr int64_t kChunk = 1 << 20; // 1 MiB

    ARROW_ASSIGN_OR_RAISE(auto input, src_fs.OpenInputStream(src_path));
    ARROW_ASSIGN_OR_RAISE(auto output, dst_fs.OpenOutputStream(dst_path));

    while (true) {
        ARROW_ASSIGN_OR_RAISE(auto buf, input->Read(kChunk));
        if (buf->size() == 0) break;
        ARROW_RETURN_NOT_OK(output->Write(buf->data(), buf->size()));
    }

    ARROW_RETURN_NOT_OK(output->Close());
    return arrow::Status::OK();
}

// Build a unique local filename for a downloaded object by prefixing the
// object's basename with a timestamp, e.g.
//   "bucket/data/costs.parquet" → "2026-05-23_10-34-35_costs.parquet"
std::string timestamped_filename(const std::string& s3_path) {
    std::time_t now = std::time(nullptr);
    std::tm     tm{};
    localtime_r(&now, &tm);
    char ts[20];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d_%H-%M-%S", &tm);
    return std::string{ts} + "_" + std::filesystem::path(s3_path).filename().string();
}

core::error_t unknown_alias(std::pmr::memory_resource* res, std::string_view op, const std::string& alias) {
    return tsl::make_error(res, core::error_code_t::do_not_exists,
                           std::string{op} + ": unknown alias '" + alias + "'");
}

// The credential store outlives the message that delivered a credential set,
// so what it keeps is rebuilt on the manager's resource instead of staying on
// whichever resource the sender allocated the strings from.
connect_params stored_on(std::pmr::memory_resource* res, const connect_params& params) {
    return connect_params{
        .region = std::pmr::string{std::string_view{params.region}, res},
        .access_key = std::pmr::string{std::string_view{params.access_key}, res},
        .secret_key = std::pmr::string{std::string_view{params.secret_key}, res},
        .session_token = std::pmr::string{std::string_view{params.session_token}, res},
        .endpoint = std::pmr::string{std::string_view{params.endpoint}, res},
        .alias = std::pmr::string{std::string_view{params.alias}, res},
    };
}

} // namespace

// ── ConnectorManager ────────────────────────────────────────────────────────────────

ConnectorManager::ConnectorManager(std::pmr::memory_resource* res, std::string_view s3_download_path)
    : resource_(res)
    , log_(get_logger(logger_tag::S3_MANAGER))
    , credentials_store_(res)
    , s3_download_path_(s3_download_path, res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(!s3_download_path_.empty() && "s3 download path must not be empty");
    auto status = arrow::fs::EnsureS3Initialized();
    if (!status.ok())
        log_->warn("EnsureS3Initialized: {}", status.ToString());
    // A directory that cannot be created is reported by the first download,
    // whose OpenOutputStream fails with the real cause.
    std::error_code ec;
    std::filesystem::create_directories(std::string_view{s3_download_path_}, ec);
    if (ec)
        log_->error("cannot create s3 download directory '{}': {}", s3_download_path_, ec.message());
}

std::pair<bool, actor_zeta::detail::enqueue_result>
ConnectorManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    std::lock_guard<std::mutex> guard(mutex_);
    current_behavior_ = behavior(msg.get());

    while (current_behavior_.is_busy()) {
        if (current_behavior_.is_awaited_ready()) {
            auto cont = current_behavior_.take_awaited_continuation();
            if (cont) cont.resume();
        } else {
            std::this_thread::yield();
        }
    }

    return {false, actor_zeta::detail::enqueue_result::success};
}

actor_zeta::behavior_t ConnectorManager::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::add_credentials>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::add_credentials, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::list>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::list, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::download>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::download, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::upload>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::upload, msg);
}

// ── handlers ─────────────────────────────────────────────────────────────────
// Arrow reports every S3 failure through a returned Status/Result, so nothing
// below throws; each verdict is converted to a core::error_t at the call site.

actor_zeta::unique_future<core::result_wrapper_t<bool>>
ConnectorManager::add_credentials(session_hash_t /*id*/, connect_params params) {
    OTX_ZONE_N("s3::ConnectorManager::add_credentials");

    if (params.alias.empty()) {
        log_->error("add_credentials: alias is empty");
        co_return tsl::make_error(resource(), core::error_code_t::invalid_parameter,
                                  "add_credentials: alias must not be empty");
    }
    if (params.access_key.empty() || params.secret_key.empty()) {
        log_->error("add_credentials: access_key or secret_key is empty for alias '{}'", params.alias);
        co_return tsl::make_error(resource(), core::error_code_t::invalid_parameter,
                                  "add_credentials: access_key and secret_key are required for alias '" +
                                      std::string{std::string_view{params.alias}} + "'");
    }

    auto it = credentials_store_.find(std::string_view{params.alias});
    const bool updated = it != credentials_store_.end();
    if (updated) {
        it->second = stored_on(resource(), params);
    } else {
        std::pmr::string key{std::string_view{params.alias}, resource()};
        it = credentials_store_.emplace(std::move(key), stored_on(resource(), params)).first;
    }
    log_->debug("add_credentials: {} alias '{}'", updated ? "updated" : "stored", it->first);
    co_return true;
}

actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::string>>>
ConnectorManager::list(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::ConnectorManager::list");
    log_->trace("ConnectorManager::list with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto it = credentials_store_.find(std::string_view{alias});
    if (it == credentials_store_.end()) {
        log_->error("list: unknown alias '{}'", alias);
        co_return unknown_alias(resource(), "list", alias);
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "list", fs_result.status());
    }
    auto& fs = *fs_result;

    arrow::fs::FileSelector sel;
    sel.base_dir       = s3_path;
    sel.recursive      = false;
    sel.allow_not_found = true;

    auto infos_result = fs->GetFileInfo(sel);
    if (!infos_result.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "list", infos_result.status());
    }

    std::pmr::vector<std::pmr::string> paths{resource()};
    paths.reserve(infos_result->size());
    for (const auto& info : *infos_result)
        paths.emplace_back(std::string_view{info.path()});

    log_->debug("list: alias '{}' s3_path '{}' → {} entries",
                alias, s3_path, paths.size());
    co_return std::move(paths);
}

actor_zeta::unique_future<core::result_wrapper_t<std::pmr::string>>
ConnectorManager::download(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::ConnectorManager::download");
    log_->trace("ConnectorManager::download with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto it = credentials_store_.find(std::string_view{alias});
    if (it == credentials_store_.end()) {
        log_->error("download: unknown alias '{}'", alias);
        co_return unknown_alias(resource(), "download", alias);
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "download", fs_result.status());
    }

    arrow::fs::LocalFileSystem local_fs;
    const std::string local_path =
        (std::filesystem::path(std::string_view{s3_download_path_}) / timestamped_filename(s3_path)).string();

    auto status = cp_impl(**fs_result, s3_path, local_fs, local_path);
    if (!status.ok()) {
        // A transfer that failed mid-way has already created the destination;
        // a truncated object must never be handed to the loader.
        std::error_code ec;
        std::filesystem::remove(local_path, ec);
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "download", status);
    }

    log_->debug("download: s3://{} → {}", s3_path, local_path);
    co_return std::pmr::string{std::string_view{local_path}, resource()};
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
ConnectorManager::upload(session_hash_t id, std::string alias, std::string s3_path, std::string local_path) {
    OTX_ZONE_N("s3::ConnectorManager::upload");
    log_->trace("ConnectorManager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Local Path: {}", id, alias, s3_path, local_path);
    auto it = credentials_store_.find(std::string_view{alias});
    if (it == credentials_store_.end()) {
        log_->error("upload: unknown alias '{}'", alias);
        co_return unknown_alias(resource(), "upload", alias);
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "upload", fs_result.status());
    }

    arrow::fs::LocalFileSystem local_fs;

    auto status = cp_impl(local_fs, local_path, **fs_result, s3_path);
    if (!status.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "upload", status);
    }

    log_->debug("upload: {} → s3://{}", local_path, s3_path);
    co_return true;
}

} // namespace conn::s3
