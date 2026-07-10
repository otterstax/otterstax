// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
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
    OTX_ZONE_N("file::add_file");
    auto opts = arrow::fs::S3Options::FromAccessKey(
        creds.access_key, creds.secret_key,
        creds.session_token);

    if (!creds.region.empty())
        opts.region = creds.region;

    if (!creds.endpoint.empty()) {
        opts.endpoint_override = creds.endpoint;
        // Disable virtual addressing for custom endpoints (e.g. LocalStack)
        opts.scheme = "http";
    }

    return arrow::fs::S3FileSystem::Make(opts);
}

// Stream all bytes from src_fs:src_path → dst_fs:dst_path.
// Used by both download (S3→local) and upload (local→S3).
arrow::Status cp_impl(arrow::fs::FileSystem& src_fs,
                      const std::string&     src_path,
                      arrow::fs::FileSystem& dst_fs,
                      const std::string&     dst_path) {
    OTX_ZONE_N("file::cp_impl");
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
    OTX_ZONE_N("file::timestamped_filename");
    
    std::time_t now = std::time(nullptr);
    std::tm     tm{};
    localtime_r(&now, &tm);
    char ts[20];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d_%H-%M-%S", &tm);
    return std::string{ts} + "_" + std::filesystem::path(s3_path).filename().string();
}

// Convert arrow::Status to a core::error_t result.
#define S3_RETURN_ERROR(status, msg)                                                              \
    if (!(status).ok()) {                                                                         \
        co_return core::error_t(                                                                  \
            core::error_code_t::other_error,                                                      \
            std::pmr::string{(std::string{msg} + ": " + (status).ToString()).c_str(), resource()}); \
    }

} // namespace

// ── ConnectorManager ────────────────────────────────────────────────────────────────

ConnectorManager::ConnectorManager(std::pmr::memory_resource* res, const std::string& s3_download_path)
    : resource_(res)
    , log_(get_logger(logger_tag::S3_MANAGER))
    , s3_download_path_(s3_download_path) {
    assert(log_.is_valid());
    assert(res != nullptr);
    auto status = arrow::fs::EnsureS3Initialized();
    if (!status.ok())
        log_->warn("EnsureS3Initialized: {}", status.ToString());
    if (s3_download_path_.empty()) {
        log_->warn("s3_download_path is empty; using default /tmp/otterstax_s3_cache/");
        s3_download_path_ = "/tmp/otterstax_s3_cache/";
    }
    if (!std::filesystem::exists(s3_download_path_)) {
        std::filesystem::create_directories(s3_download_path_);
    }
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
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::remove_credentials>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::remove_credentials, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::list>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::list, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::download>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::download, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::upload>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::upload, msg);
}

// ── handlers ─────────────────────────────────────────────────────────────────

actor_zeta::unique_future<core::result_wrapper_t<bool>>
ConnectorManager::add_credentials(session_hash_t /*id*/, connect_params params) {
    OTX_ZONE_N("s3::add_credentials");

    if (params.alias.empty()) {
        log_->error("add_credentials: alias is empty");
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"add_credentials: alias must not be empty", resource()});
    }
    if (params.access_key.empty() || params.secret_key.empty()) {
        log_->error("add_credentials: access_key or secret_key is empty");
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"add_credentials: access_key and secret_key are required", resource()});
    }

    const bool updated = credentials_store_.contains(params.alias);
    credentials_store_[params.alias] = params;
    log_->debug("add_credentials: {} alias '{}'",
                updated ? "updated" : "stored", params.alias);
    co_return true;
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
ConnectorManager::remove_credentials(session_hash_t id, std::string alias) {
    OTX_ZONE_N("s3::remove_credentials");

    log_->trace("ConnectorManager::remove_credentials with params:\n Session ID: {}\n Alias: {}", id, alias);
    if (alias.empty()) {
        log_->error("remove_credentials: alias is empty");
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"remove_credentials: alias must not be empty", resource()});
    }

    if (credentials_store_.erase(alias) == 0) {
        log_->error("remove_credentials: unknown alias '{}'", alias);
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("remove_credentials: unknown alias '" + alias + "'").c_str(), resource()});
    }

    log_->debug("remove_credentials: removed alias '{}'", alias);
    co_return true;
}

actor_zeta::unique_future<core::result_wrapper_t<std::vector<std::string>>>
ConnectorManager::list(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::list");
    log_->trace("ConnectorManager::list with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto it = credentials_store_.find(alias);
    if (it == credentials_store_.end()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("list: unknown alias '" + alias + "'").c_str(), resource()});
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("list: " + fs_result.status().ToString()).c_str(), resource()});
    }
    auto& fs = *fs_result;

    arrow::fs::FileSelector sel;
    sel.base_dir       = s3_path;
    sel.recursive      = false;
    sel.allow_not_found = true;

    auto infos_result = fs->GetFileInfo(sel);
    if (!infos_result.ok()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("list: " + infos_result.status().ToString()).c_str(), resource()});
    }

    std::vector<std::string> paths;
    paths.reserve(infos_result->size());
    for (const auto& info : *infos_result)
        paths.push_back(info.path());

    log_->debug("list: alias '{}' s3_path '{}' → {} entries",
                alias, s3_path, paths.size());
    co_return paths;
}

actor_zeta::unique_future<core::result_wrapper_t<std::string>>
ConnectorManager::download(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::download");
    log_->trace("ConnectorManager::download with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto it = credentials_store_.find(alias);
    if (it == credentials_store_.end()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("download: unknown alias '" + alias + "'").c_str(), resource()});
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("download: " + fs_result.status().ToString()).c_str(), resource()});
    }

    arrow::fs::LocalFileSystem local_fs;
    const std::string local_path =
        (std::filesystem::path(s3_download_path_) / timestamped_filename(s3_path)).string();

    auto status = cp_impl(**fs_result, s3_path, local_fs, local_path);
    S3_RETURN_ERROR(status, "download")

    log_->debug("download: s3://{} → {}", s3_path, local_path);
    co_return local_path;
}

actor_zeta::unique_future<core::result_wrapper_t<bool>>
ConnectorManager::upload(session_hash_t id, std::string alias, std::string s3_path, std::string local_path) {
    OTX_ZONE_N("s3::upload");
    log_->trace("ConnectorManager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Local Path: {}", id, alias, s3_path, local_path);
    auto it = credentials_store_.find(alias);
    if (it == credentials_store_.end()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("upload: unknown alias '" + alias + "'").c_str(), resource()});
    }

    auto fs_result = make_s3fs(it->second);
    if (!fs_result.ok()) {
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("upload: " + fs_result.status().ToString()).c_str(), resource()});
    }

    arrow::fs::LocalFileSystem local_fs;

    auto status = cp_impl(local_fs, local_path, **fs_result, s3_path);
    S3_RETURN_ERROR(status, "upload")

    log_->debug("upload: {} → s3://{}", local_path, s3_path);
    co_return true;
}

} // namespace conn::s3
