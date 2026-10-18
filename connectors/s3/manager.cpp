// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <thread>

namespace conn::s3 {

namespace {

// ── helpers ──────────────────────────────────────────────────────────────────

// Every buffer the filesystem's streams allocate (ranged-read buffers, multipart
// parts) comes from `res`, through the same pool the translators use.
arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>>
make_s3fs(std::pmr::memory_resource* res, const connect_params& creds) {
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
    // An output stream creates its multipart upload on the first write, so one
    // aborted before any data reaches the bucket costs no request.
    opts.allow_delayed_open = true;

    return arrow::fs::S3FileSystem::Make(opts, arrow::io::IOContext(tsl::arrow_pool(res)));
}

core::error_t unknown_alias(std::pmr::memory_resource* res, std::string_view op, std::string_view alias) {
    return tsl::make_error(res, core::error_code_t::do_not_exists,
                           std::string{op} + ": unknown alias '" + std::string{alias} + "'");
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

ConnectorManager::ConnectorManager(std::pmr::memory_resource* res)
    : resource_(res)
    , log_(get_logger(logger_tag::S3_MANAGER))
    , credentials_store_(res)
    , filesystems_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    auto status = arrow::fs::EnsureS3Initialized();
    if (!status.ok())
        log_->warn("EnsureS3Initialized: {}", status.ToString());
}

core::result_wrapper_t<std::shared_ptr<arrow::fs::S3FileSystem>>
ConnectorManager::filesystem_for(std::string_view op, std::string_view alias) {
    OTX_ZONE_N("s3::ConnectorManager::filesystem_for");
    auto creds = credentials_store_.find(alias);
    if (creds == credentials_store_.end()) {
        log_->error("{}: unknown alias '{}'", op, alias);
        return unknown_alias(resource(), op, alias);
    }
    if (auto cached = filesystems_.find(alias); cached != filesystems_.end()) {
        return cached->second;
    }
    auto made = make_s3fs(resource(), creds->second);
    if (!made.ok()) {
        return tsl::arrow_error(resource(), core::error_code_t::io_error, op, made.status());
    }
    std::pmr::string key{alias, resource()};
    return filesystems_.emplace(std::move(key), std::move(*made)).first->second;
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
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::open_input>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::open_input, msg);
    else if (cmd == actor_zeta::msg_id<ConnectorManager, &ConnectorManager::open_output>)
        co_await actor_zeta::dispatch(this, &ConnectorManager::open_output, msg);
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
        // The cached client was built from the old set.
        if (auto cached = filesystems_.find(std::string_view{params.alias}); cached != filesystems_.end()) {
            filesystems_.erase(cached);
        }
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
    auto fs_result = filesystem_for("list", alias);
    if (fs_result.has_error()) {
        co_return fs_result.convert_error<std::pmr::vector<std::pmr::string>>();
    }
    const auto& fs = fs_result.value();

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

actor_zeta::unique_future<core::result_wrapper_t<std::shared_ptr<arrow::io::RandomAccessFile>>>
ConnectorManager::open_input(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::ConnectorManager::open_input");
    log_->trace("ConnectorManager::open_input with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto fs = filesystem_for("open_input", alias);
    if (fs.has_error()) {
        co_return fs.convert_error<std::shared_ptr<arrow::io::RandomAccessFile>>();
    }
    auto opened = fs.value()->OpenInputFile(s3_path);
    if (!opened.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "open_input", opened.status());
    }
    log_->debug("open_input: s3://{}", s3_path);
    co_return std::move(*opened);
}

actor_zeta::unique_future<core::result_wrapper_t<std::shared_ptr<arrow::io::OutputStream>>>
ConnectorManager::open_output(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::ConnectorManager::open_output");
    log_->trace("ConnectorManager::open_output with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto fs = filesystem_for("open_output", alias);
    if (fs.has_error()) {
        co_return fs.convert_error<std::shared_ptr<arrow::io::OutputStream>>();
    }
    auto opened = fs.value()->OpenOutputStream(s3_path);
    if (!opened.ok()) {
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error, "open_output", opened.status());
    }
    log_->debug("open_output: s3://{}", s3_path);
    co_return std::move(*opened);
}

} // namespace conn::s3
