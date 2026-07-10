// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "s3_manager.hpp"

#include "connectors/file/manager.hpp"
#include "connectors/file/types.hpp"
#include "connectors/s3/manager.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <filesystem>
#include <thread>

namespace db {

namespace {

namespace fs = std::filesystem;

conn::file::FileFormat format_of(const std::string& key) {
    const auto ext = fs::path(key).extension().string();
    if (ext == ".parquet")                   return conn::file::FileFormat::Parquet;
    if (ext == ".csv" || ext == ".tsv")      return conn::file::FileFormat::CSV;
    if (ext == ".ndjson" || ext == ".jsonl") return conn::file::FileFormat::NDJSON;
    return conn::file::FileFormat::Unknown;
}

void remove_quietly(const std::string& path) {
    std::error_code ec;
    fs::remove(path, ec);
}

} // namespace

S3Manager::S3Manager(std::pmr::memory_resource* res,
                     actor_zeta::address_t s3_connector,
                     actor_zeta::address_t file_manager,
                     std::string s3_upload_path)
    : resource_(res)
    , s3_connector_(std::move(s3_connector))
    , file_manager_(std::move(file_manager))
    , s3_upload_path_(std::move(s3_upload_path))
    , log_(get_logger(logger_tag::S3_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    if (s3_upload_path_.empty()) {
        log_->warn("s3_upload_path is empty; using default /tmp/otterstax_s3_upload/");
        s3_upload_path_ = "/tmp/otterstax_s3_upload/";
    }
    std::error_code ec;
    fs::create_directories(s3_upload_path_, ec);
}

std::pair<bool, actor_zeta::detail::enqueue_result>
S3Manager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
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

actor_zeta::behavior_t S3Manager::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<S3Manager, &S3Manager::download>)
        co_await actor_zeta::dispatch(this, &S3Manager::download, msg);
    else if (cmd == actor_zeta::msg_id<S3Manager, &S3Manager::upload>)
        co_await actor_zeta::dispatch(this, &S3Manager::upload, msg);
    else if (cmd == actor_zeta::msg_id<S3Manager, &S3Manager::ls>)
        co_await actor_zeta::dispatch(this, &S3Manager::ls, msg);
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
S3Manager::download(session_hash_t id, std::string alias, std::string s3_path,
                    std::string database, std::string table) {
    OTX_ZONE_N("s3::download");
    log_->trace("S3Manager::download with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Database: {}\n Table: {}", id, alias, s3_path, database, table);
    auto dl = co_await actor_zeta::send(s3_connector_,
                                        &conn::s3::ConnectorManager::download,
                                        id, alias, s3_path)
                  .second;
    if (dl.has_error()) {
        log_->error("download: s3 download failed: {}", dl.error().what);
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("S3Manager::download: " + std::string{dl.error().what.c_str()}).c_str(), resource()});
    }
    const std::string local_path = dl.value();

    conn::file::FileAddParams params;
    params.database = database;
    params.table    = table;
    params.path     = local_path;
    params.format   = "auto";

    auto add = co_await actor_zeta::send(file_manager_,
                                         &conn::file::FileManager::add_file,
                                         id, std::move(params))
                   .second;
    remove_quietly(local_path);
    if (add.has_error() || add.value() != true) {
        const std::string why =
            add.has_error() ? std::string{add.error().what.c_str()} : std::string{"add_file returned false"};
        log_->error("download: mapping into otterbrix failed: {}", why);
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("S3Manager::download: " + why).c_str(), resource()});
    }

    log_->debug("download: s3://{} -> {}.{}", s3_path, database, table);
    co_return std::pair<bool, std::string>{true, s3_path};
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
S3Manager::upload(session_hash_t id, std::string alias, std::string s3_path,
                  OtterbrixStatementPtr statement) {
    OTX_ZONE_N("s3::upload");
    log_->trace("S3Manager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Statement: {}", id, alias, s3_path, statement ? "set" : "null");
    if (!statement) {
        log_->error("upload: statement is null");
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"S3Manager::upload: statement must not be null", resource()});
    }
    const conn::file::FileFormat fmt = format_of(s3_path);
    if (fmt == conn::file::FileFormat::Unknown) {
        log_->error("upload: cannot determine format from '{}'", s3_path);
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("S3Manager::upload: cannot determine format from '" + s3_path + "'").c_str(), resource()});
    }

    conn::file::FileMetadata meta;
    meta.statement    = std::move(statement);
    meta.path         = (fs::path(s3_upload_path_) / fs::path(s3_path).filename()).string();
    meta.format       = fmt;
    meta.is_temporary = true; // staging file in the upload dir; timestamp-prefix it

    auto dump = co_await actor_zeta::send(file_manager_,
                                          &conn::file::FileManager::dump_file,
                                          id, std::move(meta))
                    .second;
    if (dump.has_error()) {
        log_->error("upload: dump failed for s3://{}: {}", s3_path, dump.error().what);
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("S3Manager::upload: " + std::string{dump.error().what.c_str()}).c_str(), resource()});
    }

    const std::string dumped = dump.value();
    log_->trace("S3Manager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Dumped Path: {}", id, alias, s3_path, dumped);
    auto up = co_await actor_zeta::send(s3_connector_,
                                        &conn::s3::ConnectorManager::upload,
                                        id, alias, s3_path, dumped)
                  .second;
    remove_quietly(dumped);
    if (up.has_error() || up.value() != true) {
        const std::string why =
            up.has_error() ? std::string{up.error().what.c_str()} : std::string{"s3 upload returned false"};
        log_->error("upload: s3 upload failed: {}", why);
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{("S3Manager::upload: " + why).c_str(), resource()});
    }

    log_->debug("upload: query result -> s3://{}", s3_path);
    co_return std::pair<bool, std::string>{true, s3_path};
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::string>>>
S3Manager::ls(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("s3::ls");

    log_->trace("S3Manager::ls with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto res = co_await actor_zeta::send(s3_connector_,
                                         &conn::s3::ConnectorManager::list,
                                         id, alias, s3_path)
                   .second;
    if (res.has_error()) {
        log_->error("ls: list failed: {}", res.error().what);
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("S3Manager::ls: " + std::string{res.error().what.c_str()}).c_str(), resource()});
    }

    std::string joined;
    for (const auto& p : res.value()) {
        if (!joined.empty()) joined += '\n';
        joined += p;
    }
    log_->debug("ls: Session ID: {}, Alias: '{}' S3 Path: '{}' -> {} entries", id, alias, s3_path, res.value().size());
    co_return std::pair<bool, std::string>{true, std::move(joined)};
}

} // namespace db
