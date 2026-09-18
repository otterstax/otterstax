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

void remove_quietly(const std::string& path) {
    std::error_code ec;
    fs::remove(path, ec);
}

// This actor's own verdicts; the message is owned by the actor's resource.
core::error_t make_error(std::pmr::memory_resource* res, core::error_code_t code, std::string_view what) {
    std::pmr::string message{res};
    message.append(what.data(), what.size());
    return core::error_t(code, std::move(message));
}

// Re-homes a callee's verdict onto this actor's resource, keeping its code and
// prefixing the message with the failing step.
core::error_t forward_error(std::pmr::memory_resource* res, std::string_view prefix, const core::error_t& error) {
    std::pmr::string message{res};
    message.reserve(prefix.size() + error.what.size());
    message.append(prefix.data(), prefix.size());
    message.append(error.what.data(), error.what.size());
    return core::error_t(error.type, std::move(message));
}

} // namespace

S3Manager::S3Manager(std::pmr::memory_resource* res,
                     actor_zeta::address_t s3_connector,
                     actor_zeta::address_t file_manager,
                     std::string_view s3_upload_path)
    : resource_(res)
    , s3_connector_(std::move(s3_connector))
    , file_manager_(std::move(file_manager))
    , s3_upload_path_(s3_upload_path, res)
    , log_(get_logger(logger_tag::S3_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(!s3_upload_path_.empty() && "s3 upload path must not be empty");
    // A directory that cannot be created is reported by the first upload,
    // whose dump fails with the real cause.
    std::error_code ec;
    fs::create_directories(std::string_view{s3_upload_path_}, ec);
    if (ec)
        log_->error("cannot create s3 upload directory '{}': {}", s3_upload_path_, ec.message());
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

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
S3Manager::download(session_hash_t id, std::string alias, std::string s3_path,
                    std::string database, std::string table) {
    OTX_ZONE_N("S3Manager::download");
    log_->trace("S3Manager::download with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Database: {}\n Table: {}", id, alias, s3_path, database, table);
    auto dl = co_await actor_zeta::send(s3_connector_,
                                        &conn::s3::ConnectorManager::download,
                                        id, alias, s3_path)
                  .second;
    if (dl.has_error()) {
        log_->error("download: s3 download failed: {}", dl.error().what);
        co_return forward_error(resource(), "S3Manager::download: ", dl.error());
    }
    const std::string local_path{dl.value().data(), dl.value().size()};

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
    if (add.has_error()) {
        log_->error("download: mapping into otterbrix failed: {}", add.error().what);
        co_return forward_error(resource(), "S3Manager::download: ", add.error());
    }
    if (!add.value()) {
        log_->error("download: mapping into otterbrix failed: add_file returned false");
        co_return make_error(resource(), core::error_code_t::io_error,
                                  "S3Manager::download: add_file returned false");
    }

    log_->debug("download: s3://{} -> {}.{}", s3_path, database, table);
    co_return std::pair<bool, std::pmr::string>{true, std::pmr::string{std::string_view{s3_path}, resource()}};
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
S3Manager::upload(session_hash_t id, std::string alias, std::string s3_path,
                  OtterbrixStatementPtr statement) {
    OTX_ZONE_N("S3Manager::upload");
    log_->trace("S3Manager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Statement: {}", id, alias, s3_path, statement ? "set" : "null");
    if (!statement) {
        log_->error("upload: statement is null");
        co_return make_error(resource(), core::error_code_t::invalid_parameter,
                                  "S3Manager::upload: statement must not be null");
    }
    // The object key alone names the format: COPY ... TO 's3://...' carries no
    // local path to fall back on.
    const conn::file::FileFormat fmt = conn::file::resolve_format(std::string{}, s3_path);
    if (fmt == conn::file::FileFormat::Unknown) {
        log_->error("upload: cannot determine format from '{}'", s3_path);
        co_return make_error(resource(), core::error_code_t::invalid_parameter,
                                  "S3Manager::upload: cannot determine format from '" + s3_path + "'");
    }

    conn::file::FileMetadata meta;
    meta.statement    = std::move(statement);
    meta.path         = (fs::path(std::string_view{s3_upload_path_}) / fs::path(s3_path).filename()).string();
    meta.format       = fmt;
    meta.is_temporary = true; // staging file in the upload dir; timestamp-prefix it

    auto dump = co_await actor_zeta::send(file_manager_,
                                          &conn::file::FileManager::dump_file,
                                          id, std::move(meta))
                    .second;
    if (dump.has_error()) {
        log_->error("upload: dump failed for s3://{}: {}", s3_path, dump.error().what);
        co_return forward_error(resource(), "S3Manager::upload: ", dump.error());
    }

    const std::string dumped = dump.value();
    log_->trace("S3Manager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Dumped Path: {}", id, alias, s3_path, dumped);
    auto up = co_await actor_zeta::send(s3_connector_,
                                        &conn::s3::ConnectorManager::upload,
                                        id, alias, s3_path, dumped)
                  .second;
    remove_quietly(dumped);
    if (up.has_error()) {
        log_->error("upload: s3 upload failed: {}", up.error().what);
        co_return forward_error(resource(), "S3Manager::upload: ", up.error());
    }
    if (!up.value()) {
        log_->error("upload: s3 upload failed: s3 upload returned false");
        co_return make_error(resource(), core::error_code_t::io_error,
                                  "S3Manager::upload: s3 upload returned false");
    }

    log_->debug("upload: query result -> s3://{}", s3_path);
    co_return std::pair<bool, std::pmr::string>{true, std::pmr::string{std::string_view{s3_path}, resource()}};
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
S3Manager::ls(session_hash_t id, std::string alias, std::string s3_path) {
    OTX_ZONE_N("S3Manager::ls");
    log_->trace("S3Manager::ls with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}", id, alias, s3_path);
    auto res = co_await actor_zeta::send(s3_connector_,
                                         &conn::s3::ConnectorManager::list,
                                         id, alias, s3_path)
                   .second;
    if (res.has_error()) {
        log_->error("ls: list failed: {}", res.error().what);
        co_return forward_error(resource(), "S3Manager::ls: ", res.error());
    }

    std::pmr::string joined{resource()};
    for (const auto& p : res.value()) {
        if (!joined.empty()) joined += '\n';
        joined += p;
    }
    log_->debug("ls: Session ID: {}, Alias: '{}' S3 Path: '{}' -> {} entries", id, alias, s3_path, res.value().size());
    co_return std::pair<bool, std::pmr::string>{true, std::move(joined)};
}

} // namespace db
