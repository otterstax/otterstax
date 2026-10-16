// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "s3_manager.hpp"

#include "connectors/file/manager.hpp"
#include "connectors/file/types.hpp"
#include "connectors/s3/manager.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/io/interfaces.h>

#include <memory>
#include <thread>

namespace db {

namespace {

// Aborts an upload on every exit that did not Close() it: Arrow's S3 stream completes the
// upload from its destructor, so a failed COPY would publish a truncated object. Arrow marks
// the stream closed on every Close() outcome; only a successful one completes the upload.
struct upload_guard_t {
    std::shared_ptr<arrow::io::OutputStream> stream;

    explicit upload_guard_t(std::shared_ptr<arrow::io::OutputStream> opened)
        : stream(std::move(opened)) {}
    upload_guard_t(const upload_guard_t&) = delete;
    upload_guard_t& operator=(const upload_guard_t&) = delete;
    ~upload_guard_t() {
        if (stream && !stream->closed()) {
            // Nobody is left to report a failed abort to; the object is not
            // published either way.
            (void)stream->Abort();
        }
    }
};

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
                     actor_zeta::address_t file_manager)
    : resource_(res)
    , s3_connector_(std::move(s3_connector))
    , file_manager_(std::move(file_manager))
    , log_(get_logger(logger_tag::S3_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
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
                    std::string database, std::string table, std::string format) {
    OTX_ZONE_N("S3Manager::download");
    log_->trace("S3Manager::download with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Database: {}\n Table: {}\n Format: {}", id, alias, s3_path, database, table, format);
    auto opened = co_await actor_zeta::send(s3_connector_,
                                            &conn::s3::ConnectorManager::open_input,
                                            id, alias, s3_path)
                      .second;
    if (opened.has_error()) {
        log_->error("download: opening s3://{} failed: {}", s3_path, opened.error().what);
        co_return forward_error(resource(), "S3Manager::download: ", opened.error());
    }

    // The object streams straight into the loader; the key names it for format
    // resolution and messages.
    conn::file::FileAddParams params;
    params.database = database;
    params.table    = table;
    params.path     = s3_path;
    params.format   = format; // empty: the key's extension decides
    params.input    = std::move(opened.value());

    auto add = co_await actor_zeta::send(file_manager_,
                                         &conn::file::FileManager::add_file,
                                         id, std::move(params))
                   .second;
    if (add.has_error()) {
        log_->error("download: mapping into otterbrix failed: {}", add.error().what);
        co_return forward_error(resource(), "S3Manager::download: ", add.error());
    }
    if (!add.value()) {
        log_->error("download: mapping into otterbrix failed: add_file returned false");
        co_return tsl::make_error(resource(), core::error_code_t::io_error,
                                  "S3Manager::download: add_file returned false");
    }

    log_->debug("download: s3://{} -> {}.{}", s3_path, database, table);
    co_return std::pair<bool, std::pmr::string>{true, std::pmr::string{std::string_view{s3_path}, resource()}};
}

actor_zeta::unique_future<core::result_wrapper_t<std::pair<bool, std::pmr::string>>>
S3Manager::upload(session_hash_t id, std::string alias, std::string s3_path,
                  OtterbrixStatementPtr statement, std::string format) {
    OTX_ZONE_N("S3Manager::upload");
    log_->trace("S3Manager::upload with params:\n Session ID: {}\n Alias: {}\n S3 Path: {}\n Statement: {}\n Format: {}", id, alias, s3_path, statement ? "set" : "null", format);
    if (!statement) {
        log_->error("upload: statement is null");
        co_return tsl::make_error(resource(), core::error_code_t::invalid_parameter,
                                  "S3Manager::upload: statement must not be null");
    }
    // WITH (format = ...) wins; otherwise the object key's extension names it.
    const conn::file::FileFormat fmt = conn::file::resolve_format(format, s3_path);
    if (fmt == conn::file::FileFormat::Unknown) {
        log_->error("upload: cannot determine format from '{}'", s3_path);
        co_return tsl::make_error(resource(), core::error_code_t::invalid_parameter,
                                  "S3Manager::upload: cannot determine format from '" + s3_path + "'");
    }

    auto opened = co_await actor_zeta::send(s3_connector_,
                                            &conn::s3::ConnectorManager::open_output,
                                            id, alias, s3_path)
                      .second;
    if (opened.has_error()) {
        log_->error("upload: opening s3://{} failed: {}", s3_path, opened.error().what);
        co_return forward_error(resource(), "S3Manager::upload: ", opened.error());
    }
    upload_guard_t guard{std::move(opened.value())};

    conn::file::FileMetadata meta;
    meta.statement = std::move(statement);
    meta.path      = s3_path;
    meta.format    = fmt;
    meta.output    = guard.stream;

    auto dump = co_await actor_zeta::send(file_manager_,
                                          &conn::file::FileManager::dump_file,
                                          id, std::move(meta))
                    .second;
    if (dump.has_error()) {
        log_->error("upload: dump failed for s3://{}: {}", s3_path, dump.error().what);
        co_return forward_error(resource(), "S3Manager::upload: ", dump.error());
    }

    // Close completes the multipart upload: only now does the object appear.
    if (auto closed = guard.stream->Close(); !closed.ok()) {
        log_->error("upload: completing s3://{} failed: {}", s3_path, closed.ToString());
        co_return tsl::arrow_error(resource(), core::error_code_t::io_error,
                                   "S3Manager::upload: completing the upload failed", closed);
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
