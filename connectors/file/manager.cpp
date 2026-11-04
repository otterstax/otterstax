// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/session.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"
#include "otterbrix/translators/error.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

// The stream overloads take a RandomAccessFile as the InputStream it derives
// from; that conversion needs the complete types.
#include <arrow/io/interfaces.h>

#include <thread>

namespace conn::file {

namespace {

using chunk_run_t = std::pmr::vector<components::vector::data_chunk_t>;

// Translate params.input (or, when unset, the file at params.path) into a run of
// chunks in the format resolved from params.format or, failing that, the path's
// extension.
core::result_wrapper_t<chunk_run_t> load_chunks(std::pmr::memory_resource* res, const FileLoadParams& params) {
    switch (resolve_format(params.format, params.path)) {
        case FileFormat::Parquet:
            return params.input ? tsl::parquet_to_chunk(res, params.input.arrow())
                                : tsl::parquet_to_chunk(res, params.path);
        case FileFormat::CSV: {
            const char delim = params.csv_delimiter.empty() ? ',' : params.csv_delimiter[0];
            return params.input ? tsl::csv_to_chunk(res, params.input.arrow(), delim, params.csv_header)
                                : tsl::csv_to_chunk(res, params.path, delim, params.csv_header);
        }
        case FileFormat::NDJSON:
            return params.input ? tsl::ndjson_to_chunk(res, params.input.arrow())
                                : tsl::ndjson_to_chunk(res, params.path);
        case FileFormat::Unknown:
            break;
    }
    return tsl::make_error(res,
                           core::error_code_t::invalid_parameter,
                           "FileManager::load_file: cannot determine format for: " + params.path);
}

// Writes to `sink` when one is given (published by the caller), otherwise to a
// file at `path`, committed only on success (tsl::write_local_file).
core::result_wrapper_t<bool> write_chunks(std::pmr::memory_resource* res,
                                          FileFormat format,
                                          const chunk_run_t& chunks,
                                          const std::string& path,
                                          const tsl::output_stream_t& sink) {
    switch (format) {
        case FileFormat::Parquet:
            return sink ? tsl::chunk_to_parquet(res, chunks, sink.arrow()) : tsl::chunk_to_parquet(res, chunks, path);
        case FileFormat::CSV:
            return sink ? tsl::chunk_to_csv(res, chunks, sink.arrow()) : tsl::chunk_to_csv(res, chunks, path);
        case FileFormat::NDJSON:
            return sink ? tsl::chunk_to_ndjson(res, chunks, sink.arrow()) : tsl::chunk_to_ndjson(res, chunks, path);
        case FileFormat::Unknown:
            break;
    }
    return tsl::make_error(res, core::error_code_t::invalid_parameter, "FileManager::dump_file: unknown format");
}

} // namespace

FileManager::FileManager(std::pmr::memory_resource* res)
    : resource_(res)
    , log_(get_logger(logger_tag::FILE_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
}

std::pair<bool, actor_zeta::detail::enqueue_result>
FileManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    std::lock_guard<std::mutex> guard(mutex_);
    current_behavior_ = behavior(msg.get());

    while (current_behavior_.is_busy()) {
        if (current_behavior_.is_awaited_ready()) {
            auto cont = current_behavior_.take_awaited_continuation();
            if (cont) {
                cont.resume();
            }
        } else {
            std::this_thread::yield();
        }
    }

    return {false, actor_zeta::detail::enqueue_result::success};
}

actor_zeta::behavior_t FileManager::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<FileManager, &FileManager::load_file>) {
        co_await actor_zeta::dispatch(this, &FileManager::load_file, msg);
    } else if (cmd == actor_zeta::msg_id<FileManager, &FileManager::dump_file>) {
        co_await actor_zeta::dispatch(this, &FileManager::dump_file, msg);
    }
}

actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
FileManager::load_file(session_hash_t id, FileLoadParams params) {
    OTX_ZONE_N("file::load_file");
    log_->trace("FileManager::load_file id {}:\n Path: {}\n Format: {}\n Stream: {}",
                id, params.path, params.format, params.input ? "set" : "none");
    auto loaded = load_chunks(resource_, params);
    if (loaded.has_error()) {
        log_->error("load_file: {}", loaded.error().what);
    }
    co_return std::move(loaded);
}

actor_zeta::unique_future<core::result_wrapper_t<bool>> FileManager::dump_file(session_hash_t id, FileMetadata file_metadata) {
    OTX_ZONE_N("file::dump_file");
    log_->trace("FileManager::dump_file id {}:\n Path: {}\n Stream: {}", id, file_metadata.path,
                file_metadata.output ? "set" : "none");
    if (file_metadata.path.empty() || !file_metadata.rows) {
        log_->error("dump_file: input arguments are invalid: Path: '{}', rows: {}",
                    file_metadata.path, file_metadata.rows ? "set" : "null");
        co_return core::error_t(
            core::error_code_t::invalid_parameter,
            std::pmr::string{("FileManager::dump_file: input arguments are invalid: Path: '" +
                              std::string{file_metadata.path} + "', rows: " +
                              (file_metadata.rows ? "set" : "null"))
                                 .c_str(),
                             resource()});
    }

    // Engine cursors return the result as a vector of <=1024-row chunks (never
    // combined into one); the writers take the whole run.
    auto written = write_chunks(resource_, file_metadata.format, file_metadata.rows->chunks(), file_metadata.path,
                                file_metadata.output);
    if (written.has_error()) {
        // The stream (if any) is let go unclosed with file_metadata: aborted.
        log_->error("dump_file: {}", written.error().what);
        co_return std::move(written);
    }
    if (file_metadata.output) {
        if (auto closed = file_metadata.output.close(); !closed.ok()) {
            log_->error("dump_file: publishing {} failed: {}", file_metadata.path, closed.ToString());
            co_return tsl::arrow_error(resource_, core::error_code_t::io_error, "FileManager::dump_file: close", closed);
        }
    }

    log_->debug("dump_file: wrote {}", file_metadata.path);
    co_return true;
}

} // namespace conn::file
