// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "manager.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/session.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"

#include <ctime>
#include <filesystem>
#include <thread>

namespace conn::file {

namespace {

// Build a temporary output path for a dumped table by prefixing the original
// file's basename with a timestamp, keeping it in the same directory, e.g.
//   "/dir/people.parquet" → "/dir/2026-06-14_10-34-35_people.parquet"
std::string temporary_path(const std::string& path) {
    std::time_t now = std::time(nullptr);
    std::tm     tm{};
    localtime_r(&now, &tm);
    char ts[20];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d_%H-%M-%S", &tm);

    const std::filesystem::path src{path};
    return (src.parent_path() /
            (std::string{ts} + "_" + src.filename().string())).string();
}

} // namespace

FileManager::FileManager(std::pmr::memory_resource* res, actor_zeta::address_t otterbrix_manager)
    : resource_(res)
    , otterbrix_manager_(std::move(otterbrix_manager))
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
    if (cmd == actor_zeta::msg_id<FileManager, &FileManager::add_file>) {
        co_await actor_zeta::dispatch(this, &FileManager::add_file, msg);
    } else if (cmd == actor_zeta::msg_id<FileManager, &FileManager::dump_file>) {
        co_await actor_zeta::dispatch(this, &FileManager::dump_file, msg);
    }
}

actor_zeta::unique_future<core::result_wrapper_t<bool>> FileManager::add_file(session_hash_t id, FileAddParams params) {
    OTX_ZONE_N("file::add_file");
    log_->trace("FileManager::add_file with params:\n Database: {}\n Table: {}\n Path: {}\n Format: {}",
                params.database, params.table, params.path, params.format);

    try {
        // Resolve format
        FileFormat fmt = resolve_format(params.format, params.path);

        if (fmt == FileFormat::Unknown) {
            log_->error("add_file: cannot determine format for: {}", params.path);
            co_return core::error_t(
                core::error_code_t::other_error,
                std::pmr::string{("FileManager::add_file: cannot determine format for: " + params.path).c_str(),
                                 resource()});
        }

        // Translate file → data_chunk_t
        components::vector::data_chunk_t chunk = [&] {
            switch (fmt) {
                case FileFormat::Parquet:
                    return tsl::parquet_to_chunk(resource_, params.path);
                case FileFormat::CSV: {
                    char delim = params.csv_delimiter.empty() ? ',' : params.csv_delimiter[0];
                    return tsl::csv_to_chunk(resource_, params.path, delim, params.csv_header);
                }
                case FileFormat::NDJSON:{
                    return tsl::ndjson_to_chunk(resource_, params.path);
                }
                default:{
                    log_->error("add_file: cannot determine format for: {}", params.path);
                    throw std::runtime_error("FileManager::add_file: cannot determine format for: " + params.path);
                }
                    
            }
        }();

        auto fut = actor_zeta::send(otterbrix_manager_,
                                    &db::OtterbrixManager::create_table,
                                    id,
                                    std::move(params.database),
                                    std::move(params.table),
                                    std::move(chunk));
        auto result = co_await std::move(fut.second);
        if (result.has_error()) {
            log_->error("add_file: create_table failed: {}",
                                         result.error().what);
            co_return core::error_t(
                core::error_code_t::other_error,
                std::pmr::string{("FileManager::add_file: create_table failed: " +
                                  std::string{result.error().what.c_str()})
                                     .c_str(),
                                 resource()});
        }

        log_->debug("add_file: loaded {} into {}.{}", params.path, params.database, params.table);
        co_return true;

    } catch (const std::exception& e) {
        log_->error("add_file caught exception: {}", e.what());
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("FileManager::add_file: " + std::string{e.what()}).c_str(), resource()});
    } catch (...) {
        log_->error("add_file caught unknown exception");
        co_return core::error_t(core::error_code_t::other_error,
                                std::pmr::string{"FileManager::add_file: unknown exception", resource()});
    }
}

actor_zeta::unique_future<core::result_wrapper_t<std::string>> FileManager::dump_file(session_hash_t id, FileMetadata file_metadata) {
    OTX_ZONE_N("file::dump_file");
    log_->trace("FileManager::dump_file with params:\n Path: {}", file_metadata.path);
    if (file_metadata.path.empty() || !file_metadata.statement) {
        log_->error("dump_file: input arguments are invalid: Path: '{}', statement: {}",
                    file_metadata.path, file_metadata.statement ? "set" : "null");
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("FileManager::dump_file: input arguments are invalid: Path: '" +
                              std::string{file_metadata.path} + "', statement: " +
                              (file_metadata.statement ? "set" : "null"))
                                 .c_str(),
                             resource()});
    }

    try {
        // Execute the pre-parsed statement; its result chunk is what we dump.
        auto fut = actor_zeta::send(otterbrix_manager_,
                                    &db::OtterbrixManager::execute,
                                    id,
                                    std::move(file_metadata.statement));
        auto cursor = co_await std::move(fut.second);
        if (!cursor || !cursor->is_success()) {
            const std::string why = (cursor && cursor->is_error())
                                        ? std::string{cursor->get_error().what.c_str()}
                                        : std::string{"cursor error"};
            log_->error("dump_file: execute failed: {}", why);
            co_return core::error_t(core::error_code_t::other_error,
                                    std::pmr::string{("FileManager::dump_file: execute failed: " + why).c_str(),
                                                     resource()});
        }

        // Temporary dumps (e.g. S3 upload staging) get a timestamp-prefixed
        // basename in the same directory so they never clobber an existing file;
        // otherwise the table is written to the exact path requested.
        const auto out_path = file_metadata.is_temporary
                                  ? temporary_path(file_metadata.path)
                                  : file_metadata.path;

        // b1/b2 cursors return the result as a vector of <=1024-row chunks (never
        // combined into one); write them all via the multi-chunk writer overloads.
        const auto& chunks = cursor->chunks();
        switch (file_metadata.format) {
            case FileFormat::Parquet:
                tsl::chunk_to_parquet(chunks, out_path);
                break;
            case FileFormat::CSV:
                tsl::chunk_to_csv(chunks, out_path);
                break;
            case FileFormat::NDJSON:
                tsl::chunk_to_ndjson(chunks, out_path);
                break;
            default:
                co_return core::error_t(core::error_code_t::other_error,
                                        std::pmr::string{"FileManager::dump_file: unknown format", resource()});
        }

        log_->debug("dump_file: wrote {}", out_path);
        co_return out_path;

    } catch (const std::exception& e) {
        log_->error("dump_file caught exception: {}", e.what());
        co_return core::error_t(
            core::error_code_t::other_error,
            std::pmr::string{("FileManager::dump_file: " + std::string{e.what()}).c_str(), resource()});
    }
}

} // namespace conn::file
