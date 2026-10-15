// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "chunk_to_arrow.hpp"
#include "otterbrix/translators/error.hpp"

#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/io/file.h>
#include <arrow/record_batch.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <system_error>

namespace tsl {

    // A hidden sibling of `path` to write before renaming it over `path` (same directory, so the
    // rename is atomic); timestamp + sequence number keep two writes apart, e.g.
    //   "/dir/people.parquet" → "/dir/.2026-10-13_10-34-35_7_people.parquet.tmp"
    inline std::string temporary_path(const std::string& path) {
        static std::atomic<uint64_t> sequence{0};
        std::time_t now = std::time(nullptr);
        std::tm tm{};
        localtime_r(&now, &tm);
        char ts[20];
        std::strftime(ts, sizeof(ts), "%Y-%m-%d_%H-%M-%S", &tm);

        const std::filesystem::path target{path};
        return (target.parent_path() / ("." + std::string{ts} + "_" + std::to_string(sequence.fetch_add(1)) + "_" +
                                        target.filename().string() + ".tmp"))
            .string();
    }

    // The path form of every file writer: `write` fills a temporary sibling of `path` through the
    // same stream overload an S3 upload uses, and only a write that succeeded is renamed over
    // `path`. A failed COPY ... TO therefore never leaves a truncated file behind and never
    // destroys a file that was already at `path`; readers see the old file or the whole new one.
    template<typename Write>
    core::result_wrapper_t<bool> write_local_file(std::pmr::memory_resource* res,
                                                  std::string_view scope,
                                                  const std::string& path,
                                                  Write&& write) {
        const std::string staging = temporary_path(path);
        auto opened = arrow::io::FileOutputStream::Open(staging);
        if (!opened.ok()) {
            return arrow_error(res, core::error_code_t::io_error,
                               std::string{scope} + ": cannot open output", opened.status());
        }
        const auto discard = [&staging] {
            std::error_code ec;
            std::filesystem::remove(staging, ec);
        };
        std::shared_ptr<arrow::io::OutputStream> sink = *opened;
        auto written = write(sink);
        if (written.has_error()) {
            (void)sink->Close();
            discard();
            return written;
        }
        if (auto closed = sink->Close(); !closed.ok()) {
            discard();
            return arrow_error(res, core::error_code_t::io_error, std::string{scope} + ": Close failed", closed);
        }
        std::error_code renamed;
        std::filesystem::rename(staging, path, renamed);
        if (renamed) {
            discard();
            return make_error(res, core::error_code_t::io_error,
                              std::string{scope} + ": cannot replace '" + path + "': " + renamed.message());
        }
        return true;
    }

    // The csv / parquet stream form: each chunk through chunk_to_file_batch into the writer `open`
    // makes from the first batch's schema, one batch at a time, then Close() (the parquet footer).
    // `sink` stays open for the caller to commit or abort; an empty run is conversion_failure.
    template<typename Open>
    core::result_wrapper_t<bool> write_batches(std::pmr::memory_resource* res,
                                               std::string_view scope,
                                               const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                               Open&& open) {
        if (chunks.empty()) {
            return make_error(res, core::error_code_t::conversion_failure,
                              std::string{scope} + ": no chunk to take the schema from");
        }
        auto first = chunk_to_file_batch(res, chunks.front());
        if (first.has_error()) {
            return first.convert_error<bool>();
        }
        auto opened = open(first.value()->schema());
        if (!opened.ok()) {
            return arrow_error(res, core::error_code_t::io_error, std::string{scope} + ": cannot start writing",
                               opened.status());
        }
        auto writer = std::move(*opened);
        std::shared_ptr<arrow::RecordBatch> batch = std::move(first.value());
        for (size_t next = 1;; ++next) {
            if (auto written = writer->WriteRecordBatch(*batch); !written.ok()) {
                return arrow_error(res, core::error_code_t::io_error, std::string{scope} + ": write failed", written);
            }
            if (next == chunks.size()) {
                break;
            }
            auto converted = chunk_to_file_batch(res, chunks[next]);
            if (converted.has_error()) {
                return converted.convert_error<bool>();
            }
            batch = std::move(converted.value());
        }
        if (auto closed = writer->Close(); !closed.ok()) {
            return arrow_error(res, core::error_code_t::io_error, std::string{scope} + ": finishing the file failed",
                               closed);
        }
        return true;
    }

} // namespace tsl
