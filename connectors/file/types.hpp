// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/cursor/cursor.hpp>

#include "otterbrix/translators/arrow_stream.hpp"

#include <cstdint>
#include <filesystem>
#include <string>

namespace conn::file {

enum class FileFormat : uint8_t { NDJSON, CSV, Parquet, Unknown };

// Resolve the file format from an explicit format string, falling back to the
// file extension when the format is empty or unrecognised.
inline FileFormat resolve_format(const std::string& format, const std::string& path) {
    if      (format == "parquet")                     return FileFormat::Parquet;
    else if (format == "csv")                         return FileFormat::CSV;
    else if (format == "ndjson" || format == "jsonl") return FileFormat::NDJSON;

    auto ext = std::filesystem::path(path).extension().string();
    if      (ext == ".parquet")                   return FileFormat::Parquet;
    else if (ext == ".csv" || ext == ".tsv")      return FileFormat::CSV;
    else if (ext == ".ndjson" || ext == ".jsonl") return FileFormat::NDJSON;

    return FileFormat::Unknown;
}

// What dump_file writes: `rows` (a cursor's result, a run of <=1024-row chunks)
// in `format` at `path`.
struct FileMetadata {
    components::cursor::cursor_t_ptr rows;
    std::string path;
    FileFormat  format{FileFormat::Unknown};
    // When set, the result is written here instead of to a file at `path` (which
    // then only names the target in messages). dump_file holds the stream last: it
    // publishes it (close) only when the whole result was written, and a stream it
    // lets go unclosed is aborted — an S3 upload never publishes a partial object.
    tsl::output_stream_t output;
};

// What load_file reads: the file at `path` (or `input`) in `format`.
struct FileLoadParams {
    std::string path;
    std::string format;        // "parquet" | "csv" | "ndjson" | "auto"
    std::string csv_delimiter; // single char, default ","
    bool        csv_header{true};
    // When set, the rows are read from here instead of from a file at `path`,
    // which then only names the source (format resolution, messages) — an S3
    // object streamed without a local copy.
    tsl::input_stream_t input;
};

} // namespace conn::file
