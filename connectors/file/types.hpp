// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "types/otterbrix.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/io/type_fwd.h>

#include <cstdint>
#include <filesystem>
#include <memory>
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

struct FileMetadata {
    // Pre-parsed query whose result is dumped to `path`. Replaces the former
    // database/table pair, so an arbitrary SELECT (e.g. a COPY's inner query)
    // can be exported, not just a whole table.
    OtterbrixStatementPtr statement;
    std::string path;
    FileFormat  format{FileFormat::Unknown};
    // When set, the result is written here instead of to a file at `path` (which
    // then only names the target in messages), and the stream is left open:
    // committing or aborting it is the sender's call — an S3 upload must never
    // publish a partial object.
    std::shared_ptr<arrow::io::OutputStream> output;
};

struct FileAddParams {
    std::string database;
    std::string table;
    std::string path;
    std::string format;        // "parquet" | "csv" | "ndjson" | "auto"
    std::string csv_delimiter; // single char, default ","
    bool        csv_header{true};
    // When set, the rows are read from here instead of from a file at `path`,
    // which then only names the source (format resolution, messages) — an S3
    // object streamed without a local copy.
    std::shared_ptr<arrow::io::RandomAccessFile> input;
};

} // namespace conn::file
