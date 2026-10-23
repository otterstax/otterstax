// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

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

} // namespace conn::file
