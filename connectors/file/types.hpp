// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace filec {

    enum class Status {
        Created,
        Connected,
        Disconnected,
        Working,
        Closed
    };

    enum class FileFormat : uint8_t {
        JSON,
        CSV,
        Parquet,
        Auto
    };

    // Intermediate type: raw file bytes before parsing into data_chunk_t
    struct FileData {
        std::vector<uint8_t> bytes;
        FileFormat           format{FileFormat::Auto};
        std::string          file_path; // for diagnostics
    };

    struct connect_params {
        std::string path;                  // File path or directory
        FileFormat  format{FileFormat::Auto};
        char        csv_delimiter{','};
        bool        csv_header{true};
        std::string alias;
    };

} // namespace filec
