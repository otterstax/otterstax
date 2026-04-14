// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/file/types.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace s3c {

    enum class Status {
        Created,
        Connected,
        Disconnected,
        Working,
        Closed
    };

    // Intermediate type: data downloaded from S3 before parsing into data_chunk_t
    struct S3Data {
        std::vector<uint8_t>  bytes;
        filec::FileFormat     format{filec::FileFormat::Auto};
        std::string           s3_key; // for diagnostics
    };

    struct connect_params {
        std::string bucket;
        std::string region{"us-east-1"};
        std::string access_key;
        std::string secret_key;
        std::string session_token;          // optional (for IAM roles)
        std::string endpoint;               // optional (for LocalStack/MinIO)
        std::string prefix;                 // path inside bucket (may contain wildcard *.parquet)
        filec::FileFormat format{filec::FileFormat::Auto};
        std::string alias;
    };

} // namespace s3c
