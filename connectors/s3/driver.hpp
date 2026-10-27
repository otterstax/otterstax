// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/transfer.hpp"
#include "s3_connect_params.hpp"

#include <core/result_wrapper.hpp>

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/filesystem/type_fwd.h>

#include <chrono>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>

namespace conn::s3 {

    // One S3 alias: its S3FileSystem (one AWS client with its own HTTP pool), built once at startup and
    // shared by every thread of the alias's transfer pool. Nothing is staged on local disk.
    //   load_t   — OpenInputFile("bucket/key"), then file::load_chunk over the stream;
    //   export_t — OpenOutputStream("bucket/key"), file::write_chunks, then Close() completes the upload;
    //              every other path Abort()s it, so a failed export publishes no object.
    // Errors: io_error (Arrow S3 failure, prefixed by the step), invalid_parameter (unknown format) and
    // the translators' conversion_failure. Nothing throws.
    //
    // Stream buffers come from `resource` through tsl::arrow_pool, which must be thread-safe: Arrow
    // uploads parts from its own IO threads.
    class driver_t final : public ITransferDriver {
    public:
        // Initialises Arrow's S3 subsystem (finalized by subsystem_finalizer_t) and builds the filesystem.
        // `connect_timeout` bounds each socket connect. Errors: invalid_parameter (empty alias or keys),
        // io_error (Arrow refused the options).
        static core::result_wrapper_t<std::shared_ptr<driver_t>>
        make(std::pmr::memory_resource* resource, const connect_params& params, std::chrono::milliseconds connect_timeout);

        transfer_outcome_t run(const transfer_t& transfer) override;

        // The alias is copied onto `resource`.
        driver_t(std::pmr::memory_resource* resource, std::string_view alias, std::shared_ptr<arrow::fs::S3FileSystem> fs);

    private:
        transfer_outcome_t load(const load_t& job);
        transfer_outcome_t write(const export_t& job);

        std::pmr::memory_resource* resource_;
        std::pmr::string alias_;
        std::shared_ptr<arrow::fs::S3FileSystem> fs_;
    };

} // namespace conn::s3
