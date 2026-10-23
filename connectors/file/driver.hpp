// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/transfer.hpp"
#include "types.hpp"

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/io/type_fwd.h>

#include <memory>
#include <memory_resource>
#include <string>

namespace conn::file {

    // Translates `input` — or, when it is null, the file at `path` — into one chunk, in the
    // format `format` names or, when it names none ("", "auto"), the path's extension: what
    // CREATE EXTERNAL TABLE loads, from a local file or an S3 object stream. `path` names the
    // source in messages either way. An unknown format is invalid_parameter; the translators
    // report conversion_failure / io_error.
    core::result_wrapper_t<components::vector::data_chunk_t>
    load_chunk(std::pmr::memory_resource* resource,
               const std::string& path,
               const std::shared_ptr<arrow::io::RandomAccessFile>& input,
               const std::string& format);

    // Writes the engine's result chunks in `format` into `sink` — left open, committing or
    // aborting it is the caller's call — or, when it is null, to a file at `path` that is
    // removed again if the write fails.
    core::result_wrapper_t<bool> write_chunks(std::pmr::memory_resource* resource,
                                              FileFormat format,
                                              const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                              const std::string& path,
                                              const std::shared_ptr<arrow::io::OutputStream>& sink);

    // Local files: load_t reads a path, export_t writes one. Stateless, so one instance
    // serves every thread of the pool.
    class driver_t final : public ITransferDriver {
    public:
        explicit driver_t(std::pmr::memory_resource* resource);
        transfer_outcome_t run(const transfer_t& transfer) override;

    private:
        std::pmr::memory_resource* resource_;
    };

} // namespace conn::file
