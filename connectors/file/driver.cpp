// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "driver.hpp"

#include "otterbrix/translators/error.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_csv.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"
#include "otterbrix/translators/output/chunk_to_parquet.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

// The stream overloads take a RandomAccessFile as the InputStream it derives
// from; that conversion needs the complete types.
#include <arrow/io/interfaces.h>

#include <cassert>
#include <type_traits>
#include <utility>
#include <variant>

namespace conn::file {

    core::result_wrapper_t<components::vector::data_chunk_t>
    load_chunk(std::pmr::memory_resource* resource,
               std::string_view path,
               const std::shared_ptr<arrow::io::RandomAccessFile>& input,
               std::string_view format) {
        OTX_ZONE_N("file::load_chunk");
        switch (resolve_format(format, path)) {
            case FileFormat::Parquet:
                return input ? tsl::parquet_to_chunk(resource, input) : tsl::parquet_to_chunk(resource, path);
            case FileFormat::CSV:
                return input ? tsl::csv_to_chunk(resource, input, ',', true) : tsl::csv_to_chunk(resource, path, ',', true);
            case FileFormat::NDJSON:
                return input ? tsl::ndjson_to_chunk(resource, input) : tsl::ndjson_to_chunk(resource, path);
            case FileFormat::Unknown:
                break;
        }
        return tsl::make_error(resource, core::error_code_t::invalid_parameter, {"load: cannot determine format for: ", path});
    }

    core::result_wrapper_t<bool> write_chunks(std::pmr::memory_resource* resource,
                                              FileFormat format,
                                              const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                              std::string_view path,
                                              const std::shared_ptr<arrow::io::OutputStream>& sink) {
        OTX_ZONE_N("file::write_chunks");
        switch (format) {
            case FileFormat::Parquet:
                return sink ? tsl::chunk_to_parquet(resource, chunks, sink) : tsl::chunk_to_parquet(resource, chunks, path);
            case FileFormat::CSV:
                return sink ? tsl::chunk_to_csv(resource, chunks, sink) : tsl::chunk_to_csv(resource, chunks, path);
            case FileFormat::NDJSON:
                return sink ? tsl::chunk_to_ndjson(resource, chunks, sink) : tsl::chunk_to_ndjson(resource, chunks, path);
            case FileFormat::Unknown:
                break;
        }
        return tsl::make_error(resource, core::error_code_t::invalid_parameter, {"export: unknown format for: ", path});
    }

    driver_t::driver_t(std::pmr::memory_resource* resource)
        : resource_(resource) {
        assert(resource_ != nullptr);
    }

    transfer_outcome_t driver_t::run(const transfer_t& transfer) {
        OTX_ZONE_N("file::driver::run");
        return std::visit(
            [this](const auto& job) -> transfer_outcome_t {
                using job_t = std::decay_t<decltype(job)>;
                if constexpr (std::is_same_v<job_t, load_t>) {
                    auto loaded = load_chunk(resource_, job.location, nullptr, job.format);
                    if (loaded.has_error()) {
                        return loaded.template convert_error<transfer_result_t>();
                    }
                    transfer_result_t out;
                    out.loaded.emplace(std::move(loaded.value()));
                    return out;
                } else {
                    static_assert(std::is_same_v<job_t, export_t>);
                    if (!job.result) {
                        return tsl::make_error(resource_,
                                               core::error_code_t::invalid_parameter,
                                               {"export: no result to write to ", job.location});
                    }
                    auto written = write_chunks(resource_, job.format, job.result->chunks(), job.location, nullptr);
                    if (written.has_error()) {
                        return written.template convert_error<transfer_result_t>();
                    }
                    return transfer_result_t{};
                }
            },
            transfer);
    }

} // namespace conn::file
