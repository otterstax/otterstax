// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "driver.hpp"

#include "connectors/file/driver.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/filesystem/s3fs.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cassert>
#include <type_traits>
#include <utility>
#include <variant>

namespace conn::s3 {

    namespace {

        // Every buffer the filesystem's streams allocate (ranged-read buffers, multipart
        // parts) comes from `res`, through the same pool the translators use.
        arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>>
        make_s3fs(std::pmr::memory_resource* res, const connect_params& creds, std::chrono::milliseconds connect_timeout) {
            OTX_ZONE_N("s3::make_s3fs");
            auto opts = arrow::fs::S3Options::FromAccessKey(std::string{std::string_view{creds.access_key}},
                                                            std::string{std::string_view{creds.secret_key}},
                                                            std::string{std::string_view{creds.session_token}});

            if (!creds.region.empty()) {
                opts.region = std::string_view{creds.region};
            }
            if (!creds.endpoint.empty()) {
                opts.endpoint_override = std::string_view{creds.endpoint};
                // Disable virtual addressing for custom endpoints (e.g. RustFS)
                opts.scheme = "http";
            }
            // An output stream creates its multipart upload on the first write, so one
            // aborted before any data reaches the bucket costs no request.
            opts.allow_delayed_open = true;
            opts.connect_timeout = static_cast<double>(connect_timeout.count()) / 1000.0;

            return arrow::fs::S3FileSystem::Make(opts, arrow::io::IOContext(tsl::arrow_pool(res)));
        }

        // Aborts an upload that was not Close()d: Arrow's S3 stream completes the upload from its
        // destructor, so a failed COPY would publish a truncated object. Only a successful Close()
        // completes it.
        struct upload_guard_t {
            std::shared_ptr<arrow::io::OutputStream> stream;

            explicit upload_guard_t(std::shared_ptr<arrow::io::OutputStream> opened)
                : stream(std::move(opened)) {}
            upload_guard_t(const upload_guard_t&) = delete;
            upload_guard_t& operator=(const upload_guard_t&) = delete;
            ~upload_guard_t() {
                if (stream && !stream->closed()) {
                    // Nobody is left to report a failed abort to; the object is not
                    // published either way.
                    (void) stream->Abort();
                }
            }
        };

    } // namespace

    core::result_wrapper_t<std::shared_ptr<driver_t>> driver_t::make(std::pmr::memory_resource* resource,
                                                                     const connect_params& params,
                                                                     std::chrono::milliseconds connect_timeout) {
        OTX_ZONE_N("s3::driver::make");
        auto log = get_logger(logger_tag::S3_MANAGER);
        if (params.alias.empty()) {
            return tsl::make_error(resource, core::error_code_t::invalid_parameter, "s3: alias must not be empty");
        }
        if (params.access_key.empty() || params.secret_key.empty()) {
            return tsl::make_error(resource,
                                   core::error_code_t::invalid_parameter,
                                   {"s3: access_key and secret_key are required for alias '", params.alias, "'"});
        }
        if (auto status = arrow::fs::EnsureS3Initialized(); !status.ok() && log.is_valid()) {
            log->warn("EnsureS3Initialized: {}", status.ToString());
        }
        auto made = make_s3fs(resource, params, connect_timeout);
        if (!made.ok()) {
            return tsl::arrow_error(resource, core::error_code_t::io_error, "s3: building the filesystem", made.status());
        }
        return std::make_shared<driver_t>(resource, params.alias, std::move(*made));
    }

    driver_t::driver_t(std::pmr::memory_resource* resource,
                       std::string_view alias,
                       std::shared_ptr<arrow::fs::S3FileSystem> fs)
        : resource_(resource)
        , alias_(alias, resource)
        , fs_(std::move(fs)) {
        assert(resource_ != nullptr);
        assert(fs_ != nullptr);
    }

    transfer_outcome_t driver_t::run(const transfer_t& transfer) {
        return std::visit(
            [this](const auto& job) -> transfer_outcome_t {
                using job_t = std::decay_t<decltype(job)>;
                if constexpr (std::is_same_v<job_t, load_t>) {
                    return load(job);
                } else {
                    static_assert(std::is_same_v<job_t, export_t>);
                    return write(job);
                }
            },
            transfer);
    }

    transfer_outcome_t driver_t::load(const load_t& job) {
        OTX_ZONE_N("s3::driver::load");
        auto opened = fs_->OpenInputFile(std::string(job.location)); // Arrow takes a std::string
        if (!opened.ok()) {
            return tsl::arrow_error(resource_, core::error_code_t::io_error, "open_input", opened.status());
        }
        // The object streams straight into the loader; the key names it for format
        // resolution and messages.
        auto loaded = file::load_chunk(resource_, job.location, *opened, job.format);
        if (loaded.has_error()) {
            return loaded.convert_error<transfer_result_t>();
        }
        transfer_result_t out;
        out.loaded.emplace(std::move(loaded.value()));
        return out;
    }

    transfer_outcome_t driver_t::write(const export_t& job) {
        OTX_ZONE_N("s3::driver::export");
        if (!job.result) {
            return tsl::make_error(resource_,
                                   core::error_code_t::invalid_parameter,
                                   {"export: no result to write to s3://", job.location});
        }
        auto opened = fs_->OpenOutputStream(std::string(job.location)); // Arrow takes a std::string
        if (!opened.ok()) {
            return tsl::arrow_error(resource_, core::error_code_t::io_error, "open_output", opened.status());
        }
        upload_guard_t guard{std::move(*opened)};

        auto written = file::write_chunks(resource_, job.format, job.result->chunks(), job.location, guard.stream);
        if (written.has_error()) {
            return written.convert_error<transfer_result_t>();
        }
        // Close completes the multipart upload: only now does the object appear.
        if (auto closed = guard.stream->Close(); !closed.ok()) {
            return tsl::arrow_error(resource_, core::error_code_t::io_error, "close: completing the upload failed", closed);
        }
        return transfer_result_t{};
    }

} // namespace conn::s3
