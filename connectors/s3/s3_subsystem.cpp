// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "s3_subsystem.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

#include <arrow/filesystem/s3fs.h>
#include <arrow/status.h>

#include <cassert>

namespace conn::s3 {

subsystem_finalizer_t::subsystem_finalizer_t(log_t log)
    : log_(std::move(log)) {
    assert(log_.is_valid());
}

subsystem_finalizer_t::~subsystem_finalizer_t() {
    OTX_ZONE_N("s3::subsystem_finalizer_t::finalize");
    if (!arrow::fs::IsS3Initialized()) {
        return;
    }
    // Teardown: nobody is left to return a failure to, and a silent discard
    // here is how "warns and may segfault at exit" gets debugged twice.
    if (auto status = arrow::fs::FinalizeS3(); !status.ok()) {
        log_->error("FinalizeS3 failed: {}", status.ToString());
    }
}

} // namespace conn::s3
