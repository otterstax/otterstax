// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

namespace conn::s3 {

// Process-wide shutdown of Arrow's S3 subsystem, which driver_t::make initialises. Arrow needs a
// FinalizeS3 before exit or it may segfault in static teardown, and finalization is terminal. So it
// belongs to the process: declare one before the graph owning the s3 drivers, and it finalizes after
// that graph is gone on every exit path.
class subsystem_finalizer_t final {
public:
    explicit subsystem_finalizer_t(log_t log);
    ~subsystem_finalizer_t();

    subsystem_finalizer_t(const subsystem_finalizer_t&) = delete;
    subsystem_finalizer_t& operator=(const subsystem_finalizer_t&) = delete;
    subsystem_finalizer_t(subsystem_finalizer_t&&) = delete;
    subsystem_finalizer_t& operator=(subsystem_finalizer_t&&) = delete;

private:
    log_t log_;
};

} // namespace conn::s3
