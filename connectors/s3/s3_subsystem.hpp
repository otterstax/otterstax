// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

namespace conn::s3 {

// Process-wide shutdown of Arrow's S3 subsystem. ConnectorManager's constructor
// initialises it (arrow::fs::EnsureS3Initialized); Arrow requires a matching
// FinalizeS3 before the process exits or it may segfault during static
// teardown. Finalization is terminal — every S3 call fails afterwards — so it
// belongs to the process, not to any one manager: declare one of these before
// the object graph that spawns the s3 actor, and it finalizes after that graph
// is gone on every exit path, including an aborted startup.
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
