// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include "utility/logger.hpp"

#include <arrow/filesystem/s3fs.h>

#include <filesystem>

// FileManager/S3Manager construction (reachable via the Scheduler stack) calls
// arrow::fs::EnsureS3Initialized() transitively. Arrow requires a matching
// FinalizeS3() before exit or it warns and may segfault during static teardown.
struct S3Finalizer : Catch::EventListenerBase {
    using EventListenerBase::EventListenerBase;
    void testRunEnded(Catch::TestRunStats const&) override {
        if (arrow::fs::IsS3Initialized())
            (void) arrow::fs::FinalizeS3();
    }
};
CATCH_REGISTER_LISTENER(S3Finalizer)

// Several system tests construct GreenplumParser (and other components that
// log through get_logger) directly; the named loggers must exist before the
// first such call. Under CTest every test case runs in its own process, so
// no earlier test case can be relied on to have initialized them.
int main(int argc, char* argv[]) {
    initialize_all_loggers((std::filesystem::temp_directory_path() / "otterstax-test-logs").string());
    return Catch::Session().run(argc, argv);
}
