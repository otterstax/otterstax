// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include "connectors/s3/s3_subsystem.hpp"
#include "utility/logger.hpp"

#include <filesystem>

// Building an s3 driver calls arrow::fs::EnsureS3Initialized(). Arrow needs a matching FinalizeS3()
// before exit or it may segfault in static teardown; this listener does it once the run is over,
// as the server's main.cpp does.
struct S3Finalizer : Catch::EventListenerBase {
    using EventListenerBase::EventListenerBase;
    void testRunEnded(Catch::TestRunStats const&) override {
        conn::s3::subsystem_finalizer_t finalize_s3(get_logger(logger_tag::S3_MANAGER));
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
