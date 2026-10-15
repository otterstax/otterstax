// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "utility/data_directory_lock.hpp"

#include "tests/temporary_directory.hpp"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Data directory lock excludes another owner and releases on shutdown", "[storage]") {
    otterstax::test::temporary_directory directory("otterstax-lock");
    {
        otterstax::data_directory_lock owner(directory.path);
        REQUIRE_THROWS_AS(otterstax::data_directory_lock(directory.path), std::system_error);
        REQUIRE_THROWS_AS(otterstax::data_directory_lock(directory.path / "."), std::system_error);
        REQUIRE_NOTHROW(otterstax::data_directory_lock(directory.path / "tenant-b"));
    }
    REQUIRE_NOTHROW(otterstax::data_directory_lock(directory.path));
    REQUIRE(std::filesystem::exists(directory.path / ".otterstax.lock"));
}

#ifndef _WIN32
TEST_CASE("Data directory lock follows directory aliases but rejects lock-file symlinks", "[storage]") {
    otterstax::test::temporary_directory directory("otterstax-lock");
    std::filesystem::create_directories(directory.path / "tenant");
    std::filesystem::create_directory_symlink(directory.path / "tenant", directory.path / "alias");
    {
        otterstax::data_directory_lock owner(directory.path / "tenant");
        REQUIRE_THROWS_AS(otterstax::data_directory_lock(directory.path / "alias"), std::system_error);
    }
    std::filesystem::create_directories(directory.path / "other");
    std::filesystem::create_symlink(directory.path / "tenant" / ".otterstax.lock",
                                    directory.path / "other" / ".otterstax.lock");
    REQUIRE_THROWS_AS(otterstax::data_directory_lock(directory.path / "other"), std::system_error);
}
#endif
