// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax


#include "../../../utility/session.hpp"

#include <catch2/catch.hpp>
#include <iostream>

TEST_CASE("session: base test") {
    auto session_1 = session_id();
    auto session_2 = session_id();
    REQUIRE(session_1 != session_2);
}