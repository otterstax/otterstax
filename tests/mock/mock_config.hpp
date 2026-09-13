// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <memory_resource>
#include <string>

struct mock_config {
    // No default: every mock allocates on the resource its test hands it.
    std::pmr::memory_resource* resource;
    bool can_throw = false;
    bool return_empty = false;
    std::chrono::milliseconds wait_time = std::chrono::milliseconds(50);
    std::string error_message = "";
};
