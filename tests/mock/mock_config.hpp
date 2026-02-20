// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <string>

struct mock_config {
    std::pmr::memory_resource* resource = nullptr;
    bool can_throw = false;
    bool return_empty = false;
    std::chrono::milliseconds wait_time = std::chrono::milliseconds(50);
    std::string error_message = "";
};
