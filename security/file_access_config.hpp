// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace otterstax::security {
struct file_access_config {
    std::vector<std::string> read_roots;
    std::vector<std::string> write_roots;
    int64_t max_file_bytes{64 * 1024 * 1024};
};
}
