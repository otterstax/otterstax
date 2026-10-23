// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <string>

namespace ch {

    struct connect_params {
        std::string host;
        uint16_t port{9000};
        std::string username{"default"};
        std::string password;
        std::string database{"default"};
        std::string table;
    };

} // namespace ch
