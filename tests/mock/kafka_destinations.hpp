// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once
#include "security/network_destinations.hpp"
#include <memory>

inline auto fixture_kafka_destinations() {
    return std::make_shared<const otterstax::security::network_destinations>(
        std::vector<std::string>{"localhost:9092", "127.0.0.1:9092"});
}
