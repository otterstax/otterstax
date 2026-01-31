// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <otterbrix/otterbrix.hpp>

inline configuration::config make_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.log.level = log_t::level::trace;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}