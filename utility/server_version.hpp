// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <string_view>

namespace otterstax {
    // The PostgreSQL version the server speaks: the PostgreSQL wire's `server_version` ParameterStatus.
    inline constexpr std::string_view postgres_server_version = "18.0";

    // What version() answers (catalog::make_version_function). Drivers read the version from its prefix —
    // SQLAlchemy's PostgreSQL dialects match `PostgreSQL (\d+)\.?(\d+)?` — so it starts as PostgreSQL's does.
    inline constexpr std::string_view version_text = "PostgreSQL 18.0 (OtterStax)";

    static_assert(version_text.substr(std::string_view{"PostgreSQL "}.size(), postgres_server_version.size()) ==
                      postgres_server_version,
                  "version() and server_version name the same PostgreSQL version");
} // namespace otterstax
