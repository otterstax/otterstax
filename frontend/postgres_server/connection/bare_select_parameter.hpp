// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <memory_resource>
#include <optional>
#include <string_view>

namespace frontend::postgres {
    // The 1-based number of the parameter that is the bare select-list column
    // of the single SELECT `sql` — 42P18 at the Parse; none for a parameter an
    // expression types. The raw tree lives on `resource`.
    [[nodiscard]] std::optional<int> bare_select_parameter(std::pmr::memory_resource* resource,
                                                           std::string_view sql);
} // namespace frontend::postgres
