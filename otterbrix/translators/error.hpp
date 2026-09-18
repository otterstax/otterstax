// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/status.h>

#include <memory_resource>
#include <string_view>

namespace tsl {

    // Every translator reports failure as a core::error_t whose message is owned by the caller's
    // resource; nothing in this layer throws.
    inline core::error_t make_error(std::pmr::memory_resource* res, core::error_code_t code, std::string_view what) {
        std::pmr::string message{res};
        message.append(what.data(), what.size());
        return core::error_t(code, std::move(message));
    }

    // Arrow reports failure through a returned Status; this is the single point where that verdict
    // becomes a project error.
    inline core::error_t arrow_error(std::pmr::memory_resource* res,
                                     core::error_code_t code,
                                     std::string_view what,
                                     const arrow::Status& status) {
        const std::string detail = status.ToString();
        std::pmr::string message{res};
        message.reserve(what.size() + 2 + detail.size());
        message.append(what.data(), what.size());
        message.append(": ", 2);
        message.append(detail.data(), detail.size());
        return core::error_t(code, std::move(message));
    }

} // namespace tsl
