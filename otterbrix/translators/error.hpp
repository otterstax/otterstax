// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/status.h>

#include "utility/pmr_text.hpp"

#include <initializer_list>
#include <memory_resource>
#include <string>
#include <string_view>

namespace tsl {

    // Every translator reports failure as a core::error_t whose message is owned by the caller's
    // resource; nothing in this layer throws.
    inline core::error_t make_error(std::pmr::memory_resource* res, core::error_code_t code, std::string_view what) {
        std::pmr::string message{res};
        message.append(what.data(), what.size());
        return core::error_t(code, std::move(message));
    }

    // The message joined from `parts`, built once on `res`.
    inline core::error_t
    make_error(std::pmr::memory_resource* res, core::error_code_t code, std::initializer_list<std::string_view> parts) {
        return core::error_t(code, otterstax::concat(res, parts));
    }

    // Arrow reports failure through a returned Status; this is the single point where that verdict
    // becomes a project error.
    inline core::error_t arrow_error(std::pmr::memory_resource* res,
                                     core::error_code_t code,
                                     std::string_view what,
                                     const arrow::Status& status) {
        const std::string detail = status.ToString(); // Arrow's own std::string
        return core::error_t(code, otterstax::concat(res, {what, ": ", detail}));
    }

    // `parts` joined, then ": " and the status.
    inline core::error_t arrow_error(std::pmr::memory_resource* res,
                                     core::error_code_t code,
                                     std::initializer_list<std::string_view> parts,
                                     const arrow::Status& status) {
        auto message = otterstax::concat(res, parts);
        message += ": ";
        message += status.ToString();
        return core::error_t(code, std::move(message));
    }

} // namespace tsl
