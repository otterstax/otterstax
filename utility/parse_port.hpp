// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <charconv>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <system_error>

namespace otterstax {

    // A backend port as it appears in the connection config: decimal digits only,
    // in 1..65535. Anything else — empty, signed, padded, non-numeric, out of
    // range — is an invalid_parameter, never a truncated or defaulted number.
    inline core::result_wrapper_t<uint16_t> parse_port(std::string_view text, std::pmr::memory_resource* resource) {
        unsigned int value = 0;
        const char* begin = text.data();
        const char* end = begin + text.size();
        auto [ptr, ec] = std::from_chars(begin, end, value);
        if (text.empty() || ec != std::errc{} || ptr != end || value == 0 || value > 65535) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{("invalid port '" + std::string(text) + "': expected an integer in 1..65535").c_str(),
                                 resource});
        }
        return static_cast<uint16_t>(value);
    }

} // namespace otterstax
