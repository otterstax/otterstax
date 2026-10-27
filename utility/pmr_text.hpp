// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <charconv>
#include <cstddef>
#include <initializer_list>
#include <memory_resource>
#include <string>
#include <string_view>

namespace otterstax {

    // The pieces joined into one std::pmr::string on `resource`, sized once: how messages are
    // built (an error_t message, a log line) without a temporary std::string per `+`.
    inline std::pmr::string concat(std::pmr::memory_resource* resource, std::initializer_list<std::string_view> parts) {
        std::size_t size = 0;
        for (auto part : parts) {
            size += part.size();
        }
        std::pmr::string out(resource);
        out.reserve(size);
        for (auto part : parts) {
            out.append(part.data(), part.size());
        }
        return out;
    }

    // `out` += the decimal text of `number`, with no allocation of its own.
    template<typename Number>
    void append_number(std::pmr::string& out, Number number) {
        char digits[48];
        const auto written = std::to_chars(digits, digits + sizeof(digits), number);
        out.append(digits, static_cast<std::size_t>(written.ptr - digits));
    }

    // The decimal text of `number` on `resource`.
    template<typename Number>
    std::pmr::string number_text(std::pmr::memory_resource* resource, Number number) {
        std::pmr::string out(resource);
        append_number(out, number);
        return out;
    }

} // namespace otterstax
