// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "float_text.hpp"

#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <cfloat>
#include <charconv>
#include <cmath>
#include <string_view>

namespace frontend {

    namespace {
        // A finite value as 0.<digits> × 10^point (dtoa's decpt): the digits
        // without trailing zeros, at least one ("0" for a zero, point 1).
        struct decimal_t {
            bool negative{false};
            std::string digits;
            int point{0};
        };

        // `significant` 0: the shortest digits that read back as `value`
        // (std::to_chars, shortest round trip — the digits ryu and dtoa mode 0
        // give); otherwise that many, correctly rounded.
        template<typename T>
        decimal_t decimal_of(T value, int significant) {
            char buffer[64];
            char* const end = buffer + sizeof(buffer);
            const auto written = significant == 0
                                     ? std::to_chars(buffer, end, value, std::chars_format::scientific)
                                     : std::to_chars(buffer, end, value, std::chars_format::scientific, significant - 1);
            // [-]d[.ddd]e±XX
            std::string_view text(buffer, static_cast<size_t>(written.ptr - buffer));
            decimal_t decimal;
            if (text.front() == '-') {
                decimal.negative = true;
                text.remove_prefix(1);
            }
            const auto e = text.find('e');
            for (char c : text.substr(0, e)) {
                if (c != '.') {
                    decimal.digits.push_back(c);
                }
            }
            while (decimal.digits.size() > 1 && decimal.digits.back() == '0') {
                decimal.digits.pop_back();
            }
            auto exponent_text = text.substr(e + 1);
            if (exponent_text.front() == '+') {
                exponent_text.remove_prefix(1);
            }
            int exponent = 0;
            std::from_chars(exponent_text.data(), exponent_text.data() + exponent_text.size(), exponent);
            decimal.point = exponent + 1;
            return decimal;
        }

        // The digits with the point where `point` puts it: 0.000ddd, dd.ddd,
        // ddd000 (ryu's to_chars_df, my_gcvt's 'f' format).
        std::string fixed_text(const decimal_t& decimal) {
            const int length = static_cast<int>(decimal.digits.size());
            std::string out = decimal.negative ? "-" : "";
            if (decimal.point <= 0) {
                out += "0.";
                out.append(static_cast<size_t>(-decimal.point), '0');
                out += decimal.digits;
            } else if (decimal.point < length) {
                out.append(decimal.digits, 0, static_cast<size_t>(decimal.point));
                out.push_back('.');
                out.append(decimal.digits, static_cast<size_t>(decimal.point));
            } else {
                out += decimal.digits;
                out.append(static_cast<size_t>(decimal.point - length), '0');
            }
            return out;
        }

        // d[.ddd]e<exponent>, the exponent's sign always (PostgreSQL) or only
        // when negative (MySQL), at least `exponent_digits` digits.
        std::string scientific_text(const decimal_t& decimal, bool plus_sign, int exponent_digits) {
            std::string out = decimal.negative ? "-" : "";
            out.push_back(decimal.digits.front());
            if (decimal.digits.size() > 1) {
                out.push_back('.');
                out.append(decimal.digits, 1);
            }
            out.push_back('e');
            int exponent = decimal.point - 1;
            if (exponent < 0) {
                out.push_back('-');
                exponent = -exponent;
            } else if (plus_sign) {
                out.push_back('+');
            }
            const auto digits = std::to_string(exponent);
            if (static_cast<int>(digits.size()) < exponent_digits) {
                out.append(static_cast<size_t>(exponent_digits) - digits.size(), '0');
            }
            out += digits;
            return out;
        }

        // ryu's to_chars: fixed for a decimal exponent in -4 .. fixed_below - 1.
        std::string pg_shortest(const decimal_t& decimal, int fixed_below) {
            const int exponent = decimal.point - 1;
            if (exponent >= -4 && exponent < fixed_below) {
                return fixed_text(decimal);
            }
            return scientific_text(decimal, true, 2);
        }

        // pg_strfromd: "%.*g" of a finite value, the precision in 1 .. 32.
        std::string pg_printf_g(double value, int precision) {
            char buffer[64];
            const auto written = std::to_chars(buffer,
                                               buffer + sizeof(buffer),
                                               value,
                                               std::chars_format::general,
                                               std::clamp(precision, 1, 32));
            return std::string(buffer, written.ptr);
        }

        // NaN and the infinities, as both servers here spell them; nullptr for
        // a finite value.
        const char* special_text(double value) {
            if (std::isnan(value)) {
                return "NaN";
            }
            if (std::isinf(value)) {
                return value < 0 ? "-Infinity" : "Infinity";
            }
            return nullptr;
        }

        // my_gcvt with FLOATING_POINT_BUFFER: every double fits the 'f' format's
        // width, so the format is chosen by MAX_DECPT_FOR_F_FORMAT alone.
        std::string mysql_text(const decimal_t& decimal) {
            constexpr int max_decpt_for_f_format = DBL_DIG;
            const int length = static_cast<int>(decimal.digits.size());
            if (decimal.point >= -max_decpt_for_f_format + 1 &&
                (decimal.point <= max_decpt_for_f_format || length > decimal.point)) {
                return fixed_text(decimal);
            }
            return scientific_text(decimal, false, 1);
        }
    } // namespace

    std::string pg_float8_text(double value, int extra_float_digits) {
        OTX_ZONE_N("frontend::pg_float8_text");
        if (const char* special = special_text(value)) {
            return special;
        }
        if (extra_float_digits > 0) {
            return pg_shortest(decimal_of(value, 0), 15);
        }
        return pg_printf_g(value, DBL_DIG + extra_float_digits);
    }

    std::string pg_float4_text(float value, int extra_float_digits) {
        OTX_ZONE_N("frontend::pg_float4_text");
        if (const char* special = special_text(value)) {
            return special;
        }
        if (extra_float_digits > 0) {
            return pg_shortest(decimal_of(value, 0), 6);
        }
        // float4out hands the float4 to pg_strfromd as a double.
        return pg_printf_g(static_cast<double>(value), FLT_DIG + extra_float_digits);
    }

    std::string mysql_double_text(double value) {
        OTX_ZONE_N("frontend::mysql_double_text");
        if (const char* special = special_text(value)) {
            return special;
        }
        return mysql_text(decimal_of(value, 0));
    }

    std::string mysql_float_text(float value) {
        OTX_ZONE_N("frontend::mysql_float_text");
        if (const char* special = special_text(value)) {
            return special;
        }
        // dtoa(x, 4, FLT_DIG) of the float as a double: its shortest digits
        // when they are at most six, else six, rounded.
        const auto as_double = static_cast<double>(value);
        auto decimal = decimal_of(as_double, 0);
        if (decimal.digits.size() > FLT_DIG) {
            decimal = decimal_of(as_double, FLT_DIG);
        }
        return mysql_text(decimal);
    }

    std::string double_text(const text_format_t& format, double value) {
        return format.wire == frontend_type::POSTGRES ? pg_float8_text(value, format.extra_float_digits)
                                                      : mysql_double_text(value);
    }

    std::string float_text(const text_format_t& format, float value) {
        return format.wire == frontend_type::POSTGRES ? pg_float4_text(value, format.extra_float_digits)
                                                      : mysql_float_text(value);
    }

} // namespace frontend
