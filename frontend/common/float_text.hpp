// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "protocol_config.hpp"

#include <string>

// The text of a FLOAT / DOUBLE cell, as each wire's own server writes it.
namespace frontend {

    // PostgreSQL's default extra_float_digits (guc_tables.c): the shortest
    // exact form.
    inline constexpr int default_extra_float_digits = 1;

    // PostgreSQL 18's float8out / float4out (float.c): extra_float_digits > 0
    // the shortest text that reads back as the same value (ryu d2s.c / f2s.c:
    // fixed while the decimal exponent is in -4 .. 14 for a double, -4 .. 5
    // for a float4, else d.ddde±XX with at least two exponent digits);
    // otherwise printf "%.*g" with DBL_DIG / FLT_DIG + extra_float_digits
    // digits, at least one (pg_strfromd). NaN, Infinity, -Infinity, -0.
    [[nodiscard]] std::string pg_float8_text(double value, int extra_float_digits);
    [[nodiscard]] std::string pg_float4_text(float value, int extra_float_digits);

    // MySQL 8.4's text of a DOUBLE / FLOAT column of no declared scale
    // (Protocol_text::store_double / store_float → my_gcvt with
    // FLOATING_POINT_BUFFER): the shortest digits that read back as the
    // double (dtoa mode 4) — for a FLOAT at most FLT_DIG = 6 significant
    // digits —, fixed while the decimal point position (dtoa's decpt) is in
    // -14 .. 15 (-MAX_DECPT_FOR_F_FORMAT + 1 .. MAX_DECPT_FOR_F_FORMAT, which
    // is DBL_DIG) or the digits reach past the point,
    // else d.ddde[-]X (no '+', no padding: 1e20, 1.5e-16). MySQL stores no
    // NaN or infinity; they are written NaN, Infinity, -Infinity, which every
    // MySQL driver reads back as such.
    [[nodiscard]] std::string mysql_double_text(double value);
    [[nodiscard]] std::string mysql_float_text(float value);

    // How a cell is written as text: as `wire`'s server writes it (a boolean
    // t / f on PostgreSQL, 1 / 0 on MySQL; a float as below, the PostgreSQL
    // one under the connection's extra_float_digits).
    struct text_format_t {
        frontend_type wire;
        int extra_float_digits{default_extra_float_digits};
    };

    [[nodiscard]] std::string double_text(const text_format_t& format, double value);
    [[nodiscard]] std::string float_text(const text_format_t& format, float value);

} // namespace frontend
