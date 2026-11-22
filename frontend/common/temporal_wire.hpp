// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "float_text.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/date/date_to_string.hpp>
#include <core/date/date_types.hpp>
#include <core/date/time_zone.hpp>

#include <chrono>
#include <cstdint>
#include <string>

// The engine's temporal values on the wires. The engine stores them as PostgreSQL does — a DATE in days, a TIME in
// microseconds of the day, a TIMESTAMP / TIMESTAMPTZ in microseconds, both since 2000-01-01 (a TIMESTAMPTZ in UTC),
// a TIME_TZ as microseconds and an offset east of UTC, an INTERVAL as microseconds, days and months — which is
// PostgreSQL's binary format as it is (but TIMETZ's offset, which PostgreSQL counts west of UTC). The text is
// core::date's (PostgreSQL's ISO style); a TIMESTAMPTZ is shown in the session's time zone: on PostgreSQL with its
// offset (timestamptz_out), on MySQL as that zone's local time (MySQL's TIMESTAMP). MySQL's TIME / DATETIME /
// TIMESTAMP text carries six fraction digits always (the columns are announced with 6 decimals).
namespace frontend::temporal {
    // A TIMESTAMP's (or a zone-local TIMESTAMPTZ's) calendar fields.
    struct civil_t {
        int year;
        unsigned month;
        unsigned day;
        unsigned hour;
        unsigned minute;
        unsigned second;
        std::uint32_t microsecond;
    };

    inline civil_t civil_of(core::date::microseconds since_epoch) {
        const auto [day_part, time_part] = core::date::split_timestamp(since_epoch);
        const std::chrono::year_month_day ymd{core::date::pg_epoch + std::chrono::days{day_part.count()}};
        std::int64_t rest = time_part.count();
        civil_t out{};
        out.year = static_cast<int>(ymd.year());
        out.month = static_cast<unsigned>(ymd.month());
        out.day = static_cast<unsigned>(ymd.day());
        out.hour = static_cast<unsigned>(rest / 3'600'000'000);
        rest %= 3'600'000'000;
        out.minute = static_cast<unsigned>(rest / 60'000'000);
        rest %= 60'000'000;
        out.second = static_cast<unsigned>(rest / 1'000'000);
        out.microsecond = static_cast<std::uint32_t>(rest % 1'000'000);
        return out;
    }

    // The offset of `zone` (UTC when null) in effect at `instant`, in seconds east of UTC.
    inline core::date::timezone_offset_t offset_at(core::date::timestamptz_t instant,
                                                   const core::date::time_zone_t* zone) {
        return zone == nullptr ? core::date::timezone_offset_t{0} : zone->offset_at(instant);
    }

    // `instant` as the local time of `zone`.
    inline core::date::microseconds local_time(core::date::timestamptz_t instant, const core::date::time_zone_t* zone) {
        return instant.value + std::chrono::duration_cast<core::date::microseconds>(offset_at(instant, zone));
    }

    // Whether `type` is one of the engine's temporal types.
    inline bool is_temporal(components::types::logical_type type) {
        using LT = components::types::logical_type;
        return type == LT::DATE || type == LT::TIME || type == LT::TIME_TZ || type == LT::TIMESTAMP ||
               type == LT::TIMESTAMP_TZ || type == LT::INTERVAL;
    }

    // MySQL's text of a TIME(6) / DATETIME(6) / TIMESTAMP(6) (the column definitions announce 6 decimals): the
    // fraction always in six digits, as MySQL writes it and as a client of those definitions reads it back.
    inline void append_mysql_fraction(std::string& out, std::uint32_t microsecond) {
        out.push_back('.');
        core::date::detail::append_uint(out, microsecond, 6);
    }

    inline std::string mysql_datetime_text(core::date::microseconds since_epoch) {
        const auto civil = civil_of(since_epoch);
        std::string out;
        core::date::detail::append_date(out,
                                        std::chrono::sys_days{std::chrono::year{civil.year} /
                                                              std::chrono::month{civil.month} /
                                                              std::chrono::day{civil.day}});
        out.push_back(' ');
        core::date::detail::append_uint(out, civil.hour, 2);
        out.push_back(':');
        core::date::detail::append_uint(out, civil.minute, 2);
        out.push_back(':');
        core::date::detail::append_uint(out, civil.second, 2);
        append_mysql_fraction(out, civil.microsecond);
        return out;
    }

    inline std::string mysql_time_text(core::date::time_t value) {
        const std::int64_t micros = value.value.count();
        const std::int64_t seconds = micros / 1'000'000;
        std::string out;
        core::date::detail::append_uint(out, static_cast<std::uint64_t>(seconds / 3600), 2);
        out.push_back(':');
        core::date::detail::append_uint(out, static_cast<std::uint64_t>(seconds / 60 % 60), 2);
        out.push_back(':');
        core::date::detail::append_uint(out, static_cast<std::uint64_t>(seconds % 60), 2);
        append_mysql_fraction(out, static_cast<std::uint32_t>(micros % 1'000'000));
        return out;
    }

    // The text of a non-NULL temporal value `index` of the flat vector `vec`, as `format`'s wire writes it.
    inline std::string text(const components::vector::vector_t& vec, std::uint64_t index, const text_format_t& format) {
        using LT = components::types::logical_type;
        const bool mysql = format.wire == frontend_type::MYSQL;
        switch (vec.type().type()) {
            case LT::DATE:
                return core::date::to_string(vec.get_value<core::date::date_t>(index));
            case LT::TIME:
                return mysql ? mysql_time_text(vec.get_value<core::date::time_t>(index))
                             : core::date::to_string(vec.get_value<core::date::time_t>(index));
            case LT::TIME_TZ:
                return core::date::to_string(vec.get_value<core::date::timetz_t>(index));
            case LT::TIMESTAMP:
                return mysql ? mysql_datetime_text(vec.get_value<core::date::timestamp_t>(index).value)
                             : core::date::to_string(vec.get_value<core::date::timestamp_t>(index));
            case LT::TIMESTAMP_TZ: {
                const auto instant = vec.get_value<core::date::timestamptz_t>(index);
                if (mysql) {
                    return mysql_datetime_text(local_time(instant, format.time_zone));
                }
                std::string out = core::date::to_string(core::date::timestamp_t{local_time(instant, format.time_zone)});
                if (format.wire == frontend_type::POSTGRES) {
                    core::date::detail::append_zone(out, offset_at(instant, format.time_zone));
                }
                return out;
            }
            case LT::INTERVAL:
                return core::date::to_string(vec.get_value<core::date::interval_t>(index));
            default:
                return {};
        }
    }
} // namespace frontend::temporal
