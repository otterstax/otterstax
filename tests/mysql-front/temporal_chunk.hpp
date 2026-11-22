// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// One row of every engine temporal type, the values the temporal wire tests read back.

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/date_types.hpp>
#include <core/date/time_zone.hpp>

#include <chrono>
#include <memory_resource>

namespace temporal_test {
    using namespace std::chrono_literals;
    using components::types::logical_type;

    // 2026-01-02: 26 years after 2000-01-01, 7 of them leap, and one day.
    inline constexpr int32_t date_days = 9498;
    // 2026-01-02 10:00:00.5
    inline constexpr int64_t timestamp_micros = int64_t{date_days} * 86'400'000'000 + 36'000'500'000;
    // 2026-01-02 10:00:00 UTC
    inline constexpr int64_t instant_micros = int64_t{date_days} * 86'400'000'000 + 36'000'000'000;
    // 10:11:12.25
    inline constexpr int64_t time_micros = 36'672'250'000;

    // Columns: d DATE, t TIME, ts TIMESTAMP, tz TIMESTAMPTZ and, `pg_only`, ttz TIMETZ (10:00:00+03), iv INTERVAL
    // (2 mons 3 days 04:00:00) — the two types MySQL has none for.
    inline components::vector::data_chunk_t chunk(std::pmr::memory_resource* resource, bool pg_only = true) {
        std::pmr::vector<components::types::complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::DATE, "d");
        fields.emplace_back(logical_type::TIME, "t");
        fields.emplace_back(logical_type::TIMESTAMP, "ts");
        fields.emplace_back(logical_type::TIMESTAMP_TZ, "tz");
        if (pg_only) {
            fields.emplace_back(logical_type::TIME_TZ, "ttz");
            fields.emplace_back(logical_type::INTERVAL, "iv");
        }
        components::vector::data_chunk_t out(resource, fields, 1);
        out.data[0].set_value(0, core::date::date_t{core::date::days{date_days}});
        out.data[1].set_value(0, core::date::time_t{core::date::microseconds{time_micros}});
        out.data[2].set_value(0, core::date::timestamp_t{core::date::microseconds{timestamp_micros}});
        out.data[3].set_value(0, core::date::timestamptz_t{core::date::microseconds{instant_micros}});
        if (pg_only) {
            out.data[4].set_value(
                0,
                core::date::timetz_t{core::date::microseconds{36'000'000'000}, core::date::timezone_offset_t{10800}});
            out.data[5].set_value(0,
                                  core::date::interval_t{core::date::microseconds{14'400'000'000},
                                                         core::date::days{3},
                                                         core::date::months{2}});
        }
        out.set_cardinality(1);
        return out;
    }

    inline core::date::time_zone_t plus_three() {
        core::date::time_zone_t zone;
        [[maybe_unused]] const bool named = core::date::setting_time_zone("+03:00", &zone);
        return zone;
    }
} // namespace temporal_test
