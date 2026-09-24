// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/translators/input/csv_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>

namespace otterstax::test {

    // A chunk with columns (id BIGINT, name STRING) holding `rows` rows, ids running from
    // `first_id` upwards and name "row_<id>", built through the CSV input translator.
    inline components::vector::data_chunk_t make_rows_chunk(std::pmr::memory_resource* res,
                                                            int64_t first_id,
                                                            size_t rows) {
        std::string csv = "id,name\n";
        for (size_t i = 0; i < rows; ++i) {
            const auto id = std::to_string(first_id + static_cast<int64_t>(i));
            csv += id + ",row_" + id + "\n";
        }
        auto loaded = tsl::csv_to_chunk(res, reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
        REQUIRE_FALSE(loaded.has_error());
        return std::move(loaded.value());
    }

} // namespace otterstax::test
