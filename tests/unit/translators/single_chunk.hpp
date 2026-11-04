// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/vector/data_chunk.hpp>

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <vector>

namespace otterstax::test {

    // The one chunk of a loader's run (an input within DEFAULT_VECTOR_CAPACITY rows, an
    // empty one included, is one chunk).
    inline components::vector::data_chunk_t& single(std::pmr::vector<components::vector::data_chunk_t>& run) {
        REQUIRE(run.size() == 1);
        return run.front();
    }

    inline const components::vector::data_chunk_t&
    single(const std::pmr::vector<components::vector::data_chunk_t>& run) {
        REQUIRE(run.size() == 1);
        return run.front();
    }

} // namespace otterstax::test
