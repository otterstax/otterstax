// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <memory_resource>

namespace otterstax {

    // A memory_resource over a stack buffer for the scratch containers of one call: the first `Bytes`
    // come from the frame, the rest from `upstream`. Nothing is released before the arena dies.
    // Only for data that dies with the declaring scope; never hand an arena-backed container to the
    // engine (its types keep the caller's allocator, so the engine would point into this frame).
    // Not thread-safe.
    template<std::size_t Bytes>
    class stack_arena_t {
    public:
        explicit stack_arena_t(std::pmr::memory_resource* upstream)
            : resource_(buffer_, Bytes, upstream) {}

        stack_arena_t(const stack_arena_t&) = delete;
        stack_arena_t& operator=(const stack_arena_t&) = delete;

        std::pmr::memory_resource* resource() noexcept { return &resource_; }

    private:
        alignas(std::max_align_t) std::byte buffer_[Bytes];
        std::pmr::monotonic_buffer_resource resource_;
    };

} // namespace otterstax
