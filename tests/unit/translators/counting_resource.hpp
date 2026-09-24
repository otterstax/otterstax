// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <memory_resource>

namespace otterstax::test {

    // Forwards to an explicit upstream and records how many bytes were requested, so a
    // test can pin an allocation bound (e.g. "no per-row buffer for a column-less chunk").
    class counting_resource final : public std::pmr::memory_resource {
    public:
        explicit counting_resource(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        std::size_t allocated_bytes() const noexcept { return allocated_; }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            allocated_ += bytes;
            return upstream_->allocate(bytes, alignment);
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            upstream_->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        std::size_t allocated_{0};
    };

} // namespace otterstax::test
