// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>

namespace tsl {

    // An arrow::MemoryPool over the caller's std::pmr::memory_resource, so Arrow's buffers live where
    // the chunks do. Arrow allocates from helper threads too (readahead, pre-buffering, S3 parts), so
    // calls into the resource are serialised; code using the resource directly meanwhile needs a
    // thread-safe one (the engine's is). Buffers must die before the resource, as chunks do.
    class pmr_memory_pool final : public arrow::MemoryPool {
    public:
        explicit pmr_memory_pool(std::pmr::memory_resource* resource) noexcept
            : resource_(resource) {
            assert(resource_ != nullptr);
        }

        using arrow::MemoryPool::Allocate;
        using arrow::MemoryPool::Free;
        using arrow::MemoryPool::Reallocate;

        arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
            if (size < 0) {
                return arrow::Status::Invalid("negative allocation size requested: ", size);
            }
            // A zero-size buffer never reaches the resource, as in Arrow's own pools: it is a
            // static aligned area that Free recognises by its size.
            if (size == 0) {
                *out = zero_size_area();
                return arrow::Status::OK();
            }
            // pmr reports exhaustion by throwing; Arrow expects a Status. The exception ends at this
            // boundary, so nothing throws out of the translators.
            try {
                std::lock_guard guard(mutex_);
                *out = static_cast<uint8_t*>(
                    resource_->allocate(static_cast<std::size_t>(size), static_cast<std::size_t>(alignment)));
            } catch (const std::bad_alloc&) {
                return arrow::Status::OutOfMemory("allocation of ", size, " bytes failed");
            }
            account(size);
            return arrow::Status::OK();
        }

        // pmr has no realloc: a fresh block, the common prefix copied, the old block returned.
        arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
            if (new_size < 0) {
                return arrow::Status::Invalid("negative reallocation size requested: ", new_size);
            }
            if (new_size == old_size) {
                return arrow::Status::OK();
            }
            uint8_t* fresh = nullptr;
            ARROW_RETURN_NOT_OK(Allocate(new_size, alignment, &fresh));
            const int64_t kept = std::min(old_size, new_size);
            if (kept > 0) {
                std::memcpy(fresh, *ptr, static_cast<std::size_t>(kept));
            }
            Free(*ptr, old_size, alignment);
            *ptr = fresh;
            return arrow::Status::OK();
        }

        // Arrow hands back the size and alignment the block was allocated with, which is what
        // pmr's deallocate requires.
        void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
            if (size == 0) {
                return;
            }
            {
                std::lock_guard guard(mutex_);
                resource_->deallocate(buffer, static_cast<std::size_t>(size), static_cast<std::size_t>(alignment));
            }
            bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
        }

        int64_t bytes_allocated() const override { return bytes_allocated_.load(std::memory_order_relaxed); }
        int64_t max_memory() const override { return max_memory_.load(std::memory_order_relaxed); }
        int64_t total_bytes_allocated() const override {
            return total_bytes_allocated_.load(std::memory_order_relaxed);
        }
        int64_t num_allocations() const override { return num_allocations_.load(std::memory_order_relaxed); }
        std::string backend_name() const override { return "otterstax-pmr"; }

    private:
        static uint8_t* zero_size_area() noexcept {
            alignas(arrow::kDefaultBufferAlignment) static uint8_t area[arrow::kDefaultBufferAlignment];
            return area;
        }

        void account(int64_t size) noexcept {
            const int64_t now = bytes_allocated_.fetch_add(size, std::memory_order_relaxed) + size;
            total_bytes_allocated_.fetch_add(size, std::memory_order_relaxed);
            num_allocations_.fetch_add(1, std::memory_order_relaxed);
            int64_t peak = max_memory_.load(std::memory_order_relaxed);
            while (now > peak && !max_memory_.compare_exchange_weak(peak, now, std::memory_order_relaxed)) {
            }
        }

        std::pmr::memory_resource* resource_;
        std::mutex mutex_;
        std::atomic<int64_t> bytes_allocated_{0};
        std::atomic<int64_t> max_memory_{0};
        std::atomic<int64_t> total_bytes_allocated_{0};
        std::atomic<int64_t> num_allocations_{0};
    };

    // The pool for `resource`, interned for the process: a buffer frees through the pool pointer it
    // was made with, possibly long after the call (FlightSQL's ticket cache, cached S3 streams). The
    // pool holds only the pointer, so address reuse is safe; the registry is never destroyed.
    inline arrow::MemoryPool* arrow_pool(std::pmr::memory_resource* resource) {
        assert(resource != nullptr && "arrow_pool needs the caller's resource");
        struct registry_t {
            std::mutex mutex;
            std::unordered_map<std::pmr::memory_resource*, std::unique_ptr<pmr_memory_pool>> pools;
        };
        static auto* registry = new registry_t;
        std::lock_guard guard(registry->mutex);
        auto& slot = registry->pools[resource];
        if (!slot) {
            slot = std::make_unique<pmr_memory_pool>(resource);
        }
        return slot.get();
    }

} // namespace tsl
