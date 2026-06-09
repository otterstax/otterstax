// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include "tracy_profiler.hpp"
#include <cassert>
#include <memory_resource>

// Thin PMR interposer that reports every upstream slab to Tracy under a named
// pool. One instance per logical component gives per-component breakdown in the
// Memory → Pools view.
//
// Lifetime: the wrapper must outlive every PMR container using it as its
// resource. Declare as a static local in ComponentManager's constructor so the
// address stays stable for the process lifetime.
class tracy_memory_resource : public std::pmr::memory_resource {
public:
    // name must be a string literal (static lifetime). Tracy stores the raw
    // pointer without copying — passing a temporary is a bug.
    tracy_memory_resource(std::pmr::memory_resource* upstream, const char* name) noexcept
        : upstream_(upstream)
        , name_(name) {
        assert(upstream_ != nullptr);
        assert(name_ != nullptr);
    }

    // Non-copyable, non-movable: the object's address is baked into every PMR
    // container that was constructed with this resource pointer.
    tracy_memory_resource(const tracy_memory_resource&)            = delete;
    tracy_memory_resource& operator=(const tracy_memory_resource&) = delete;

private:
    void* do_allocate(std::size_t bytes, std::size_t align) override {
        void* ptr = upstream_->allocate(bytes, align);
#ifdef TRACY_ENABLE
        TracyAllocN(ptr, bytes, name_);
#endif
        return ptr;
    }

    void do_deallocate(void* ptr, std::size_t bytes, std::size_t align) override {
        // Report to Tracy BEFORE releasing: after deallocate() the address may
        // be immediately reused, which would confuse Tracy's tracking.
#ifdef TRACY_ENABLE
        TracyFreeN(ptr, name_);
#endif
        upstream_->deallocate(ptr, bytes, align);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

    std::pmr::memory_resource* upstream_;
    const char*                name_;
};
