// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// tsl::arrow_pool: Arrow's buffers land on the caller's resource and return to it, aligned;
// zero-size buffers never reach it; exhaustion is a Status; one resource has one pool.

#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"

#include <catch2/catch_all.hpp>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <new>
#include <string>

namespace {

    // Forwards to new_delete and keeps both sides of the ledger, thread-safely:
    // Arrow allocates from its own reader threads.
    class ledger_resource final : public std::pmr::memory_resource {
    public:
        std::size_t allocated() const noexcept { return allocated_.load(); }
        std::int64_t outstanding() const noexcept { return outstanding_.load(); }
        std::size_t calls() const noexcept { return calls_.load(); }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            allocated_ += bytes;
            outstanding_ += static_cast<std::int64_t>(bytes);
            ++calls_;
            return std::pmr::new_delete_resource()->allocate(bytes, alignment);
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            outstanding_ -= static_cast<std::int64_t>(bytes);
            std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::atomic<std::size_t> allocated_{0};
        std::atomic<std::int64_t> outstanding_{0};
        std::atomic<std::size_t> calls_{0};
    };

    // Every allocation is refused, the way an exhausted resource reports it.
    class exhausted_resource final : public std::pmr::memory_resource {
        void* do_allocate(std::size_t, std::size_t) override { throw std::bad_alloc{}; }
        void do_deallocate(void*, std::size_t, std::size_t) override {}
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    };

} // namespace

// A pool is interned by its resource's address, and a stack resource can land where
// one of an earlier case lived, so the pool's counters are read as deltas.
TEST_CASE("arrow_pool: allocate, grow and free go through the caller's resource") {
    ledger_resource ledger;
    arrow::MemoryPool* pool = tsl::arrow_pool(&ledger);
    const int64_t live_before = pool->bytes_allocated();
    const int64_t calls_before = pool->num_allocations();

    uint8_t* data = nullptr;
    REQUIRE(pool->Allocate(100, &data).ok());
    REQUIRE(data != nullptr);
    CHECK(reinterpret_cast<std::uintptr_t>(data) % arrow::kDefaultBufferAlignment == 0);
    CHECK(ledger.allocated() == 100);
    CHECK(pool->bytes_allocated() - live_before == 100);
    std::memset(data, 0x5a, 100);

    // Growing keeps the prefix and returns the old block.
    REQUIRE(pool->Reallocate(100, 300, &data).ok());
    CHECK(reinterpret_cast<std::uintptr_t>(data) % arrow::kDefaultBufferAlignment == 0);
    CHECK(data[0] == 0x5a);
    CHECK(data[99] == 0x5a);
    CHECK(ledger.outstanding() == 300);
    CHECK(pool->bytes_allocated() - live_before == 300);
    CHECK(pool->max_memory() >= 300);

    pool->Free(data, 300);
    CHECK(ledger.outstanding() == 0);
    CHECK(pool->bytes_allocated() == live_before);
    CHECK(pool->num_allocations() - calls_before == 2);
}

TEST_CASE("arrow_pool: a zero-size buffer never reaches the resource") {
    ledger_resource ledger;
    arrow::MemoryPool* pool = tsl::arrow_pool(&ledger);

    uint8_t* data = nullptr;
    REQUIRE(pool->Allocate(0, &data).ok());
    REQUIRE(data != nullptr);
    CHECK(reinterpret_cast<std::uintptr_t>(data) % arrow::kDefaultBufferAlignment == 0);
    pool->Free(data, 0);
    CHECK(ledger.calls() == 0);

    // Growing from zero allocates; shrinking back to zero returns the block.
    REQUIRE(pool->Reallocate(0, 64, &data).ok());
    CHECK(ledger.outstanding() == 64);
    REQUIRE(pool->Reallocate(64, 0, &data).ok());
    CHECK(ledger.outstanding() == 0);
    pool->Free(data, 0);
}

TEST_CASE("arrow_pool: an exhausted resource is OutOfMemory, not an exception") {
    exhausted_resource exhausted;
    arrow::MemoryPool* pool = tsl::arrow_pool(&exhausted);

    const int64_t live_before = pool->bytes_allocated();

    uint8_t* data = nullptr;
    arrow::Status status;
    REQUIRE_NOTHROW(status = pool->Allocate(1024, &data));
    CHECK(status.IsOutOfMemory());
    CHECK(pool->bytes_allocated() == live_before);
}

TEST_CASE("arrow_pool: one resource has one pool") {
    ledger_resource first;
    ledger_resource second;
    CHECK(tsl::arrow_pool(&first) == tsl::arrow_pool(&first));
    CHECK(tsl::arrow_pool(&first) != tsl::arrow_pool(&second));
    CHECK(tsl::arrow_pool(&first)->backend_name() == "otterstax-pmr");
}

TEST_CASE("arrow_pool: a csv load allocates Arrow's buffers on the caller's resource and returns them") {
    const std::string path = (std::filesystem::temp_directory_path() / "otterstax_test_arrow_pool.csv").string();
    {
        std::ofstream out(path);
        out << "id,name\n";
        for (int i = 0; i < 2000; ++i) {
            out << i << ",row_" << i << "\n";
        }
    }
    const auto file_size = std::filesystem::file_size(path);

    ledger_resource ledger;
    const int64_t total_before = tsl::arrow_pool(&ledger)->total_bytes_allocated();
    const int64_t live_before = tsl::arrow_pool(&ledger)->bytes_allocated();
    {
        auto loaded = tsl::csv_to_chunk(&ledger, path);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 2000);
        // The file's read buffers alone are as large as the file; before the pool
        // they came from Arrow's default pool and never touched this resource.
        CHECK(tsl::arrow_pool(&ledger)->total_bytes_allocated() - total_before >= static_cast<int64_t>(file_size));
        CHECK(ledger.allocated() >= file_size);
    }
    // The chunk and every Arrow buffer of the load are gone with it.
    CHECK(ledger.outstanding() == 0);
    CHECK(tsl::arrow_pool(&ledger)->bytes_allocated() == live_before);

    std::filesystem::remove(path);
}
