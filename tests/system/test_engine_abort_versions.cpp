// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include <components/table/collection.hpp>
#include <catch2/catch_all.hpp>

namespace {
    using namespace components::table;
    constexpr auto capacity = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t owner = TRANSACTION_ID_START + 1;
    constexpr uint64_t other = TRANSACTION_ID_START + 2;

    void append(row_version_manager_t& versions, uint64_t txn, uint64_t start, uint64_t count) {
        versions.append_version_info(transaction_data{txn, 0}, count, start, start + count);
    }
}

TEST_CASE("Engine append abort removes only its owned range across version vectors", "[rollback-checkpoint]") {
    constexpr uint64_t origin = 4096;
    row_version_manager_t versions(origin);
    append(versions, 0, 0, 1);
    append(versions, owner, 1, 2 * capacity);
    append(versions, other, 2 * capacity + 1, 1);
    versions.commit_append(8, 2 * capacity + 1, 1);
    REQUIRE(versions.has_version_above(8, 2 * capacity + 2));

    REQUIRE(versions.abort_append(owner, 1, 2 * capacity));
    CHECK_FALSE(versions.has_version_above(8, 2 * capacity + 2));
    CHECK(versions.committed_deleted_count(2 * capacity + 2) == 2 * capacity);
    for (uint64_t horizon : {uint64_t{0}, uint64_t{7}, uint64_t{8}}) {
        transaction_data reader{other + 1, 0};
        reader.snapshot_horizon = horizon;
        CHECK(versions.fetch(reader, origin));
        for (uint64_t i = 1; i <= 2 * capacity; ++i) {
            REQUIRE_FALSE(versions.fetch(reader, origin + i));
        }
        CHECK(versions.fetch(reader, origin + 2 * capacity + 1) == (horizon >= 8));
    }

    versions.cleanup_append(8, 0, 2 * capacity + 2);
    CHECK_FALSE(versions.has_version_above(8, 2 * capacity + 2));
    CHECK(versions.committed_deleted_count(2 * capacity + 2) == 2 * capacity);
    for (uint64_t i = 1; i <= 2 * capacity; ++i) {
        REQUIRE_FALSE(versions.fetch(transaction_data{}, origin + i));
    }
    CHECK(versions.fetch(transaction_data{}, origin + 2 * capacity + 1));
}

TEST_CASE("Engine append abort refuses stale ownership before changing any row", "[rollback-checkpoint]") {
    row_version_manager_t versions(0);
    append(versions, owner, 0, capacity);
    append(versions, other, capacity, 1);
    REQUIRE_FALSE(versions.abort_append(owner, 0, capacity + 1));
    CHECK(versions.committed_deleted_count(capacity + 1) == 0);
    CHECK(versions.fetch(transaction_data{owner, 0}, 0));
    CHECK(versions.fetch(transaction_data{other, 0}, capacity));
    CHECK_FALSE(versions.abort_append(0, 0, capacity));
    CHECK_FALSE(versions.abort_append(owner, UINT64_MAX, 2));
}

TEST_CASE("Engine version expansion preserves deletion of rows outside an aborted range", "[rollback-checkpoint]") {
    row_version_manager_t versions(0);
    append(versions, owner, 0, capacity);
    REQUIRE(versions.abort_append(owner, 0, capacity));
    versions.cleanup_append(8, 0, capacity); // Collapse to constant deleted metadata.

    // Expanding a constant vector for a later delete must keep every tombstone.
    int64_t row[] = {0};
    CHECK(versions.delete_rows(0, other, row, 1) == 0);
    versions.revert_all_deletes(other);
    CHECK(versions.committed_deleted_count(capacity) == capacity);
    for (uint64_t i = 0; i < capacity; ++i) {
        REQUIRE_FALSE(versions.fetch(transaction_data{}, i));
    }
}
