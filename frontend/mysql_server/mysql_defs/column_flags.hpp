// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::mysql {
    // Values for the flags bitmask, currently need to fit into 32 bits
    using column_flags_t = uint32_t;

    constexpr uint32_t NOT_NULL_FLAG = 1;
    constexpr uint32_t PRI_KEY_FLAG = 2;
    constexpr uint32_t UNIQUE_KEY_FLAG = 4;
    constexpr uint32_t MULTIPLE_KEY_FLAG = 8;
    constexpr uint32_t BLOB_FLAG = 16;
    constexpr uint32_t UNSIGNED_FLAG = 32;
    constexpr uint32_t ZEROFILL_FLAG = 64;
    constexpr uint32_t BINARY_FLAG = 128;
    constexpr uint32_t ENUM_FLAG = 256;
    constexpr uint32_t AUTO_INCREMENT_FLAG = 512;
    constexpr uint32_t TIMESTAMP_FLAG = 1024;
    constexpr uint32_t SET_FLAG = 2048;
    constexpr uint32_t NO_DEFAULT_VALUE_FLAG = 4096;
    constexpr uint32_t ON_UPDATE_NOW_FLAG = 8192;
    constexpr uint32_t NUM_FLAG = 32768;
    constexpr uint32_t PART_KEY_FLAG = 16384;
    constexpr uint32_t GROUP_FLAG = 32768;
    constexpr uint32_t UNIQUE_FLAG = 65536;
    constexpr uint32_t BINCMP_FLAG = 131072;
    constexpr uint32_t GET_FIXED_FIELDS_FLAG = (1 << 18);
    constexpr uint32_t FIELD_IN_PART_FUNC_FLAG = (1 << 19);
    constexpr uint32_t FIELD_IN_ADD_INDEX = (1 << 20);
    constexpr uint32_t FIELD_IS_RENAMED = (1 << 21);
    constexpr uint32_t FIELD_FLAGS_STORAGE_MEDIA = 22; // Field storage media, bit 22-23. More...
    constexpr uint32_t FIELD_FLAGS_STORAGE_MEDIA_MASK = (3 << FIELD_FLAGS_STORAGE_MEDIA);
    constexpr uint32_t FIELD_FLAGS_COLUMN_FORMAT = 24; // Field column format, bit 24-25. More...
    constexpr uint32_t FIELD_FLAGS_COLUMN_FORMAT_MASK = (3 << FIELD_FLAGS_COLUMN_FORMAT);
    constexpr uint32_t FIELD_IS_DROPPED = (1 << 26);
    constexpr uint32_t EXPLICIT_NULL_FLAG = (1 << 27); // Field is explicitly specified as \ NULL by the user. More...
    constexpr uint32_t NOT_SECONDARY_FLAG = (1 << 29);
    constexpr uint32_t FIELD_IS_INVISIBLE = (1 << 30); //Field is explicitly marked as invisible by the user
} // namespace frontend::mysql