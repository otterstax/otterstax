// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <cstdint>
#include <iostream>

enum class DocumentTypes : uint8_t
{
    BOOL = 0,
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,
    UINT8 = 6,
    UINT16 = 7,
    UINT32 = 8,
    UINT64 = 9,
    FLOAT = 10,
    DOUBLE = 11,
    STRING = 12,
    NA = 127, // NULL
    INVALID = 255,
};

inline std::ostream& operator<<(std::ostream& os, DocumentTypes type) {
    const static std::unordered_map<DocumentTypes, std::string> document_types_to_str = {
        {DocumentTypes::BOOL, "BOOL"},
        {DocumentTypes::INT8, "INT8"},
        {DocumentTypes::INT16, "INT16"},
        {DocumentTypes::INT32, "INT32"},
        {DocumentTypes::INT64, "INT64"},
        {DocumentTypes::UINT8, "UINT8"},
        {DocumentTypes::UINT16, "UINT16"},
        {DocumentTypes::UINT32, "UINT32"},
        {DocumentTypes::UINT64, "UINT64"},
        {DocumentTypes::FLOAT, "FLOAT"},
        {DocumentTypes::DOUBLE, "DOUBLE"},
        {DocumentTypes::STRING, "STRING"},
        {DocumentTypes::NA, "NA"}, // NULL
        {DocumentTypes::INVALID, "INVALID"}};

    os << document_types_to_str.at(type);
    return os;
}