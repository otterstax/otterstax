// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <boost/mysql.hpp>
#include <components/types/types.hpp>
#include <iostream>
#include <memory_resource>

namespace tsl {
    components::types::complex_logical_type mysql_to_struct(std::pmr::memory_resource* resource,
                                                            const boost::mysql::metadata_collection_view& result);
    components::types::complex_logical_type mysql_to_complex(boost::mysql::column_type result, const bool is_unsigned);
} // namespace tsl
