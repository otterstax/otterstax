// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "arrow/flight/sql/server.h"
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>

#include "../../types.hpp"
#include <otterbrix/otterbrix.hpp>

std::shared_ptr<arrow::Schema> to_arrow_schema(const std::pmr::vector<components::types::complex_logical_type>& types);
std::shared_ptr<arrow::Schema> to_arrow_schema(const components::types::complex_logical_type& struct_t);