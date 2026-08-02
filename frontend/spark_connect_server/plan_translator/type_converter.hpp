// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Converts Otterbrix logical types into Spark Connect `DataType` protobuf
// messages (spark/connect/types.proto). Used to build AnalyzePlan(schema)
// responses. Conversion keys off `complex_logical_type::type()` (the
// logical_type discriminator), never the physical_type. Unknown / unmappable
// types degrade to `DataType.unparsed` rather than throwing.

#include <spark/connect/types.pb.h>

#include <otterbrix/otterbrix.hpp>

#include <memory_resource>

namespace frontend::spark {

// Maps a single Otterbrix logical type to a Spark Connect DataType.
// Recursive for nested STRUCT / LIST / ARRAY / MAP types.
::spark::connect::DataType to_spark_data_type(const components::types::complex_logical_type& type);

// Builds a top-level row schema (DataType.struct) from a STRUCT logical type.
// Non-STRUCT inputs (e.g. logical_type::NA for an empty result set) yield an
// empty struct, mirroring to_arrow_schema().
::spark::connect::DataType to_spark_schema(const components::types::complex_logical_type& schema);

// Convenience overload: wraps a flat column list into a STRUCT schema. This is
// the common shape for AnalyzePlan(schema) responses produced from a query's
// output column vector.
::spark::connect::DataType to_spark_schema(const std::pmr::vector<components::types::complex_logical_type>& columns);

}  // namespace frontend::spark
