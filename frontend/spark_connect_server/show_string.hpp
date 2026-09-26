// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// df.show() for the Spark Connect frontend. PySpark asks for it with a
// ShowString relation and prints the one string that comes back: a result of
// one row whose one STRING column is named "show_string" (a 3.5 client reads it
// by that name, a 4.x client by position). Spark's own server builds the string
// in Dataset.showString; this is that rendering as Spark 4.2.0 has it, applied
// to the rows the Scheduler returns for the ShowString's input:
//  - a cell reads as Spark's ToPrettyString spells it: NULL, true / false,
//    integers in decimal, FLOAT / DOUBLE as Java's toString (the shortest digits
//    that read back as the value; plain with at least one fractional digit for
//    1e-3 <= |v| < 1e7, else d.dddE<n>), DECIMAL as its plain digits, DATE /
//    TIME / TIMESTAMP in the session zone (UTC), text as it is; the header row
//    is the column names;
//  - control characters are escaped (\n \r \t \f \b \v \a); with truncate > 0 a
//    cell of more than truncate characters keeps its first truncate - 3 and
//    "...", or its first truncate when truncate < 4 — the header is never cut;
//  - a column is at least 3 wide and a full-width character counts twice;
//    cells are right-aligned when truncate > 0, left-aligned otherwise, and the
//    table is framed by +---+ lines; vertical mode prints one -RECORD n block
//    per row instead;
//  - num_rows is clamped to [0, 2147483631]; the row after the shown ones tells
//    whether the footer "only showing top N row(s)" follows, and a vertical
//    rendering without rows ends "(0 rows)" — neither is followed by a newline
//    (Spark 3.5 ended both with one).
// A cell of any other type is a conversion_failure naming its column. Nothing
// here throws.

#include "scheduler/session_data.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <spark/connect/base.pb.h>
#include <spark/connect/relations.pb.h>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

namespace frontend::spark {

    // The one column a ShowString is answered with.
    inline constexpr std::string_view show_string_column = "show_string";

    // The plan that fetches a ShowString's input: the input under a limit of one
    // row more than is shown.
    ::spark::connect::Plan show_string_input_plan(const ::spark::connect::ShowString& show);

    // The rendering of a result's rows. `schema` and `chunks` are the result's
    // (a session_payload's): the columns are the chunks' — the schema's fields
    // when there is no chunk — named by the schema's fields when it is a STRUCT
    // of as many, else by their own alias, else col<i>, as the Arrow batch names
    // them. The message of an error lives on `resource`.
    core::result_wrapper_t<std::pmr::string>
    format_show_string(const components::types::complex_logical_type& schema,
                       const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                       int32_t num_rows,
                       int32_t truncate,
                       bool vertical,
                       std::pmr::memory_resource* resource);

    // The answer to a ShowString: `text` as the one row of one STRING column
    // named show_string, described by a STRUCT of that column.
    session_payload make_show_string_payload(std::string_view text, std::pmr::memory_resource* resource);

} // namespace frontend::spark
