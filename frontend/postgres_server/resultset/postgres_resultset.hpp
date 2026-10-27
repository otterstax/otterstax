// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/resultset_utils.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "field_description.hpp"

#include <iostream>
#include <memory_resource>
#include <optional>
#include <string>

namespace frontend::postgres {
    // True when an executed chunk goes out under exactly the wire shape
    // `described` was announced as: the same number of columns, each under the
    // same type OID. It is what an Execute checks before it streams rows under
    // a RowDescription the client was given before Bind.
    //
    // Names are not compared. A DataRow is decoded by position and type, and an
    // executed column legitimately carries a name the prepared schema had none
    // for — a set operation's column, a column of a SELECT over a VIEW — so a
    // name is no part of the shape the client decodes by.
    bool same_wire_shape(const std::pmr::vector<components::types::complex_logical_type>& described,
                         const components::vector::data_chunk_t& executed);

    class postgres_resultset {
    public:
        postgres_resultset(packet_writer& writer, bool datarow_only = false);

        void add_encoding(std::vector<result_encoding> format);
        void add_chunk_columns(const components::vector::data_chunk_t& chunk,
                               result_encoding encoding = result_encoding::TEXT);

        void add_row(const components::vector::data_chunk_t& chunk, size_t row_index);

        // The RowDescription (unless datarow_only) and every DataRow, back to back in wire
        // order: what one write sends, with whatever the caller appends behind it.
        [[nodiscard]] static std::vector<uint8_t> build_packets(postgres_resultset&& resultset);

    private:
        std::vector<result_encoding> format_;
        std::vector<field_description> field_desc_;
        // The RowDescription (written in front of the first row, once the formats are set) and
        // every DataRow, framed back to back: one buffer for the whole resultset.
        std::vector<uint8_t> packets_;
        bool head_written_ = false;
        void write_head();
        std::reference_wrapper<packet_writer> writer_;
        bool datarow_only_;
        // encode_to_text's buffer, reused for every cell. On the default resource until the
        // frontends take one.
        std::pmr::string text_scratch_{std::pmr::get_default_resource()};
    };
} // namespace frontend::postgres
