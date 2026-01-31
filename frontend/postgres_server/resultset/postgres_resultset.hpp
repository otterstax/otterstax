// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/resultset_utils.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "field_description.hpp"

#include <iostream>
#include <optional>
#include <string>

namespace frontend::postgres {
    class postgres_resultset {
    public:
        postgres_resultset(packet_writer& writer, bool datarow_only = false);

        void add_encoding(std::vector<result_encoding> format);
        void add_chunk_columns(const components::vector::data_chunk_t& chunk,
                               result_encoding encoding = result_encoding::TEXT);

        void add_row(const components::vector::data_chunk_t& chunk, size_t row_index);

        [[nodiscard]] static std::vector<std::vector<uint8_t>> build_packets(postgres_resultset&& resultset);

    private:
        std::vector<result_encoding> format_;
        std::vector<field_description> field_desc_;
        std::vector<std::vector<uint8_t>> encoded_rows_;
        std::reference_wrapper<packet_writer> writer_;
        bool datarow_only_;
    };
} // namespace frontend::postgres
