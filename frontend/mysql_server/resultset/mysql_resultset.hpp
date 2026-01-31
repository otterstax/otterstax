// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/resultset_utils.hpp"
#include "../mysql_defs/column_flags.hpp"
#include "../mysql_defs/field_type.hpp"
#include "../packet/packet_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "column_definition_41.hpp"

#include <components/document/document.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <iostream>
#include <optional>
#include <string>

namespace frontend::mysql {
    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_com_query_response_text_resultset.html
    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_binary_resultset.html
    class mysql_resultset {
    public:
        mysql_resultset(packet_writer& writer,
                        result_encoding encoding,
                        std::string database = "",
                        std::string table = "");

        void add_chunk_columns(const components::vector::data_chunk_t& chunk);

        void add_row(const components::vector::data_chunk_t& chunk, size_t row_index);

        [[nodiscard]] static std::vector<std::vector<uint8_t>> build_packets(mysql_resultset&& resultset,
                                                                             uint8_t& sequence_id);

    private:
        size_t compute_text_row_size(const components::vector::data_chunk_t& chunk, size_t row_index) const;
        void encode_row_text(const components::vector::data_chunk_t& chunk, size_t row_index);

        size_t compute_binary_row_size(const components::vector::data_chunk_t& chunk, size_t row_index) const;
        void encode_row_binary(const components::vector::data_chunk_t& chunk, size_t row_index);

        std::vector<column_definition_41> column_defs_;
        std::vector<std::vector<uint8_t>> encoded_rows_;
        std::reference_wrapper<packet_writer> writer_;
        std::string database_;
        std::string table_;
        result_encoding encoding_;
    };

} // namespace frontend::mysql
