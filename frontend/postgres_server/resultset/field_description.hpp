// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/resultset_utils.hpp"
#include "../packet/packet_writer.hpp"
#include "../postgres_defs/field_type.hpp"
#include <string>

namespace frontend::postgres {
    enum class n;

    struct field_description {
        field_description() = default;
        field_description(std::string col_name, field_type type);

        std::string name;
        oid_t table_oid = 0;
        int16_t column_attr_number = 0;
        oid_t type_oid = 0;
        int16_t type_size = -1;
        int32_t type_modifier = -1;

        [[nodiscard]] int32_t field_size() const;
        static void write_field(field_description&& desc, packet_writer& writer, result_encoding encoding);

    private:
        void init_type(field_type type);
    };

} // namespace frontend::postgres
