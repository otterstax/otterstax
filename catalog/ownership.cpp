// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ownership.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <string>
#include <string_view>

namespace otterstax::catalog {
    static_assert(std::string_view{kafka::KAFKA_DATABASE_NAME} == federation::local_database,
                  "the Kafka objects live in the local database");

    core::error_t
    check_write_ownership(std::pmr::memory_resource* resource, const qualified_name_t& target, bool drops_schema) {
        OTX_ZONE_N("catalog::check_write_ownership");
        const auto database = federation::fold_identifier(target.database.t);
        std::pmr::string what{resource};
        if (target.connection.t.empty() && database == federation::fold_identifier(federation::metadata_database)) {
            what.append("'");
            what.append(target.to_string());
            what.append("' is owned by OtterStax (");
            what.append(federation::metadata_database);
            what.append(")");
        } else if (drops_schema && target.connection.t.empty() &&
                   (database.empty() || database == kafka::KAFKA_DATABASE_NAME) &&
                   federation::fold_identifier(target.schema.t) == kafka::KAFKA_SCHEMA_NAME) {
            what.append("schema '");
            what.append(kafka::KAFKA_DATABASE_NAME);
            what.append(".");
            what.append(kafka::KAFKA_SCHEMA_NAME);
            what.append("' is owned by OtterStax (the Kafka objects); DROP SOURCE / DROP STREAM drop them");
        } else {
            return core::error_t::no_error();
        }
        return core::error_t(core::error_code_t::invalid_parameter, std::move(what));
    }
} // namespace otterstax::catalog
