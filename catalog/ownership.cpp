// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ownership.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <optional>

namespace otterstax::catalog {

    namespace {

        // The owner of `name`, or nullopt when nobody owns it.
        std::optional<std::string> database_owner(std::string_view name, std::span<const std::string> connections) {
            const auto folded = federation::fold_identifier(name);
            if (folded == federation::fold_identifier(federation::metadata_database)) {
                return std::string{federation::metadata_database};
            }
            for (const auto& connection : connections) {
                if (federation::fold_identifier(connection) == folded) {
                    return connection;
                }
            }
            return std::nullopt;
        }

    } // namespace

    core::error_t check_database_ownership(std::pmr::memory_resource* resource,
                                           std::string_view name,
                                           std::span<const std::string> connections) {
        OTX_ZONE_N("catalog::check_database_ownership");
        auto owner = database_owner(name, connections);
        if (!owner) {
            return core::error_t::no_error();
        }
        std::pmr::string what{resource};
        what.append("database '");
        what.append(name);
        what.append("' is owned by connection '");
        what.append(*owner);
        what.push_back('\'');
        return core::error_t(core::error_code_t::invalid_parameter, std::move(what));
    }

    core::error_t check_write_ownership(std::pmr::memory_resource* resource,
                                        const qualified_name_t& target,
                                        bool drops_schema) {
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
