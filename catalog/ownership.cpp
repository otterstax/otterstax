// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ownership.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/provider.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <optional>
#include <string>

namespace otterstax::catalog {

    namespace {

        // The owner of `name`, or nullopt when nobody owns it.
        std::optional<std::string> database_owner(std::string_view name) {
            OTX_ZONE_N("catalog::database_owner");
            const auto folded = federation::fold_identifier(name);
            if (folded == federation::fold_identifier(otterstax::kafka::KAFKA_DATABASE_NAME)) {
                return std::string{otterstax::kafka::KAFKA_DATABASE_NAME};
            }
            if (folded == federation::fold_identifier(federation::metadata_database)) {
                return std::string{federation::metadata_database};
            }
            if (const auto* server = federation::registry().find(name)) {
                return server->uid;
            }
            return std::nullopt;
        }

    } // namespace

    core::error_t check_database_ownership(std::pmr::memory_resource* resource, std::string_view name) {
        OTX_ZONE_N("catalog::check_database_ownership");
        auto owner = database_owner(name);
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

} // namespace otterstax::catalog
