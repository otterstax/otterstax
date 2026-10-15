// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "runtime_config.hpp"
#include "utility/tracy_profiler.hpp"

#include <array>
#include <optional>
#include <string_view>

namespace frontend::spark {
    namespace {
        using entry = std::pair<std::string_view, std::string_view>;
        constexpr std::array<entry, 4> settings{{
            {"spark.sql.execution.pyspark.binaryAsBytes", "true"},
            {"spark.sql.session.timeZone", "UTC"},
            {"spark.sql.execution.pandas.structHandlingMode", "LEGACY"},
            {"spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "false"},
        }};

        std::optional<std::string_view> lookup(std::string_view key) {
            for (const auto& [name, value] : settings) {
                if (name == key) return value;
            }
            return std::nullopt;
        }
    }

    grpc::Status runtime_config(const ::spark::connect::ConfigRequest& request,
                                ::spark::connect::ConfigResponse& response) {
        OTX_ZONE_N("spark::runtime_config");
        namespace sc = ::spark::connect;
        response.Clear();
        response.set_session_id(request.session_id());
        const auto& op = request.operation();
        auto append = [&](std::string_view key, std::optional<std::string_view> value) {
            auto* pair = response.add_pairs();
            pair->set_key(std::string{key});
            if (value) pair->set_value(std::string{*value});
        };
        switch (op.op_type_case()) {
            case sc::ConfigRequest::Operation::kSet:
                if (op.set().pairs().empty()) break;
                if (op.set().silent()) {
                    // Spark's best-effort SET explicitly requests per-key
                    // warnings instead of an RPC failure. Never echo values:
                    // configuration strings can contain credentials.
                    for (const auto& pair : op.set().pairs()) {
                        response.add_warnings("Runtime configuration was not changed: " + pair.key() +
                                              " is not modifiable");
                    }
                    break;
                }
                return {grpc::StatusCode::UNIMPLEMENTED, "Runtime configuration changes are not supported"};
            case sc::ConfigRequest::Operation::kUnset:
                if (op.unset().keys().empty()) break;
                return {grpc::StatusCode::UNIMPLEMENTED, "Runtime configuration changes are not supported"};
            case sc::ConfigRequest::Operation::kGet:
                for (const auto& key : op.get().keys()) {
                    const auto value = lookup(key);
                    if (!value) {
                        response.clear_pairs();
                        return {grpc::StatusCode::NOT_FOUND, "Unknown runtime configuration: " + key};
                    }
                    append(key, value);
                }
                break;
            case sc::ConfigRequest::Operation::kGetOption:
                for (const auto& key : op.get_option().keys()) append(key, lookup(key));
                break;
            case sc::ConfigRequest::Operation::kGetWithDefault:
                for (const auto& pair : op.get_with_default().pairs()) {
                    auto value = lookup(pair.key());
                    if (!value && pair.has_value()) value = pair.value();
                    append(pair.key(), value);
                }
                break;
            case sc::ConfigRequest::Operation::kGetAll: {
                const std::string_view prefix = op.get_all().prefix();
                for (const auto& [key, value] : settings) {
                    if (key.starts_with(prefix)) append(key.substr(prefix.size()), value);
                }
                break;
            }
            case sc::ConfigRequest::Operation::kIsModifiable:
                for (const auto& key : op.is_modifiable().keys()) append(key, "false");
                break;
            default:
                return {grpc::StatusCode::INVALID_ARGUMENT, "Config requires an operation"};
        }
        return grpc::Status::OK;
    }
}
