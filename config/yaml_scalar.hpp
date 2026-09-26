// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <yaml-cpp/yaml.h>

#include <memory_resource>
#include <string>
#include <string_view>

namespace config {

// Reads a present config value as T. This is the only place a yaml-cpp
// conversion failure is turned into a value: a node that cannot be read as T
// (a map or sequence where a string is expected, text where an integer is
// expected) is an invalid_parameter naming `field`; no exception leaves here.
template <typename T>
core::result_wrapper_t<T> yaml_scalar(const YAML::Node& node, std::string_view field, std::pmr::memory_resource* resource) {
    try {
        return node.as<T>();
    } catch (const YAML::Exception& e) {
        return core::error_t(core::error_code_t::invalid_parameter,
                             std::pmr::string{("field '" + std::string(field) + "': " + e.what()).c_str(), resource});
    }
}

}  // namespace config
