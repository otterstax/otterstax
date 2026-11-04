// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <initializer_list>
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

// Every key of the mapping `node` is one of `known`: a key the reader does not know — a typo, a key
// no longer read — is an invalid_parameter naming the key and `place` (`unknown key "pool_sise" in
// connections.mysql[0]`), so it never silently leaves a default in force. A key that is not a
// scalar is refused through yaml_scalar.
inline core::error_t require_known_keys(const YAML::Node& node,
                                        std::string_view place,
                                        std::initializer_list<std::string_view> known,
                                        std::pmr::memory_resource* resource) {
    for (const auto& item : node) {
        auto key = yaml_scalar<std::string>(item.first, place, resource);
        if (key.has_error()) {
            return key.error();
        }
        if (std::find(known.begin(), known.end(), key.value()) == known.end()) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{("unknown key \"" + key.value() + "\" in " + std::string(place)).c_str(),
                                                  resource});
        }
    }
    return core::error_t::no_error();
}

}  // namespace config
