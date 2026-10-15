// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once
#include "security/network_destinations.hpp"
#include <optional>
#include <string>
#include <string_view>

namespace otterstax {
inline std::optional<std::string> validate_s3_transport(std::string_view scheme, std::string_view endpoint,
                                                       std::string_view ca_file) {
    if (scheme != "https" && scheme != "http") return "S3 scheme must be https or http";
    if (endpoint.empty()) return "S3 endpoint must be explicitly configured, including for AWS";
    if (endpoint.find_first_of("/?#@ \t\r\n") != std::string_view::npos || endpoint.find('\0') != std::string_view::npos) {
        return "S3 endpoint must be a hostname with optional port, without a URL scheme, path or credentials";
    }
    if (ca_file.find('\0') != std::string_view::npos) return "S3 ca_file must not contain NUL";
    if (scheme == "http" && !ca_file.empty()) return "S3 ca_file requires https";
    std::string destination(endpoint);
    if (endpoint.back() == ']' || endpoint.find(':') == std::string_view::npos)
        destination += scheme == "https" ? ":443" : ":80";
    if (!security::canonical_destination(destination)) return "S3 endpoint must contain a valid hostname or IP address and port";
    return std::nullopt;
}

inline bool valid_s3_bucket_path(std::string_view path) {
    if (path.find('\0') != std::string_view::npos) return false;
    const auto bucket = path.substr(0, path.find('/'));
    if (bucket.size() < 3 || bucket.size() > 63) return false;
    const auto alphanumeric = [](char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    if (!alphanumeric(bucket.front()) || !alphanumeric(bucket.back())) return false;
    for (const auto c : bucket) if (!alphanumeric(c) && c != '-' && c != '.') return false;
    return bucket.find("..") == std::string_view::npos;
}
}
