// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once

#include <boost/asio/ip/address.hpp>
#include <algorithm>
#include <charconv>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace otterstax::security {

// No wildcards, URL syntax, implicit ports or resolver-specific numeric aliases.
// DNS names are administrator-controlled names, not suffix patterns.
inline std::optional<std::string> canonical_destination(std::string_view endpoint) {
    if (endpoint.empty()) return std::nullopt;
    std::string_view host, port;
    if (endpoint.front() == '[') {
        const auto close = endpoint.find(']');
        if (close == std::string_view::npos || close + 1 >= endpoint.size() || endpoint[close + 1] != ':')
            return std::nullopt;
        host = endpoint.substr(1, close - 1);
        port = endpoint.substr(close + 2);
    } else {
        const auto colon = endpoint.find(':');
        if (colon == std::string_view::npos || endpoint.find(':', colon + 1) != std::string_view::npos)
            return std::nullopt;
        host = endpoint.substr(0, colon);
        port = endpoint.substr(colon + 1);
    }
    unsigned number{};
    const auto parsed = std::from_chars(port.data(), port.data() + port.size(), number);
    if (port.empty() || parsed.ec != std::errc{} || parsed.ptr != port.data() + port.size() || number == 0 || number > 65535)
        return std::nullopt;
    if (host.empty() || host.find('%') != std::string_view::npos) return std::nullopt;
    boost::system::error_code error;
    auto address = boost::asio::ip::make_address(std::string(host), error);
    std::string normalized;
    if (!error) {
        if (address.is_v6()) {
            if (endpoint.front() != '[') return std::nullopt;
            normalized = '[' + address.to_string() + ']';
        } else {
            if (endpoint.front() == '[') return std::nullopt;
            normalized = address.to_string();
        }
    } else {
        if (endpoint.front() == '[' || host.size() > 253) return std::nullopt;
        normalized = std::string(host);
        for (auto& c : normalized) {
            if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.'))
                return std::nullopt;
        }
        // Refuse inet_aton-style shorthand, octal and hexadecimal IPv4 spellings.
        const auto last_dot = normalized.rfind('.');
        const auto last = std::string_view(normalized).substr(last_dot == std::string::npos ? 0 : last_dot + 1);
        const auto decimal = [](char c) { return c >= '0' && c <= '9'; };
        const auto hexadecimal = [&](char c) { return decimal(c) || (c >= 'a' && c <= 'f'); };
        if (last.empty() || std::all_of(last.begin(), last.end(), decimal) ||
            (last.starts_with("0x") && std::all_of(last.begin() + 2, last.end(), hexadecimal))) return std::nullopt;
        std::size_t begin = 0;
        while (begin < normalized.size()) {
            const auto end = normalized.find('.', begin);
            const auto label = std::string_view(normalized).substr(begin, end == std::string::npos ? end : end - begin);
            if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') return std::nullopt;
            if (end == std::string::npos) break;
            begin = end + 1;
        }
    }
    return normalized + ':' + std::to_string(number);
}

class network_destinations final {
public:
    network_destinations() = default; // Empty means deny every destination.
    explicit network_destinations(const std::vector<std::string>& endpoints) {
        for (const auto& endpoint : endpoints) {
            auto value = canonical_destination(endpoint);
            if (!value) throw std::invalid_argument("destination must be an exact hostname:port or [IPv6]:port");
            endpoints_.insert(std::move(*value));
        }
    }
    bool allows(std::string_view endpoint) const {
        auto value = canonical_destination(endpoint);
        return value && endpoints_.contains(*value);
    }
    bool allows_list(std::string_view endpoints) const {
        if (endpoints.empty()) return false;
        for (;;) {
            const auto comma = endpoints.find(',');
            if (!allows(endpoints.substr(0, comma))) return false;
            if (comma == std::string_view::npos) return true;
            endpoints.remove_prefix(comma + 1);
        }
    }
private:
    std::unordered_set<std::string> endpoints_;
};

} // namespace otterstax::security
