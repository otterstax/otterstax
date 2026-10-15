// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "access.hpp"

#include <boost/json.hpp>
#include <curl/curl.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <utility>

namespace otterstax::security {
    namespace {
        using curl_handle = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
        using curl_text = std::unique_ptr<char, decltype(&curl_free)>;

        void initialize_curl() {
            struct runtime {
                runtime() {
                    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
                        throw std::runtime_error("Cannot initialize the HTTPS client");
                    }
                }
                ~runtime() { curl_global_cleanup(); }
            };
            static const runtime initialized;
        }

        bool valid_issuer(const std::string& issuer) {
            if (issuer.empty() || issuer.back() == '/' ||
                std::any_of(issuer.begin(), issuer.end(), [](unsigned char c) { return c <= 32 || c == 127; })) {
                return false;
            }
            std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> url(curl_url(), curl_url_cleanup);
            if (!url || curl_url_set(url.get(), CURLUPART_URL, issuer.c_str(), 0) != CURLUE_OK) return false;
            auto part = [&](CURLUPart name) {
                char* value = nullptr;
                const auto code = curl_url_get(url.get(), name, &value, 0);
                curl_text owned(value, curl_free);
                return code == CURLUE_OK && value ? std::string{value} : std::string{};
            };
            return part(CURLUPART_SCHEME) == "https" && !part(CURLUPART_HOST).empty() &&
                   part(CURLUPART_USER).empty() && part(CURLUPART_PASSWORD).empty() &&
                   part(CURLUPART_QUERY).empty() && part(CURLUPART_FRAGMENT).empty();
        }

        struct response_buffer {
            std::string body;
            std::size_t limit;
        };

        std::size_t receive_body(char* data, std::size_t size, std::size_t count, void* context) noexcept {
            auto& output = *static_cast<response_buffer*>(context);
            if (size != 0 && count > std::numeric_limits<std::size_t>::max() / size) return 0;
            const auto bytes = size * count;
            if (bytes > output.limit - output.body.size()) return 0;
            try {
                output.body.append(data, bytes);
                return bytes;
            } catch (...) {
                return 0;
            }
        }

        std::string form_escape(CURL* curl, const std::string_view value) {
            if (value.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
                throw std::length_error("Authentication field exceeds the HTTP client limit");
            }
            curl_text escaped(curl_easy_escape(curl, value.data(), static_cast<int>(value.size())), curl_free);
            if (!escaped) throw std::bad_alloc();
            return escaped.get();
        }

        bool text_matches(const boost::json::object& object, const char* key, std::string_view expected) {
            const auto* value = object.if_contains(key);
            return value && value->is_string() && value->as_string() == expected;
        }

        bool integer_claim(const boost::json::object& object, const char* key, int64_t& result) {
            const auto* value = object.if_contains(key);
            if (!value || !value->is_int64()) return false;
            result = value->as_int64();
            return result >= 0;
        }
    }

    bool valid_bearer_token(std::string_view token, std::size_t max_bytes) {
        if (token.empty() || token.size() > max_bytes) return false;
        bool padding = false;
        for (unsigned char c : token) {
            if (c == '=') {
                padding = true;
            } else if (padding || !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                    (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
                                    c == '~' || c == '+' || c == '/')) {
                return false;
            }
        }
        return token.front() != '=';
    }

    token_result validate_introspection(std::string_view response,
                                        const keycloak_config& config,
                                        std::chrono::system_clock::time_point now) {
        if (response.size() > config.max_response_bytes) return {};
        boost::system::error_code error;
        const auto parsed = boost::json::parse(response, error);
        if (error || !parsed.is_object()) return {};
        const auto& object = parsed.as_object();
        const auto* active = object.if_contains("active");
        if (!active || !active->is_bool() || !active->as_bool() ||
            !text_matches(object, "iss", config.issuer) || !text_matches(object, "token_type", "Bearer")) {
            return {};
        }
        const auto* subject = object.if_contains("sub");
        if (!subject || !subject->is_string() || subject->as_string().empty()) return {};
        const auto* audience = object.if_contains("aud");
        if (!audience) return {};
        if (audience->is_array() && std::any_of(audience->as_array().begin(), audience->as_array().end(),
                                               [](const auto& value) { return !value.is_string(); })) return {};
        const bool matches = audience->is_string() ? audience->as_string() == config.client_id
            : audience->is_array() && std::any_of(audience->as_array().begin(), audience->as_array().end(),
                [&](const auto& value) { return value.is_string() && value.as_string() == config.client_id; });
        if (!matches) return {};
        const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        int64_t expiration = 0;
        if (!integer_claim(object, "exp", expiration) || expiration <= now_seconds) return {};
        int64_t not_before = 0;
        if (object.if_contains("nbf") && (!integer_claim(object, "nbf", not_before) || not_before > now_seconds)) return {};
        // Bound the conversion to the clock's representable range before constructing a time point.
        const auto latest = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::time_point::max().time_since_epoch()).count();
        if (expiration > latest) return {};
        return {token_status::accepted, std::string{subject->as_string()},
                std::chrono::system_clock::time_point{std::chrono::seconds{expiration}}};
    }

    keycloak_verifier::keycloak_verifier(keycloak_config config) : config_(std::move(config)) {
        initialize_curl();
        if ((curl_version_info(CURLVERSION_NOW)->features & CURL_VERSION_ASYNCHDNS) == 0) {
            throw std::invalid_argument("Keycloak requires libcurl with asynchronous DNS for bounded request timeouts");
        }
        if (!valid_issuer(config_.issuer) || config_.client_id.empty() || config_.client_secret_env.empty() ||
            config_.request_timeout.count() <= 0 || config_.request_timeout.count() > std::numeric_limits<long>::max() ||
            config_.max_token_bytes == 0 || config_.max_token_bytes > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            config_.max_response_bytes == 0 || config_.max_in_flight == 0) {
            throw std::invalid_argument("Invalid Keycloak authentication configuration");
        }
        const char* secret = std::getenv(config_.client_secret_env.c_str());
        if (!secret || !*secret) throw std::invalid_argument("Keycloak client secret environment variable is empty or unset");
        client_secret_ = secret;
        endpoint_ = config_.issuer + "/protocol/openid-connect/token/introspect";
    }

    token_result keycloak_verifier::verify(std::string_view token) const {
        if (!valid_bearer_token(token, config_.max_token_bytes)) return {};
        auto count = in_flight_.load(std::memory_order_relaxed);
        do {
            if (count >= config_.max_in_flight) return {token_status::unavailable, {}, {}};
        } while (!in_flight_.compare_exchange_weak(count, count + 1, std::memory_order_acquire));
        struct release_slot {
            std::atomic<std::size_t>& count;
            ~release_slot() { count.fetch_sub(1, std::memory_order_release); }
        } release{in_flight_};

        try {
            curl_handle curl(curl_easy_init(), curl_easy_cleanup);
            if (!curl) return {token_status::unavailable, {}, {}};
            const std::string body = "token=" + form_escape(curl.get(), token) +
                "&token_type_hint=access_token&client_id=" + form_escape(curl.get(), config_.client_id) +
                "&client_secret=" + form_escape(curl.get(), client_secret_);
            response_buffer response{{}, config_.max_response_bytes};
            const auto set = [&](CURLoption option, auto value) {
                if (curl_easy_setopt(curl.get(), option, value) != CURLE_OK) {
                    throw std::runtime_error("Cannot configure the HTTPS client");
                }
            };
            set(CURLOPT_URL, endpoint_.c_str());
            set(CURLOPT_PROTOCOLS_STR, "https");
            set(CURLOPT_REDIR_PROTOCOLS_STR, "https");
            set(CURLOPT_FOLLOWLOCATION, 0L);
            set(CURLOPT_SSL_VERIFYPEER, 1L);
            set(CURLOPT_SSL_VERIFYHOST, 2L);
            set(CURLOPT_SSLVERSION, static_cast<long>(CURL_SSLVERSION_TLSv1_2));
            set(CURLOPT_NOSIGNAL, 1L);
            set(CURLOPT_TIMEOUT_MS, static_cast<long>(config_.request_timeout.count()));
            set(CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(config_.request_timeout.count()));
            set(CURLOPT_POSTFIELDS, body.c_str());
            set(CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
            set(CURLOPT_WRITEFUNCTION, &receive_body);
            set(CURLOPT_WRITEDATA, &response);
            if (!config_.ca_file.empty()) set(CURLOPT_CAINFO, config_.ca_file.c_str());
            if (curl_easy_perform(curl.get()) != CURLE_OK) return {token_status::unavailable, {}, {}};
            long http_status = 0;
            if (curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &http_status) != CURLE_OK || http_status != 200) {
                return {token_status::unavailable, {}, {}};
            }
            return validate_introspection(response.body, config_, std::chrono::system_clock::now());
        } catch (...) {
            return {token_status::unavailable, {}, {}};
        }
    }
} // namespace otterstax::security
