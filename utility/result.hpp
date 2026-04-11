// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Generic result wrapper for actor pipeline error handling.
// Based on core::result_wrapper_t from otterbrix/unified-error-handling.
// When otterbrix merges that branch, this can be replaced with #include <core/result_wrapper.hpp>.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace otterstax {

    // basic_error<ErrorCode, ErrorTag>
    // Template parameters allow each module to define its own error enums.
    // Assumes default-constructed ErrorCode means "no error".
    template<typename ErrorCode, typename ErrorTag>
    struct basic_error {
        ErrorCode code;
        ErrorTag tag;
        std::string what;

        basic_error()
            : code{}
            , tag{}
            , what{} {}

        basic_error(ErrorCode code, ErrorTag tag, std::string what)
            : code(code)
            , tag(tag)
            , what(std::move(what)) {}

        basic_error(ErrorCode code, std::string what)
            : code(code)
            , tag{}
            , what(std::move(what)) {}

        bool contains_error() const noexcept { return code != ErrorCode{}; }
        explicit operator bool() const noexcept { return contains_error(); }
        static basic_error no_error() { return basic_error{}; }
    };

    // result_t<Error, T>: holds either an Error or a value of type T.
    //
    // Error first — allows fixing Error via alias:
    //   template<typename T> using result = result_t<pipeline_error, T>;
    //
    // Features:
    // - Implicit construction from T (success) or Error (failure)
    // - assert on value() when error is present
    // - convert_error<U>() to propagate errors across types
    // - Non-default-constructible T is auto-wrapped in unique_ptr
    template<typename Error, typename T>
    class result_t {
    private:
        static constexpr bool trivial_store = std::is_default_constructible_v<T>;
        using store_t = std::conditional_t<trivial_store, T, std::unique_ptr<T>>;

    public:
        // --- Success: implicit from T (trivial store) ---
        result_t(T&& val)
            requires(trivial_store && !std::is_same_v<std::decay_t<T>, Error>)
            : value_(std::move(val))
            , error_(Error::no_error()) {}

        result_t(const T& val)
            requires(trivial_store && std::is_copy_constructible_v<T> &&
                     !std::is_same_v<std::decay_t<T>, Error>)
            : value_(val)
            , error_(Error::no_error()) {}

        // --- Success: implicit from unique_ptr<T> (non-trivial store) ---
        result_t(std::unique_ptr<T>&& ptr)
            requires(!trivial_store)
            : value_(std::move(ptr))
            , error_(Error::no_error()) {}

        // --- Error: implicit from Error ---
        result_t(const Error& error)
            : error_(error) {}
        result_t(Error&& error)
            : error_(std::move(error)) {}

        // --- Move only ---
        result_t(result_t&&) noexcept = default;
        result_t& operator=(result_t&&) noexcept = default;
        result_t(const result_t&) = delete;
        result_t& operator=(const result_t&) = delete;

        // --- Queries ---
        bool has_error() const noexcept { return error_.contains_error(); }
        bool is_success() const noexcept { return !has_error(); }
        explicit operator bool() const noexcept { return is_success(); }

        // --- Error access ---
        const Error& error() const noexcept { return error_; }

        // --- Value access (assert in debug if error) ---
        const T& value() const noexcept {
            assert(!has_error() && "accessing value on errored result");
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }

        T& value() noexcept {
            assert(!has_error() && "accessing value on errored result");
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }

        // --- Move value out ---
        T take_value() noexcept {
            assert(!has_error() && "taking value from errored result");
            if constexpr (trivial_store) {
                return std::move(value_);
            } else {
                auto ptr = std::move(value_);
                return std::move(*ptr);
            }
        }

        // --- Move the store (unique_ptr) out directly ---
        store_t take_store() noexcept {
            assert(!has_error() && "taking store from errored result");
            return std::move(value_);
        }

        // --- Propagate error to a result with different T ---
        template<typename U>
        result_t<Error, U> convert_error() const& {
            assert(has_error() && "convert_error on success result");
            return result_t<Error, U>(error_);
        }

        template<typename U>
        result_t<Error, U> convert_error() && {
            assert(has_error() && "convert_error on success result");
            return result_t<Error, U>(std::move(error_));
        }

    private:
        store_t value_;
        Error error_;
    };

} // namespace otterstax
