// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <memory>
#include <type_traits>
#include <utility>

namespace otterstax {

    template<typename Signature>
    class function_ref_t;

    // A non-owning, non-allocating reference to a callable: the callable's address
    // plus a trampoline that calls it through its real type. This is how a handler
    // crosses a virtual boundary (IConnector::runQuery in the three SQL connectors)
    // without a type-erased owning wrapper.
    //
    // Lifetime: the reference does not keep the callable alive. The callable must
    // outlive every call made through the reference; for a connector query that
    // means until the awaitable the reference was handed to has completed.
    // otterstax::run_with_owned_handler (wait_barrier.hpp) is where executeQuery
    // binds a query's handler, and it binds the copy held in its own coroutine
    // frame.
    //
    // Only a named, non-const object binds: a temporary would be gone before the
    // call, so an rvalue argument does not compile.
    template<typename R, typename... Args>
    class function_ref_t<R(Args...)> {
    public:
        template<typename Callable>
            requires(std::is_object_v<Callable> && !std::is_const_v<Callable> &&
                     !std::is_same_v<Callable, function_ref_t> && std::is_invocable_r_v<R, Callable&, Args...>)
        explicit function_ref_t(Callable& callable) noexcept
            : callable_(std::addressof(callable))
            , invoke_(&invoke<Callable>) {}

        R operator()(Args... args) const { return invoke_(callable_, std::forward<Args>(args)...); }

    private:
        template<typename Callable>
        static R invoke(void* callable, Args... args) {
            return (*static_cast<Callable*>(callable))(std::forward<Args>(args)...);
        }

        void* callable_;
        R (*invoke_)(void*, Args...);
    };

} // namespace otterstax
