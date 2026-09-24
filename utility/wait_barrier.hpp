// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include "utility/asio_error.hpp"
#include "utility/function_ref.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <exception>
#include <future>
#include <memory_resource>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace otterstax {

    // The connector handler's return type selects the shape of the marshaled
    // outcome:
    //   asio_error_t  — the query has no payload, the outcome IS the error
    //                   -> core::error_t
    //   anything else — a value that may fail
    //                   -> core::result_wrapper_t<R>
    // Spelling the asio_error_t case out keeps result_wrapper_t<asio_error_t> off
    // the table: it would carry the same failure twice, behind two different
    // "did it fail?" predicates.
    template<typename R>
    using query_result_t =
        std::conditional_t<std::is_same_v<R, asio_error_t>, core::error_t, core::result_wrapper_t<R>>;

    template<typename R>
    using query_future_t = std::future<query_result_t<R>>;

    // Shapes a handler's return value into the marshaled outcome: an asio_error_t
    // handler yields its bare core::error_t, any other handler yields its value
    // wrapped as a success.
    template<typename R>
    query_result_t<R> as_query_result(R&& value) {
        if constexpr (std::is_same_v<R, asio_error_t>) {
            return std::move(value).release();
        } else {
            return query_result_t<R>{std::move(value)};
        }
    }

    namespace detail {

        // Owns the consumer's std::promise for exactly one outcome.
        //
        // The coroutine frame holding it can be destroyed before the body ever ran:
        // the io_context is torn down (or never run) while the launch is still
        // queued. A bare std::promise then stores broken_promise, which future.get()
        // rethrows on the consumer thread — an exception crossing the io -> consumer
        // boundary this file exists to keep exception-free. The destructor settles
        // the promise with io_error instead, so the consumer always receives a value.
        template<typename R>
        class outcome_promise_t {
        public:
            outcome_promise_t(std::promise<query_result_t<R>> promise, std::pmr::memory_resource* resource) noexcept
                : promise_(std::move(promise))
                , resource_(resource)
                , pending_(true) {}

            outcome_promise_t(outcome_promise_t&& other) noexcept
                : promise_(std::move(other.promise_))
                , resource_(other.resource_)
                , pending_(std::exchange(other.pending_, false)) {}

            outcome_promise_t(const outcome_promise_t&) = delete;
            outcome_promise_t& operator=(const outcome_promise_t&) = delete;
            outcome_promise_t& operator=(outcome_promise_t&&) = delete;

            ~outcome_promise_t() {
                if (pending_) {
                    promise_.set_value(query_result_t<R>{
                        core::error_t(core::error_code_t::io_error,
                                      std::pmr::string{"connector io_context was shut down before the query ran",
                                                       resource_})});
                }
            }

            void set(query_result_t<R> outcome) {
                pending_ = false;
                promise_.set_value(std::move(outcome));
            }

        private:
            std::promise<query_result_t<R>> promise_;
            std::pmr::memory_resource* resource_;
            bool pending_;
        };

        // Runs the connector awaitable on the io_context and marshals its outcome —
        // value or error — into the promise as a plain VALUE.
        //
        // A live std::exception must NEVER cross the io_context worker -> consumer
        // thread boundary: boost.asio's use_future captures a thrown exception into
        // an executor_op whose exception_ptr copy is destroyed on the io thread,
        // racing the consumer's future.get() read of the same object (ThreadSanitizer
        // data race in operator delete under boost 1.88). This coroutine is the
        // boundary: the try/catch around the co_await is where a driver or handler
        // exception raised on the io thread becomes an io_error value.
        //
        // The outcome is handed over through promise.set() — an ordinary call inside
        // the body — rather than through the coroutine's own return value: gcc's
        // return-value machinery combined with asio::use_future bitwise-copies a
        // non-trivial result out of the frame (an SSO std::pmr::string, which
        // core::error_t carries, then points into freed memory; ASAN bad-free at -O0
        // on gcc-11). The promise carrier is a by-value coroutine PARAMETER, i.e. it
        // is move-constructed into the frame and lives exactly as long as the frame:
        // a lambda capture would be a temporary that dies at the end of the spawning
        // full-expression while the suspended coroutine still references it, which
        // is why this is a free function template and not a lambda.
        template<typename R>
        boost::asio::awaitable<void> marshal_outcome(outcome_promise_t<R> promise,
                                                     boost::asio::awaitable<query_result_t<R>> inner,
                                                     std::pmr::memory_resource* resource) {
            // Held in an optional so promise.set() runs exactly once, after the
            // try/catch: a throw out of set() must not be caught as a driver error.
            std::optional<query_result_t<R>> outcome;
            try {
                outcome.emplace(co_await std::move(inner));
            } catch (const std::exception& e) {
                outcome.emplace(core::error_t(core::error_code_t::io_error, std::pmr::string{e.what(), resource}));
            } catch (...) {
                outcome.emplace(
                    core::error_t(core::error_code_t::io_error, std::pmr::string{"unknown connector error", resource}));
            }
            promise.set(std::move(*outcome));
        }

    } // namespace detail

    // Spawn a connector awaitable on `where` and hand the consumer a future
    // carrying the marshaled outcome. The only way a connector result reaches
    // another thread. The future always yields a value: an error the connector
    // reported, an error converted from an exception on the io thread, or
    // io_error when the io_context went away before the query ran.
    //
    // `where` is whatever co_spawn accepts: an execution context (the
    // ConnectorManagers pass their pool's io_context) or an executor. The MySQL
    // connector passes its connection's strand executor, so the coroutine — and
    // with it every completion handler the awaited operation derives from it,
    // asio::cancel_after's deadline timer included — runs serialized on that
    // strand instead of on any pool thread.
    template<typename R, typename ExecutorOrContext>
    query_future_t<R> spawn_marshaled(ExecutorOrContext&& where,
                                      boost::asio::awaitable<query_result_t<R>> inner,
                                      std::pmr::memory_resource* resource) {
        std::promise<query_result_t<R>> promise;
        auto future = promise.get_future();
        boost::asio::co_spawn(std::forward<ExecutorOrContext>(where),
                              detail::marshal_outcome<R>(detail::outcome_promise_t<R>{std::move(promise), resource},
                                                         std::move(inner),
                                                         resource),
                              boost::asio::detached);
        return future;
    }

    // The awaitable ConnectorManager::executeQuery spawns: the connector's
    // runQuery, with the handler OWNED by this coroutine's frame.
    //
    // IConnector::runQuery is virtual, so it takes its handler as a non-owning
    // function_ref_t. executeQuery holds the handler only until it has spawned the
    // query and returned; the handler is moved into this frame instead — a by-value
    // coroutine PARAMETER, like marshal_outcome's promise — and runQuery is called
    // from here, on the io thread, with a reference to that copy, which lives until
    // the co_await below has completed. A caller may therefore pass a temporary or
    // a loop-local lambda; what the handler captures by reference is still the
    // caller's to keep alive until the future settles (QueryHandleWaiter).
    //
    // runQuery's own frame is created inside marshal_outcome's try, so a throw from
    // that comes back as io_error. `connector` and `query` are borrowed: the
    // connector belongs to the manager's registry, which has no remove path, and
    // `query` to the caller, who keeps it until the future settles.
    template<typename R, typename Arg, typename Connector, typename Callable>
    boost::asio::awaitable<query_result_t<R>>
    run_with_owned_handler(Connector& connector, std::string_view query, Callable handler) {
        auto outcome = co_await connector.runQuery(query, function_ref_t<R(Arg)>{handler});
        co_return std::move(outcome);
    }

    // An already-satisfied future for a failure detected before anything was
    // spawned, so executeQuery has exactly one way to report an error.
    template<typename R>
    query_future_t<R> make_failed_future(core::error_t error) {
        std::promise<query_result_t<R>> promise;
        auto future = promise.get_future();
        promise.set_value(query_result_t<R>{std::move(error)});
        return future;
    }

    template<typename T>
    struct QueryHandleWaiter {
        std::pmr::vector<query_future_t<T>> futures;
        std::pmr::vector<T> results;

        explicit QueryHandleWaiter(std::pmr::memory_resource* resource)
            : futures(resource)
            , results(resource) {}

        // Consume every future in order and return the FIRST error. On failure
        // `results` holds only the values produced before it, so callers must bound
        // their loops by results.size() — never by futures.size().
        //
        // [[nodiscard]]: the three integration managers index results[j]
        // positionally right after this call; an ignored error would turn a backend
        // failure into an out-of-bounds read in a Release build.
        //
        // The error is moved out via convert_error: result_wrapper_t::error() only
        // hands back a const&, and BOTH copying an error_t and move-ASSIGNING a
        // result_wrapper_t (debug builds copy-assign error_) re-home its pmr::string
        // onto the default resource through
        // polymorphic_allocator::select_on_container_copy_construction. Only
        // constructing/returning a prvalue preserves the connector's resource.
        [[nodiscard]] core::result_wrapper_t<bool> wait() {
            for (auto& future : futures) {
                auto outcome = future.get();
                if (outcome.has_error()) {
                    return outcome.template convert_error<bool>();
                }
                results.emplace_back(std::move(outcome.value()));
            }
            return true;
        }

        // Unlike std::async, these futures do NOT block on destruction. Draining is
        // load-bearing: the connector handlers capture the caller's frame by
        // reference, so an in-flight io-thread handler must not outlive the scope
        // that spawned it. wait() returning early on the first error leaves the tail
        // futures to this drain.
        ~QueryHandleWaiter() {
            for (auto& future : futures) {
                if (future.valid()) {
                    future.wait();
                }
            }
        }

        QueryHandleWaiter(const QueryHandleWaiter&) = delete;
        QueryHandleWaiter& operator=(const QueryHandleWaiter&) = delete;
        QueryHandleWaiter(QueryHandleWaiter&&) = delete;
        QueryHandleWaiter& operator=(QueryHandleWaiter&&) = delete;
    };

} // namespace otterstax
