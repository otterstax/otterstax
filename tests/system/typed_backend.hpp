// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "plan_stubs.hpp"
#include "sent_queries.hpp"
#include "utility/asio_error.hpp"
#include "utility/function_ref.hpp"
#include "utility/wait_barrier.hpp"

#include <boost/asio/awaitable.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace otterstax::test {
    namespace cell {
        inline int32_t int32(std::size_t row) { return static_cast<int32_t>(row); }
        inline int64_t int64(std::size_t row) { return static_cast<int64_t>(row) + 3000000000LL; }
        inline uint64_t uint64(std::size_t row) { return static_cast<uint64_t>(row) + 7; }
        inline double float64(std::size_t row) { return static_cast<double>(row) + 0.5; }
        inline std::string text(std::size_t row) { return "v" + std::to_string(row); }
    } // namespace cell

    inline constexpr std::size_t kDataRows = 3;

    template<class Wire>
    struct wire_column {
        std::string name;
        Wire wire;
    };

    inline core::error_t probe_refusal(std::pmr::memory_resource* resource) {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{"simulated probe refusal", resource});
    }

    template<class B>
    class mock {
    public:
        using column = wire_column<typename B::wire>;
        using probe_mode = typename B::probe_mode;
        using answer_fn = std::function<std::vector<column>(std::string_view statement)>;

        struct probe_t {
            std::vector<column> columns;
            probe_mode mode;
        };

        static void reset(answer_fn answer, probe_mode mode = probe_mode::header) {
            std::lock_guard guard(state_.mutex);
            state_.answer = std::move(answer);
            state_.mode = mode;
            state_.probes.clear();
            state_.statements.clear();
        }

        static void reset(std::vector<column> columns = {}, probe_mode mode = probe_mode::header) {
            reset([columns = std::move(columns)](std::string_view) { return columns; }, mode);
        }

        static std::vector<std::string> probe_queries() {
            std::lock_guard guard(state_.mutex);
            return state_.probes;
        }

        static std::vector<std::string> data_queries() {
            std::lock_guard guard(state_.mutex);
            return state_.statements;
        }

        static probe_t probe(std::string_view query) {
            std::lock_guard guard(state_.mutex);
            state_.probes.emplace_back(query);
            return {state_.answer(wrapped_statement(query)), state_.mode};
        }

        static std::vector<column> data(std::string_view query) {
            std::lock_guard guard(state_.mutex);
            state_.statements.emplace_back(query);
            return state_.answer(query);
        }

    private:
        static std::string_view wrapped_statement(std::string_view probe) {
            return probe.substr(kProbePrefix.size(), probe.rfind(')') - kProbePrefix.size());
        }

        struct state_t {
            std::mutex mutex;
            answer_fn answer = [](std::string_view) { return std::vector<column>{}; };
            probe_mode mode = probe_mode::header;
            std::vector<std::string> probes;
            std::vector<std::string> statements;
        };

        static inline state_t state_;
    };

    template<class B>
    class typed_connector final : public B::connector_iface {
        using chunk_ptr = std::unique_ptr<components::vector::data_chunk_t>;

    public:
        typed_connector(std::pmr::memory_resource* resource, typename B::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        typename B::status status() const noexcept override { return B::status::Connected; }
        typename B::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<chunk_ptr>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<chunk_ptr(typename B::handler_arg)> handler) override {
            auto rows = B::rows(mock<B>::data(query), kDataRows);
            record_sent(alias_, query);
            co_return otterstax::as_query_result<chunk_ptr>(handler(B::view(rows)));
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(typename B::handler_arg)>) override {
            record_sent(alias_, query);
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(typename B::handler_arg)> handler) override {
            if (!query.starts_with(kProbePrefix)) {
                auto discovered = B::discovery(query);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(B::view(discovered)));
            }
            auto probe = mock<B>::probe(query);
            auto described = B::describe(probe.columns, probe.mode, resource_);
            if (described.has_error()) {
                co_return described.error();
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(B::view(described.value())));
        }

    private:
        std::pmr::memory_resource* resource_;
        typename B::connect_params params_;
        std::string alias_;
    };
} // namespace otterstax::test
