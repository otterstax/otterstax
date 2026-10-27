// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mock/mock_driver.hpp"
#include "connectors/common/driver.hpp"
#include "plan_stubs.hpp"
#include "sent_queries.hpp"

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

    // A connection of backend B answering from mock<B>: a describe probe (kProbePrefix) with the case's
    // columns, B's own discovery answer, and kDataRows rows for a slot statement. Answers go through B's
    // wire format and its real decode. Slot statements are recorded per alias (sent_queries.hpp); an
    // affected_rows statement answers a count of 0.
    template<class B>
    class typed_driver final : public conn::IDriver {
    public:
        typed_driver(std::pmr::memory_resource* resource, std::string alias)
            : resource_(resource)
            , alias_(std::move(alias)) {}

        core::error_t connect() override { return core::error_t::no_error(); }
        core::error_t ping() override { return core::error_t::no_error(); }
        void close() noexcept override {}
        std::unique_ptr<conn::IDriver> clone_disconnected() const override {
            return std::make_unique<typed_driver>(resource_, alias_);
        }

        conn::statement_outcome_t execute(const conn::statement_t& statement) override {
            const std::string_view query = statement.sql;
            if (query.starts_with(kProbePrefix)) {
                auto probe = mock<B>::probe(query);
                auto described = B::describe(probe.columns, probe.mode, resource_);
                if (described.has_error()) {
                    return described.error();
                }
                return B::decode(resource_, described.value(), statement);
            }
            if (is_discovery(statement)) {
                return B::decode(resource_, B::discovery(query), statement);
            }
            record_sent(alias_, query, statement.repeatable);
            if (statement.kind == conn::result_kind::affected_rows) {
                return conn::statement_result_t{};
            }
            return B::decode(resource_, B::rows(mock<B>::data(query), kDataRows), statement);
        }

    private:
        std::pmr::memory_resource* resource_;
        std::string alias_;
    };
} // namespace otterstax::test
