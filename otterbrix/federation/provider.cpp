// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "provider.hpp"

#include <atomic>
#include <mutex>

namespace otterstax::federation {

    registry_t::registry_t(std::vector<server_t> servers)
        : servers_(std::move(servers)) {
        for (std::size_t i = 0; i < servers_.size(); ++i) {
            // The first server of a folded uid wins; config validation refuses duplicates anyway.
            by_folded_uid_.emplace(fold_identifier(servers_[i].uid), i);
        }
    }

    const server_t* registry_t::find(std::string_view uid) const {
        if (uid.empty()) {
            return nullptr;
        }
        auto it = by_folded_uid_.find(fold_identifier(uid));
        return it == by_folded_uid_.end() ? nullptr : &servers_[it->second];
    }

    namespace {

        struct registry_slot_t {
            std::mutex mutex;
            // Every registry installed since the last reset, so a reference a hook took stays valid
            // until the reset.
            std::vector<std::shared_ptr<const registry_t>> installed;
            std::atomic<const registry_t*> current{nullptr};
        };

        registry_slot_t& slot() {
            static registry_slot_t instance;
            return instance;
        }

        const registry_t& empty_registry() {
            static const registry_t empty;
            return empty;
        }

    } // namespace

    void install_registry(std::shared_ptr<const registry_t> registry) {
        auto& s = slot();
        std::lock_guard guard(s.mutex);
        const registry_t* raw = registry ? registry.get() : &empty_registry();
        if (registry) {
            s.installed.push_back(std::move(registry));
        }
        s.current.store(raw, std::memory_order_release);
    }

    void reset_registry() noexcept {
        auto& s = slot();
        std::vector<std::shared_ptr<const registry_t>> dropped;
        {
            std::lock_guard guard(s.mutex);
            s.current.store(&empty_registry(), std::memory_order_release);
            dropped.swap(s.installed);
        }
        // `dropped` (and the backends it holds) goes here, outside the lock.
    }

    const registry_t& registry() noexcept {
        const registry_t* current = slot().current.load(std::memory_order_acquire);
        return current != nullptr ? *current : empty_registry();
    }

} // namespace otterstax::federation
