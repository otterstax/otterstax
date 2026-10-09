// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <integration/cpp/base_spaces.hpp>

#include <actor-zeta.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <utility>

namespace db {
    // The engine opens through base_otterbrix_t::open (services::engine::open_engine underneath);
    // the subclass only reaches the protected engine() for the dispatcher's address.
    class otterbrix_engine_t final
        : public otterbrix::base_otterbrix_t
        , public boost::intrusive_ref_counter<otterbrix_engine_t> {
    public:
        explicit otterbrix_engine_t(host_ptr host)
            : base_otterbrix_t(std::move(host)) {}

        // Address of the async manager_dispatcher_t actor.
        actor_zeta::address_t engine_dispatcher_address() const;
    };

    using otterbrix_engine_ptr = boost::intrusive_ptr<otterbrix_engine_t>;

    // A refused start (bad config, held directory lock, unreadable WAL) prints the reason and aborts:
    // callers construct the engine before anything else exists, as with the previous constructor.
    otterbrix_engine_ptr make_otterbrix_engine(const configuration::config& config);
} // namespace db
