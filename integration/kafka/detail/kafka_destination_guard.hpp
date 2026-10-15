// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once

#include "security/network_destinations.hpp"
#include <memory>
#include <librdkafka/rdkafka.h>

namespace otterstax::kafka::detail {

// Owns the resolver policy for one librdkafka handle, including advertised
// brokers. Must outlive that handle and remain at a stable address across moves.
class kafka_destination_guard final {
public:
    explicit kafka_destination_guard(std::shared_ptr<const security::network_destinations> destinations);
    ~kafka_destination_guard();
    kafka_destination_guard(const kafka_destination_guard&) = delete;
    kafka_destination_guard& operator=(const kafka_destination_guard&) = delete;
    bool install(rd_kafka_conf_t* config);
    bool attached() const noexcept { return attached_; }
private:
    static rd_kafka_resp_err_t on_new(rd_kafka_t*, const rd_kafka_conf_t*, void*, char*, size_t) noexcept;
    static rd_kafka_resp_err_t on_thread_start(rd_kafka_t*, rd_kafka_thread_type_t, const char*, void*) noexcept;
    static rd_kafka_resp_err_t on_conf_dup(rd_kafka_conf_t*, const rd_kafka_conf_t*, size_t, const char**, void*) noexcept;
    std::shared_ptr<const security::network_destinations> destinations_;
    bool attached_{false};
};

} // namespace otterstax::kafka::detail
