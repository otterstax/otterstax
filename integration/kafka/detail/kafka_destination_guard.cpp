// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "kafka_destination_guard.hpp"

#include <netdb.h>

namespace otterstax::kafka::detail {
namespace {
    // resolve_cb runs on a librdkafka thread. Each such thread belongs to one
    // handle; the public thread-start interceptor installs that handle's policy
    // before it can resolve any broker. This avoids overwriting the C++ API's
    // opaque pointer or depending on its private implementation types.
    thread_local std::shared_ptr<const security::network_destinations> thread_destinations;

    int resolve(const char* node, const char* service, const addrinfo* hints, addrinfo** result, void*) noexcept {
        if (!node && !service && !hints) {
            if (result && *result) { freeaddrinfo(*result); *result = nullptr; }
            return 0;
        }
        if (!result) return EAI_FAIL;
        *result = nullptr;
        if (!node || !service) return EAI_NONAME;
        try {
            if (!thread_destinations) return EAI_NONAME;
            std::string host(node);
            if (host.find(':') != std::string::npos && host.front() != '[') host = '[' + host + ']';
            if (!thread_destinations->allows(host + ':' + service)) return EAI_NONAME;
            return getaddrinfo(node, service, hints, result);
        } catch (...) {
            return EAI_MEMORY;
        }
    }
}

kafka_destination_guard::kafka_destination_guard(std::shared_ptr<const security::network_destinations> destinations)
    : destinations_(std::move(destinations)) {}

kafka_destination_guard::~kafka_destination_guard() = default;

rd_kafka_resp_err_t kafka_destination_guard::on_new(rd_kafka_t* handle, const rd_kafka_conf_t*,
                                                  void* context, char*, size_t) noexcept {
    auto& guard = *static_cast<kafka_destination_guard*>(context);
    const auto result = rd_kafka_interceptor_add_on_thread_start(handle, "otterstax-destinations", &on_thread_start, context);
    guard.attached_ = result == RD_KAFKA_RESP_ERR_NO_ERROR;
    return result;
}

rd_kafka_resp_err_t kafka_destination_guard::on_thread_start(rd_kafka_t*, rd_kafka_thread_type_t,
                                                            const char*, void* context) noexcept {
    thread_destinations = static_cast<kafka_destination_guard*>(context)->destinations_;
    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

bool kafka_destination_guard::install(rd_kafka_conf_t* config) {
    if (!destinations_ || !config) return false;
    rd_kafka_conf_set_resolve_cb(config, &resolve);
    return rd_kafka_conf_interceptor_add_on_new(config, "otterstax-destinations", &on_new, this) == RD_KAFKA_RESP_ERR_NO_ERROR &&
           rd_kafka_conf_interceptor_add_on_conf_dup(config, "otterstax-destinations", &on_conf_dup, this) == RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t kafka_destination_guard::on_conf_dup(rd_kafka_conf_t* copy, const rd_kafka_conf_t*,
                                                        size_t, const char**, void* context) noexcept {
    // Producer::create and KafkaConsumer::create duplicate their Conf. The C
    // library intentionally does not copy interceptors; register them on the
    // duplicate too, with the same owner that outlives the resulting client.
    return static_cast<kafka_destination_guard*>(context)->install(copy)
        ? RD_KAFKA_RESP_ERR_NO_ERROR : RD_KAFKA_RESP_ERR__INVALID_ARG;
}

} // namespace otterstax::kafka::detail
