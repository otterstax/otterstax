// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_producer.hpp"
#include "kafka_const.hpp"
#include "utility/tracy_profiler.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <thread>

namespace otterstax::kafka::detail {
    namespace {
        core::error_t producer_error(const std::string& msg) {
            return core::error_t(core::error_code_t::other_error, std::pmr::string(("kafka producer: " + msg).c_str()));
        }

        // Consume librdkafka's Error (nullptr == success) into a value, keeping its
        // classification — the EOS contract rides on fatal/retriable/requires_abort
        kafka_txn_result to_txn_result(RdKafka::Error* error, const char* what) {
            if (!error) {
                return kafka_txn_result{};
            }
            kafka_txn_result result;
            result.fatal = error->is_fatal();
            result.retriable = error->is_retriable();
            result.requires_abort = error->txn_requires_abort();
            result.error = producer_error(std::string(what) + " failed: " + error->str());
            delete error;
            return result;
        }
    } // namespace

    core::result_wrapper_t<kafka_producer_t> kafka_producer_t::create(producer_config_t config) {
        const bool transactional = !config.transactional_id.empty();
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (!conf) {
            return producer_error("failed to create configuration");
        }
        std::string errstr;
        auto set = [&](const char* key, const std::string& value) {
            return conf->set(key, value, errstr) == RdKafka::Conf::CONF_OK;
        };
        // acks=all + idempotence = no loss and the precondition for transactions
        if (!set("bootstrap.servers", config.bootstrap_servers) || !set("acks", "all") ||
            !set("enable.idempotence", "true") ||
            (transactional && !set("transactional.id", config.transactional_id))) {
            return producer_error("config: " + errstr);
        }

        std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), errstr));
        if (!producer) {
            return producer_error("create failed: " + errstr);
        }
        if (transactional) {
            // A retriable init_transactions failure means the operation is still
            // running broker-side ("retry call to resume") — a single shot that loses
            // the PID-fencing race made launch_stream() return silently and wedged
            // fan-in crash-recovery, so keep resuming until the deadline
            const auto init_deadline = std::chrono::steady_clock::now() + TXN_INIT_DEADLINE;
            for (;;) {
                auto init = to_txn_result(producer->init_transactions(to_ms(TXN_INIT_TIMEOUT)), "init_transactions");
                if (init.ok()) {
                    break;
                }
                if (init.retriable && std::chrono::steady_clock::now() < init_deadline) {
                    std::this_thread::sleep_for(TXN_INIT_BACKOFF);
                    continue;
                }
                return init.error;
            }
        }
        return kafka_producer_t(std::move(producer), std::move(config.topic), transactional);
    }

    kafka_producer_t::kafka_producer_t(std::unique_ptr<RdKafka::Producer> producer,
                                       std::string topic,
                                       bool transactional) noexcept
        : producer_(std::move(producer))
        , topic_(std::move(topic))
        , transactional_(transactional) {}

    kafka_producer_t::kafka_producer_t(kafka_producer_t&&) noexcept = default;
    kafka_producer_t& kafka_producer_t::operator=(kafka_producer_t&&) noexcept = default;

    kafka_producer_t::~kafka_producer_t() {
        if (producer_) {
            producer_->flush(to_ms(PRODUCER_DRAIN_TIMEOUT));
        }
    }

    core::error_t kafka_producer_t::produce(const std::string& payload) {
        const RdKafka::ErrorCode err = producer_->produce(topic_,
                                                          RdKafka::Topic::PARTITION_UA,   // partitioner chooses
                                                          RdKafka::Producer::RK_MSG_COPY, // copy payload
                                                          const_cast<char*>(payload.data()),
                                                          payload.size(),
                                                          /*key*/ nullptr,
                                                          /*key_len*/ 0,
                                                          /*timestamp*/ 0,
                                                          /*msg_opaque*/ nullptr);
        if (err != RdKafka::ERR_NO_ERROR) {
            return producer_error("produce to '" + topic_ + "' failed: " + RdKafka::err2str(err));
        }
        producer_->poll(0); // serve delivery reports without blocking
        return core::error_t::no_error();
    }

    core::error_t kafka_producer_t::flush(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::flush");
        const RdKafka::ErrorCode err = producer_->flush(to_ms(timeout));
        if (err != RdKafka::ERR_NO_ERROR) {
            return producer_error("flush '" + topic_ + "' failed: " + RdKafka::err2str(err));
        }
        return core::error_t::no_error();
    }

    kafka_txn_result kafka_producer_t::begin_transaction() {
        OTX_ZONE_N("kafka::producer::begin_transaction");
        return to_txn_result(producer_->begin_transaction(), "begin_transaction");
    }

    kafka_txn_result kafka_producer_t::send_offsets_to_transaction(const std::vector<RdKafka::TopicPartition*>& offsets,
                                                                   const RdKafka::ConsumerGroupMetadata* group_metadata,
                                                                   std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::send_offsets_to_transaction");
        return to_txn_result(producer_->send_offsets_to_transaction(offsets, group_metadata, to_ms(timeout)),
                             "send_offsets_to_transaction");
    }

    kafka_txn_result kafka_producer_t::commit_transaction(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::commit_transaction");
        return to_txn_result(producer_->commit_transaction(to_ms(timeout)), "commit_transaction");
    }

    kafka_txn_result kafka_producer_t::abort_transaction(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::abort_transaction");
        return to_txn_result(producer_->abort_transaction(to_ms(timeout)), "abort_transaction");
    }
} // namespace otterstax::kafka::detail
