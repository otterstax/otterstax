// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_producer.hpp"
#include "utility/tracy_profiler.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <stdexcept>

namespace otterstax::kafka::detail {
    namespace {
        core::error_t producer_error(const std::string& msg) {
            return core::error_t(core::error_code_t::other_error, std::pmr::string(("kafka producer: " + msg).c_str()));
        }

        // Runtime txn helpers still surface librdkafka failures as exceptions (the
        // caller's per-batch backstop converts them); create() reports setup failures
        // through result_wrapper_t instead
        void check_txn_error(RdKafka::Error* error, const char* what) {
            if (error) {
                const std::string msg = std::string("kafka producer: ") + what + " failed: " + error->str();
                delete error;
                throw std::runtime_error(msg);
            }
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
            if (RdKafka::Error* err = producer->init_transactions(10000)) {
                const std::string msg = std::string("init_transactions failed: ") + err->str();
                delete err;
                return producer_error(msg);
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
            producer_->flush(5000); // best-effort drain before the handle is destroyed
        }
    }

    void kafka_producer_t::produce(const std::string& payload) {
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
            throw std::runtime_error("kafka producer: produce to '" + topic_ + "' failed: " + RdKafka::err2str(err));
        }
        producer_->poll(0); // serve delivery reports without blocking
    }

    void kafka_producer_t::flush(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::flush");
        const RdKafka::ErrorCode err = producer_->flush(static_cast<int>(timeout.count()));
        if (err != RdKafka::ERR_NO_ERROR) {
            throw std::runtime_error("kafka producer: flush '" + topic_ + "' failed: " + RdKafka::err2str(err));
        }
    }

    void kafka_producer_t::begin_transaction() {
        OTX_ZONE_N("kafka::producer::begin_transaction");
        check_txn_error(producer_->begin_transaction(), "begin_transaction");
    }

    void kafka_producer_t::send_offsets_to_transaction(const std::vector<RdKafka::TopicPartition*>& offsets,
                                                       const RdKafka::ConsumerGroupMetadata* group_metadata,
                                                       std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::send_offsets_to_transaction");
        check_txn_error(
            producer_->send_offsets_to_transaction(offsets, group_metadata, static_cast<int>(timeout.count())),
            "send_offsets_to_transaction");
    }

    void kafka_producer_t::commit_transaction(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::commit_transaction");
        check_txn_error(producer_->commit_transaction(static_cast<int>(timeout.count())), "commit_transaction");
    }

    void kafka_producer_t::abort_transaction(std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::producer::abort_transaction");
        check_txn_error(producer_->abort_transaction(static_cast<int>(timeout.count())), "abort_transaction");
    }
} // namespace otterstax::kafka::detail
