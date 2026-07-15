// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_consumer.hpp"
#include "kafka_const.hpp"
#include "kafka_producer.hpp"
#include "utility/tracy_profiler.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
#include <map>
#include <memory>

namespace otterstax::kafka::detail {
    namespace {
        core::error_t consumer_error(const std::string& msg) {
            return core::error_t(core::error_code_t::other_error, std::pmr::string("kafka consumer: " + msg));
        }
    } // namespace

    // A RebalanceCb turns off librdkafka's auto-assignment so we seek each partition to
    // its stored offset on assign (empty map -> broker-group resume). Eager assignor
    // only (the default) -> plain assign()/unassign()
    struct offset_rebalance_cb final : RdKafka::RebalanceCb {
        std::map<int32_t, int64_t> committed_offsets; // partition -> next offset to read

        void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                          RdKafka::ErrorCode err,
                          std::vector<RdKafka::TopicPartition*>& partitions) override {
            if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
                for (auto* tp : partitions) {
                    if (auto it = committed_offsets.find(tp->partition()); it != committed_offsets.end()) {
                        tp->set_offset(it->second);
                    }
                }
                consumer->assign(partitions);
            } else {
                consumer->unassign(); // revoke / rebalance error: drop the assignment to resync
            }
        }
    };

    core::result_wrapper_t<kafka_consumer_t> kafka_consumer_t::create(consumer_config_t config) {
        auto rebalance_cb = std::make_unique<offset_rebalance_cb>();
        rebalance_cb->committed_offsets = std::move(config.committed_offsets);

        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (!conf) {
            return consumer_error("failed to create configuration");
        }

        std::string errstr;
        auto set = [&](const char* key, const std::string& value) {
            return conf->set(key, value, errstr) == RdKafka::Conf::CONF_OK;
        };

        // We own offset persistence (offsets table) -> no broker auto-commit
        if (!set("bootstrap.servers", config.bootstrap_servers) || !set("group.id", config.group_id) ||
            !set("auto.offset.reset", config.offset_reset) || !set("enable.auto.commit", "false")) {
            return consumer_error("config: " + errstr);
        }
        if (conf->set("rebalance_cb", rebalance_cb.get(), errstr) != RdKafka::Conf::CONF_OK) {
            return consumer_error("set rebalance_cb: " + errstr);
        }

        std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!consumer) {
            return consumer_error("create failed: " + errstr);
        }
        if (auto err = consumer->subscribe({config.topic}); err != RdKafka::ERR_NO_ERROR) {
            return consumer_error("subscribe to '" + config.topic + "' failed: " + RdKafka::err2str(err));
        }
        return kafka_consumer_t(std::move(rebalance_cb), std::move(consumer), std::move(config.topic));
    }

    kafka_consumer_t::kafka_consumer_t(std::unique_ptr<offset_rebalance_cb> rebalance_cb,
                                       std::unique_ptr<RdKafka::KafkaConsumer> consumer,
                                       std::string topic) noexcept
        : rebalance_cb_(std::move(rebalance_cb))
        , consumer_(std::move(consumer))
        , topic_(std::move(topic)) {}

    kafka_consumer_t::kafka_consumer_t(kafka_consumer_t&&) noexcept = default;
    kafka_consumer_t& kafka_consumer_t::operator=(kafka_consumer_t&&) noexcept = default;

    kafka_consumer_t::~kafka_consumer_t() {
        if (consumer_) {
            consumer_->close(); // leave the group cleanly before the unique_ptr deletes it
        }
    }

    std::vector<kafka_record_t> kafka_consumer_t::poll_batch(std::size_t max_records,
                                                             std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::consumer::poll_batch");
        std::vector<kafka_record_t> batch;
        batch.reserve(max_records);
        const auto timeout_ms = to_ms(timeout);

        while (batch.size() < max_records) {
            std::unique_ptr<RdKafka::Message> msg(consumer_->consume(timeout_ms));
            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR:
                    batch.push_back(kafka_record_t{std::string(static_cast<const char*>(msg->payload()), msg->len()),
                                                   msg->partition(),
                                                   msg->offset()});
                    break;
                case RdKafka::ERR__PARTITION_EOF: // caught up on this partition
                case RdKafka::ERR__TIMED_OUT:     // nothing available this tick
                default:                          // transient/other: end the batch, retry next tick
                    return batch;
            }
        }
        return batch;
    }

    void kafka_consumer_t::commit() {
        OTX_ZONE_N("kafka::consumer::commit");
        if (consumer_) {
            consumer_->commitSync();
        }
    }

    kafka_txn_result kafka_consumer_t::send_offsets_to_transaction(kafka_producer_t& producer,
                                                                   const std::vector<kafka_record_t>& batch,
                                                                   std::chrono::milliseconds timeout) {
        OTX_ZONE_N("kafka::consumer::send_offsets_to_transaction");
        if (batch.empty()) {
            return kafka_txn_result{};
        }

        std::map<int32_t, int64_t> next_offset;
        for (const auto& record : batch) {
            next_offset[record.partition] = std::max(next_offset[record.partition], record.offset + 1);
        }

        std::vector<RdKafka::TopicPartition*> offsets;
        offsets.reserve(next_offset.size());
        for (const auto& [partition, offset] : next_offset) {
            RdKafka::TopicPartition* tp = RdKafka::TopicPartition::create(topic_, partition);
            tp->set_offset(offset);
            offsets.push_back(tp);
        }

        struct offsets_guard {
            std::vector<RdKafka::TopicPartition*>& v;
            ~offsets_guard() { RdKafka::TopicPartition::destroy(v); } // free on every return path
        } guard{offsets};

        std::unique_ptr<RdKafka::ConsumerGroupMetadata> group_metadata(consumer_->groupMetadata());
        return producer.send_offsets_to_transaction(offsets, group_metadata.get(), timeout);
    }

    core::error_t kafka_consumer_t::seek_to_batch_start(const std::vector<kafka_record_t>& batch) {
        OTX_ZONE_N("kafka::consumer::seek_to_batch_start");
        if (batch.empty()) {
            return core::error_t::no_error();
        }
        // Rewind each partition to the first offset consumed in this batch — where we
        // would have resumed had the transaction committed
        std::map<int32_t, int64_t> first_offset;
        for (const auto& record : batch) {
            auto it = first_offset.find(record.partition);
            if (it == first_offset.end() || record.offset < it->second) {
                first_offset[record.partition] = record.offset;
            }
        }
        for (const auto& [partition, offset] : first_offset) {
            std::unique_ptr<RdKafka::TopicPartition> tp(RdKafka::TopicPartition::create(topic_, partition, offset));
            if (auto err = consumer_->seek(*tp, to_ms(SEEK_TIMEOUT)); err != RdKafka::ERR_NO_ERROR) {
                return consumer_error("seek '" + topic_ + "' failed: " + RdKafka::err2str(err));
            }
        }
        return core::error_t::no_error();
    }
} // namespace otterstax::kafka::detail
