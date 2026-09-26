// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "core.hpp"

#include "../scheduler_engine.hpp"

#include <arrow/io/memory.h>

#include <stdexcept>

namespace flight::core {

namespace {

std::string new_ticket_id(std::uint64_t counter) {
    // An opaque identifier; the ticket's content is an internal handle only.
    return "otterstax-tickets/" + std::to_string(counter) + "-" +
           std::to_string(std::hash<std::string>{}(std::to_string(counter) + "otterstax"));
}

} // namespace

FlightSqlCore::FlightSqlCore(AuthService auth, std::unique_ptr<engine::SchedulerEngine> engine)
    : auth_(std::move(auth)),
      engine_(std::move(engine)) {}

FlightSqlCore::~FlightSqlCore() = default;

std::string FlightSqlCore::register_result(const std::shared_ptr<arrow::Schema>& schema,
                                           std::vector<std::shared_ptr<arrow::RecordBatch>> batches) {
    CachedResult cached;
    auto schema_bytes = arrow::ipc::SerializeSchema(*schema);
    if (!schema_bytes.ok()) {
        throw EngineError("serializing the result schema: " + schema_bytes.status().ToString());
    }
    cached.schema_ipc.assign(reinterpret_cast<const char*>((*schema_bytes)->data()),
                             static_cast<std::size_t>((*schema_bytes)->size()));
    // SerializeSchema answers an ENCAPSULATED message (continuation + u32
    // length); DoGet's first FlightData carries the same schema as a BARE
    // message — like the batch headers beside it — so the framing is stripped
    // (8 bytes; the flatbuffer itself is already zero-padded to 8 inside).
    constexpr std::size_t kFramingBytes = 8;
    cached.schema_message = cached.schema_ipc.substr(kFramingBytes);

    const auto options = arrow::ipc::IpcWriteOptions::Defaults();
    for (const auto& batch : batches) {
        arrow::ipc::IpcPayload payload;
        auto status = arrow::ipc::GetRecordBatchPayload(*batch, options, &payload);
        if (!status.ok()) {
            throw EngineError("serializing a record batch: " + status.ToString());
        }
        BatchPayload out;
        out.metadata.assign(reinterpret_cast<const char*>(payload.metadata->data()),
                            static_cast<std::size_t>(payload.metadata->size()));
        // The body buffers arrive UNALIGNED — the 8-byte alignment (and the
        // buffer offsets the Message metadata declares) is applied by the
        // stream writer, which we do not use: pad each buffer here, or every
        // buffer after the first lands at a wrong offset and a later read
        // overruns the body ("File too short").
        for (const auto& buffer : payload.body_buffers) {
            if (!buffer) {
                continue; // an absent optional buffer
            }
            out.body.append(reinterpret_cast<const char*>(buffer->data()),
                            static_cast<std::size_t>(buffer->size()));
            const auto pad = (8 - (static_cast<std::size_t>(buffer->size()) % 8)) % 8;
            out.body.append(pad, '\0');
        }
        cached.total_rows += batch->num_rows();
        cached.batches.push_back(std::move(out));
    }

    const std::string ticket = new_ticket_id(ticket_counter_.fetch_add(1));
    std::lock_guard lock(mutex_);
    results_[ticket] = std::move(cached);
    result_order_.push_back(ticket);
    while (result_order_.size() > kMaxCachedResults) {
        results_.erase(result_order_.front());
        result_order_.pop_front();
    }
    return ticket;
}

grpc::Status FlightSqlCore::make_flight_info(const fp::FlightDescriptor& descriptor,
                                             const std::string& ticket, fp::FlightInfo* out) const {
    CachedResult const* cached;
    if (const auto st = lookup(ticket, &cached); !st.ok()) return st;

    out->mutable_flight_descriptor()->CopyFrom(descriptor);
    auto* endpoint = out->add_endpoint();
    endpoint->mutable_ticket()->set_ticket(ticket);
    // No Location is set: an empty list means "this same server" (the client's channel).
    out->set_total_records(cached->total_rows);
    out->set_ordered(false);
    // Encapsulated schema (continuation+len+flatbuffer)
    out->set_schema(cached->schema_ipc.data(), cached->schema_ipc.size());
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::make_schema_result(const std::string& ticket,
                                               fp::SchemaResult* out) const {
    CachedResult const* cached;
    if (const auto st = lookup(ticket, &cached); !st.ok()) return st;
    out->set_schema(cached->schema_ipc.data(), cached->schema_ipc.size());
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::lookup(const std::string& ticket, CachedResult const** out) const {
    std::lock_guard lock(mutex_);
    const auto it = results_.find(ticket);
    if (it == results_.end()) {
        return {grpc::StatusCode::NOT_FOUND, "flight-sql: unknown or expired ticket"};
    }
    *out = &it->second;
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::create_prepared(const std::string& query, std::string* handle_out) {
    Prepared prepared;
    try {
        prepared = engine_->prepare(query);
    } catch (const EngineError& e) {
        return {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
    } catch (const std::exception& e) {
        return {grpc::StatusCode::INTERNAL, e.what()};
    }
    const std::string handle = "otterstax-prepared/" + std::to_string(prepared_counter_.fetch_add(1));
    std::lock_guard lock(mutex_);
    prepared_[handle] = PreparedStatementState{std::move(prepared), {}};
    *handle_out = handle;
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::close_prepared(const std::string& handle) {
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end()) {
        return {grpc::StatusCode::NOT_FOUND, "flight-sql: unknown prepared statement handle"};
    }
    // Let the engine release its resource (e.g. a never-executed statement on
    // a Worker) before the PreparedStatementState is destroyed.
    engine_->close_prepared(it->second.prepared);
    prepared_.erase(it);
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::prepared_lookup(const std::string& handle,
                                            PreparedStatementState const** out) const {
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end()) {
        return {grpc::StatusCode::NOT_FOUND, "flight-sql: unknown prepared statement handle"};
    }
    *out = &it->second;
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::prepared_bind(const std::string& handle, BoundParams rows) {
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end()) {
        return {grpc::StatusCode::NOT_FOUND, "flight-sql: unknown prepared statement handle"};
    }
    it->second.bound = std::move(rows);
    return grpc::Status::OK;
}

} // namespace flight::core
