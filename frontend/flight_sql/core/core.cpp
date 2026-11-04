// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "core.hpp"

#include "../session_engine.hpp"
#include "frontend/common/utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <arrow/io/memory.h>

#include <cassert>
#include <iterator>
#include <optional>
#include <string_view>
#include <stdexcept>
#include <tuple>

namespace flight::core {

namespace {

constexpr std::string_view kTicketPrefix = "otterstax-tickets/";
constexpr std::string_view kPreparedPrefix = "otterstax-prepared/";
constexpr std::string_view kNotFoundTicket = "flight-sql: unknown, already redeemed or expired ticket";
constexpr std::string_view kNotFoundPrepared = "flight-sql: unknown prepared statement handle";

// `prefix` + kIdBytes of secure random hex: unguessable, so a ticket or a
// handle cannot be enumerated by another client. nullopt without secure bytes.
std::optional<std::string> new_id(std::string_view prefix) {
    auto random = frontend::secure_random_hex(FlightSqlCore::kIdBytes);
    if (!random.has_value()) {
        return std::nullopt;
    }
    std::string id{prefix};
    id += *random;
    return id;
}

grpc::Status no_secure_id() {
    return {grpc::StatusCode::UNAVAILABLE, "flight-sql: cannot generate a secure identifier"};
}

} // namespace

FlightSqlCore::FlightSqlCore(AuthService auth, std::unique_ptr<engine::SessionEngine> engine,
                             ResultCacheLimits limits)
    : auth_(std::move(auth)),
      engine_(std::move(engine)),
      limits_(limits) {
    assert(limits_.results > 0 && "the cache holds at least the result just registered");
}

FlightSqlCore::~FlightSqlCore() = default;

CachedResultPtr materialize(const std::shared_ptr<arrow::Schema>& schema,
                            const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    OTX_ZONE_N("flight::materialize");
    auto result = std::make_unique<CachedResult>();
    CachedResult& cached = *result;
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

    cached.bytes = cached.schema_ipc.size() + cached.schema_message.size();
    for (const auto& batch : cached.batches) {
        cached.bytes += batch.metadata.size() + batch.body.size();
    }
    return result;
}

void make_flight_info(const fp::FlightDescriptor& descriptor, const CachedResult& result, fp::FlightInfo* out) {
    out->mutable_flight_descriptor()->CopyFrom(descriptor);
    // One endpoint, its ticket set by set_flight_ticket. No Location is set: an
    // empty list means "this same server" (the client's channel).
    out->add_endpoint();
    out->set_total_records(result.total_rows);
    out->set_ordered(false);
    // Encapsulated schema (continuation+len+flatbuffer)
    out->set_schema(result.schema_ipc.data(), result.schema_ipc.size());
}

void set_flight_ticket(std::string ticket, fp::FlightInfo* out) {
    assert(out->endpoint_size() == 1 && "make_flight_info adds the one endpoint");
    out->mutable_endpoint(0)->mutable_ticket()->set_ticket(std::move(ticket));
}

grpc::Status FlightSqlCore::register_result(CachedResultPtr result, const std::string& owner,
                                            std::string* ticket_out) {
    OTX_ZONE_N("flight::register_result");
    assert(result != nullptr);
    assert(ticket_out != nullptr);
    auto ticket = new_id(kTicketPrefix);
    if (!ticket.has_value()) {
        return no_secure_id();
    }
    const std::size_t bytes = result->bytes;
    std::lock_guard lock(mutex_);
    result_order_.push_back(*ticket);
    // 128 random bits: a repeat is never expected; were it to happen it would
    // overwrite nothing (emplace keeps the first) — refuse instead.
    const bool inserted =
        results_.emplace(*ticket, CacheEntry{std::move(result), owner, std::prev(result_order_.end())}).second;
    if (!inserted) {
        result_order_.pop_back();
        return no_secure_id();
    }
    cached_bytes_ += bytes;
    // The oldest go first; the result just registered (the back) stays.
    while (results_.size() > 1 &&
           (results_.size() > limits_.results || cached_bytes_ > limits_.bytes)) {
        drop_locked(results_.find(result_order_.front()));
    }
    *ticket_out = std::move(*ticket);
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::take_result(const std::string& ticket, const std::string& owner, CachedResultPtr* out) {
    OTX_ZONE_N("flight::take_result");
    std::lock_guard lock(mutex_);
    const auto it = results_.find(ticket);
    // Another client's ticket is answered as an unknown one and stays cached.
    if (it == results_.end() || it->second.owner != owner) {
        return {grpc::StatusCode::NOT_FOUND, std::string{kNotFoundTicket}};
    }
    *out = drop_locked(it);
    return grpc::Status::OK;
}

CachedResultPtr FlightSqlCore::drop_locked(std::unordered_map<std::string, CacheEntry>::iterator it) {
    assert(it != results_.end());
    auto result = std::move(it->second.result);
    cached_bytes_ -= result->bytes;
    result_order_.erase(it->second.order);
    results_.erase(it);
    return result;
}

std::size_t FlightSqlCore::cached_results() const {
    std::lock_guard lock(mutex_);
    return results_.size();
}

std::size_t FlightSqlCore::cached_bytes() const {
    std::lock_guard lock(mutex_);
    return cached_bytes_;
}

grpc::Status FlightSqlCore::create_prepared(const std::string& query, const std::string& owner,
                                            std::string* handle_out) {
    OTX_ZONE_N("flight::create_prepared");
    // The handle first: without secure bytes nothing is prepared.
    auto handle = new_id(kPreparedPrefix);
    if (!handle.has_value()) {
        return no_secure_id();
    }
    Prepared prepared;
    otterstax::session::session_ptr session;
    try {
        std::tie(prepared, session) = engine_->prepare(query);
    } catch (const EngineError& e) {
        return {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
    } catch (const std::exception& e) {
        return {grpc::StatusCode::INTERNAL, e.what()};
    }
    std::lock_guard lock(mutex_);
    // A repeat of 128 random bits: never expected, never an overwrite (the
    // entry not taken closes its session).
    if (!prepared_.emplace(*handle, PreparedEntry{owner, PreparedStatementState{std::move(prepared), {}}, std::move(session)})
             .second) {
        return no_secure_id();
    }
    *handle_out = std::move(*handle);
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::close_prepared(const std::string& handle, const std::string& owner) {
    OTX_ZONE_N("flight::close_prepared");
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end() || it->second.owner != owner) {
        return {grpc::StatusCode::NOT_FOUND, std::string{kNotFoundPrepared}};
    }
    // Closes the statement's session under the lock, so no execution step can
    // reach it any more; a statement it took runs to its end.
    prepared_.erase(it);
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::prepared_lookup(const std::string& handle, const std::string& owner,
                                            PreparedStatementState* out) const {
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end() || it->second.owner != owner) {
        return {grpc::StatusCode::NOT_FOUND, std::string{kNotFoundPrepared}};
    }
    *out = it->second.state;
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::prepared_bind(const std::string& handle, const std::string& owner, BoundParams rows) {
    OTX_ZONE_N("flight::prepared_bind");
    std::lock_guard lock(mutex_);
    const auto it = prepared_.find(handle);
    if (it == prepared_.end() || it->second.owner != owner) {
        return {grpc::StatusCode::NOT_FOUND, std::string{kNotFoundPrepared}};
    }
    // An execution that copied the old state keeps reading it.
    it->second.state.bound = std::move(rows);
    return grpc::Status::OK;
}

template<typename Run>
grpc::Status FlightSqlCore::run_prepared(const std::string& handle, const std::string& owner, Run run) {
    PreparedStatementState state;
    if (auto status = prepared_lookup(handle, owner, &state); !status.ok()) {
        return status;
    }
    try {
        run(state);
    } catch (const EngineError& e) {
        // Closed under the execution: a step that found no session, or a
        // statement the closed session answered without running it.
        std::lock_guard lock(mutex_);
        if (!prepared_.contains(handle)) {
            return {grpc::StatusCode::NOT_FOUND, std::string{kNotFoundPrepared}};
        }
        return {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
    } catch (const std::exception& e) {
        return {grpc::StatusCode::INTERNAL, e.what()};
    }
    return grpc::Status::OK;
}

grpc::Status FlightSqlCore::execute_prepared(const std::string& handle, const std::string& owner, QueryResult* out) {
    OTX_ZONE_N("flight::execute_prepared");
    return run_prepared(handle, owner, [&](const PreparedStatementState& state) {
        *out = engine_->execute_prepared(*this, handle, state.prepared, state.bound);
    });
}

grpc::Status FlightSqlCore::execute_update_prepared(const std::string& handle, const std::string& owner,
                                                    std::int64_t* out) {
    OTX_ZONE_N("flight::execute_update_prepared");
    return run_prepared(handle, owner, [&](const PreparedStatementState& state) {
        *out = engine_->execute_update_prepared(*this, handle, state.prepared, state.bound);
    });
}

} // namespace flight::core
