// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "flight_server.hpp"

#include "../scheduler_engine.hpp"

#include <agrpc/register_awaitable_rpc_handler.hpp>

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#include <core/commands.hpp>

#include <google/protobuf/any.pb.h>
#include <google/protobuf/empty.pb.h>
#include <FlightSql.pb.h>

#include <cstdint>
#include <cstring>
#include <exception>
#include <string>
#include <vector>

namespace flight::rpc {

namespace {

// Flight's dissociated IPC: the client sends each message as a bare
// flatbuffer in data_header (no continuation framing), with its body in
// data_body. arrow's RecordBatchStreamReader wants a regular encapsulated
// stream, so each message is re-wrapped: continuation + u32 length + padded
// metadata + body — the only serialization glue this server owns.
void append_encapsulated(std::string& out, const std::string& header, const std::string& body) {
    const auto pad8 = [](std::size_t v) { return (v + 7) / 8 * 8; };
    std::size_t meta_len = pad8(header.size());
    out.append("\xff\xff\xff\xff", 4);
    const std::uint32_t len = static_cast<std::uint32_t>(meta_len);
    out.push_back(static_cast<char>(len));
    out.push_back(static_cast<char>(len >> 8));
    out.push_back(static_cast<char>(len >> 16));
    out.push_back(static_cast<char>(len >> 24));
    out.append(header);
    out.append(meta_len - header.size(), '\0');
    out.append(body);
}

// DoPut-bind batches -> arrow RecordBatches (the parameters as the client
// sent them; the engine adapter reads rows through tsl::arrow_to_chunk).
// The batches are zero-copy slices of the stream and outlive the DoPut call
// (prepared_bind keeps them), so the buffer takes ownership of the bytes.
std::vector<std::shared_ptr<arrow::RecordBatch>> read_bind_batches(std::string stream) {
    auto buffer = arrow::Buffer::FromString(std::move(stream));
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer));
    if (!reader.ok()) {
        throw std::runtime_error("bind stream: " + reader.status().ToString());
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    while (true) {
        auto batch = (*reader)->Next();
        if (!batch.ok()) {
            throw std::runtime_error("bind stream: " + batch.status().ToString());
        }
        if (!*batch) {
            break; // end of stream
        }
        batches.push_back(*batch);
    }
    return batches;
}

} // namespace

namespace {

constexpr std::uint64_t kFlightProtocolVersion = 1;

// asio-grpc 3.7 keeps RethrowFirstArg in detail — our own equivalent.
struct RethrowFirstArg {
    template <typename... Args>
    void operator()(const std::exception_ptr& ep, Args&&...) const {
        if (ep) {
            std::rethrow_exception(ep);
        }
    }
};

using AsyncService = fp::FlightService::AsyncService;
namespace fps = arrow::flight::protocol::sql;

void fill_flight_data(std::string_view header, std::string_view body, fp::FlightData* out) {
    out->set_data_header(header.data(), header.size());
    if (!body.empty()) {
        out->set_data_body(body.data(), body.size());
    }
}

} // namespace

// --- Handshake -------------------------------------------------------------

asio::awaitable<void> FlightServer::handle_handshake(auto& rpc) {
    fp::HandshakeRequest request;
    std::string bearer;
    // Basic-auth clients (arrow C++/pyarrow) send just the header and WritesDone —
    // not a single message; so we authorize before reading.
    const bool got_request = co_await rpc.read(request);
    const grpc::Status st =
        core_.auth().handshake(rpc.context(), got_request ? request : fp::HandshakeRequest{}, bearer);
    if (!st.ok()) {
        co_await rpc.finish(st);
        co_return;
    }
    // The client reads the token from initial metadata (arrow C++/Go clients).
    rpc.context().AddInitialMetadata("authorization", "Bearer " + bearer);
    while (co_await rpc.read(request)) {
    }
    fp::HandshakeResponse response;
    response.set_protocol_version(kFlightProtocolVersion);
    response.set_payload(bearer);
    co_await rpc.write(response);
    co_await rpc.finish(grpc::Status::OK);
}

// --- ListActions metadata ---------------------------------------------------

asio::awaitable<void> FlightServer::handle_list_actions(auto& rpc, fp::Empty&) {
    for (const auto& [type, description] : std::initializer_list<std::pair<const char*, const char*>>{
             {"CreatePreparedStatement", "Create a prepared statement"},
             {"ClosePreparedStatement", "Close a prepared statement"},
             {"CancelQuery", "Cancel a query"},
             {"CancelFlightInfo", "Cancel a query by FlightInfo"},
         }) {
        fp::ActionType action;
        action.set_type(type);
        action.set_description(description);
        if (!co_await rpc.write(action)) {
            co_return;
        }
    }
    co_await rpc.finish(grpc::Status::OK);
}

// --- Actions (prepared statements) ------------------------------------------

asio::awaitable<void> FlightServer::handle_do_action(auto& rpc, fp::Action& request) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    google::protobuf::Any request_any;
    if (!request_any.ParseFromString(request.body())) {
        co_await rpc.finish({grpc::StatusCode::INVALID_ARGUMENT, "flight-sql: malformed action body"});
        co_return;
    }

    grpc::Status status = grpc::Status::OK;
    fps::ActionCreatePreparedStatementResult create_result;

    if (request.type() == "CreatePreparedStatement") {
        fps::ActionCreatePreparedStatementRequest req;
        if (!request_any.Is<fps::ActionCreatePreparedStatementRequest>() ||
            !request_any.UnpackTo(&req)) {
            co_await rpc.finish({grpc::StatusCode::INVALID_ARGUMENT,
                                 "flight-sql: expected ActionCreatePreparedStatementRequest"});
            co_return;
        }
        std::string handle;
        status = core_.create_prepared(req.query(), &handle);
        if (!status.ok()) {
            co_await rpc.finish(status);
            co_return;
        }
        core::PreparedStatementState const* state = nullptr;
        if ((status = core_.prepared_lookup(handle, &state)); !status.ok()) {
            co_await rpc.finish(status);
            co_return;
        }
        create_result.set_prepared_statement_handle(handle);
        const auto schema_bytes = [](const std::shared_ptr<arrow::Schema>& schema,
                                     std::string* out) -> bool {
            if (!schema) return false;
            auto bytes = arrow::ipc::SerializeSchema(*schema);
            if (!bytes.ok()) return false;
            out->assign(reinterpret_cast<const char*>((*bytes)->data()),
                        static_cast<std::size_t>((*bytes)->size()));
            return true;
        };
        std::string bytes;
        if (schema_bytes(state->prepared.dataset_schema, &bytes)) {
            create_result.set_dataset_schema(bytes);
        }
        if (schema_bytes(state->prepared.parameter_schema, &bytes)) {
            create_result.set_parameter_schema(bytes);
        }
        google::protobuf::Any result_any;
        result_any.PackFrom(create_result);
        fp::Result result;
        result.set_body(result_any.SerializeAsString());
        if (!co_await rpc.write(result)) {
            co_return;
        }
    } else if (request.type() == "ClosePreparedStatement") {
        fps::ActionClosePreparedStatementRequest req;
        if (!request_any.Is<fps::ActionClosePreparedStatementRequest>() ||
            !request_any.UnpackTo(&req)) {
            co_await rpc.finish({grpc::StatusCode::INVALID_ARGUMENT,
                                 "flight-sql: expected ActionClosePreparedStatementRequest"});
            co_return;
        }
        if ((status = core_.close_prepared(req.prepared_statement_handle())); !status.ok()) {
            co_await rpc.finish(status);
            co_return;
        }
        google::protobuf::Any result_any;
        result_any.PackFrom(google::protobuf::Empty{});
        fp::Result result;
        result.set_body(result_any.SerializeAsString());
        if (!co_await rpc.write(result)) {
            co_return;
        }
    } else if (request.type() == "CancelQuery") {
        // Queries run to completion — nothing to cancel; answer the
        // unspecified result honestly.
        fps::ActionCancelQueryResult cancel_result;
        cancel_result.set_result(fps::ActionCancelQueryResult::CANCEL_RESULT_UNSPECIFIED);
        google::protobuf::Any result_any;
        result_any.PackFrom(cancel_result);
        fp::Result result;
        result.set_body(result_any.SerializeAsString());
        if (!co_await rpc.write(result)) {
            co_return;
        }
    } else if (request.type() == "CancelFlightInfo") {
        fp::CancelFlightInfoRequest req;
        if (!request_any.Is<fp::CancelFlightInfoRequest>() || !request_any.UnpackTo(&req)) {
            co_await rpc.finish({grpc::StatusCode::INVALID_ARGUMENT,
                                 "flight-sql: expected CancelFlightInfoRequest"});
            co_return;
        }
        fp::CancelFlightInfoResult cancel_result;
        cancel_result.set_status(fp::CANCEL_STATUS_UNSPECIFIED);
        google::protobuf::Any result_any;
        result_any.PackFrom(cancel_result);
        fp::Result result;
        result.set_body(result_any.SerializeAsString());
        if (!co_await rpc.write(result)) {
            co_return;
        }
    } else {
        co_await rpc.finish({grpc::StatusCode::UNIMPLEMENTED,
                             "flight-sql: unknown action type '" + request.type() + "'"});
        co_return;
    }
    co_await rpc.finish(grpc::Status::OK);
}

// --- Control plane ----------------------------------------------------------

asio::awaitable<void> FlightServer::handle_get_flight_info(auto& rpc, fp::FlightDescriptor& request) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish_with_error({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    std::string ticket;
    if (const auto st = core::execute_descriptor(core_, core_.engine(), request, &ticket);
        !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    fp::FlightInfo info;
    if (const auto st = core_.make_flight_info(request, ticket, &info); !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    co_await rpc.finish(info, grpc::Status::OK);
}

asio::awaitable<void> FlightServer::handle_poll_flight_info(auto& rpc, fp::FlightDescriptor& request) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish_with_error({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    // Queries run synchronously — return a completed PollInfo right away
    // (no flight_descriptor: the query is complete; progress = 1.0).
    std::string ticket;
    if (const auto st = core::execute_descriptor(core_, core_.engine(), request, &ticket);
        !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    fp::FlightInfo info;
    if (const auto st = core_.make_flight_info(request, ticket, &info); !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    fp::PollInfo poll;
    *poll.mutable_info() = std::move(info);
    poll.set_progress(1.0);
    co_await rpc.finish(poll, grpc::Status::OK);
}

asio::awaitable<void> FlightServer::handle_get_schema(auto& rpc, fp::FlightDescriptor& request) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish_with_error({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    std::string ticket;
    if (const auto st = core::execute_descriptor(core_, core_.engine(), request, &ticket);
        !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    fp::SchemaResult result;
    if (const auto st = core_.make_schema_result(ticket, &result); !st.ok()) {
        co_await rpc.finish_with_error(st);
        co_return;
    }
    co_await rpc.finish(result, grpc::Status::OK);
}

// --- Data plane: DoGet ------------------------------------------------------

asio::awaitable<void> FlightServer::handle_do_get(auto& rpc, fp::Ticket& request) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    core::CachedResult const* cached = nullptr;
    if (const auto st = core_.lookup(request.ticket(), &cached); !st.ok()) {
        co_await rpc.finish(st);
        co_return;
    }
    // The schema goes as the first message (dissociated IPC: header in data_header)
    {
        fp::FlightData data;
        fill_flight_data(cached->schema_message, {}, &data);
        if (!co_await rpc.write(data)) {
            co_return; // the client is gone
        }
    }
    for (const auto& batch : cached->batches) {
        fp::FlightData data;
        fill_flight_data(batch.metadata, batch.body, &data);
        if (!co_await rpc.write(data)) {
            co_return;
        }
    }
    co_await rpc.finish(grpc::Status::OK);
}

// --- Data plane: DoPut ------------------------------------------------------

asio::awaitable<void> FlightServer::handle_do_put(auto& rpc) {
    if (!core_.auth().authorize(rpc.context())) {
        co_await rpc.finish({grpc::StatusCode::UNAUTHENTICATED, "flight-sql: invalid bearer token"});
        co_return;
    }
    fp::FlightData data;
    bool have_descriptor = false;
    core::DoPutCommand command;
    grpc::Status command_status = grpc::Status::OK;

    std::string bind_stream;

    // An update (Statement/PreparedStatement) carries nothing but the command
    // descriptor, and clients read the PutResult BEFORE closing their side
    // (python flightsql-dbapi: `reader.read(); writer.close()`) — answering
    // only after the client's WritesDone would deadlock both ends. So the
    // first FlightData is read, and for an update the answer goes out at once,
    // finishing the RPC: the client's later close lands on an already-finished
    // call, which every driver handles.
    while (co_await rpc.read(data)) {
        if (!have_descriptor) {
            have_descriptor = true;
            command_status =
                core::do_put_command(data.flight_descriptor(), &command);
            if (!command_status.ok()) break;
            if (command.kind != core::DoPutKind::PreparedBind) {
                break; // Update / PreparedUpdate: nothing more to read
            }
        }
        if (command.kind == core::DoPutKind::PreparedBind) {
            append_encapsulated(bind_stream, data.data_header(), data.data_body());
        }
    }
    core::BoundParams bound_rows;
    if (command.kind == core::DoPutKind::PreparedBind && command_status.ok()) {
        try {
            bound_rows = read_bind_batches(std::move(bind_stream));
        } catch (const std::exception& e) {
            command_status = {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
        }
    }
    if (!command_status.ok()) {
        co_await rpc.finish(command_status);
        co_return;
    }
    if (!have_descriptor) {
        co_await rpc.finish(grpc::Status{grpc::StatusCode::INVALID_ARGUMENT,
                                         "flight-sql: DoPut requires a command descriptor"});
        co_return;
    }

    std::int64_t record_count = 0;
    grpc::Status status = grpc::Status::OK;
    try {
        switch (command.kind) {
            case core::DoPutKind::Update: {
                record_count = core_.engine().execute_update(command.query);
                break;
            }
            case core::DoPutKind::PreparedUpdate: {
                core::PreparedStatementState const* state = nullptr;
                if ((status = core_.prepared_lookup(command.handle, &state)); status.ok()) {
                    record_count =
                        core_.engine().execute_update_prepared(state->prepared, state->bound);
                }
                break;
            }
            case core::DoPutKind::PreparedBind: {
                status = core_.prepared_bind(command.handle, std::move(bound_rows));
                break;
            }
        }
    } catch (const core::EngineError& e) {
        status = {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
    } catch (const std::exception& e) {
        status = {grpc::StatusCode::INTERNAL, e.what()};
    }
    if (!status.ok()) {
        co_await rpc.finish(status);
        co_return;
    }

    fp::PutResult result;
    if (command.kind != core::DoPutKind::PreparedBind) {
        fps::DoPutUpdateResult update_result;
        update_result.set_record_count(record_count);
        // De-facto standard (arrow-go, the reference server): app_metadata = the bare
        // serialized DoPutUpdateResult, without an Any wrapper.
        result.set_app_metadata(update_result.SerializeAsString());
        // The PutResult goes out WITHOUT finishing the call, then the client's
        // side is drained to its WritesDone: finishing first races the message
        // with the stream teardown (a pyarrow client that closes its writer
        // before reading sees no metadata at all), while answering only after
        // the client's close deadlocks the driver that reads the PutResult
        // first (python flightsql-dbapi). Write-then-drain serves both.
        if (!co_await rpc.write(result)) {
            co_return; // the client is gone
        }
        while (co_await rpc.read(data)) {
            // nothing more to take: an update carries no batches
        }
        co_await rpc.finish(grpc::Status::OK);
        co_return;
    }
    co_await rpc.write_and_finish(result, grpc::Status::OK);
}

// --- Stubs -------------------------------------------------------------------

asio::awaitable<void> FlightServer::handle_unimplemented(auto& rpc, auto&) {
    const grpc::Status status{grpc::StatusCode::UNIMPLEMENTED, "flight-sql: not implemented yet"};
    if constexpr (requires(decltype(rpc) & r, grpc::Status s) { r.finish_with_error(s); }) {
        co_await rpc.finish_with_error(status);
    } else {
        co_await rpc.finish(status);
    }
}

asio::awaitable<void> FlightServer::handle_unimplemented(auto& rpc) {
    co_await rpc.finish(grpc::Status{grpc::StatusCode::UNIMPLEMENTED, "flight-sql: not implemented yet"});
}

// --- Registration ------------------------------------------------------------

void FlightServer::register_handlers(agrpc::GrpcContext& grpc_context) {
    using agrpc::register_awaitable_rpc_handler;

    {
        using R = agrpc::ServerRPC<&AsyncService::RequestHandshake>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc) { return handle_handshake(rpc); },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestListActions>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::Empty& req) {
                                              return handle_list_actions(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestGetFlightInfo>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::FlightDescriptor& req) {
                                              return handle_get_flight_info(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestGetSchema>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::FlightDescriptor& req) {
                                              return handle_get_schema(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestDoGet>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::Ticket& req) {
                                              return handle_do_get(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestDoPut>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc) { return handle_do_put(rpc); },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestListFlights>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::Criteria& req) {
                                              return handle_unimplemented(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestPollFlightInfo>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::FlightDescriptor& req) {
                                              return handle_poll_flight_info(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestDoAction>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc, fp::Action& req) {
                                              return handle_do_action(rpc, req);
                                          },
                                          RethrowFirstArg{});
    }
    {
        using R = agrpc::ServerRPC<&AsyncService::RequestDoExchange>;
        register_awaitable_rpc_handler<R>(grpc_context, service_,
                                          [this](R& rpc) { return handle_unimplemented(rpc); },
                                          RethrowFirstArg{});
    }
}

} // namespace flight::rpc
