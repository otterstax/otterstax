// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "server.hpp"
#include "batch_reader.hpp"

#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "utility/connection_uid.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include "catalog/catalog_manager.hpp"
#include "otterbrix/config.hpp"
#include "scheduler/scheduler.hpp"

#include "../../utility/session.hpp"

#include <spdlog/spdlog.h>

#include <charconv>
#include <chrono>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/utils.hpp>
#include <string_view>
#include <system_error>

#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"

#include "../../otterbrix/query_generation/sql_query_generator.hpp"

using namespace components;
using namespace components::cursor;
using namespace std::chrono_literals;

namespace {

    // A column the translator cannot map to Arrow is reported as a Status; the
    // error text is owned by the server's resource for the length of the call.
    arrow::Result<std::shared_ptr<arrow::Schema>> arrow_schema_of(std::pmr::memory_resource* resource,
                                                                  const types::complex_logical_type& schema) {
        auto converted = to_arrow_schema(resource, schema);
        if (converted.has_error()) {
            return arrow::Status::Invalid("Cannot map the result schema to Arrow: ", converted.error().what.c_str());
        }
        return std::move(converted.value());
    }

    // The schema handed out in FlightInfo is the stream's contract, so a
    // row-producing statement whose prepare left it unresolved cannot be
    // served: DoGet would stream rows of no columns. A parameterized statement
    // is refused whatever its prepared schema says — the catalog resolves the
    // projection of a remote SELECT independently of its parameters, but
    // nothing binds them before the DoGet of this RPC.
    arrow::Status check_schema_resolved(const session_payload& prepared) {
        if (prepared.tag != T_SelectStmt) {
            return arrow::Status::OK();
        }
        if (prepared.parameter_count > 0) {
            return arrow::Status::Invalid("FlightSQL: the statement has ", prepared.parameter_count,
                                          " unbound parameter(s); GetFlightInfoStatement takes no parameters");
        }
        if (prepared.schema.type() != types::logical_type::STRUCT) {
            return arrow::Status::Invalid("FlightSQL: the result schema of the statement could not be resolved");
        }
        return arrow::Status::OK();
    }

    // The record batch stream encodes scalar columns only. A nested column is
    // refused when the schema is handed out, so a client never receives a
    // FlightInfo whose DoGet cannot be served.
    arrow::Status check_stream_encodable(const arrow::Schema& schema) {
        for (const auto& field : schema.fields()) {
            if (arrow::is_nested(field->type()->id())) {
                return arrow::Status::NotImplemented("FlightSQL: column '", field->name(), "' has nested type ",
                                                     field->type()->ToString(),
                                                     ", which the record batch stream cannot encode");
            }
        }
        return arrow::Status::OK();
    }

    // Everything GetFlightInfoStatement decides after the Worker has stored the
    // statement, in one place: a refusal here leaves an entry on the Worker
    // that no DoGet will ever consume, and the caller releases it.
    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
    make_statement_info(std::pmr::memory_resource* resource,
                        const session_payload& prepared,
                        arrow::flight::Ticket ticket,
                        const arrow::flight::FlightDescriptor& descriptor) {
        ARROW_RETURN_NOT_OK(check_schema_resolved(prepared));
        ARROW_ASSIGN_OR_RAISE(auto schema, arrow_schema_of(resource, prepared.schema));
        ARROW_RETURN_NOT_OK(check_stream_encodable(*schema));
        std::vector<arrow::flight::FlightEndpoint> endpoints{
            arrow::flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};

        const bool ordered = false;
        ARROW_ASSIGN_OR_RAISE(auto result,
                              arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));
        return std::make_unique<arrow::flight::FlightInfo>(std::move(result));
    }

    arrow::Status await_error(const core::error_t& error, const char* stage) {
        if (error.type == otterstax::AWAIT_TIMEOUT_CODE) {
            return arrow::Status::Invalid("Timeout while ", stage);
        }
        return arrow::Status::Invalid("Error while ", stage, ": ", error.what.c_str());
    }

} // namespace

// otterbrix related
inline void clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

arrow::Result<arrow::flight::Ticket> EncodeTransactionQuery(TicketData data) {
    std::string transaction_query = data.sql_query;
    transaction_query += ':';
    transaction_query += data.transaction_id;
    transaction_query += ':';
    transaction_query += std::to_string(data.session_hash);
    ARROW_ASSIGN_OR_RAISE(auto ticket_string, arrow::flight::sql::CreateStatementQueryTicket(transaction_query));
    return arrow::flight::Ticket{std::move(ticket_string)};
}

arrow::Result<TicketData> DecodeTransactionQuery(const std::string& ticket) {
    const auto hash_divider = ticket.rfind(':');
    if (hash_divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket: missing session hash divider");
    }
    const auto txn_divider = hash_divider == 0 ? std::string::npos : ticket.rfind(':', hash_divider - 1);
    if (txn_divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket: missing transaction id divider");
    }

    const std::string_view session_str{ticket.data() + hash_divider + 1, ticket.size() - hash_divider - 1};
    session_hash_t session_hash = 0;
    const char* const end = session_str.data() + session_str.size();
    const auto [parsed_to, ec] = std::from_chars(session_str.data(), end, session_hash);
    if (session_str.empty() || ec != std::errc{} || parsed_to != end) {
        return arrow::Status::Invalid("Malformed ticket: session hash '", session_str, "' is not an unsigned integer");
    }

    return TicketData{ticket.substr(0, txn_divider),
                      ticket.substr(txn_divider + 1, hash_divider - txn_divider - 1),
                      session_hash};
}

SimpleFlightSQLServer::SimpleFlightSQLServer(const Config& config)
    : log_(get_logger(logger_tag::FLIGHTSQL_SERVER))
    , host_(config.host)
    , port_(config.port)
    , resource_(config.resource)
    , catalog_address_(config.catalog_address)
    , scheduler_address_(config.scheduler_address) {
    assert(log_.is_valid());
    assert(resource_ != nullptr && "memory resource must not be null");
    log_->info("FlightSQLServer initialized successfully");
}

// Method in arrow::flight::sql to start communication
arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
SimpleFlightSQLServer::GetFlightInfoStatement(const arrow::flight::ServerCallContext& context,
                                              const arrow::flight::sql::StatementQuery& command,
                                              const arrow::flight::FlightDescriptor& descriptor) {
    OTX_ZONE_N("flight::GetFlightInfoStatement");
    log_->debug("[GetFlightInfoStatement] Start");
    session_id id;
    const std::string& query = command.query;
    log_->debug("[GetFlightInfoStatement] Received query: {}", query);
    log_->debug("[GetFlightInfoStatement] Encoding ticket...");
    ARROW_ASSIGN_OR_RAISE(auto ticket, EncodeTransactionQuery({query, command.transaction_id, id.hash()}));
    log_->debug("[GetFlightInfoStatement] Encoded ticket, sending to scheduler...");

    // sending to the Scheduler event-loop always returns needs_sched=false
    [[maybe_unused]] auto [needs_sched, fut] =
        actor_zeta::send(scheduler_address_, &Scheduler::prepare_schema, id.hash(), query);
    log_->debug("[GetFlightInfoStatement] Waiting for response...");
    auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

    if (r.has_error()) {
        if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
            log_->warn("Timeout while preparing query: {}", query);
        } else {
            log_->error("Error while GetFlightInfoStatement: {}", r.error().what);
        }
        return await_error(r.error(), "GetFlightInfoStatement");
    }

    auto info = make_statement_info(resource_, r.value(), std::move(ticket), descriptor);
    if (!info.ok()) {
        // No ticket goes out, so no DoGet will consume the stored statement:
        // release it now. A failed release is logged; the refusal itself is
        // what the client gets.
        // sending to the Scheduler event-loop always returns needs_sched=false
        [[maybe_unused]] auto [ns_close, close_fut] =
            actor_zeta::send(scheduler_address_, &Scheduler::close_statement, id.hash());
        auto closed = otterstax::await_future_blocking<session_payload>(std::move(close_fut), resource_);
        if (closed.has_error()) {
            log_->warn("close_statement after a refused GetFlightInfoStatement failed: {}", closed.error().what);
        }
    }
    return info;
}

// main execute method
arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
SimpleFlightSQLServer::DoGetStatement(const arrow::flight::ServerCallContext& context,
                                      const arrow::flight::sql::StatementQueryTicket& command) {
    OTX_ZONE_N("flight::DoGetStatement");
    ARROW_ASSIGN_OR_RAISE(auto ticket, DecodeTransactionQuery(command.statement_handle));
    const auto& [query, transaction_id, session_hash] = ticket;

    log_->debug("Received query in ticket: {} Session hash: {} Transaction ID: {}",
                query,
                session_hash,
                transaction_id);
    // sending to the Scheduler event-loop always returns needs_sched=false
    [[maybe_unused]] auto [needs_sched, fut] =
        actor_zeta::send(scheduler_address_, &Scheduler::execute_statement, session_hash);
    auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

    if (r.has_error()) {
        if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
            log_->warn("Timeout while executing query: {}", query);
        } else {
            log_->error("Error while DOGET: {}", r.error().what);
        }
        return await_error(r.error(), "DOGET");
    }

    session_payload sdata_result = std::move(r.value());
    log_->debug("[DOGET] Scheduler finished successfully, rows size: {}", sdata_result.size());
    // The stream carries the schema the prepare phase resolved — the one
    // FlightInfo promised the client — and the reader matches every chunk
    // column to it; a chunk that does not fit is a stream error, never a
    // re-derived schema.
    ARROW_ASSIGN_OR_RAISE(auto schema, arrow_schema_of(resource_, sdata_result.schema));
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, ChunkBatchReader::Make(std::move(schema), std::move(sdata_result.chunks)));
    log_->trace("[ARROW FLIGHT SERVER] Send data");
    return std::make_unique<arrow::flight::RecordBatchStream>(batch_reader);
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
SimpleFlightSQLServer::GetFlightInfoTables(const arrow::flight::ServerCallContext& context,
                                           const arrow::flight::sql::GetTables& command,
                                           const arrow::flight::FlightDescriptor& descriptor) {
    OTX_ZONE_N("flight::GetFlightInfoTables");
    std::vector<arrow::flight::FlightEndpoint> endpoints{{arrow::flight::Ticket{descriptor.cmd}, {}, std::nullopt, ""}};
    auto schema = command.include_schema ? *arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                                         : *arrow::flight::sql::SqlSchema::GetTablesSchema();
    ARROW_ASSIGN_OR_RAISE(auto result, arrow::flight::FlightInfo::Make(schema, descriptor, endpoints, -1, -1, false))

    return std::make_unique<arrow::flight::FlightInfo>(std::move(result));
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
SimpleFlightSQLServer::DoGetTables(const arrow::flight::ServerCallContext& context,
                                   const arrow::flight::sql::GetTables& command) {
    OTX_ZONE_N("flight::DoGetTables");
    bool include_schema = command.include_schema;
    arrow::StringBuilder catalog_builder;
    arrow::StringBuilder db_schema_builder;
    arrow::StringBuilder table_name_builder;
    arrow::StringBuilder table_type_builder;
    std::unique_ptr<arrow::BinaryBuilder> table_schema_builder = nullptr;

    if (include_schema) {
        table_schema_builder = std::make_unique<arrow::BinaryBuilder>();
    }

    [[maybe_unused]] auto [needs_sched, fut] =
        actor_zeta::send(catalog_address_, &mysql::CatalogManager::get_tables, command);
    auto r = otterstax::await_future_blocking<std::pmr::vector<table_info>>(std::move(fut), resource_);

    if (r.has_error()) {
        if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
            log_->warn("Timeout while getting tables");
        } else {
            log_->error("Error while DoGetTables: {}", r.error().what);
        }
        return await_error(r.error(), "DoGetTables");
    }
    auto sdata_result = std::move(r.value());

    for (const auto& table : sdata_result) {
        ARROW_RETURN_NOT_OK(catalog_builder.Append(table.name.database));
        ARROW_RETURN_NOT_OK(db_schema_builder.Append(table.name.schema));
        ARROW_RETURN_NOT_OK(table_name_builder.Append(table.name.collection));
        // the same value CatalogManager::get_tables filters `table_types` by
        ARROW_RETURN_NOT_OK(table_type_builder.Append(catalog_ext::table_type_name));

        if (include_schema && table_schema_builder) {
            ARROW_ASSIGN_OR_RAISE(auto table_arrow_schema, arrow_schema_of(resource_, table.schema));
            ARROW_ASSIGN_OR_RAISE(auto serialized_schema, arrow::ipc::SerializeSchema(*table_arrow_schema));
            ARROW_RETURN_NOT_OK(table_schema_builder->Append(serialized_schema->data(), serialized_schema->size()));
        }
    }

    ARROW_ASSIGN_OR_RAISE(auto catalog_array, catalog_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto db_schema_array, db_schema_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto table_name_array, table_name_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto table_type_array, table_type_builder.Finish());

    std::vector<std::shared_ptr<arrow::Array>> arrays = {catalog_array,
                                                         db_schema_array,
                                                         table_name_array,
                                                         table_type_array};

    if (include_schema && table_schema_builder) {
        ARROW_ASSIGN_OR_RAISE(auto table_schema_array, table_schema_builder->Finish());
        arrays.push_back(table_schema_array);
    }

    auto schema = include_schema ? arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                                 : arrow::flight::sql::SqlSchema::GetTablesSchema();
    auto batch = arrow::RecordBatch::Make(schema, sdata_result.size(), arrays);
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
    return std::make_unique<arrow::flight::RecordBatchStream>(reader);
}

arrow::Result<int64_t>
SimpleFlightSQLServer::DoPutCommandStatementUpdate(const arrow::flight::ServerCallContext& context,
                                                   const arrow::flight::sql::StatementUpdate& command) {
    OTX_ZONE_N("flight::DoPutCommandStatementUpdate");
    log_->debug("Received query in ticket: {} Id: {}", command.query, command.transaction_id);
    session_id id;
    // sending to the Scheduler event-loop always returns needs_sched=false
    [[maybe_unused]] auto [needs_sched, fut] =
        actor_zeta::send(scheduler_address_, &Scheduler::execute, id.hash(), command.query);
    auto r = otterstax::await_future_blocking<session_payload>(std::move(fut), resource_);

    if (r.has_error()) {
        if (r.error().type == otterstax::AWAIT_TIMEOUT_CODE) {
            log_->warn("Timeout while executing query: {}", command.query);
        } else {
            log_->error("Error while DoPutCommandStatementUpdate: {}", r.error().what);
        }
        return await_error(r.error(), "DoPutCommandStatementUpdate");
    }

    const int64_t affected_rows = static_cast<int64_t>(r.value().size());
    log_->debug("[DoPutCommandStatementUpdate] Scheduler finished successfully, affected rows: {}", affected_rows);
    log_->trace("[ARROW FLIGHT SERVER] Send data");
    return affected_rows;
}

// Start the Flight SQL server
arrow::Status SimpleFlightSQLServer::Start() {
    ARROW_ASSIGN_OR_RAISE(auto location, arrow::flight::Location::ForGrpcTcp(host_, port_));
    arrow::flight::FlightServerOptions options(location);
    options.auth_handler = std::make_shared<arrow::flight::NoOpAuthHandler>();
    const auto status_init = this->Init(options);
    if (status_init != arrow::Status::OK()) {
        log_->error("Init error: {}", status_init.ToString());
        return status_init;
    }

    const auto status_set_sig = this->SetShutdownOnSignals({SIGTERM});
    if (status_set_sig != arrow::Status::OK()) {
        log_->error("SetShutdownOnSignals error: {}", status_set_sig.ToString());
        return status_set_sig;
    }

    spdlog::info("Flight SQL server try started on {}", options.location.ToString());
    const auto status_serve = this->Serve();
    if (status_serve != arrow::Status::OK()) {
        log_->error("Serve error: {}", status_serve.ToString());
        return status_serve;
    }
    return arrow::Status::OK();
}
