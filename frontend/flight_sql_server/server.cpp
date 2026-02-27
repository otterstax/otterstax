// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "server.hpp"
#include "batch_reader.hpp"

#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "utility/connection_uid.hpp"
#include "utility/timer.hpp"
#include "utility/logger.hpp"

#include "otterbrix/config.hpp"
#include "routes/scheduler.hpp"

#include "../../utility/cv_wrapper.hpp"
#include "../../utility/session.hpp"

#include <boost/mysql/results.hpp>
#include <spdlog/spdlog.h>

#include <chrono>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/utils.hpp>
#include <deque>
#include <thread>

#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/status.h"

#include "../../otterbrix/query_generation/sql_query_generator.hpp"

using namespace components;
using namespace components::cursor;
using namespace std::chrono_literals;

// otterbrix related
inline void clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

template<typename T>
struct QueryHandleWaiter {
    std::vector<std::future<T>> futures;
    std::vector<T> results;
    void wait() {
        for (auto& future : futures) {
            auto result = future.get();
            // Received from DB rows
            results.push_back(std::move(result));
        }
        // Finished
    }
};

// Create a Ticket that combines a SQL query, transaction ID, and session hash.
arrow::Result<arrow::flight::Ticket> EncodeTransactionQuery(TicketData data) {
    std::string transaction_query = data.sql_query;
    transaction_query += ':';
    transaction_query += data.transaction_id;
    transaction_query += ':';
    transaction_query += std::to_string(data.session_hash);
    auto ticket_string = arrow::flight::sql::CreateStatementQueryTicket(transaction_query).ValueOrDie();
    return arrow::flight::Ticket{std::move(ticket_string)};
}

// Decode input ticket to query, transaction ID, and session hash
arrow::Result<TicketData> DecodeTransactionQuery(const std::string& ticket) {
    // Decode ticket

    // SQL & transaction_id
    auto first_divider = ticket.find(':');
    if (first_divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket: missing first divider");
    }

    // transaction_id & session_hash
    auto second_divider = ticket.find(':', first_divider + 1);
    if (second_divider == std::string::npos) {
        return arrow::Status::Invalid("Malformed ticket: missing second divider");
    }

    std::string sql_query = ticket.substr(0, first_divider);
    std::string transaction_id = ticket.substr(first_divider + 1, second_divider - first_divider - 1);
    std::string session_str = ticket.substr(second_divider + 1);

    try {
        size_t session_hash = std::stoul(session_str);
        return TicketData(std::move(sql_query), std::move(transaction_id), session_hash);
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to extract session hash from string: " + session_str +
                                      " error: " + e.what());
    }
}

SimpleFlightSQLServer::SimpleFlightSQLServer(const Config& config)
    : log_(get_logger(logger_tag::FLIGHTSQL_SERVER))
    , location_(arrow::flight::Location::ForGrpcTcp(config.host, config.port).ValueOrDie())
    , resource_(config.resource)
    , catalog_address_(config.catalog_address)
    , scheduler_address_(config.scheduler_address) {
    assert(log_.is_valid());
    log_->info("FlightSQLServer initialized successfully");
}

// Method in arrow::flight::sql to start communication
arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
SimpleFlightSQLServer::GetFlightInfoStatement(const arrow::flight::ServerCallContext& context,
                                              const arrow::flight::sql::StatementQuery& command,
                                              const arrow::flight::FlightDescriptor& descriptor) {
    Timer timer("GetFlightInfoStatement");
    session_id id;
    const std::string& query = command.query;
    log_->debug("Received query in ticket: {}", query);
    auto ticket = EncodeTransactionQuery({query, command.transaction_id, id.hash()}).ValueOrDie();
    log_->trace("ticket: {}", ticket.ToString());

    auto shared_data = create_cv_wrapper(flight_data(resource_));
    actor_zeta::send(scheduler_address_,
                     scheduler_address_,
                     scheduler::handler_id(scheduler::route::prepare_schema),
                     id.hash(),
                     shared_data,
                     query);
    shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

    if (shared_data->status() == cv_wrapper::Status::Ok) {
        std::shared_ptr<arrow::Schema> schema = to_arrow_schema(shared_data->result.schema);
        std::vector<arrow::flight::FlightEndpoint> endpoints{
            arrow::flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};

        const bool ordered = false;
        auto result = arrow::flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered).ValueOrDie();
        return std::make_unique<arrow::flight::FlightInfo>(result);
    } else if (shared_data->status() == cv_wrapper::Status::Timeout) {
        log_->warn("Timeout while preparing query: {}", query);
        return arrow::Status::Invalid("Timeout while preparing query: " + query);
    } else {
        log_->error("Error while GetFlightInfoStatement: {}", shared_data->error_message());
        return arrow::Status::Invalid("Error while GetFlightInfoStatement: " + shared_data->error_message());
    }
}

// main execute method
arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
SimpleFlightSQLServer::DoGetStatement(const arrow::flight::ServerCallContext& context,
                                      const arrow::flight::sql::StatementQueryTicket& command) {
    Timer timer("DoGetStatement");
    // log_->trace("[DOGET] Thread id: {}", std::this_thread::get_id()); // fmt doesn't format thread::id

    try {
        const auto& [query, transaction_id, session_hash] =
            DecodeTransactionQuery(command.statement_handle).ValueOrDie();

        // Log the received ticket, assuming the query is stored in the ticket
        log_->debug("Received query in ticket: {} Session hash: {} Transaction ID: {}",
                    query,
                    session_hash,
                    transaction_id);
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        actor_zeta::send(scheduler_address_,
                         scheduler_address_,
                         scheduler::handler_id(scheduler::route::execute_statement),
                         session_hash,
                         shared_data);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        if (shared_data->status() == cv_wrapper::Status::Ok) {
            log_->debug("[DOGET] Scheduler finished successfully, rows size: {}", shared_data->result.chunk.size());
            auto chunk_res = std::move(shared_data->result.chunk);
            timer.timePoint("[DOGET] Scheduler finished successfully");

            auto schema = to_arrow_schema(shared_data->result.schema);
            auto batch_reader = ChunkBatchReader::Make(std::move(schema), std::move(chunk_res)).ValueOrDie();
            // Use a record batch stream
            log_->trace("[ARROW FLIGHT SERVER] Send data");
            timer.timePoint("[DOGET] datastream created");
            return std::make_unique<arrow::flight::RecordBatchStream>(batch_reader);
        } else if (shared_data->status() == cv_wrapper::Status::Empty) {
            std::shared_ptr<arrow::Schema> schema;
            auto chunk_res = std::move(shared_data->result.chunk);
            log_->warn("[Otterbrix]: result cursor size : {}", chunk_res.size());
            auto batch_reader = ChunkBatchReader::Make(arrow::schema({}), std::move(chunk_res)).ValueOrDie();
            return std::make_unique<arrow::flight::RecordBatchStream>(batch_reader);
        } else if (shared_data->status() == cv_wrapper::Status::Timeout) {
            log_->warn("Timeout while executing query: {}", query);
            return arrow::Status::Invalid("Timeout while executing query: " + query);
        } else {
            log_->error("Error while DOGET: {}", shared_data->error_message());
            return arrow::Status::Invalid("Error while DOGET: " + shared_data->error_message());
        }
    } catch (const std::exception& e) {
        log_->error("Error: {}", e.what());
        return arrow::Status::Invalid("Error: " + std::string(e.what()));
    } catch (...) {
        log_->error("Error: unknown");
        return arrow::Status::Invalid("Error while DOGET: unknown");
    }
}

arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
SimpleFlightSQLServer::GetFlightInfoTables(const arrow::flight::ServerCallContext& context,
                                           const arrow::flight::sql::GetTables& command,
                                           const arrow::flight::FlightDescriptor& descriptor) {
    Timer timer("GetFlightInfoTables");
    std::vector<arrow::flight::FlightEndpoint> endpoints{{arrow::flight::Ticket{descriptor.cmd}, {}, std::nullopt, ""}};
    auto schema = command.include_schema ? *arrow::flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                                         : *arrow::flight::sql::SqlSchema::GetTablesSchema();
    ARROW_ASSIGN_OR_RAISE(auto result, arrow::flight::FlightInfo::Make(schema, descriptor, endpoints, -1, -1, false))

    return std::make_unique<arrow::flight::FlightInfo>(std::move(result));
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
SimpleFlightSQLServer::DoGetTables(const arrow::flight::ServerCallContext& context,
                                   const arrow::flight::sql::GetTables& command) {
    Timer timer("DoGetTables");
    bool include_schema = command.include_schema;
    arrow::StringBuilder catalog_builder;
    arrow::StringBuilder db_schema_builder;
    arrow::StringBuilder table_name_builder;
    arrow::StringBuilder table_type_builder;
    std::unique_ptr<arrow::BinaryBuilder> table_schema_builder = nullptr;

    if (include_schema) {
        table_schema_builder = std::make_unique<arrow::BinaryBuilder>();
    }

    auto shared_data = create_cv_wrapper(std::pmr::vector<table_info>(resource_));
    actor_zeta::send(catalog_address_,
                     catalog_address_,
                     catalog_manager::handler_id(catalog_manager::route::get_tables),
                     command,
                     shared_data);
    shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

    // otherwise always Ok or Empty
    if (shared_data->status() == cv_wrapper::Status::Timeout) {
        log_->warn("Timeout while getting tables");
        return arrow::Status::Invalid("Timeout while getting tables");
    }

    for (const auto& table : shared_data->result) {
        ARROW_RETURN_NOT_OK(catalog_builder.Append(table.name.database));
        ARROW_RETURN_NOT_OK(db_schema_builder.Append(table.name.schema));
        ARROW_RETURN_NOT_OK(table_name_builder.Append(table.name.collection));
        ARROW_RETURN_NOT_OK(table_type_builder.Append("TABLE")); // "TABLE", "VIEW", etc.

        if (include_schema && table_schema_builder) {
            auto table_arrow_schema = to_arrow_schema(table.schema);
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
    auto batch = arrow::RecordBatch::Make(schema, shared_data->result.size(), arrays);
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
    return std::make_unique<arrow::flight::RecordBatchStream>(reader);
}

arrow::Result<int64_t>
SimpleFlightSQLServer::DoPutCommandStatementUpdate(const arrow::flight::ServerCallContext& context,
                                                   const arrow::flight::sql::StatementUpdate& command) {
    Timer timer("DoPutCommandStatementUpdate");
    try {
        // Log the received ticket, assuming the query is stored in the ticket
        log_->debug("Received query in ticket: {} Id: {}", command.query, command.transaction_id);
        auto shared_data = create_cv_wrapper(flight_data(resource_));
        session_id id;
        actor_zeta::send(scheduler_address_,
                         scheduler_address_,
                         scheduler::handler_id(scheduler::route::execute),
                         id.hash(),
                         shared_data,
                         command.query);
        shared_data->wait_for(cv_wrapper::DEFAULT_TIMEOUT);

        int64_t affected_rows = 0;

        if (shared_data->status() == cv_wrapper::Status::Ok) {
            affected_rows = shared_data->result.chunk.size();
            log_->debug("[DoPutCommandStatementUpdate] Scheduler finished successfully, rows size: {}", affected_rows);
            timer.timePoint("[DoPutCommandStatementUpdate] Scheduler finished successfully");

            log_->debug("[DoPutCommandStatementUpdate] Affected rows: {}", affected_rows);
            log_->trace("[ARROW FLIGHT SERVER] Send data");
            timer.timePoint("[DoPutCommandStatementUpdate] datastream created");
            return affected_rows;
        } else if (shared_data->status() == cv_wrapper::Status::Empty) {
            affected_rows = shared_data->result.chunk.size();
            log_->warn("[Otterbrix]: result cursor size : {}", affected_rows);
            return affected_rows;
        } else if (shared_data->status() == cv_wrapper::Status::Timeout) {
            log_->warn("Timeout while executing query: {}", command.query);
            return arrow::Status::Invalid("Timeout while executing query: " + command.query);
        } else {
            log_->error("Error while DoPutCommandStatementUpdate: {}", shared_data->error_message());
            return arrow::Status::Invalid("Error while DoPutCommandStatementUpdate: " + shared_data->error_message());
        }
    } catch (const boost::mysql::error_with_diagnostics& err) {
        // Some errors include additional diagnostics, like server-provided error messages.
        // Security note: diagnostics::server_message may contain s-supplied values (e.g. the
        // field value that caused the error) and is encoded using to the connection's character set
        // (UTF-8 by default). Treat is as untrusted input.
        log_->error("Error: {}, error code: {} Server diagnostics: {}",
                    err.what(),
                    err.code().value(),
                    err.get_diagnostics().server_message());
        return arrow::Status::Invalid("Arrow server error: " + std::string(err.what()) +
                                      " message: " + std::string(err.get_diagnostics().server_message()));
    } catch (const std::exception& e) {
        log_->error("Error: {}", e.what());
        return arrow::Status::Invalid("Error: " + std::string(e.what()));
    } catch (...) {
        log_->error("Error: unknown");
        return arrow::Status::Invalid("Error while DoPutCommandStatementUpdate: unknown");
    }
}

// Start the Flight SQL server
arrow::Status SimpleFlightSQLServer::Start() {
    arrow::flight::FlightServerOptions options(location_);
    options.auth_handler = std::make_shared<arrow::flight::NoOpAuthHandler>();
    // auto auth_handler = std::make_shared<MockAuthHandler>();
    // options.auth_handler = auth_handler;
    // options.verify_client = false;
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