// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../catalog/catalog_manager.hpp"
#include "../../connectors/mysql_manager.hpp"
#include "../../otterbrix/operators/execute_plan.hpp"
#include "../../otterbrix/parser/parser.hpp"
// #include "../db_integration/nosql/connection_manager.hpp"
#include "../../db_integration/otterbrix/otterbrix_manager.hpp"
#include "../../db_integration/sql/connection_manager.hpp"
#include "../../otterbrix/config.hpp"
#include "../../scheduler/scheduler.hpp"
#include "../../utility/shared_flight_data.hpp"
#include "../../utility/table_info.hpp"

#include <boost/mysql/results.hpp>
#include <components/log/log.hpp>

#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"
#include "arrow/flight/sql/server.h"

#include <actor-zeta.hpp>
#include <otterbrix/otterbrix.hpp>

#include <memory_resource>
#include <string>

struct Config {
    std::string host;
    int port;
    std::pmr::memory_resource* resource{nullptr};
    actor_zeta::address_t catalog_address;
    actor_zeta::address_t scheduler_address;
};

struct TicketData {
    std::string sql_query;
    std::string transaction_id;
    session_hash_t session_hash;
};

class SimpleFlightSQLServer : public arrow::flight::sql::FlightSqlServerBase {
public:
    explicit SimpleFlightSQLServer(const Config& config);
    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
    GetFlightInfoStatement(const arrow::flight::ServerCallContext& context,
                           const arrow::flight::sql::StatementQuery& command,
                           const arrow::flight::FlightDescriptor& descriptor) override;
    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
    DoGetStatement(const arrow::flight::ServerCallContext& context,
                   const arrow::flight::sql::StatementQueryTicket& command) override;
    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
    GetFlightInfoTables(const arrow::flight::ServerCallContext& context,
                        const arrow::flight::sql::GetTables& command,
                        const arrow::flight::FlightDescriptor& descriptor) override;
    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
    DoGetTables(const arrow::flight::ServerCallContext& context, const arrow::flight::sql::GetTables& command) override;
    arrow::Result<int64_t> DoPutCommandStatementUpdate(const arrow::flight::ServerCallContext& context,
                                                       const arrow::flight::sql::StatementUpdate& command) override;
    arrow::Status Start();

private:
    log_t log_;
    arrow::flight::Location location_;
    std::pmr::memory_resource* resource_{nullptr};
    actor_zeta::address_t catalog_address_;
    actor_zeta::address_t scheduler_address_;
};
