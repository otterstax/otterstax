// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Flight SQL command parsing (the Any in FlightDescriptor.cmd / the DoPut
// descriptor) and result assembly (including the fixed metadata schemas).

#include "engine.hpp"
#include <core/core.hpp>

namespace flight::core {

// Execute the descriptor's command: a SELECT or a metadata request.
// ticket_out receives the registered result ticket (for GetFlightInfo/GetSchema).
grpc::Status execute_descriptor(FlightSqlCore& core, IEngine& engine,
                                const arrow::flight::protocol::FlightDescriptor& descriptor,
                                std::string* ticket_out);

// Parse the DoPut command (the Any from the first FlightData).
enum class DoPutKind { Update, PreparedUpdate, PreparedBind };
struct DoPutCommand {
    DoPutKind kind = DoPutKind::Update;
    std::string query;  // Update
    std::string handle; // Prepared*
};
grpc::Status do_put_command(const arrow::flight::protocol::FlightDescriptor& descriptor,
                            DoPutCommand* out);

} // namespace flight::core
