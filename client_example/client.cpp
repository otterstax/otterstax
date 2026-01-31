// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

arrow::Status PrintResultsForEndpoint(arrow::flight::sql::FlightSqlClient& client,
                                      const arrow::flight::FlightCallOptions& call_options,
                                      const arrow::flight::FlightEndpoint& endpoint) {
    std::cout << "run DoGet\n";
    auto stream = client.DoGet(call_options, endpoint.ticket).ValueOrDie();

    const arrow::Result<std::shared_ptr<arrow::Schema>>& schema = stream->GetSchema();
    ARROW_RETURN_NOT_OK(schema);

    std::cout << "Schema:" << std::endl;
    std::cout << schema->get()->ToString() << std::endl << std::endl;

    std::cout << "Results:" << std::endl;

    int64_t num_rows = 0;

    while (true) {
        std::cout << "Iteration" << std::endl;
        arrow::flight::FlightStreamChunk chunk = stream->Next().ValueOrDie();
        std::cout << "Get chunk" << std::endl;
        if (chunk.data == nullptr) {
            break;
        }
        std::cout << chunk.data->ToString() << std::endl;
        num_rows += chunk.data->num_rows();
    }

    std::cout << "Total: " << num_rows << std::endl;

    return arrow::Status::OK();
}

arrow::Status PrintResults(arrow::flight::sql::FlightSqlClient& client,
                           const arrow::flight::FlightCallOptions& call_options,
                           const std::unique_ptr<arrow::flight::FlightInfo>& info) {
    const std::vector<arrow::flight::FlightEndpoint>& endpoints = info->endpoints();

    for (size_t i = 0; i < endpoints.size(); i++) {
        std::cout << "Results from endpoint " << i + 1 << " of " << endpoints.size() << std::endl;
        PrintResultsForEndpoint(client, call_options, endpoints[i]);
    }

    return arrow::Status::OK();
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>" << std::endl;
        return 1;
    }
    // Create a client connection
    auto location = arrow::flight::Location::ForGrpcTcp("0.0.0.0", 8815).ValueOrDie();
    arrow::flight::FlightClientOptions options = arrow::flight::FlightClientOptions::Defaults();
    auto client = arrow::flight::FlightClient::Connect(location, options).ValueOrDie();

    std::ifstream file(argv[1]);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << argv[1] << std::endl;
        return 1;
    }

    std::string query;
    std::getline(file, query, '\0'); // Read entire file

    if (query.empty()) {
        std::cerr << "Error: File is empty!\n";
        return 1;
    }

    std::cout << "Received Query: " << query << "\n";
    file.close();

    // Create call_options
    arrow::flight::FlightCallOptions call_options;
    // Execute the query
    arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

    std::unique_ptr<arrow::flight::FlightInfo> info;
    info = sql_client.Execute(call_options, query).ValueOrDie();

    const std::vector<arrow::flight::FlightEndpoint>& endpoints = info->endpoints();

    for (size_t i = 0; i < endpoints.size(); i++) {
        std::cout << "Results from endpoint " << i + 1 << " of " << endpoints.size() << std::endl;
        PrintResultsForEndpoint(sql_client, call_options, endpoints[i]);
    }

    return 0;
}
