// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "scheduler_stack.hpp"
#include "security/file_access.hpp"
#include "tests/temporary_directory.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"
#include <fstream>

namespace {
namespace fs = std::filesystem;
using namespace otterstax::security;

OtterbrixStatementPtr people(std::pmr::memory_resource* resource) {
    auto parser = make_parser(resource, no_aliases());
    auto parsed = parser->parse("SELECT * FROM authorized.people;");
    REQUIRE_FALSE(parsed.has_error());
    return std::move(parsed.value()->otterbrix_params);
}
std::string contents(const fs::path& path) {
    std::ifstream file(path);
    return {std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
}
}

TEST_CASE("FileManager enforces file permissions through ingestion and all export formats") {
    const auto format = GENERATE(conn::file::FileFormat::CSV, conn::file::FileFormat::NDJSON, conn::file::FileFormat::Parquet);
    otterstax::test::temporary_directory directory{"file-manager-policy"};
    const auto input = directory.path / "input";
    const auto output = directory.path / "output";
    fs::create_directory(input); fs::create_directory(output);
    const auto source = input / "people.csv";
    { std::ofstream file(source); file << "id,name\n1,Alice\n2,Bob\n"; }
    auto engine = otterstax::test::init_test_otterbrix((directory.path / "data").string());
    auto* resource = engine->dispatcher()->resource();
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(engine));
    auto permissions = std::make_shared<file_access>(file_access_config{{input.string(), output.string()}, {output.string()}, 65536});
    auto files = actor_zeta::spawn<conn::file::FileManager>(resource, manager->address(), permissions);
    conn::file::FileAddParams params;
    params.database = "authorized"; params.table = "people"; params.path = source.string(); params.format = "csv";
    auto imported = actor_zeta::send(files->address(), &conn::file::FileManager::add_file, next_request_id(), params).second.take_ready();
    REQUIRE_FALSE(imported.has_error());
    params.path = (directory.path / "secret.csv").string();
    { std::ofstream file(params.path); file << "secret\nprivate\n"; }
    params.table = "secret";
    auto denied = actor_zeta::send(files->address(), &conn::file::FileManager::add_file, next_request_id(), params).second.take_ready();
    REQUIRE(denied.has_error());

    const auto destination = output / "result";
    auto exported = actor_zeta::send(files->address(), &conn::file::FileManager::dump_file, next_request_id(),
                                     conn::file::FileMetadata{people(resource), destination.string(), format, false}).second.take_ready();
    INFO((exported.has_error() ? exported.error().what.c_str() : "success"));
    REQUIRE_FALSE(exported.has_error());
    auto bytes = permissions->read(destination.string());
    REQUIRE(bytes.ok());
    auto rows = format == conn::file::FileFormat::CSV ? tsl::csv_to_chunk(resource, bytes->data(), bytes->size()) :
                format == conn::file::FileFormat::NDJSON ? tsl::ndjson_to_chunk(resource, bytes->data(), bytes->size()) :
                tsl::parquet_to_chunk(resource, bytes->data(), bytes->size());
    REQUIRE_FALSE(rows.has_error());
    CHECK(rows.value().size() == 2);

    const auto before = contents(source);
    auto forbidden = actor_zeta::send(files->address(), &conn::file::FileManager::dump_file, next_request_id(),
                                      conn::file::FileMetadata{people(resource), source.string(), format, false}).second.take_ready();
    CHECK(forbidden.has_error());
    CHECK(contents(source) == before);

    auto limited_policy = std::make_shared<file_access>(file_access_config{{}, {output.string()}, 1});
    auto limited = actor_zeta::spawn<conn::file::FileManager>(resource, manager->address(), limited_policy);
    const auto exported_before = contents(destination);
    auto failed = actor_zeta::send(limited->address(), &conn::file::FileManager::dump_file, next_request_id(),
                                   conn::file::FileMetadata{people(resource), destination.string(), format, false}).second.take_ready();
    REQUIRE(failed.has_error());
    CHECK(contents(destination) == exported_before);
    CHECK(std::distance(fs::directory_iterator(output), fs::directory_iterator{}) == 1);
}
