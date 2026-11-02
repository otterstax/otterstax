// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/common/asio_future_bridge.hpp"
#include "../mock/cell.hpp"
#include "session_stack.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/parser/parser.hpp"
#include "connectors/file/manager.hpp"
#include "connectors/s3/manager.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <thread>

#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "otterbrix/translators/input/csv_to_chunk.hpp"
#include "otterbrix/translators/input/ndjson_to_chunk.hpp"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

#include <catch2/catch_all.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace {


void write_test_parquet(const std::string& path) {
    arrow::Int32Builder  id_b;
    arrow::StringBuilder name_b;

    auto s1 = id_b.AppendValues({1, 2, 3, 4, 5});
    auto s2 = name_b.AppendValues(
        std::vector<std::string>{"Alice", "Bob", "Charlie", "Dave", "Eve"});
    if (!s1.ok() || !s2.ok())
        throw std::runtime_error("Arrow builder append failed");

    std::shared_ptr<arrow::Array> id_arr, name_arr;
    if (!id_b.Finish(&id_arr).ok() || !name_b.Finish(&name_arr).ok())
        throw std::runtime_error("Arrow builder finish failed");

    auto schema = arrow::schema({
        arrow::field("id",   arrow::int32()),
        arrow::field("name", arrow::utf8()),
    });
    auto table = arrow::Table::Make(schema, {id_arr, name_arr});

    auto sink_result = arrow::io::FileOutputStream::Open(path);
    if (!sink_result.ok())
        throw std::runtime_error("Cannot open parquet output: " + sink_result.status().ToString());

    auto status = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), *sink_result, /*chunk_size=*/1024);
    if (!status.ok())
        throw std::runtime_error("WriteTable failed: " + status.ToString());

    if (!(*sink_result)->Close().ok())
        throw std::runtime_error("Close failed");
}

void write_test_csv(const std::string& path) {
    std::ofstream f(path);
    f << "id,name\n"
      << "1,Alice\n"
      << "2,Bob\n"
      << "3,Charlie\n"
      << "4,Dave\n"
      << "5,Eve\n";
}

void write_test_ndjson(const std::string& path) {
    std::ofstream f(path);
    f << "{\"id\":1,\"name\":\"Alice\"}\n"
      << "{\"id\":2,\"name\":\"Bob\"}\n"
      << "{\"id\":3,\"name\":\"Charlie\"}\n"
      << "{\"id\":4,\"name\":\"Dave\"}\n"
      << "{\"id\":5,\"name\":\"Eve\"}\n";
}

// The load behind CREATE EXTERNAL TABLE, the session's steps on the test's
// engine session: the file read by the file manager, the table created from its
// columns and the rows inserted (one transaction of its own).
core::result_wrapper_t<bool> load_into(otterstax::test::test_engine_t& engine,
                                       const actor_zeta::address_t& file_manager,
                                       conn::file::FileLoadParams params,
                                       const qualified_name_t& target) {
    auto read = actor_zeta::send(file_manager, &conn::file::FileManager::load_file, session_hash_t{1}, std::move(params))
                    .second.take_ready();
    if (read.has_error()) {
        return read.convert_error<bool>();
    }
    auto create = otterstax::engine::create_table_plan(engine.resource(), target, read.value());
    if (create.has_error()) {
        return create.convert_error<bool>();
    }
    auto cursor = engine.drive(otterstax::engine::load_table(engine.resource(),
                                                             engine.dispatcher(),
                                                             engine.session(),
                                                             std::move(create.value()),
                                                             target,
                                                             std::move(read.value()),
                                                             /*own_block=*/true));
    if (cursor->is_error()) {
        return core::error_t(cursor->get_error().type,
                             std::pmr::string{cursor->get_error().what.c_str(), engine.resource()});
    }
    return true;
}

conn::file::FileLoadParams load_params(const std::string& path, const std::string& format) {
    conn::file::FileLoadParams params;
    params.path   = path;
    params.format = format;
    return params;
}

// main.public.<table>: a one-part name from the test session's default place.
qualified_name_t people_table(const std::string& table = "people") {
    return qualified_name_t{core::relname_t{table}};
}

// Loads `path` into main.public.people and reads it back.
void require_loaded(const std::string& path, const std::string& format, const std::string& data_dir) {
    std::filesystem::remove_all(data_dir);
    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(otterbrix->resource());

    auto res = load_into(*otterbrix, file_manager->address(), load_params(path, format), people_table());
    INFO("load: " << (res.has_error() ? res.error().what.c_str() : "ok"));
    REQUIRE_FALSE(res.has_error());
    auto cur = otterbrix->sql("SELECT * FROM people;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunks().front().column_count() == 2);

    // Destroy otterbrix before removing data_dir: the destructor runs a disk
    // checkpoint that writes to data_dir; removing it first causes a SIGSEGV.
    cur.reset();
    file_manager.reset();
    otterbrix.reset();
    std::filesystem::remove_all(data_dir);
}

// The rows of a loaded run, and the columns its chunks share.
uint64_t rows_of(const std::pmr::vector<components::vector::data_chunk_t>& run) {
    uint64_t rows = 0;
    for (const auto& chunk : run) {
        rows += chunk.size();
    }
    return rows;
}

uint64_t columns_of(const std::pmr::vector<components::vector::data_chunk_t>& run) {
    return run.empty() ? 0 : run.front().column_count();
}

// Loads `src` into main.public.people (when the table is not there yet) and
// dumps SELECT * of it to `out` in `format` — or to `output` when it is set.
void load_and_dump(otterstax::test::test_engine_t& engine,
                   const actor_zeta::address_t& file_manager,
                   const std::string& src,
                   const std::string& out,
                   conn::file::FileFormat format,
                   tsl::output_stream_t output = {}) {
    auto rows = engine.sql("SELECT * FROM people;");
    if (rows->is_error()) {
        auto loaded = load_into(engine, file_manager, load_params(src, "auto"), people_table());
        INFO("load: " << (loaded.has_error() ? loaded.error().what.c_str() : "ok"));
        REQUIRE_FALSE(loaded.has_error());
        rows = engine.sql("SELECT * FROM people;");
    }
    REQUIRE(rows->is_success());
    auto dump_res = actor_zeta::send(file_manager,
                                     &conn::file::FileManager::dump_file,
                                     session_hash_t{1},
                                     conn::file::FileMetadata{std::move(rows), out, format, std::move(output)})
                        .second.take_ready();
    INFO("dump: " << (dump_res.has_error() ? dump_res.error().what.c_str() : "ok"));
    REQUIRE_FALSE(dump_res.has_error());
    REQUIRE(dump_res.value());
}

} // namespace


TEST_CASE("FileManager: parquet ingestion and SELECT") {
    const std::string parquet = "/tmp/test_file_ingestion.parquet";
    write_test_parquet(parquet);
    require_loaded(parquet, "parquet", "/tmp/test_file_ingestion_otterbrix");
    std::filesystem::remove(parquet);
}

TEST_CASE("FileManager: csv ingestion and SELECT") {
    const std::string csv = "/tmp/test_file_ingestion.csv";
    write_test_csv(csv);
    require_loaded(csv, "csv", "/tmp/test_file_ingestion_csv_otterbrix");
    std::filesystem::remove(csv);
}

TEST_CASE("FileManager: ndjson ingestion and SELECT") {
    const std::string ndjson = "/tmp/test_file_ingestion.ndjson";
    write_test_ndjson(ndjson);
    require_loaded(ndjson, "ndjson", "/tmp/test_file_ingestion_json_otterbrix");
    std::filesystem::remove(ndjson);
}

TEST_CASE("FileManager: a file that is not there is io_error and nothing is created") {
    const std::string data_dir = "/tmp/test_file_ingestion_missing_otterbrix";
    std::filesystem::remove_all(data_dir);
    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(otterbrix->resource());

    {
        // The error's text lives on the engine's resource: it goes before the engine.
        auto res = load_into(*otterbrix,
                             file_manager->address(),
                             load_params("/tmp/test_file_ingestion_not_there.csv", "csv"),
                             people_table());
        REQUIRE(res.has_error());
        CHECK(res.error().type == core::error_code_t::io_error);
        CHECK(otterbrix->sql("SELECT * FROM people;")->is_error());
    }

    file_manager.reset();
    otterbrix.reset();
    std::filesystem::remove_all(data_dir);
}

// ── dump tests ────────────────────────────────────────────────────────────────
// Each dump test: load a file → dump_file of a SELECT's rows → verify the output
// file exists and re-reads correctly via the input translator.

TEST_CASE("FileManager: parquet dump and re-read") {
    const std::string parquet  = "/tmp/test_dump.parquet";
    const std::string out      = "/tmp/test_dump_out.parquet";
    const std::string data_dir = "/tmp/test_dump_parquet_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(parquet);
    std::filesystem::remove(out);
    write_test_parquet(parquet);

    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto resource = otterbrix->resource();
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(resource);

    // File dump: written to the exact path requested.
    load_and_dump(*otterbrix, file_manager->address(), parquet, out, conn::file::FileFormat::Parquet);
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto loaded = tsl::parquet_to_chunk(resource, dumped);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);
        REQUIRE(columns_of(loaded.value()) == 2);
    }

    // Stream dump (the S3 upload path): the rows go to the given stream, nothing is
    // written at `path`, and the file manager publishes (closes) the stream once the
    // whole result is written.
    {
        // The test keeps Arrow's own handle to read the bytes back.
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        const std::string label = "/tmp/test_dump_stream_label.parquet";
        std::filesystem::remove(label);
        load_and_dump(*otterbrix, file_manager->address(), parquet, label, conn::file::FileFormat::Parquet,
                      tsl::output_stream_t{sink});
        REQUIRE_FALSE(std::filesystem::exists(label));
        REQUIRE(sink->closed());

        auto bytes = sink->Finish().ValueOrDie();
        auto loaded = tsl::parquet_to_chunk(resource, bytes->data(), static_cast<size_t>(bytes->size()));
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);
        REQUIRE(columns_of(loaded.value()) == 2);
    }

    file_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(parquet);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: csv dump and re-read") {
    const std::string csv      = "/tmp/test_dump.csv";
    const std::string out      = "/tmp/test_dump_out.csv";
    const std::string data_dir = "/tmp/test_dump_csv_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(csv);
    std::filesystem::remove(out);
    write_test_csv(csv);

    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto resource = otterbrix->resource();
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(resource);

    load_and_dump(*otterbrix, file_manager->address(), csv, out, conn::file::FileFormat::CSV);
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));
    {
        auto loaded = tsl::csv_to_chunk(resource, dumped, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);
        REQUIRE(columns_of(loaded.value()) == 2);
    }

    file_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(csv);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: ndjson dump and re-read") {
    const std::string ndjson   = "/tmp/test_dump.ndjson";
    const std::string out      = "/tmp/test_dump_out.ndjson";
    const std::string data_dir = "/tmp/test_dump_json_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(ndjson);
    std::filesystem::remove(out);
    write_test_ndjson(ndjson);

    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto resource = otterbrix->resource();
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(resource);

    load_and_dump(*otterbrix, file_manager->address(), ndjson, out, conn::file::FileFormat::NDJSON);
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));
    {
        auto loaded = tsl::ndjson_to_chunk(resource, dumped);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);
        REQUIRE(columns_of(loaded.value()) == 2);
    }

    file_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(ndjson);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

// ── end-to-end through a query session ─────────────────────────────────────
// Drive CREATE EXTERNAL TABLE / COPY ... TO through the real GreenplumParser +
// the session's routing (the s3/file grammar-extension wiring), for local files.
// The stack itself lives in session_stack.hpp — test_dml_result_shape.cpp and
// test_session_exception_safety.cpp drive the same one.

using otterstax::test::await_session;
using otterstax::test::engine_row_count;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

TEST_CASE("session: CREATE EXTERNAL TABLE + COPY ... TO (parquet) through the session") {
    with_session_stack("/tmp/test_ext_routing_parquet_otb", [](session_stack s) {
        const std::string src = "/tmp/test_ext_create.parquet", out = "/tmp/test_ext_copy_out.parquet";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_parquet(src);

        run_session_or_fail(s, 4000, "CREATE SCHEMA ExtParquet");
        std::string err;
        auto st = run_session_sql(
            s, 4001, "CREATE EXTERNAL TABLE ExtParquet.People WITH (location = '" + src + "', format = 'parquet')",
            err);
        INFO("CREATE error: " << err);
        REQUIRE(st);
        REQUIRE(engine_row_count(s, "main.extparquet", "people") == 5);

        st = run_session_sql(
            s, 4002, "COPY (SELECT * FROM ExtParquet.People) TO '" + out + "' WITH (format = 'parquet')", err);
        INFO("COPY error: " << err);
        REQUIRE(st);
        REQUIRE(std::filesystem::exists(out));
        auto loaded = tsl::parquet_to_chunk(s.resource, out);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("session: CREATE EXTERNAL TABLE + COPY ... TO (csv) with the format from the extension") {
    with_session_stack("/tmp/test_ext_routing_csv_otb", [](session_stack s) {
        const std::string src = "/tmp/test_ext_create.csv", out = "/tmp/test_ext_copy_out.csv";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_csv(src);

        run_session_or_fail(s, 4100, "CREATE SCHEMA ExtCsv");
        std::string err;
        // No format option on CREATE — resolved from the ".csv" extension.
        auto st = run_session_sql(s, 4101, "CREATE EXTERNAL TABLE ExtCsv.People WITH (location = '" + src + "')", err);
        INFO("CREATE error: " << err);
        REQUIRE(st);
        REQUIRE(engine_row_count(s, "main.extcsv", "people") == 5);

        st = run_session_sql(s, 4102, "COPY (SELECT * FROM ExtCsv.People) TO '" + out + "' WITH (format = 'csv')", err);
        INFO("COPY error: " << err);
        REQUIRE(st);
        REQUIRE(std::filesystem::exists(out));
        auto loaded = tsl::csv_to_chunk(s.resource, out, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(rows_of(loaded.value()) == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("session: CREATE EXTERNAL TABLE on a missing file fails cleanly") {
    with_session_stack("/tmp/test_ext_routing_err_otb", [](session_stack s) {
        const std::string missing = "/tmp/test_ext_does_not_exist.parquet";
        std::filesystem::remove(missing);

        std::string err;
        auto st = run_session_sql(
            s, 4201, "CREATE EXTERNAL TABLE people WITH (location = '" + missing + "', format = 'parquet')", err);
        REQUIRE_FALSE(st);
        REQUIRE_FALSE(err.empty());
    });
}

TEST_CASE("session: external statement via prepare_schema + execute_statement (FlightSQL two-phase)") {
    using namespace std::chrono_literals;
    with_session_stack("/tmp/test_ext_routing_twophase_otb", [](session_stack s) {
        const std::string src = "/tmp/test_ext_twophase.parquet";
        std::filesystem::remove(src);
        write_test_parquet(src);

        run_session_or_fail(s, 4300, "CREATE SCHEMA ExtTwoPhase");
        const session_hash_t id = 4301;
        const std::string sql =
            "CREATE EXTERNAL TABLE ExtTwoPhase.People WITH (location = '" + src + "', format = 'parquet')";

        // GetFlightInfo phase — returns an (empty) schema and stores the metadata.
        auto [ns1, fut1] = actor_zeta::send(s.session, &otterstax::session::query_session_t::prepare_schema, otterstax::test::stmt(id), sql);
        auto r1 = await_session(std::move(fut1), 10000ms, s.resource);
        INFO("prepare_schema error: " << (r1.has_error() ? r1.error().what.c_str() : ""));
        REQUIRE_FALSE(r1.has_error());

        // DoGet phase — reuses the stored statement and performs the load.
        auto [ns2, fut2] = actor_zeta::send(s.session, &otterstax::session::query_session_t::execute_statement, otterstax::test::stmt(id));
        auto r2 = await_session(std::move(fut2), 10000ms, s.resource);
        INFO("execute_statement error: " << (r2.has_error() ? r2.error().what.c_str() : ""));
        REQUIRE_FALSE(r2.has_error());
        REQUIRE(engine_row_count(s, "main.exttwophase", "people") == 5);

        std::filesystem::remove(src);
    });
}

// ── wider than one engine chunk ───────────────────────────────────────────────
// A loader builds the whole file as one chunk; the engine takes at most
// DEFAULT_VECTOR_CAPACITY (1024) rows per chunk, so the rows reach the insert as a
// run of such chunks (otterstax::engine::load_table). Every row must arrive
// with its values: probed on both sides of both chunk boundaries of 2500 rows.

namespace {

constexpr size_t wide_file_rows = 2500;
constexpr size_t wide_file_probe_rows[] = {0, 1023, 1024, 1025, 2047, 2048, wide_file_rows - 1};

std::string wide_file_name(size_t row) { return "name_" + std::to_string(row); }

// (id, name) with id = row and name = "name_<row>".
void write_wide_csv(const std::string& path) {
    std::ofstream f(path);
    f << "id,name\n";
    for (size_t row = 0; row < wide_file_rows; ++row) {
        f << row << ',' << wide_file_name(row) << '\n';
    }
}

// (id INT64, name UTF8) as write_wide_csv, in row groups of 1000 rows — the file's
// own chunking does not line up with the engine's.
void write_wide_parquet(const std::string& path) {
    arrow::Int64Builder  id_b;
    arrow::StringBuilder name_b;
    bool appended = true;
    for (size_t row = 0; row < wide_file_rows; ++row) {
        appended = appended && id_b.Append(static_cast<int64_t>(row)).ok();
        appended = appended && name_b.Append(wide_file_name(row)).ok();
    }
    REQUIRE(appended);
    std::shared_ptr<arrow::Array> id_arr, name_arr;
    REQUIRE(id_b.Finish(&id_arr).ok());
    REQUIRE(name_b.Finish(&name_arr).ok());
    auto schema = arrow::schema({arrow::field("id", arrow::int64()), arrow::field("name", arrow::utf8())});
    auto table  = arrow::Table::Make(schema, {id_arr, name_arr});
    auto sink   = arrow::io::FileOutputStream::Open(path);
    REQUIRE(sink.ok());
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *sink, /*chunk_size=*/1000).ok());
    REQUIRE((*sink)->Close().ok());
}

// Loads `params` through the file manager into main.public.people of a fresh
// engine at `data_dir` and reads the table back ordered by id: every chunk within
// the engine's bound, all wide_file_rows rows, the probe rows with their values.
void require_wide_file_ingested(conn::file::FileLoadParams params, const std::string& data_dir) {
    using components::vector::DEFAULT_VECTOR_CAPACITY;
    std::filesystem::remove_all(data_dir);
    const std::string select = "SELECT * FROM people ORDER BY id;";

    auto otterbrix = std::make_unique<otterstax::test::test_engine_t>(data_dir);
    auto file_manager = actor_zeta::spawn<conn::file::FileManager>(otterbrix->resource());

    auto res = load_into(*otterbrix, file_manager->address(), std::move(params), people_table());
    INFO("load: " << (res.has_error() ? res.error().what.c_str() : "ok"));
    REQUIRE_FALSE(res.has_error());
    auto cur = otterbrix->sql(select);
    INFO("select: " << (cur->is_error() ? cur->get_error().what.c_str() : "ok"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == wide_file_rows);

    size_t total = 0;
    for (const auto& chunk : cur->chunks()) {
        REQUIRE(chunk.size() <= DEFAULT_VECTOR_CAPACITY);
        REQUIRE(chunk.column_count() == 2);
        total += chunk.size();
    }
    REQUIRE(total == wide_file_rows);

    for (const size_t row : wide_file_probe_rows) {
        INFO("row " << row);
        size_t at = row;
        const components::vector::data_chunk_t* holder = nullptr;
        for (const auto& chunk : cur->chunks()) {
            if (at < chunk.size()) {
                holder = &chunk;
                break;
            }
            at -= chunk.size();
        }
        REQUIRE(holder != nullptr);
        REQUIRE(otterstax::test::cell(*holder, 0, at).value<int64_t>() == static_cast<int64_t>(row));
        REQUIRE(otterstax::test::cell(*holder, 1, at).value<std::string_view>() == wide_file_name(row));
    }

    cur.reset();
    file_manager.reset();
    otterbrix.reset();
    std::filesystem::remove_all(data_dir);
}

} // namespace

TEST_CASE("FileManager: a csv wider than one engine chunk is ingested with every row") {
    const std::string csv = "/tmp/test_file_ingestion_wide.csv";
    write_wide_csv(csv);

    require_wide_file_ingested(load_params(csv, "csv"), "/tmp/test_file_ingestion_wide_csv_otterbrix");

    std::filesystem::remove(csv);
}

TEST_CASE("FileManager: a parquet wider than one engine chunk is ingested with every row") {
    const std::string parquet = "/tmp/test_file_ingestion_wide.parquet";
    write_wide_parquet(parquet);

    require_wide_file_ingested(load_params(parquet, "parquet"), "/tmp/test_file_ingestion_wide_parquet_otterbrix");

    std::filesystem::remove(parquet);
}
