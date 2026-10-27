// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/common/asio_future_bridge.hpp"
#include "scheduler_stack.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "connectors/file/driver.hpp"
#include "scheduler/scheduler.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <otterbrix/otterbrix.hpp>

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

using otterstax::test::init_test_otterbrix;

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

// What the Worker does for a local-file CREATE EXTERNAL TABLE and COPY ... TO, minus the pool: the
// file driver loads or writes, the engine creates the table or runs the statement. The engine runs a
// handler on the sender's thread, so its future is settled when send() returns.
struct file_stack_t {
    std::pmr::memory_resource* resource;
    actor_zeta::address_t otterbrix_manager;
    conn::file::driver_t driver{resource};

    core::result_wrapper_t<bool>
    add_file(const std::string& db, const std::string& tbl, const std::string& path, const std::string& format) {
        auto loaded = driver.run(conn::load_t{std::pmr::string(path, resource), std::pmr::string(format, resource)});
        if (loaded.has_error()) {
            return loaded.convert_error<bool>();
        }
        auto [ns, created] = actor_zeta::send(otterbrix_manager,
                                              &db::OtterbrixManager::create_table,
                                              session_id().hash(),
                                              db,
                                              tbl,
                                              std::move(*loaded.value().loaded));
        otterstax::test::wait_until_ready(created);
        return std::move(created).take_ready();
    }

    // Writes the statement's result into `sink` when one is given (left open, as
    // the S3 export leaves it for its commit), otherwise to a file at `path`.
    core::result_wrapper_t<bool> dump_file(OtterbrixStatementPtr statement,
                                           const std::string& path,
                                           conn::file::FileFormat format,
                                           std::shared_ptr<arrow::io::OutputStream> sink = nullptr) {
        auto [ns, executed] =
            actor_zeta::send(otterbrix_manager, &db::OtterbrixManager::execute, session_id().hash(), std::move(statement));
        otterstax::test::wait_until_ready(executed);
        auto cursor = std::move(executed).take_ready();
        if (!cursor || !cursor->is_success()) {
            return core::error_t(cursor && cursor->is_error() ? cursor->get_error().type
                                                              : core::error_code_t::other_error,
                                 std::pmr::string{"execute failed", resource});
        }
        if (sink) {
            return conn::file::write_chunks(resource, format, cursor->chunks(), path, sink);
        }
        auto written = driver.run(conn::export_t{std::pmr::string(path, resource), format, cursor});
        if (written.has_error()) {
            return written.convert_error<bool>();
        }
        return true;
    }
};

// Build a "SELECT * FROM <db>.<tbl>" plan for a whole-table dump.
OtterbrixStatementPtr select_all(std::pmr::memory_resource* res,
                                 const std::string& db, const std::string& tbl) {
    auto parser = make_parser(res, no_aliases());
    auto parsed = parser->parse("SELECT * FROM " + db + "." + tbl + ";");
    REQUIRE_FALSE(parsed.has_error());
    return std::move(parsed.value()->otterbrix_params);
}

} // namespace


TEST_CASE("local files: parquet ingestion and SELECT") {
    const std::string db       = "FileIngestionDb";
    const std::string tbl      = "People";
    const std::string parquet  = "/tmp/test_file_ingestion.parquet";
    const std::string data_dir = "/tmp/test_file_ingestion_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_parquet(parquet);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto res = files.add_file(db, tbl, parquet, "parquet");
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunks().front().column_count() == 2);

    // Destroy otterbrix before removing data_dir: the destructor runs a disk
    // checkpoint that writes to data_dir; removing it first causes a SIGSEGV.
    cur.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(parquet);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("local files: csv ingestion and SELECT") {
    const std::string db       = "FileIngestionCsvDb";
    const std::string tbl      = "People";
    const std::string csv      = "/tmp/test_file_ingestion.csv";
    const std::string data_dir = "/tmp/test_file_ingestion_csv_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_csv(csv);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto res = files.add_file(db, tbl, csv, "csv");
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunks().front().column_count() == 2);

    cur.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(csv);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("local files: ndjson ingestion and SELECT") {
    const std::string db       = "FileIngestionJsonDb";
    const std::string tbl      = "People";
    const std::string ndjson   = "/tmp/test_file_ingestion.ndjson";
    const std::string data_dir = "/tmp/test_file_ingestion_json_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_ndjson(ndjson);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto res = files.add_file(db, tbl, ndjson, "ndjson");
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunks().front().column_count() == 2);

    cur.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(ndjson);
    std::filesystem::remove_all(data_dir);
}

// ── dump tests ────────────────────────────────────────────────────────────────
// Each dump test: ingest a file → dump_file → verify the output file exists and
// re-reads correctly via the input translator (no second otterbrix needed).

TEST_CASE("local files: parquet dump and re-read") {
    const std::string db       = "DumpParquetDb";
    const std::string tbl      = "People";
    const std::string parquet  = "/tmp/test_dump.parquet";
    const std::string out       = "/tmp/test_dump_out.parquet";
    const std::string data_dir = "/tmp/test_dump_parquet_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(parquet);
    std::filesystem::remove(out);
    write_test_parquet(parquet);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto add_res = files.add_file(db, tbl, parquet, "parquet");
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    // File dump: written to the exact path requested.
    auto dump_res = files.dump_file(select_all(resource, db, tbl), out, conn::file::FileFormat::Parquet);
    REQUIRE_FALSE(dump_res.has_error());
    REQUIRE(dump_res.value());
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto loaded = tsl::parquet_to_chunk(resource, dumped);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);
        REQUIRE(loaded.value().column_count() == 2);
    }

    // Stream dump (the S3 upload path): the rows go to the given stream, nothing is
    // written at `path`, and the stream is left open for the sender to commit.
    {
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        const std::string label = "/tmp/test_dump_stream_label.parquet";
        std::filesystem::remove(label);
        auto stream_res = files.dump_file(select_all(resource, db, tbl), label, conn::file::FileFormat::Parquet, sink);
        REQUIRE_FALSE(stream_res.has_error());
        REQUIRE(stream_res.value());
        REQUIRE_FALSE(std::filesystem::exists(label));
        REQUIRE_FALSE(sink->closed());

        auto bytes = sink->Finish().ValueOrDie();
        auto loaded = tsl::parquet_to_chunk(resource, bytes->data(), static_cast<size_t>(bytes->size()));
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);
        REQUIRE(loaded.value().column_count() == 2);
    }

    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(parquet);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("local files: csv dump and re-read") {
    const std::string db       = "DumpCsvDb";
    const std::string tbl      = "People";
    const std::string csv      = "/tmp/test_dump.csv";
    const std::string out       = "/tmp/test_dump_out.csv";
    const std::string data_dir = "/tmp/test_dump_csv_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(csv);
    std::filesystem::remove(out);
    write_test_csv(csv);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto add_res = files.add_file(db, tbl, csv, "csv");
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    auto dump_res = files.dump_file(select_all(resource, db, tbl), out, conn::file::FileFormat::CSV);
    REQUIRE_FALSE(dump_res.has_error());
    REQUIRE(dump_res.value());
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto loaded = tsl::csv_to_chunk(resource, dumped, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);
        REQUIRE(loaded.value().column_count() == 2);
    }

    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(csv);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("local files: ndjson dump and re-read") {
    const std::string db       = "DumpJsonDb";
    const std::string tbl      = "People";
    const std::string ndjson   = "/tmp/test_dump.ndjson";
    const std::string out       = "/tmp/test_dump_out.ndjson";
    const std::string data_dir = "/tmp/test_dump_json_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(ndjson);
    std::filesystem::remove(out);
    write_test_ndjson(ndjson);

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto add_res = files.add_file(db, tbl, ndjson, "ndjson");
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    auto dump_res = files.dump_file(select_all(resource, db, tbl), out, conn::file::FileFormat::NDJSON);
    REQUIRE_FALSE(dump_res.has_error());
    REQUIRE(dump_res.value());
    const std::string& dumped = out;
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto loaded = tsl::ndjson_to_chunk(resource, dumped);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);
        REQUIRE(loaded.value().column_count() == 2);
    }

    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(ndjson);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

// ── end-to-end via the Scheduler ────────────────────────────────────────────
// Drive CREATE EXTERNAL TABLE / COPY ... TO through the real GreenplumParser +
// Scheduler routing (the s3/file grammar-extension wiring), for local files.
// The stack itself lives in scheduler_stack.hpp — test_dml_result_shape.cpp and
// test_worker_exception_safety.cpp drive the same one.

using otterstax::test::await_session;
using otterstax::test::engine_row_count;
using otterstax::test::run_scheduler_sql;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

TEST_CASE("Scheduler: CREATE EXTERNAL TABLE + COPY ... TO (parquet) run on the local-file transfer pool") {
    with_scheduler_stack("/tmp/test_ext_routing_parquet_otb", [](scheduler_stack s) {
        const std::string db = "ExtParquetDb", tbl = "People";
        const std::string src = "/tmp/test_ext_create.parquet", out = "/tmp/test_ext_copy_out.parquet";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_parquet(src);

        std::string err;
        auto st = run_scheduler_sql(
            s, 4001, "CREATE EXTERNAL TABLE " + db + "." + tbl + " WITH (location = '" + src + "', format = 'parquet')",
            err);
        INFO("CREATE error: " << err);
        REQUIRE(st);
        REQUIRE(engine_row_count(s, db, tbl) == 5);

        st = run_scheduler_sql(
            s, 4002, "COPY (SELECT * FROM " + db + "." + tbl + ") TO '" + out + "' WITH (format = 'parquet')", err);
        INFO("COPY error: " << err);
        REQUIRE(st);
        REQUIRE(std::filesystem::exists(out));
        auto loaded = tsl::parquet_to_chunk(s.resource, out);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("Scheduler: CREATE EXTERNAL TABLE + COPY ... TO (csv), format auto-detected from extension") {
    with_scheduler_stack("/tmp/test_ext_routing_csv_otb", [](scheduler_stack s) {
        const std::string db = "ExtCsvDb", tbl = "People";
        const std::string src = "/tmp/test_ext_create.csv", out = "/tmp/test_ext_copy_out.csv";
        std::filesystem::remove(src);
        std::filesystem::remove(out);
        write_test_csv(src);

        std::string err;
        // No format option on CREATE — resolved from the ".csv" extension.
        auto st = run_scheduler_sql(s, 4101,
                                    "CREATE EXTERNAL TABLE " + db + "." + tbl + " WITH (location = '" + src + "')", err);
        INFO("CREATE error: " << err);
        REQUIRE(st);
        REQUIRE(engine_row_count(s, db, tbl) == 5);

        st = run_scheduler_sql(s, 4102,
                               "COPY (SELECT * FROM " + db + "." + tbl + ") TO '" + out + "' WITH (format = 'csv')", err);
        INFO("COPY error: " << err);
        REQUIRE(st);
        REQUIRE(std::filesystem::exists(out));
        auto loaded = tsl::csv_to_chunk(s.resource, out, ',', /*has_header=*/true);
        REQUIRE_FALSE(loaded.has_error());
        REQUIRE(loaded.value().size() == 5);

        std::filesystem::remove(src);
        std::filesystem::remove(out);
    });
}

TEST_CASE("Scheduler: CREATE EXTERNAL TABLE on a missing file fails cleanly") {
    with_scheduler_stack("/tmp/test_ext_routing_err_otb", [](scheduler_stack s) {
        const std::string missing = "/tmp/test_ext_does_not_exist.parquet";
        std::filesystem::remove(missing);

        std::string err;
        auto st = run_scheduler_sql(
            s, 4201, "CREATE EXTERNAL TABLE ErrDb.People WITH (location = '" + missing + "', format = 'parquet')", err);
        REQUIRE_FALSE(st);
        REQUIRE_FALSE(err.empty());
    });
}

TEST_CASE("Scheduler: external statement via prepare_schema + execute_statement (FlightSQL two-phase)") {
    using namespace std::chrono_literals;
    with_scheduler_stack("/tmp/test_ext_routing_twophase_otb", [](scheduler_stack s) {
        const std::string db = "ExtTwoPhaseDb", tbl = "People";
        const std::string src = "/tmp/test_ext_twophase.parquet";
        std::filesystem::remove(src);
        write_test_parquet(src);

        const session_hash_t id = 4301;
        const std::string sql =
            "CREATE EXTERNAL TABLE " + db + "." + tbl + " WITH (location = '" + src + "', format = 'parquet')";

        // GetFlightInfo phase — returns an (empty) schema and stores the metadata.
        auto [ns1, fut1] = actor_zeta::send(s.scheduler, &Scheduler::prepare_schema, id, sql);
        auto r1 = await_session(std::move(fut1), 10000ms, s.resource);
        INFO("prepare_schema error: " << (r1.has_error() ? r1.error().what.c_str() : ""));
        REQUIRE_FALSE(r1.has_error());

        // DoGet phase — reuses the stored statement and performs the load.
        auto [ns2, fut2] = actor_zeta::send(s.scheduler, &Scheduler::execute_statement, id);
        auto r2 = await_session(std::move(fut2), 10000ms, s.resource);
        INFO("execute_statement error: " << (r2.has_error() ? r2.error().what.c_str() : ""));
        REQUIRE_FALSE(r2.has_error());
        REQUIRE(engine_row_count(s, db, tbl) == 5);

        std::filesystem::remove(src);
    });
}

// ── wider than one engine chunk ───────────────────────────────────────────────
// A loader builds the whole file as one chunk; the engine takes at most
// DEFAULT_VECTOR_CAPACITY (1024) rows per chunk, so the rows reach the insert as a
// run of such chunks (OtterbrixDataManager::insert_rows). Every row must arrive
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

// Loads `path` the way a local CREATE EXTERNAL TABLE does into a fresh engine at `data_dir`
// and reads the table back ordered by id: every chunk within the engine's bound,
// all wide_file_rows rows, the probe rows with their values.
void require_wide_file_ingested(const std::string& db,
                                const std::string& tbl,
                                const std::string& path,
                                const std::string& format,
                                const std::string& data_dir) {
    using components::vector::DEFAULT_VECTOR_CAPACITY;
    std::filesystem::remove_all(data_dir);
    const std::string select = "SELECT * FROM " + db + "." + tbl + " ORDER BY id;";

    db::otterbrix_engine_ptr otterbrix = init_test_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    file_stack_t files{resource, otterbrix_manager->address()};

    auto res = files.add_file(db, tbl, path, format);
    INFO("add_file: " << (res.has_error() ? res.error().what.c_str() : "ok"));
    REQUIRE_FALSE(res.has_error());

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(session, select);
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
        REQUIRE(holder->value(0, at).value<int64_t>() == static_cast<int64_t>(row));
        REQUIRE(holder->value(1, at).value<std::string_view>() == wide_file_name(row));
    }

    cur.reset();
    otterbrix_manager.reset();
    otterbrix.reset();
    std::filesystem::remove_all(data_dir);
}

} // namespace

TEST_CASE("local files: a csv wider than one engine chunk is ingested with every row") {
    const std::string csv = "/tmp/test_file_ingestion_wide.csv";
    write_wide_csv(csv);

    require_wide_file_ingested("WideCsvDb", "People", csv, "csv", "/tmp/test_file_ingestion_wide_csv_otterbrix");

    std::filesystem::remove(csv);
}

TEST_CASE("local files: a parquet wider than one engine chunk is ingested with every row") {
    const std::string parquet = "/tmp/test_file_ingestion_wide.parquet";
    write_wide_parquet(parquet);

    require_wide_file_ingested("WideParquetDb",
                               "People",
                               parquet,
                               "parquet",
                               "/tmp/test_file_ingestion_wide_parquet_otterbrix");

    std::filesystem::remove(parquet);
}
