// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/common/asio_future_bridge.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "connectors/file/manager.hpp"
#include "connectors/s3/manager.hpp"
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
#include <parquet/arrow/writer.h>

#include <catch2/catch.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

otterbrix::otterbrix_ptr init_otterbrix(const std::string& data_dir) {
    auto config = configuration::config::create_config(data_dir);
    initialize_all_loggers(config.log.path.string());
    return otterbrix::make_otterbrix(std::move(config));
}

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

// dump_file now takes a pre-parsed statement instead of database/table; build a
// "SELECT * FROM <db>.<tbl>" plan to reproduce the old whole-table dump.
OtterbrixStatementPtr select_all(std::pmr::memory_resource* res,
                                 const std::string& db, const std::string& tbl) {
    auto parser = make_parser(res);
    auto parsed = parser->parse("SELECT * FROM " + db + "." + tbl + ";");
    REQUIRE_FALSE(parsed.has_error());
    return std::move(parsed.value()->otterbrix_params);
}

} // namespace


TEST_CASE("FileManager: parquet ingestion and SELECT") {
    const std::string db       = "FileIngestionDb";
    const std::string tbl      = "People";
    const std::string parquet  = "/tmp/test_file_ingestion.parquet";
    const std::string data_dir = "/tmp/test_file_ingestion_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_parquet(parquet);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = parquet;
    params.format   = "parquet";

    auto res = actor_zeta::send(file_manager->address(),
                                &conn::file::FileManager::add_file, session_id().hash(),
                                std::move(params))
                   .second.take_ready();
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunk_data().column_count() == 2);

    // Destroy otterbrix before removing data_dir: the destructor runs a disk
    // checkpoint that writes to data_dir; removing it first causes a SIGSEGV.
    cur.reset();
    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(parquet);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: csv ingestion and SELECT") {
    const std::string db       = "FileIngestionCsvDb";
    const std::string tbl      = "People";
    const std::string csv      = "/tmp/test_file_ingestion.csv";
    const std::string data_dir = "/tmp/test_file_ingestion_csv_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_csv(csv);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = csv;
    params.format   = "csv";

    auto res = actor_zeta::send(file_manager->address(),
                                &conn::file::FileManager::add_file, session_id().hash(),
                                std::move(params))
                   .second.take_ready();
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunk_data().column_count() == 2);

    cur.reset();
    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(csv);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: ndjson ingestion and SELECT") {
    const std::string db       = "FileIngestionJsonDb";
    const std::string tbl      = "People";
    const std::string ndjson   = "/tmp/test_file_ingestion.ndjson";
    const std::string data_dir = "/tmp/test_file_ingestion_json_otterbrix";

    std::filesystem::remove_all(data_dir);
    write_test_ndjson(ndjson);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = ndjson;
    params.format   = "ndjson";

    auto res = actor_zeta::send(file_manager->address(),
                                &conn::file::FileManager::add_file, session_id().hash(),
                                std::move(params))
                   .second.take_ready();
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() == true);

    session_id session;
    auto cur = otterbrix->dispatcher()->execute_sql(
        session, "SELECT * FROM " + db + "." + tbl + ";");

    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);
    REQUIRE(cur->chunk_data().column_count() == 2);

    cur.reset();
    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(ndjson);
    std::filesystem::remove_all(data_dir);
}

// ── dump tests ────────────────────────────────────────────────────────────────
// Each dump test: ingest a file → dump_file → verify the output file exists and
// re-reads correctly via the input translator (no second otterbrix needed).

TEST_CASE("FileManager: parquet dump and re-read") {
    const std::string db       = "DumpParquetDb";
    const std::string tbl      = "People";
    const std::string parquet  = "/tmp/test_dump.parquet";
    const std::string out       = "/tmp/test_dump_out.parquet";
    const std::string data_dir = "/tmp/test_dump_parquet_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(parquet);
    std::filesystem::remove(out);
    write_test_parquet(parquet);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = parquet;
    params.format   = "parquet";

    auto add_res = actor_zeta::send(file_manager->address(),
                                    &conn::file::FileManager::add_file, session_id().hash(),
                                    std::move(params))
                       .second.take_ready();
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    // Non-temporary dump: written to the exact path requested.
    auto dump_res = actor_zeta::send(file_manager->address(),
                                     &conn::file::FileManager::dump_file, session_id().hash(),
                                     conn::file::FileMetadata{select_all(resource, db, tbl), out,
                                                                  conn::file::FileFormat::Parquet,
                                                                  /*is_temporary=*/false})
                        .second.take_ready();
    REQUIRE_FALSE(dump_res.has_error());
    const std::string dumped = dump_res.value();
    REQUIRE(dumped == out);
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto chunk = tsl::parquet_to_chunk(resource, dumped);
        REQUIRE(chunk.size() == 5);
        REQUIRE(chunk.column_count() == 2);
    }

    // Temporary dump: timestamp-prefixed basename in the same directory, so it
    // never matches the requested path but keeps the original filename.
    auto tmp_res = actor_zeta::send(file_manager->address(),
                                    &conn::file::FileManager::dump_file, session_id().hash(),
                                    conn::file::FileMetadata{select_all(resource, db, tbl), out,
                                                                 conn::file::FileFormat::Parquet,
                                                                 /*is_temporary=*/true})
                       .second.take_ready();
    REQUIRE_FALSE(tmp_res.has_error());
    const std::string tmp_dumped = tmp_res.value();
    REQUIRE(tmp_dumped != out);
    REQUIRE(std::filesystem::path(tmp_dumped).parent_path() ==
            std::filesystem::path(out).parent_path());
    REQUIRE(std::filesystem::path(tmp_dumped).filename().string().find(
                std::filesystem::path(out).filename().string()) != std::string::npos);
    REQUIRE(std::filesystem::exists(tmp_dumped));

    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(parquet);
    std::filesystem::remove(dumped);
    std::filesystem::remove(tmp_dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: csv dump and re-read") {
    const std::string db       = "DumpCsvDb";
    const std::string tbl      = "People";
    const std::string csv      = "/tmp/test_dump.csv";
    const std::string out       = "/tmp/test_dump_out.csv";
    const std::string data_dir = "/tmp/test_dump_csv_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(csv);
    std::filesystem::remove(out);
    write_test_csv(csv);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = csv;
    params.format   = "csv";

    auto add_res = actor_zeta::send(file_manager->address(),
                                    &conn::file::FileManager::add_file, session_id().hash(),
                                    std::move(params))
                       .second.take_ready();
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    auto dump_res = actor_zeta::send(file_manager->address(),
                                     &conn::file::FileManager::dump_file, session_id().hash(),
                                     conn::file::FileMetadata{select_all(resource, db, tbl), out,
                                                                  conn::file::FileFormat::CSV,
                                                                  /*is_temporary=*/false})
                        .second.take_ready();
    REQUIRE_FALSE(dump_res.has_error());
    const std::string dumped = dump_res.value();
    REQUIRE(dumped == out);
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto chunk = tsl::csv_to_chunk(resource, dumped, ',', /*has_header=*/true);
        REQUIRE(chunk.size() == 5);
        REQUIRE(chunk.column_count() == 2);
    }

    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(csv);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

TEST_CASE("FileManager: ndjson dump and re-read") {
    const std::string db       = "DumpJsonDb";
    const std::string tbl      = "People";
    const std::string ndjson   = "/tmp/test_dump.ndjson";
    const std::string out       = "/tmp/test_dump_out.ndjson";
    const std::string data_dir = "/tmp/test_dump_json_otterbrix";

    std::filesystem::remove_all(data_dir);
    std::filesystem::remove(ndjson);
    std::filesystem::remove(out);
    write_test_ndjson(ndjson);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto otterbrix_manager =
        actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_manager =
        actor_zeta::spawn<conn::file::FileManager>(resource, otterbrix_manager->address());

    conn::file::FileAddParams params;
    params.database = db;
    params.table    = tbl;
    params.path     = ndjson;
    params.format   = "ndjson";

    auto add_res = actor_zeta::send(file_manager->address(),
                                    &conn::file::FileManager::add_file, session_id().hash(),
                                    std::move(params))
                       .second.take_ready();
    REQUIRE_FALSE(add_res.has_error());
    REQUIRE(add_res.value() == true);

    auto dump_res = actor_zeta::send(file_manager->address(),
                                     &conn::file::FileManager::dump_file, session_id().hash(),
                                     conn::file::FileMetadata{select_all(resource, db, tbl), out,
                                                                  conn::file::FileFormat::NDJSON,
                                                                  /*is_temporary=*/false})
                        .second.take_ready();
    REQUIRE_FALSE(dump_res.has_error());
    const std::string dumped = dump_res.value();
    REQUIRE(dumped == out);
    REQUIRE(std::filesystem::exists(dumped));

    // Verify dumped file content via the input translator.
    // Scope the chunk so its pmr-backed buffers are released before we tear
    // down otterbrix (which owns the resource the chunk was allocated from).
    {
        auto chunk = tsl::ndjson_to_chunk(resource, dumped);
        REQUIRE(chunk.size() == 5);
        REQUIRE(chunk.column_count() == 2);
    }

    file_manager.reset();
    otterbrix_manager.reset();
    otterbrix.reset();

    std::filesystem::remove(ndjson);
    std::filesystem::remove(dumped);
    std::filesystem::remove_all(data_dir);
}

// ── end-to-end via the Scheduler ────────────────────────────────────────────
// Drive CREATE EXTERNAL TABLE / COPY ... TO through the real GreenplumParser +
// Scheduler routing (the s3/file grammar-extension wiring), for local files.
// sql/pg/ch/catalog addresses are empty: the external-table path is intercepted
// before any backend routing, and verification SELECTs are pure-otterbrix.

namespace {

struct scheduler_stack {
    actor_zeta::address_t      scheduler;
    otterbrix::otterbrix_ptr   otterbrix;
    std::pmr::memory_resource* resource;
};

// The worker pool runs on an actor-zeta sharing_scheduler; mirrors the helper
// used by test_scheduler.cpp / test_scheduler_concurrent.cpp.
std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> make_az_scheduler() {
    auto sched = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        /*max_throughput*/ 1000);
    sched->start();
    return sched;
}

// Drive the Scheduler's returned future from the test thread through the asio
// bridge (poll-based, no cv_wrapper).
core::result_wrapper_t<session_payload>
await_session(actor_zeta::unique_future<core::result_wrapper_t<session_payload>> fut,
              std::chrono::milliseconds timeout,
              std::pmr::memory_resource* resource) {
    core::result_wrapper_t<session_payload> r{resource};
    boost::asio::io_context local;
    boost::asio::co_spawn(
        local,
        [&]() -> boost::asio::awaitable<void> {
            r = co_await otterstax::async_await_future(std::move(fut), timeout);
        },
        boost::asio::detached);
    local.run();
    return r;
}

// Build otterbrix + file/s3 managers + a real-parser Scheduler, run `body`, then
// tear down (actors before otterbrix, whose dtor checkpoints to data_dir).
template<typename Fn>
void with_scheduler_stack(const std::string& data_dir, Fn&& body) {
    std::filesystem::remove_all(data_dir);

    otterbrix::otterbrix_ptr otterbrix = init_otterbrix(data_dir);
    auto resource = otterbrix->dispatcher()->resource();

    auto az_scheduler = make_az_scheduler();

    auto otb_mgr  = actor_zeta::spawn<db::OtterbrixManager>(resource, make_otterbrix_manager(otterbrix));
    auto file_mgr = actor_zeta::spawn<conn::file::FileManager>(resource, otb_mgr->address());
    auto s3_conn  = actor_zeta::spawn<conn::s3::ConnectorManager>(resource);
    auto s3_mgr   = actor_zeta::spawn<db::S3Manager>(resource, s3_conn->address(), file_mgr->address());

    auto scheduler = actor_zeta::spawn<Scheduler>(
        resource,
        az_scheduler.get(),
        std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        &make_parser,
        actor_zeta::address_t::empty_address(), // sql
        actor_zeta::address_t::empty_address(), // pg
        actor_zeta::address_t::empty_address(), // ch
        otb_mgr->address(),
        actor_zeta::address_t::empty_address(), // catalog
        s3_mgr->address(),
        file_mgr->address());

    body(scheduler_stack{scheduler->address(), otterbrix, resource});

    scheduler.reset();
    s3_mgr.reset();
    s3_conn.reset();
    file_mgr.reset();
    otb_mgr.reset();
    az_scheduler->stop();
    otterbrix.reset();
    std::filesystem::remove_all(data_dir);
}

// Run `sql` through Scheduler::execute and block on the returned future. Returns
// true on success, false on error; `err` carries the message in the error case.
bool run_scheduler_sql(const scheduler_stack& s,
                       session_hash_t id,
                       const std::string& sql,
                       std::string& err) {
    auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::execute, id, sql);
    auto r = await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
    if (r.has_error()) {
        err = std::string{r.error().what.c_str()};
        return false;
    }
    err.clear();
    return true;
}

size_t engine_row_count(const scheduler_stack& s, const std::string& db, const std::string& tbl) {
    session_id sid;
    auto cur = s.otterbrix->dispatcher()->execute_sql(sid, "SELECT * FROM " + db + "." + tbl + ";");
    return cur->is_success() ? cur->size() : 0;
}

} // namespace

TEST_CASE("Scheduler: CREATE EXTERNAL TABLE + COPY ... TO (parquet) route to the file manager") {
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
        auto chunk = tsl::parquet_to_chunk(s.resource, out);
        REQUIRE(chunk.size() == 5);

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
        auto chunk = tsl::csv_to_chunk(s.resource, out, ',', /*has_header=*/true);
        REQUIRE(chunk.size() == 5);

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
