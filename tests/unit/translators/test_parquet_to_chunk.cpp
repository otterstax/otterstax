// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/translators/input/arrow_to_chunk.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "single_chunk.hpp"
#include "otterbrix/translators/input/parquet_to_chunk.hpp"
#include "../../mock/cell.hpp"

// otterbrix's parser headers (pulled in above) #define DAY / SECOND, which clash
// with Arrow's TimeUnit/DateUnit enum values.
#undef DAY
#undef SECOND

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/thread_pool.h>
#include <parquet/arrow/writer.h>

#include <catch2/catch_all.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory_resource>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using components::vector::data_chunk_t;

namespace {

// Write a 5-row parquet file (int32 id, utf8 name) at `path`.
void write_people_parquet(const std::string& path) {
    arrow::Int32Builder  id_b;
    arrow::StringBuilder name_b;
    REQUIRE(id_b.AppendValues({1, 2, 3, 4, 5}).ok());
    REQUIRE(name_b.AppendValues({"Alice", "Bob", "Charlie", "Dave", "Eve"}).ok());

    std::shared_ptr<arrow::Array> id_arr, name_arr;
    REQUIRE(id_b.Finish(&id_arr).ok());
    REQUIRE(name_b.Finish(&name_arr).ok());

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())});
    auto table  = arrow::Table::Make(schema, {id_arr, name_arr});

    auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    REQUIRE(sink->Close().ok());
}

std::string read_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::string{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

} // namespace

TEST_CASE("parquet_to_chunk: file overload parses columns, rows and values") {
    const std::string path = "/tmp/otterstax_test_parquet_to_chunk.parquet";
    write_people_parquet(path);

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);
    REQUIRE_FALSE(loaded.has_error());
    const auto& chunk = otterstax::test::single(loaded.value());

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    // int32 id column round-trips as INTEGER.
    REQUIRE(otterstax::test::cell(chunk, 0, 0).value<int32_t>() == 1);
    REQUIRE(otterstax::test::cell(chunk, 0, 4).value<int32_t>() == 5);
    REQUIRE(otterstax::test::cell(chunk, 1, 0).value<std::string_view>() == "Alice");
    REQUIRE(otterstax::test::cell(chunk, 1, 4).value<std::string_view>() == "Eve");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: buffer overload matches file overload") {
    const std::string path = "/tmp/otterstax_test_parquet_to_chunk_buf.parquet";
    write_people_parquet(path);
    const std::string bytes = read_bytes(path);

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(),
                                        reinterpret_cast<const uint8_t*>(bytes.data()),
                                        bytes.size());
    REQUIRE_FALSE(loaded.has_error());
    const auto& chunk = otterstax::test::single(loaded.value());

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 5);
    REQUIRE(otterstax::test::cell(chunk, 0, 2).value<int32_t>() == 3);
    REQUIRE(otterstax::test::cell(chunk, 1, 2).value<std::string_view>() == "Charlie");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: extracts the schema only") {
    const std::string path = "/tmp/otterstax_test_parquet_to_struct.parquet";
    write_people_parquet(path);

    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), path);
    REQUIRE_FALSE(type.has_error());
    REQUIRE(type.value().child_types().size() == 2);
    REQUIRE(type.value().child_types()[0].alias() == "id");
    REQUIRE(type.value().child_types()[1].alias() == "name");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: a missing file is io_error") {
    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), "/nonexistent_dir_otterstax/missing.parquet");

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::io_error);
}

TEST_CASE("parquet_to_struct: a missing file is io_error") {
    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), "/nonexistent_dir_otterstax/missing.parquet");

    REQUIRE(type.has_error());
    REQUIRE(type.error().type == core::error_code_t::io_error);
}

TEST_CASE("parquet_to_chunk: an empty buffer is invalid_parameter") {
    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), nullptr, 0);

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::invalid_parameter);
}

// ── the 128-bit width and the other decimals ──────────────────────────────────
// chunk_to_arrow carries a HUGEINT as decimal128(38, 0), Arrow having no 128-bit integer
// type, so a parquet file this project writes has decimal columns in it. Before these
// cases the reader declared every unlisted Arrow type STRING_LITERAL and then read the
// array through arrow::StringArray — a Decimal128Array read under a layout with two more
// buffers and different members. It did not even fail loudly: 2^64 came back as the empty
// string, and the loaded table looked like a table of empty names.

namespace {

    components::types::int128_t pow10_i128(int n) {
        components::types::int128_t v = 1;
        for (int i = 0; i < n; ++i) {
            v *= 10;
        }
        return v;
    }

    // One-column parquet file of `values` under decimal128(precision, scale).
    void write_decimal_parquet(const std::string& path,
                               int32_t precision,
                               int32_t scale,
                               const std::vector<components::types::int128_t>& values) {
        auto type = arrow::decimal128(precision, scale);
        arrow::Decimal128Builder b{type};
        for (const auto& v : values) {
            REQUIRE(b.Append(arrow::Decimal128(absl::Int128High64(v), absl::Int128Low64(v))).ok());
        }
        REQUIRE(b.AppendNull().ok());
        std::shared_ptr<arrow::Array> arr;
        REQUIRE(b.Finish(&arr).ok());

        auto schema = arrow::schema({arrow::field("wide", type)});
        auto table = arrow::Table::Make(schema, {arr});
        auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
        REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
        REQUIRE(sink->Close().ok());
    }

} // namespace

TEST_CASE("parquet_to_chunk: a decimal128(38, 0) column is read back as HUGEINT with its digits intact") {
    const std::string path = "/tmp/otterstax_test_parquet_hugeint.parquet";
    std::filesystem::remove(path);
    const auto big = absl::MakeInt128(1, 0); // 2^64, outside int64
    write_decimal_parquet(path, 38, 0, {big, -big});

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);
    INFO("error: " << loaded.error().what.c_str());
    REQUIRE_FALSE(loaded.has_error());
    const auto& chunk = otterstax::test::single(loaded.value());

    REQUIRE(chunk.column_count() == 1);
    REQUIRE(chunk.size() == 3);
    REQUIRE(chunk.types()[0].type() == components::types::logical_type::HUGEINT);
    REQUIRE(otterstax::test::cell(chunk, 0, 0).value<components::types::int128_t>() == big);
    REQUIRE(otterstax::test::cell(chunk, 0, 1).value<components::types::int128_t>() == -big);
    REQUIRE(otterstax::test::cell(chunk, 0, 2).is_null());

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: a decimal128(38, 0) value past its own precision still reads exactly") {
    // A file no compliant writer produces (this project's writer refuses to), but one a
    // foreign writer can leave behind: the stored integer exceeds 10^38 - 1. HUGEINT
    // spans the whole int128 range, so the value is carried as written rather than
    // clipped or turned into NULL.
    const std::string path = "/tmp/otterstax_test_parquet_tail.parquet";
    std::filesystem::remove(path);
    const auto tail = pow10_i128(38); // one past what precision 38 may declare
    write_decimal_parquet(path, 38, 0, {tail});

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);
    INFO("error: " << loaded.error().what.c_str());
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(otterstax::test::single(loaded.value()).types()[0].type() == components::types::logical_type::HUGEINT);
    REQUIRE(otterstax::test::cell(otterstax::test::single(loaded.value()), 0, 0).value<components::types::int128_t>() == tail);

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: a decimal with a scale reads back as the engine's own DECIMAL") {
    // The symmetric half of chunk_to_arrow's writer, which emits a DECIMAL as
    // decimal128(width, scale): one arriving under that pair is read as DECIMAL(width, scale)
    // holding the same unscaled integer — 123 under scale 2 being 1.23. This was a refusal for
    // as long as the writer could only put the unscaled integer back out as a bare int64;
    // carrying it now is what makes the two directions agree.
    const std::string path = "/tmp/otterstax_test_parquet_scaled.parquet";
    std::filesystem::remove(path);
    write_decimal_parquet(path, 10, 2, {components::types::int128_t{123}});

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);
    INFO("error: " << loaded.error().what.c_str());
    REQUIRE_FALSE(loaded.has_error());
    // types() answers a vector by value, so it is kept alive for the checks below.
    const auto loaded_types = otterstax::test::single(loaded.value()).types();
    const auto& type = loaded_types[0];
    REQUIRE(type.type() == components::types::logical_type::DECIMAL);
    const auto* spec = type.extension_as<components::types::decimal_logical_type_extension>();
    REQUIRE(spec->width() == 10);
    REQUIRE(spec->scale() == 2);
    // Precision 10 is stored as an int64 payload, and the payload is the unscaled integer.
    REQUIRE(type.to_physical_type() == components::types::physical_type::INT64);
    REQUIRE(otterstax::test::cell(otterstax::test::single(loaded.value()), 0, 0).value<int64_t>() == 123);

    std::filesystem::remove(path);
}

TEST_CASE("arrow_schema_to_struct: a decimal whose scale the engine cannot construct keeps its refusal") {
    // The engine builds a DECIMAL only with `scale <= width`, while Arrow will declare a
    // decimal128 whose scale exceeds its precision. Such a column has no spec the engine would
    // hold without reinterpreting it, so it is refused where the schema is read rather than
    // carried under a changed one. Asserted on the schema directly: the point is the mapping,
    // and no compliant parquet writer would produce the file anyway.
    auto schema = arrow::schema({arrow::field("wide", arrow::decimal128(4, 6))});

    auto mapped = tsl::arrow_schema_to_struct(std::pmr::new_delete_resource(), schema);

    REQUIRE(mapped.has_error());
    REQUIRE(mapped.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string_view{mapped.error().what.c_str()}.find("wide") != std::string_view::npos);
}

TEST_CASE("parquet_to_chunk: a date column is read by otterbrix's importer as the engine's DATE") {
    // 19000 days after 1970-01-01 is 2022-01-08; the engine counts from 2000-01-01, so the
    // importer moves the value by the 10957 days between the epochs.
    const std::string path = "/tmp/otterstax_test_parquet_date.parquet";
    std::filesystem::remove(path);

    arrow::Date32Builder b;
    REQUIRE(b.Append(19000).ok());
    REQUIRE(b.AppendNull().ok());
    std::shared_ptr<arrow::Array> arr;
    REQUIRE(b.Finish(&arr).ok());
    auto schema = arrow::schema({arrow::field("day", arrow::date32())});
    auto table = arrow::Table::Make(schema, {arr});
    auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    REQUIRE(sink->Close().ok());

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);
    INFO("error: " << (loaded.has_error() ? loaded.error().what.c_str() : ""));
    REQUIRE_FALSE(loaded.has_error());
    REQUIRE(otterstax::test::single(loaded.value()).size() == 2);
    const auto types = otterstax::test::single(loaded.value()).types();
    REQUIRE(types[0].type() == components::types::logical_type::DATE);
    REQUIRE(types[0].alias() == "day");
    REQUIRE(otterstax::test::cell(otterstax::test::single(loaded.value()), 0, 0).value<core::date::date_t>().value.count() == 19000 - 10957);
    REQUIRE(otterstax::test::cell(otterstax::test::single(loaded.value()), 0, 1).is_null());

    std::filesystem::remove(path);
}

namespace {
    // A column neither otterbrix's importer nor the shim has a reading for: half-precision float.
    void write_half_float_parquet(const std::string& path) {
        arrow::HalfFloatBuilder b;
        REQUIRE(b.Append(static_cast<uint16_t>(0x3c00)).ok()); // 1.0
        std::shared_ptr<arrow::Array> arr;
        REQUIRE(b.Finish(&arr).ok());
        auto schema = arrow::schema({arrow::field("half", arrow::float16())});
        auto table = arrow::Table::Make(schema, {arr});
        auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
        REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
        REQUIRE(sink->Close().ok());
    }
} // namespace

TEST_CASE("parquet_to_chunk: a column with no reading is unimplemented_yet naming the column") {
    const std::string path = "/tmp/otterstax_test_parquet_half.parquet";
    std::filesystem::remove(path);
    write_half_float_parquet(path);

    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(), path);

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string_view{loaded.error().what.c_str()}.find("half") != std::string_view::npos);

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: a decimal128(38, 0) column is discovered as HUGEINT") {
    // Discovery answers the same mapping the loader uses, so a table is never created
    // with a column type the load would then refuse or read differently.
    const std::string path = "/tmp/otterstax_test_parquet_struct_hugeint.parquet";
    std::filesystem::remove(path);
    write_decimal_parquet(path, 38, 0, {absl::MakeInt128(1, 0)});

    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), path);
    REQUIRE_FALSE(type.has_error());
    REQUIRE(type.value().child_types().size() == 1);
    REQUIRE(type.value().child_types()[0].type() == components::types::logical_type::HUGEINT);

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: a decimal with a scale is discovered as the engine's own DECIMAL") {
    // Discovery must answer the same mapping the loader uses, or a table gets created with a
    // column type the load then refuses or reads differently. The loader carries a scaled
    // decimal now, so discovery carries it too — this was the refusal case while it did not.
    const std::string path = "/tmp/otterstax_test_parquet_struct_scaled.parquet";
    std::filesystem::remove(path);
    write_decimal_parquet(path, 10, 2, {components::types::int128_t{123}});

    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), path);
    INFO("error: " << type.error().what.c_str());
    REQUIRE_FALSE(type.has_error());
    REQUIRE(type.value().child_types().size() == 1);
    const auto& discovered = type.value().child_types()[0];
    REQUIRE(discovered.type() == components::types::logical_type::DECIMAL);
    const auto* spec = discovered.extension_as<components::types::decimal_logical_type_extension>();
    REQUIRE(spec->width() == 10);
    REQUIRE(spec->scale() == 2);

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: a column with no reading is refused at discovery") {
    // The column the loader refuses (the case above): discovery must refuse it too, rather than
    // create a table whose load then fails on the first row.
    const std::string path = "/tmp/otterstax_test_parquet_struct_half.parquet";
    std::filesystem::remove(path);
    write_half_float_parquet(path);

    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), path);

    REQUIRE(type.has_error());
    REQUIRE(type.error().type == core::error_code_t::unimplemented_yet);

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_struct: a date column is discovered as the DATE the load produces") {
    const std::string path = "/tmp/otterstax_test_parquet_struct_date.parquet";
    std::filesystem::remove(path);

    arrow::Date32Builder day_b;
    REQUIRE(day_b.Append(19000).ok());
    std::shared_ptr<arrow::Array> day_arr;
    REQUIRE(day_b.Finish(&day_arr).ok());
    auto day_schema = arrow::schema({arrow::field("day", arrow::date32())});
    auto day_table = arrow::Table::Make(day_schema, {day_arr});
    auto sink = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*day_table, arrow::default_memory_pool(), sink, 1024).ok());
    REQUIRE(sink->Close().ok());

    auto type = tsl::parquet_to_struct(std::pmr::new_delete_resource(), path);
    INFO("error: " << (type.has_error() ? type.error().what.c_str() : ""));
    REQUIRE_FALSE(type.has_error());
    REQUIRE(type.value().child_types().size() == 1);
    REQUIRE(type.value().child_types()[0].type() == components::types::logical_type::DATE);
    REQUIRE(type.value().child_types()[0].alias() == "day");

    std::filesystem::remove(path);
}

TEST_CASE("parquet_to_chunk: a buffer that is not parquet is invalid_text_representation") {
    const std::string bytes = "id,name\n1,Alice\n";
    auto loaded = tsl::parquet_to_chunk(std::pmr::new_delete_resource(),
                                        reinterpret_cast<const uint8_t*>(bytes.data()),
                                        bytes.size());

    REQUIRE(loaded.has_error());
    REQUIRE(loaded.error().type == core::error_code_t::invalid_text_representation);
}

namespace {

// Counts the calls that reach it once closed: one after the load returned came from
// something the load left running.
class closing_resource final : public std::pmr::memory_resource {
public:
    void close() noexcept { closed_ = true; }
    int calls_after_close() const noexcept { return late_calls_.load(); }

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        count();
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
        count();
        std::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
    }
    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    void count() noexcept {
        if (closed_.load()) {
            ++late_calls_;
        }
    }

    std::atomic<bool> closed_{false};
    std::atomic<int> late_calls_{0};
};

// A parquet file whose asynchronous reads (the reader's pre-buffering) run as Arrow's own do,
// as a task of the executor the reader's IOContext names, which lets the read's future and its
// buffer go only after it has woken the reader. The pause before it does makes that window wide
// enough to be seen.
class late_release_file final : public arrow::io::RandomAccessFile {
public:
    explicit late_release_file(std::shared_ptr<arrow::io::RandomAccessFile> file)
        : file_(std::move(file)) {}

    arrow::Status Close() override { return file_->Close(); }
    bool closed() const override { return file_->closed(); }
    arrow::Result<int64_t> Tell() const override { return file_->Tell(); }
    arrow::Status Seek(int64_t position) override { return file_->Seek(position); }
    arrow::Result<int64_t> GetSize() override { return file_->GetSize(); }
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override { return file_->Read(nbytes, out); }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override { return file_->Read(nbytes); }
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        return file_->ReadAt(position, nbytes, out);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
        return file_->ReadAt(position, nbytes);
    }

    arrow::Future<std::shared_ptr<arrow::Buffer>>
    ReadAsync(const arrow::io::IOContext& context, int64_t position, int64_t nbytes) override {
        auto read = arrow::Future<std::shared_ptr<arrow::Buffer>>::Make();
        auto spawned = context.executor()->Spawn([file = file_, read, position, nbytes]() mutable {
            auto buffer = file->ReadAt(position, nbytes);
            auto held = buffer.ok() ? *buffer : nullptr;
            read.MarkFinished(std::move(buffer));
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            held.reset();
            read = {};
        });
        if (!spawned.ok()) {
            return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(spawned);
        }
        return read;
    }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
};

} // namespace

TEST_CASE("parquet_to_chunk: nothing of the load reaches the resource after it returned") {
    // The reads land on the caller's resource (the file is opened on its pool, as the path
    // overload opens it); a read task still holding its buffer when the load returns would
    // free it into a resource the caller may already have destroyed.
    const std::string path = "/tmp/otterstax_test_parquet_to_chunk_late.parquet";
    write_people_parquet(path);
    closing_resource resource;
    auto file = arrow::io::ReadableFile::Open(path, tsl::arrow_pool(&resource));
    REQUIRE(file.ok());
    {
        auto loaded = tsl::parquet_to_chunk(&resource, std::make_shared<late_release_file>(*file));
        REQUIRE_FALSE(loaded.has_error());
        CHECK(otterstax::test::single(loaded.value()).size() == 5);
    }
    resource.close();
    // Whatever Arrow's shared IO pool still runs for the load ends before the count.
    static_cast<arrow::internal::ThreadPool*>(arrow::io::default_io_context().executor())->WaitForIdle();
    CHECK(resource.calls_after_close() == 0);
    std::filesystem::remove(path);
}
