// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Allocation microbenchmarks: what the memory_resource / stack-buffer work changed, measured by
// time AND by the heap allocations each operation makes. Every benchmark reports
//   allocs/op, bytes/op       — heap allocations per iteration (global operator new, which
//                               std::pmr::new_delete_resource and every std container reach)
//   allocs/item               — per row or per cell where the operation has items
//
// Two kinds of benchmark:
//   BM_real_*   — the production code path. Built in both trees (this one and a worktree of the
//                 commit before the change), so the same benchmark compares the two builds;
//                 OTX_BENCH_NEW picks the call shape where a signature changed.
//   BM_old_* / BM_new_* — a pattern the change replaced, side by side in one run: the old one
//                 written out as it was, the new one through the production helpers. New tree
//                 only (the helpers do not exist before it).
//
// Every resource here is new_delete_resource, so each allocation is a counted operator new; a
// pool resource would hide them behind its own reuse.

#include "frontend/common/resultset_utils.hpp"
#include "frontend/mysql_server/resultset/mysql_resultset.hpp"
#include "frontend/postgres_server/resultset/postgres_resultset.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "otterbrix/translators/output/chunk_to_ndjson.hpp"

#if __has_include("utility/stack_arena.hpp")
#define OTX_BENCH_NEW 1
#include "connectors/common/wait.hpp"
#include "utility/pmr_text.hpp"
#include "utility/stack_arena.hpp"
#include "utility/string_hash.hpp"
#else
#define OTX_BENCH_NEW 0
#endif

#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <arrow/io/memory.h>
#include <benchmark/benchmark.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <atomic>
#include <cstring>
#include <cassert>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <functional>
#include <memory_resource>
#include <new>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// ─── Allocation counter ────────────────────────────────────────────────────────
// The global operator new of the whole bench binary, counting per thread (a benchmark runs on
// its own thread; a relaxed thread_local add costs nothing measurable).

namespace {
    thread_local std::size_t t_allocs = 0;
    thread_local std::size_t t_bytes = 0;

    void* counted_alloc(std::size_t size, std::size_t align = alignof(std::max_align_t)) {
        ++t_allocs;
        t_bytes += size;
        void* p = align > alignof(std::max_align_t) ? std::aligned_alloc(align, (size + align - 1) / align * align)
                                                     : std::malloc(size == 0 ? 1 : size);
        if (p == nullptr) {
            throw std::bad_alloc{};
        }
        return p;
    }
} // namespace

void* operator new(std::size_t size) { return counted_alloc(size); }
void* operator new[](std::size_t size) { return counted_alloc(size); }
void* operator new(std::size_t size, std::align_val_t align) { return counted_alloc(size, static_cast<std::size_t>(align)); }
void* operator new[](std::size_t size, std::align_val_t align) {
    return counted_alloc(size, static_cast<std::size_t>(align));
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return counted_alloc(size);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
    try {
        return counted_alloc(size);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }

namespace {

    // Counts the allocations of the benchmark loop it is declared right before; on destruction
    // reports them per iteration and, given `items`, per item.
    class alloc_report_t {
    public:
        explicit alloc_report_t(benchmark::State& state, std::size_t items_per_iteration = 0)
            : state_(state)
            , items_(items_per_iteration)
            , allocs_(t_allocs)
            , bytes_(t_bytes) {}
        ~alloc_report_t() {
            const double iterations = static_cast<double>(std::max<benchmark::IterationCount>(state_.iterations(), 1));
            const double allocs = static_cast<double>(t_allocs - allocs_) / iterations;
            state_.counters["allocs/op"] = allocs;
            state_.counters["bytes/op"] = static_cast<double>(t_bytes - bytes_) / iterations;
            if (items_ > 0) {
                state_.counters["allocs/item"] = allocs / static_cast<double>(items_);
                state_.SetItemsProcessed(state_.iterations() * static_cast<int64_t>(items_));
            }
        }

    private:
        benchmark::State& state_;
        std::size_t items_;
        std::size_t allocs_;
        std::size_t bytes_;
    };

    std::pmr::memory_resource* heap() { return std::pmr::new_delete_resource(); }

    constexpr std::size_t kRows = 1024;

    // ─── Fixtures ──────────────────────────────────────────────────────────────

    // The ClickHouse columns read as text: DateTime64(6), Decimal(38, 10), UInt128, UUID and a
    // 32-character String (past every short-string buffer).
    clickhouse::Block ch_text_block() {
        auto dt64 = std::make_shared<clickhouse::ColumnDateTime64>(6);
        auto dec = std::make_shared<clickhouse::ColumnDecimal>(38, 10);
        auto u128 = std::make_shared<clickhouse::ColumnUInt128>();
        auto uuid = std::make_shared<clickhouse::ColumnUUID>();
        auto name = std::make_shared<clickhouse::ColumnString>();
        for (std::size_t i = 0; i < kRows; ++i) {
            dt64->Append(static_cast<int64_t>(1'700'000'000'000'000LL + static_cast<int64_t>(i) * 1'234'567));
            dec->Append(clickhouse::Int128(static_cast<int64_t>(i) * 1'000'000'007LL) * 1'000'000'000LL);
            u128->Append(absl::MakeUint128(i, i * 31));
            uuid->Append(clickhouse::UUID{0x0123456789abcdefULL + i, 0xfedcba9876543210ULL - i});
            name->Append("row-" + std::to_string(i) + "-padded-to-32-characters!!");
        }
        clickhouse::Block block;
        block.AppendColumn("ts", dt64);
        block.AppendColumn("amount", dec);
        block.AppendColumn("big", u128);
        block.AppendColumn("id", uuid);
        block.AppendColumn("name", name);
        return block;
    }

    // The frontend's text cells: INTEGER, BIGINT, DOUBLE, a 32-character STRING, DECIMAL(18, 4).
    components::vector::data_chunk_t encode_chunk(std::pmr::memory_resource* resource) {
        namespace ct = components::types;
        std::pmr::vector<ct::complex_logical_type> columns(resource);
        columns.emplace_back(ct::logical_type::INTEGER, "i");
        columns.emplace_back(ct::logical_type::BIGINT, "b");
        columns.emplace_back(ct::logical_type::DOUBLE, "d");
        columns.emplace_back(ct::logical_type::STRING_LITERAL, "s");
        auto decimal = ct::complex_logical_type::create_decimal(resource, 18, 4, "amount");
        assert(!decimal.has_error());
        columns.push_back(decimal.value());
        components::vector::data_chunk_t chunk(resource, columns, kRows);
        for (std::size_t row = 0; row < kRows; ++row) {
            chunk.set_value(0, row, ct::logical_value_t{resource, static_cast<int32_t>(row * 7)});
            chunk.set_value(1, row, ct::logical_value_t{resource, static_cast<int64_t>(row) * 1'000'003});
            chunk.set_value(2, row, ct::logical_value_t{resource, static_cast<double>(row) * 1.25});
            const std::string text = "row-" + std::to_string(row) + "-padded-to-32-characters!!";
            chunk.set_value(3, row, ct::logical_value_t{resource, std::string_view(text)});
            chunk.set_value(
                4,
                row,
                ct::logical_value_t::create_decimal(resource, decimal.value(), static_cast<ct::int128_t>(row) * 12345));
        }
        chunk.set_cardinality(kRows);
        return chunk;
    }

    const otterstax::names::alias_registry_t& alloc_bench_aliases() {
        static const auto aliases = [] {
            otterstax::names::alias_registry_t registry;
            registry.add("mysql", {backend_type_t::MySQL, "bill", ""});
            return registry;
        }();
        return aliases;
    }

} // namespace

// ─── Real paths (both trees) ─────────────────────────────────────────────────

// ch_to_chunk over the columns read as text: every value's text is built per row. Before: a
// heap string per value, then the engine's copy of it; now: a stack buffer and the engine's copy.
static void BM_real_ch_to_chunk_text_columns(benchmark::State& state) {
    const auto block = ch_text_block();
    alloc_report_t report(state, kRows * block.GetColumnCount());
    for (auto _ : state) {
        auto chunk = tsl::ch_to_chunk(heap(), block);
        benchmark::DoNotOptimize(chunk.value().size());
    }
}
BENCHMARK(BM_real_ch_to_chunk_text_columns);

// pg_to_chunk over a 1024-row text-protocol result: three 32-character varchar columns, an int4,
// a float8. Before: a std::string and the engine's heap copy per string cell; now: the vector's
// string buffer.
static void BM_real_pg_to_chunk_text_columns(benchmark::State& state) {
    PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK);
    struct column_t {
        const char* name;
        Oid oid;
    };
    const column_t columns[] = {{"a", 1043}, {"b", 1043}, {"c", 1043}, {"n", 23}, {"x", 701}};
    PGresAttDesc attrs[5];
    for (int c = 0; c < 5; ++c) {
        attrs[c] = PGresAttDesc{};
        attrs[c].name = const_cast<char*>(columns[c].name);
        attrs[c].typid = columns[c].oid;
        attrs[c].typlen = -1;
        attrs[c].atttypmod = -1;
    }
    PQsetResultAttrs(result, 5, attrs);
    for (int r = 0; r < static_cast<int>(kRows); ++r) {
        const std::string text = "row-" + std::to_string(r) + "-padded-to-32-characters!!";
        const std::string number = std::to_string(r * 7);
        const std::string real = std::to_string(r * 1.25);
        for (int c = 0; c < 3; ++c) {
            PQsetvalue(result, r, c, const_cast<char*>(text.c_str()), static_cast<int>(text.size()));
        }
        PQsetvalue(result, r, 3, const_cast<char*>(number.c_str()), static_cast<int>(number.size()));
        PQsetvalue(result, r, 4, const_cast<char*>(real.c_str()), static_cast<int>(real.size()));
    }
    {
        alloc_report_t report(state, kRows * 5);
        for (auto _ : state) {
            auto chunk = tsl::pg_to_chunk(heap(), result);
            benchmark::DoNotOptimize(chunk.value().size());
        }
    }
    PQclear(result);
}
BENCHMARK(BM_real_pg_to_chunk_text_columns);

// encode_to_text over every cell of a 1024-row, 5-column chunk, the MySQL / PostgreSQL text rows.
// Before: a std::string per cell (a STRING cell copied); now: a view into the chunk or a reused
// scratch buffer.
static void BM_real_encode_to_text(benchmark::State& state) {
    const auto chunk = encode_chunk(heap());
#if OTX_BENCH_NEW
    std::pmr::string scratch(heap());
#endif
    alloc_report_t report(state, kRows * chunk.column_count());
    for (auto _ : state) {
        std::size_t bytes = 0;
        for (std::size_t row = 0; row < kRows; ++row) {
            for (std::size_t col = 0; col < chunk.column_count(); ++col) {
#if OTX_BENCH_NEW
                bytes += frontend::encode_to_text(chunk, col, row, scratch).size();
#else
                bytes += frontend::encode_to_text(chunk, col, row).size();
#endif
            }
        }
        benchmark::DoNotOptimize(bytes);
    }
}
BENCHMARK(BM_real_encode_to_text);

// A whole MySQL text / PostgreSQL resultset of the 1024-row chunk: every row framed and the
// packets collected for the send. Before: a vector per row (and per packet); now: one buffer.
static void BM_real_mysql_resultset_text(benchmark::State& state) {
    const auto chunk = encode_chunk(heap());
    frontend::mysql::packet_writer writer;
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        frontend::mysql::mysql_resultset result(writer, frontend::result_encoding::TEXT);
        result.add_chunk_columns(chunk);
        for (std::size_t row = 0; row < kRows; ++row) {
            result.add_row(chunk, row);
        }
        uint8_t sequence = 1;
        auto packets = frontend::mysql::mysql_resultset::build_packets(std::move(result), sequence);
        benchmark::DoNotOptimize(packets.size());
    }
}
BENCHMARK(BM_real_mysql_resultset_text);

static void BM_real_postgres_resultset_text(benchmark::State& state) {
    const auto chunk = encode_chunk(heap());
    frontend::postgres::packet_writer writer;
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        frontend::postgres::postgres_resultset result(writer);
        result.add_chunk_columns(chunk);
        for (std::size_t row = 0; row < kRows; ++row) {
            result.add_row(chunk, row);
        }
        auto packets = frontend::postgres::postgres_resultset::build_packets(std::move(result));
        benchmark::DoNotOptimize(packets.size());
    }
}
BENCHMARK(BM_real_postgres_resultset_text);

// chunk_to_ndjson of a 1024-row chunk into a sink that keeps nothing (the COPY ... TO writer).
static void BM_real_chunk_to_ndjson(benchmark::State& state) {
    std::pmr::vector<components::vector::data_chunk_t> chunks(heap());
    chunks.push_back(encode_chunk(heap()));
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        auto sink = std::make_shared<arrow::io::MockOutputStream>();
        auto written = tsl::chunk_to_ndjson(heap(), chunks, sink);
        benchmark::DoNotOptimize(written.has_error());
    }
}
BENCHMARK(BM_real_chunk_to_ndjson);

// sql_gen::generate_query of one parsed slot (a WHERE / ORDER BY / LIMIT over a remote table).
// Before: a std::stringstream, its str() copy, then the statement's own std::string; now: a pmr
// stream whose buffer becomes the statement.
static void BM_real_generate_query(benchmark::State& state) {
    GreenplumParser parser(heap(), alloc_bench_aliases());
    auto parsed = parser.parse("SELECT id, name, status FROM mysql.bill.orders WHERE id > 10 AND status = 'paid' "
                               "ORDER BY name LIMIT 5");
    assert(!parsed.has_error());
    auto& statement = *parsed.value()->otterbrix_params;
    auto& batch = statement.external_nodes.front();
    auto& slot = batch.front();
    const auto& parameters = statement.params_node->parameters();
    alloc_report_t report(state);
    for (auto _ : state) {
        auto sql = sql_gen::generate_query(*slot.node, &parameters, backend_type_t::MySQL, slot.target, batch, heap());
        benchmark::DoNotOptimize(sql.value().size());
    }
}
BENCHMARK(BM_real_generate_query);

#if OTX_BENCH_NEW

// ─── Replaced patterns, old vs new (this tree only) ──────────────────────────

namespace {
    constexpr std::string_view kAlias = "mysql_production_1";
    constexpr std::string_view kSql = "SELECT `id`, `name` FROM `bill`.`orders` WHERE `id` > 10 LIMIT 5;";
    constexpr std::string_view kReason = "Lost connection to MySQL server during query";
} // namespace

// An error message: "[Run query] Alias: … query [ … ] failed: (2013) …" into an error_t.
static void BM_old_error_message_std_concat(benchmark::State& state) {
    const std::string alias(kAlias);
    alloc_report_t report(state);
    for (auto _ : state) {
        std::string what = "[Run query] Alias: " + alias + " query [" + std::string(kSql) + "] failed: (" +
                           std::to_string(2013) + ") " + std::string(kReason);
        core::error_t error(core::error_code_t::io_error, std::pmr::string{what.c_str(), heap()});
        benchmark::DoNotOptimize(error.what.size());
    }
}
BENCHMARK(BM_old_error_message_std_concat);

static void BM_new_error_message_concat(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        auto what = otterstax::concat(heap(), {"[Run query] Alias: ", kAlias, " query [", kSql, "] failed: ("});
        otterstax::append_number(what, 2013);
        what += ") ";
        what += kReason;
        core::error_t error(core::error_code_t::io_error, std::move(what));
        benchmark::DoNotOptimize(error.what.size());
    }
}
BENCHMARK(BM_new_error_message_concat);

// A lookup by uid in a string-keyed registry (the catalog's connection registry, the pools'
// alias maps, the ClickHouse metadata).
static void BM_old_lookup_builds_key(benchmark::State& state) {
    std::pmr::unordered_map<std::pmr::string, int> map(heap());
    map.emplace(std::pmr::string(kAlias, heap()), 1);
    const std::string uid(kAlias);
    alloc_report_t report(state);
    for (auto _ : state) {
        auto it = map.find(std::pmr::string{uid.c_str(), heap()});
        benchmark::DoNotOptimize(it->second);
    }
}
BENCHMARK(BM_old_lookup_builds_key);

static void BM_new_lookup_transparent(benchmark::State& state) {
    otterstax::pmr_string_map_t<int> map(heap());
    map.emplace(kAlias, 1);
    const std::string uid(kAlias);
    alloc_report_t report(state);
    for (auto _ : state) {
        auto it = map.find(uid);
        benchmark::DoNotOptimize(it->second);
    }
}
BENCHMARK(BM_new_lookup_transparent);

// One value's text handed to the engine (a DateTime64(6) as ClickHouse values are read): a heap
// string first, which the engine copies, or a stack buffer the engine copies.
static void BM_old_value_text_heap_string(benchmark::State& state) {
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        for (std::size_t i = 0; i < kRows; ++i) {
            char clock[48];
            std::snprintf(clock, sizeof(clock), "2024-01-02 03:04:%02zu.%06zu", i % 60, i);
            std::pmr::string text(clock, heap());
            components::types::logical_value_t value{heap(), std::move(text)};
            benchmark::DoNotOptimize(value);
        }
    }
}
BENCHMARK(BM_old_value_text_heap_string);

static void BM_new_value_text_stack(benchmark::State& state) {
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        for (std::size_t i = 0; i < kRows; ++i) {
            char clock[48];
            const int n = std::snprintf(clock, sizeof(clock), "2024-01-02 03:04:%02zu.%06zu", i % 60, i);
            components::types::logical_value_t value{heap(), std::string_view(clock, static_cast<std::size_t>(n))};
            benchmark::DoNotOptimize(value);
        }
    }
}
BENCHMARK(BM_new_value_text_stack);

// A statement's scratch containers: the slots of one batch before they are sent (4 entries of a
// statement's size), and the breadth-first queue over a 30-node plan.
namespace {
    struct ready_like_t {
        std::size_t index;
        std::string_view uid;
        char statement[96];
    };
} // namespace

static void BM_old_scratch_heap(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        std::pmr::vector<ready_like_t> ready(heap());
        for (std::size_t i = 0; i < 4; ++i) {
            ready.push_back(ready_like_t{i, kAlias, {}});
        }
        std::pmr::vector<const void*> queue(heap());
        for (std::size_t i = 0; i < 30; ++i) {
            queue.push_back(&ready);
        }
        benchmark::DoNotOptimize(ready.size() + queue.size());
    }
}
BENCHMARK(BM_old_scratch_heap);

static void BM_new_scratch_stack_arena(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        otterstax::stack_arena_t<1024> scratch(heap());
        std::pmr::vector<ready_like_t> ready(scratch.resource());
        ready.reserve(4);
        for (std::size_t i = 0; i < 4; ++i) {
            ready.push_back(ready_like_t{i, kAlias, {}});
        }
        otterstax::stack_arena_t<512> bfs(heap());
        std::pmr::vector<const void*> queue(bfs.resource());
        for (std::size_t i = 0; i < 30; ++i) {
            queue.push_back(&ready);
        }
        benchmark::DoNotOptimize(ready.size() + queue.size());
    }
}
BENCHMARK(BM_new_scratch_stack_arena);

namespace {
    using pmr_ostringstream_t =
        std::basic_ostringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;
} // namespace

// JSON string escaping of a 64-character value with one quote in it: a character at a time
// through operator<<, or unescaped runs in one write each (no allocation either way: CPU only).
namespace {
    constexpr std::string_view kEscapeValue = "Lorem ipsum dolor sit amet, \"consectetur\" adipiscing elit, sed do.";

    void escape_per_char(std::ostream& out, std::string_view s) {
        for (unsigned char c : s) {
            switch (c) {
                case '"':  out << "\\\""; break;
                case '\\': out << "\\\\"; break;
                default:   out << static_cast<char>(c); break;
            }
        }
    }

    void escape_runs(std::ostream& out, std::string_view s) {
        std::size_t run = 0;
        for (std::size_t i = 0; i < s.size(); ++i) {
            const auto c = static_cast<unsigned char>(s[i]);
            if (c >= 0x20 && c != '"' && c != '\\') {
                continue;
            }
            out.write(s.data() + run, static_cast<std::streamsize>(i - run));
            run = i + 1;
            out << (c == '"' ? "\\\"" : "\\\\");
        }
        out.write(s.data() + run, static_cast<std::streamsize>(s.size() - run));
    }
} // namespace

static void BM_old_escape_per_char(benchmark::State& state) {
    pmr_ostringstream_t out(std::ios_base::out, heap());
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        for (std::size_t i = 0; i < kRows; ++i) {
            escape_per_char(out, kEscapeValue);
        }
        benchmark::DoNotOptimize(out.tellp());
        auto buffer = std::move(out).str();
        buffer.clear();
        out.str(std::move(buffer));
    }
}
BENCHMARK(BM_old_escape_per_char);

static void BM_new_escape_runs(benchmark::State& state) {
    pmr_ostringstream_t out(std::ios_base::out, heap());
    alloc_report_t report(state, kRows);
    for (auto _ : state) {
        for (std::size_t i = 0; i < kRows; ++i) {
            escape_runs(out, kEscapeValue);
        }
        benchmark::DoNotOptimize(out.tellp());
        auto buffer = std::move(out).str();
        buffer.clear();
        out.str(std::move(buffer));
    }
}
BENCHMARK(BM_new_escape_runs);

// A generated statement handed to the pool: std::stringstream → str() → std::string, or a pmr
// stream whose buffer is moved out as the statement's std::pmr::string.
static void BM_old_sql_stream_std(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        std::stringstream stream;
        stream << "SELECT `id`, `name`, `status` FROM `bill`.`orders` WHERE (`id` > 10 AND `status` = 'paid') "
                  "ORDER BY `name` ASC LIMIT 5"
               << ';';
        std::string sql = stream.str();
        benchmark::DoNotOptimize(sql.size());
    }
}
BENCHMARK(BM_old_sql_stream_std);

static void BM_new_sql_stream_pmr(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        sql_gen::sql_stream_t stream(std::ios_base::out, heap());
        stream << "SELECT `id`, `name`, `status` FROM `bill`.`orders` WHERE (`id` > 10 AND `status` = 'paid') "
                  "ORDER BY `name` ASC LIMIT 5"
               << ';';
        std::pmr::string sql = std::move(stream).str();
        benchmark::DoNotOptimize(sql.size());
    }
}
BENCHMARK(BM_new_sql_stream_pmr);

// The wait ticket of one pool request: make_shared (global heap) or allocate_shared on the
// caller's resource. The same one allocation; what moves is whose allocator serves it.
static void BM_old_wait_ticket_make_shared(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        auto ticket = std::make_shared<conn::wait_ticket_t>();
        benchmark::DoNotOptimize(ticket.get());
    }
}
BENCHMARK(BM_old_wait_ticket_make_shared);

static void BM_new_wait_ticket_allocate_shared(benchmark::State& state) {
    alloc_report_t report(state);
    for (auto _ : state) {
        auto ticket = std::allocate_shared<conn::wait_ticket_t>(std::pmr::polymorphic_allocator<conn::wait_ticket_t>(heap()));
        benchmark::DoNotOptimize(ticket.get());
    }
}
BENCHMARK(BM_new_wait_ticket_allocate_shared);

// The same from 8 threads at once, the second on one shared synchronized_pool_resource — what a
// Worker's resource is in production (the engine's). Time only: the threads' counts would sum.
static void BM_old_wait_ticket_make_shared_mt(benchmark::State& state) {
    for (auto _ : state) {
        auto ticket = std::make_shared<conn::wait_ticket_t>();
        benchmark::DoNotOptimize(ticket.get());
    }
}
BENCHMARK(BM_old_wait_ticket_make_shared_mt)->Threads(8)->UseRealTime();

static void BM_new_wait_ticket_synchronized_pool_mt(benchmark::State& state) {
    static std::pmr::synchronized_pool_resource pool;
    for (auto _ : state) {
        auto ticket =
            std::allocate_shared<conn::wait_ticket_t>(std::pmr::polymorphic_allocator<conn::wait_ticket_t>(&pool));
        benchmark::DoNotOptimize(ticket.get());
    }
}
BENCHMARK(BM_new_wait_ticket_synchronized_pool_mt)->Threads(8)->UseRealTime();

// Compares one pool request's waker in two shapes: actor_parking_t::watch's std::function lambda
// (two captured shared_ptrs, a heap block per request outside any resource) and the ticket as
// waker (one object, virtual wake(), one allocation on the caller's resource); timed 1 and 8 threads.
namespace {
    struct parking_target_t {
        std::mutex mutex;
        bool parked = false;
    };

    struct waker_ticket_t {
        std::atomic<bool> settled{false};
    };

    struct waking_ticket_t {
        explicit waking_ticket_t(std::shared_ptr<parking_target_t> t)
            : target(std::move(t)) {}
        virtual ~waking_ticket_t() = default;
        virtual void wake() {
            settled.store(true);
            std::lock_guard guard(target->mutex);
            target->parked = false;
        }
        std::atomic<bool> settled{false};
        std::shared_ptr<parking_target_t> target;
    };

    void waker_std_function(benchmark::State& state, std::pmr::memory_resource* resource) {
        // Each Worker has its own parking target; one per thread here.
        static thread_local auto target = std::make_shared<parking_target_t>();
        for (auto _ : state) {
            auto ticket =
                std::allocate_shared<waker_ticket_t>(std::pmr::polymorphic_allocator<waker_ticket_t>(resource));
            std::function<void()> waker = [target = target, ticket] {
                ticket->settled.store(true);
                std::lock_guard guard(target->mutex);
                target->parked = false;
            };
            auto job_waker = std::move(waker); // into the request, as submit does
            job_waker();                        // on the pool thread
            benchmark::DoNotOptimize(ticket->settled.load());
        }
    }

    void waker_single_object(benchmark::State& state, std::pmr::memory_resource* resource) {
        // Each Worker has its own parking target; one per thread here.
        static thread_local auto target = std::make_shared<parking_target_t>();
        for (auto _ : state) {
            std::shared_ptr<waking_ticket_t> ticket = std::allocate_shared<waking_ticket_t>(
                std::pmr::polymorphic_allocator<waking_ticket_t>(resource), target);
            auto job_waker = ticket; // the request keeps the ticket; the owner keeps it too
            job_waker->wake();
            benchmark::DoNotOptimize(ticket->settled.load());
        }
    }
} // namespace

static void BM_waker_std_function(benchmark::State& state) {
    alloc_report_t report(state);
    waker_std_function(state, heap());
}
BENCHMARK(BM_waker_std_function);

static void BM_waker_single_object(benchmark::State& state) {
    alloc_report_t report(state);
    waker_single_object(state, heap());
}
BENCHMARK(BM_waker_single_object);

static void BM_waker_std_function_mt(benchmark::State& state) { waker_std_function(state, heap()); }
BENCHMARK(BM_waker_std_function_mt)->Threads(8)->UseRealTime();

static void BM_waker_single_object_mt(benchmark::State& state) { waker_single_object(state, heap()); }
BENCHMARK(BM_waker_single_object_mt)->Threads(8)->UseRealTime();

#endif // OTX_BENCH_NEW
