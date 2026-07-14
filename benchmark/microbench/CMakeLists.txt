project(otterstax_benchmarks)

find_package(benchmark REQUIRED)

add_executable(otterstax_bench
    bench_translators.cpp
    bench_file_translators.cpp
    bench_parser.cpp
    bench_query_gen.cpp
    bench_grammar_extension.cpp
    bench_kafka_translators.cpp
)

target_include_directories(otterstax_bench PRIVATE ${CMAKE_SOURCE_DIR})

target_link_libraries(otterstax_bench PRIVATE
    lib_otterstax
    otterbrix::otterbrix
    otterbrix::s3_extension
    otterbrix::file_extension
    arrow::arrow
    benchmark::benchmark
    benchmark::benchmark_main
)

if(ENABLE_TRACY)
    target_link_libraries(otterstax_bench PRIVATE Tracy::TracyClient)
endif()

# Multiple Arrow conan packages exist in the cache; only the one CMake links here
# ships the CSV/JSON/Parquet readers used by bench_file_translators. run-bench.sh
# sets LD_LIBRARY_PATH from a glob of all conan lib dirs, which can resolve a
# different (minimal) libarrow first. Pin an absolute DT_RPATH to the build lib
# dir (where the correct shared libs are staged) and use --disable-new-dtags so
# DT_RPATH takes precedence over LD_LIBRARY_PATH.
set_target_properties(otterstax_bench PROPERTIES
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib"
    INSTALL_RPATH "${CMAKE_BINARY_DIR}/lib")
target_link_options(otterstax_bench PRIVATE "-Wl,--disable-new-dtags")

# kafka_runtime provides json_to_chunk / chunk_to_json for bench_kafka_translators
# (and transitively kafka_grammar for kafka_column_t). Kept as its own call so the
# diff stays clear of the Arrow-target line above.
target_link_libraries(otterstax_bench PRIVATE kafka_runtime)
