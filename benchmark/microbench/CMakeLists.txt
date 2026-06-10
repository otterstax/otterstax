project(otterstax_benchmarks)

find_package(benchmark REQUIRED)

add_executable(otterstax_bench
    bench_translators.cpp
    bench_parser.cpp
    bench_query_gen.cpp
)

target_include_directories(otterstax_bench PRIVATE ${CMAKE_SOURCE_DIR})

target_link_libraries(otterstax_bench PRIVATE
    lib_otterstax
    otterbrix::otterbrix
    benchmark::benchmark
    benchmark::benchmark_main
)

if(ENABLE_TRACY)
    target_link_libraries(otterstax_bench PRIVATE Tracy::TracyClient)
endif()
