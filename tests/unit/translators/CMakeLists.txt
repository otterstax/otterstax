project(test_unit_translators)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    pg_result_fixture.cpp
    test_ch_to_chunk.cpp
    test_chunk_to_arrow.cpp
    test_mysql_types.cpp
    test_mysql_to_chunk_dml.cpp
    test_pg_to_chunk.cpp
    test_csv_to_chunk.cpp
    test_ndjson_to_chunk.cpp
    test_parquet_to_chunk.cpp
    test_chunk_to_csv.cpp
    test_chunk_to_ndjson.cpp
    test_chunk_to_parquet.cpp
    test_chunk_windows.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_SOURCE_DIR})

# pg_result_fixture.cpp manufactures PGresult objects with a command tag through
# libpq's layout header, postgresql/internal/libpq-int.h. libpq installs it next
# to its public headers but off the public include path; getaddrinfo.h, which it
# includes, is a source-tree-only shim provided under libpq_internal/.
if(NOT PostgreSQL_INCLUDE_DIRS)
    message(FATAL_ERROR "test_unit_translators: PostgreSQL_INCLUDE_DIRS is empty; the PostgreSQL package must be found before this directory")
endif()
foreach(_pg_include_dir ${PostgreSQL_INCLUDE_DIRS})
    target_include_directories(${PROJECT_NAME} PRIVATE ${_pg_include_dir}/postgresql/internal)
endforeach()
target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libpq_internal)

target_link_libraries(${PROJECT_NAME} PRIVATE
    lib_otterstax
    otterbrix::otterbrix
    arrow::arrow
    PostgreSQL::PostgreSQL
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
