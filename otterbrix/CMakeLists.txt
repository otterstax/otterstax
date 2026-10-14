add_subdirectory(parser/grammar_extension/kafka)
add_subdirectory(federation)

set(OTTERBRIX_HEADERS
    backend_type.hpp
    translators/input/mysql_to_chunk.hpp
    translators/input/pg_to_chunk.hpp
    translators/input/ch_to_chunk.hpp
    translators/input/arrow_to_chunk.hpp
    translators/input/parquet_to_chunk.hpp
    translators/input/csv_to_chunk.hpp
    translators/input/ndjson_to_chunk.hpp
    translators/input/chunk_windows.hpp
    translators/input/decimal_text.hpp
    translators/error.hpp
    translators/output/chunk_to_arrow.hpp
    translators/output/chunk_to_parquet.hpp
    translators/output/chunk_to_csv.hpp
    translators/output/chunk_to_ndjson.hpp
    translators/output/writable_columns.hpp
    operators/engine_requests.hpp
    operators/schema_probe.hpp
    parser/parser.hpp
    parser/qualified_name.hpp
    query_generation/sql_query_generator.hpp
    schema/schema_utils.hpp
)

set(OTTERBRIX_SOURCES
    translators/input/mysql_to_chunk.cpp
    translators/input/pg_to_chunk.cpp
    translators/input/ch_to_chunk.cpp
    translators/input/arrow_to_chunk.cpp
    translators/input/parquet_to_chunk.cpp
    translators/input/csv_to_chunk.cpp
    translators/input/ndjson_to_chunk.cpp
    translators/input/chunk_windows.cpp
    translators/output/chunk_to_arrow.cpp
    translators/output/chunk_to_parquet.cpp
    translators/output/chunk_to_csv.cpp
    translators/output/chunk_to_ndjson.cpp
    translators/internal/doc_to_chunk.cpp
    operators/engine_requests.cpp
    parser/parser.cpp
    query_generation/sql_query_generator.cpp
    schema/schema_utils.cpp
)

# SQL grammar extensions (s3 / file): each builds its own flex+bison grammar via
# the vendored otterbrix_add_parser_extension() helper (cmake/, on CMAKE_MODULE_PATH).
# Defined before otterbrix_local so it can link the produced ALIAS targets. The
# GreenplumParser registers them (parser/grammar_extention/CLAUDE.md).
add_subdirectory(parser/grammar_extention/s3)
add_subdirectory(parser/grammar_extention/file)

add_library(otterbrix_local
    ${OTTERBRIX_HEADERS}
    ${OTTERBRIX_SOURCES}
)

target_link_libraries(otterbrix_local PUBLIC
    arrow::arrow
    Parquet::parquet_shared
    Boost::boost
    otterbrix::otterbrix
    otterbrix::s3_extension
    otterbrix::file_extension
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    kafka_grammar
)

if(ENABLE_TRACY)
    target_link_libraries(otterbrix_local PUBLIC Tracy::TracyClient)
endif()

target_include_directories(otterbrix_local PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)