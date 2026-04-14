set(OTTERBRIX_HEADERS
    config.hpp
    types.hpp
    translators/input/mysql_to_chunk.hpp
    translators/input/mysql_to_complex.hpp
    translators/input/pg_to_chunk.hpp
    translators/input/ch_to_chunk.hpp
    translators/input/arrow_to_chunk.hpp
    translators/input/parquet_to_chunk.hpp
    translators/input/csv_to_chunk.hpp
    translators/input/json_to_chunk.hpp
    translators/output/chunk_to_arrow.hpp
    operators/execute_plan.hpp
    parser/parser.hpp
    query_generation/sql_query_generator.hpp
)

set(OTTERBRIX_SOURCES
    translators/input/mysql_to_chunk.cpp
    translators/input/mysql_to_complex.cpp
    translators/input/pg_to_chunk.cpp
    translators/input/ch_to_chunk.cpp
    translators/input/arrow_to_chunk.cpp
    translators/input/parquet_to_chunk.cpp
    translators/input/csv_to_chunk.cpp
    translators/input/json_to_chunk.cpp
    translators/output/chunk_to_arrow.cpp
    translators/internal/doc_to_chunk.cpp
    operators/execute_plan.cpp
    parser/parser.cpp
    query_generation/sql_query_generator.cpp
)

add_library(otterbrix_local
    ${OTTERBRIX_HEADERS}
    ${OTTERBRIX_SOURCES}
)

target_link_libraries(otterbrix_local PUBLIC
    arrow::arrow
    Parquet::parquet_shared
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
)

target_include_directories(otterbrix_local PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)