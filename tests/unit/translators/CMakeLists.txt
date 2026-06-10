project(test_unit_translators)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_ch_to_chunk.cpp
    test_chunk_to_arrow.cpp
    test_mysql_to_complex.cpp
    test_pg_to_chunk.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_SOURCE_DIR})

target_link_libraries(${PROJECT_NAME} PRIVATE
    lib_otterstax
    otterbrix::otterbrix
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
