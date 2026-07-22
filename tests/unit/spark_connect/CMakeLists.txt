# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

project(test_unit_spark_connect)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_expression_to_plan.cpp
    test_type_converter.cpp
    test_relation_to_plan.cpp
    test_result_encoder.cpp
    test_catalog_relations.cpp
    test_show_string.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    spark_connect_server
    lib_otterstax
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
