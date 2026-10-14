project(test_unit_connectors)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_connection_pool.cpp
    test_connector_cancel.cpp
    test_pooled_managers.cpp
    test_pooled_clickhouse.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(
    ${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    # test_pooled_managers.cpp / test_pooled_clickhouse.cpp drive the real
    # mysql / pg / clickhouse ConnectorManagers (with stub connectors) through
    # their integration backends, so the binary
    # links the internal libraries the way test_utils does.
    lib_otterstax
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Boost::boost
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
