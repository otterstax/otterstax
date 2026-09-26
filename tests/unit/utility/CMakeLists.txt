project(test_utils)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_asio_future_bridge.cpp
    test_ch_connector.cpp
    test_ch_statement.cpp
    test_connector_errors.cpp
    test_connector_manager.cpp
    test_mysql_connector.cpp
    test_parse_port.cpp
    test_session.cpp
    test_wait_barrier.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(
    ${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    # test_connector_manager.cpp drives the real ConnectorManagers (with mock
    # connectors) through a real CatalogManager over the otterbrix mock, so the
    # binary links the internal libraries the same way test_system does.
    lib_otterstax
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    # wait_barrier.hpp marshals connector results off an asio io_context, and
    # test_asio_future_bridge.cpp runs one directly, so Boost.Asio is a real
    # dependency of this binary, not just a transitive include.
    Boost::boost
    Catch2::Catch2
    otterbrix_log
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
