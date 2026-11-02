project(test_unit_connectors)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_backend_agent.cpp
    test_connector_cancel.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(
    ${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    # test_backend_agent.cpp drives the backend agents and actors
    # (otterstax_federation) over fake connectors.
    otterstax_federation
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Boost::boost
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
