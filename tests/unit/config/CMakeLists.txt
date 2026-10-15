project(test_unit_config)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_connection_config.cpp
    test_keycloak_introspection.cpp
    test_security_config.cpp
    test_network_destinations.cpp
    test_file_access.cpp
    test_s3_security_config.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(
    ${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    config::config
    client_security
    otterbrix::otterbrix
    Catch2::Catch2
    Boost::filesystem
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
