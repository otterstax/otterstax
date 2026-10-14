project(test_federation)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_federation_units.cpp
    test_federation_engine.cpp
    test_federation_rediscovery.cpp
    test_federation_pushdown.cpp
    test_federation_joins.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    catalog
    runtime
    otterstax_federation
    otterbrix_local
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
