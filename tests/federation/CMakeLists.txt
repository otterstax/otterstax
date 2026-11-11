project(test_federation)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_federation_registration.cpp
    test_connection_names.cpp
    test_federation_scans.cpp
    test_federation_offer.cpp
    test_federation_transactions.cpp
    test_federation_units.cpp
    test_federation_joins.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(${PROJECT_NAME} PRIVATE
    catalog
    otterstax_federation
    otterbrix_local
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    Catch2::Catch2
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PRIVATE Tracy::TracyClient)
endif()

otterstax_discover_tests(${PROJECT_NAME})
