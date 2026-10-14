project(test_session)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_session_pipeline.cpp
    test_session_prepared.cpp
    test_session_subquery.cpp
    test_session_external.cpp
    test_session_plans.cpp
    test_session_lifetime.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

# The grammar extensions register from otterbrix_local's static initialisers,
# so the archive is linked whole, as test_system links it.
# `session` comes through lib_otterstax's internal link group: listed on its
# own ahead of the group, CMake drops the transitive dependencies of the group
# members it reaches through it (the s3 / file grammar extensions, librdkafka)
# from the GNU ld link line.
if(APPLE)
    set(${PROJECT_NAME}_WHOLE_ARCHIVE -Wl,-force_load,$<TARGET_FILE:otterbrix_local>)
else()
    set(${PROJECT_NAME}_WHOLE_ARCHIVE
        -Wl,--whole-archive $<TARGET_FILE:otterbrix_local> -Wl,--no-whole-archive)
endif()

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    lib_otterstax
    spark_connect_server
    file
    ${${PROJECT_NAME}_WHOLE_ARCHIVE}
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    Catch2::Catch2
)

otterstax_discover_tests(${PROJECT_NAME})
