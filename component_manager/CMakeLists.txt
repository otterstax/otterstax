set(COMPONENT_MANAGER_HEADERS
    component_manager.hpp
)

set(COMPONENT_MANAGER_SOURCES
    component_manager.cpp
)

add_library(component_manager
    ${COMPONENT_MANAGER_HEADERS}
    ${COMPONENT_MANAGER_SOURCES}
)

target_link_libraries(component_manager PUBLIC
    Boost::boost
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    arrow::arrow
)

target_include_directories(component_manager PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)