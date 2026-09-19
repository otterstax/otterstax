# Flight SQL frontend: the custom Flight SQL wire-protocol server
# (asio-grpc coroutine handlers, in-house Arrow IPC on flatbuffers).
#
# Layers:
#   flight_sql_proto   - generated Flight/FlightSql protobuf + gRPC stubs
#   flight_sql_flatbuf - flatc-generated Arrow format headers (interface lib)
#   flight_sql_ipc     - Arrow IPC model + writer/reader
#   flight_sql_core    - Flight SQL protocol core (commands, auth, tickets)
#   flight_sql         - the server itself: rpc handlers + engine adapter
#                        (built from scheduler_engine.cpp/chunk_to_ipc.cpp;
#                        added once those sources exist)

find_package(gRPC CONFIG REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(flatbuffers CONFIG REQUIRED)

# ---------------------------------------------------------------------------
# Codegen: protoc + grpc_cpp_plugin + flatc
# The Conan recipes export protoc/grpc_cpp_plugin as imported executable
# targets; flatc is looked up in the flatbuffers package bin dir.
# ---------------------------------------------------------------------------
if(TARGET protobuf::protoc)
    set(PROTOC_EXE $<TARGET_FILE:protobuf::protoc>)
else()
    find_program(PROTOC_EXE NAMES protoc REQUIRED)
endif()
if(TARGET gRPC::grpc_cpp_plugin)
    set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    find_program(GRPC_CPP_PLUGIN NAMES grpc_cpp_plugin REQUIRED)
endif()
if(TARGET flatbuffers::flatc)
    set(FLATC_EXE $<TARGET_FILE:flatbuffers::flatc>)
else()
    set(_FLATC_HINTS "")
    get_target_property(_FLB_INC flatbuffers::flatbuffers INTERFACE_INCLUDE_DIRECTORIES)
    foreach(_d ${_FLB_INC})
        if(_d)
            list(APPEND _FLATC_HINTS "${_d}/../bin")
        endif()
    endforeach()
    find_program(FLATC_EXE NAMES flatc HINTS ${_FLATC_HINTS} REQUIRED)
endif()

# Well-known proto files (google/protobuf/any.proto & co) live inside the
# protobuf package include dir.
set(_PB_HINT_DIRS "")
foreach(_t protobuf::libprotobuf protobuf::libprotoc)
    if(TARGET ${_t})
        get_target_property(_inc ${_t} INTERFACE_INCLUDE_DIRECTORIES)
        if(_inc)
            list(APPEND _PB_HINT_DIRS ${_inc})
        endif()
    endif()
endforeach()
find_path(PROTOBUF_WKT_DIR
          NAMES google/protobuf/any.proto
          HINTS ${_PB_HINT_DIRS})
if(NOT PROTOBUF_WKT_DIR)
    message(FATAL_ERROR "google/protobuf/any.proto not found; run inside the Conan build environment")
endif()

set(ARROW_FORMAT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/format)
set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(GEN_PROTO_DIR ${GEN_DIR}/proto)
set(GEN_FBS_DIR ${GEN_DIR}/flatbuf)
file(MAKE_DIRECTORY ${GEN_PROTO_DIR} ${GEN_FBS_DIR})

set(PROTO_SRCS "")
set(PROTO_HDRS "")
foreach(_p Flight FlightSql)
    list(APPEND PROTO_SRCS ${GEN_PROTO_DIR}/${_p}.pb.cc ${GEN_PROTO_DIR}/${_p}.grpc.pb.cc)
    list(APPEND PROTO_HDRS ${GEN_PROTO_DIR}/${_p}.pb.h ${GEN_PROTO_DIR}/${_p}.grpc.pb.h)
endforeach()
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${PROTOC_EXE}
            --cpp_out=${GEN_PROTO_DIR}
            --grpc_out=${GEN_PROTO_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            -I${PROTOBUF_WKT_DIR}
            -I${ARROW_FORMAT_DIR}
            ${ARROW_FORMAT_DIR}/Flight.proto
            ${ARROW_FORMAT_DIR}/FlightSql.proto
    DEPENDS ${ARROW_FORMAT_DIR}/Flight.proto ${ARROW_FORMAT_DIR}/FlightSql.proto
    COMMENT "protoc: Flight.proto, FlightSql.proto"
    VERBATIM)

add_library(flight_sql_proto STATIC ${PROTO_SRCS})
target_include_directories(flight_sql_proto PUBLIC ${GEN_PROTO_DIR})
target_link_libraries(flight_sql_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

set(FBS_FILES Schema.fbs Message.fbs File.fbs Tensor.fbs SparseTensor.fbs)
set(FBS_GENERATED "")
set(FBS_DEPENDS "")
foreach(_f ${FBS_FILES})
    string(REPLACE ".fbs" "_generated.h" _h ${_f})
    list(APPEND FBS_GENERATED ${GEN_FBS_DIR}/${_h})
    list(APPEND FBS_DEPENDS ${ARROW_FORMAT_DIR}/${_f})
endforeach()
add_custom_command(
    OUTPUT ${FBS_GENERATED}
    COMMAND ${FLATC_EXE} --cpp --gen-mutable --scoped-enums
            -o ${GEN_FBS_DIR}
            -I ${ARROW_FORMAT_DIR}
            ${FBS_FILES}
    WORKING_DIRECTORY ${ARROW_FORMAT_DIR}
    DEPENDS ${FBS_DEPENDS}
    COMMENT "flatc: arrow flatbuffers headers"
    VERBATIM)
add_custom_target(flight_sql_flatbuf_gen DEPENDS ${FBS_GENERATED})

add_library(flight_sql_flatbuf INTERFACE)
target_include_directories(flight_sql_flatbuf INTERFACE ${GEN_FBS_DIR})
target_link_libraries(flight_sql_flatbuf INTERFACE flatbuffers::flatbuffers)
add_dependencies(flight_sql_flatbuf flight_sql_flatbuf_gen)

# ---------------------------------------------------------------------------
# Arrow IPC: in-house model + serialization
# ---------------------------------------------------------------------------
add_library(flight_sql_ipc
    ipc/ipc_writer.cpp
    ipc/ipc_reader.cpp)
target_include_directories(flight_sql_ipc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flight_sql_ipc PUBLIC flight_sql_flatbuf)

# ---------------------------------------------------------------------------
# Flight SQL protocol core: commands, auth, ticket manager
# ---------------------------------------------------------------------------
add_library(flight_sql_core
    core/core.cpp
    core/commands.cpp
    core/auth.cpp)
target_include_directories(flight_sql_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flight_sql_core PUBLIC flight_sql_ipc flight_sql_proto)

# ---------------------------------------------------------------------------
# The Flight SQL server: rpc handlers + the Scheduler engine adapter
# ---------------------------------------------------------------------------
find_package(asio-grpc CONFIG REQUIRED)
add_library(flight_sql
    server.cpp
    rpc/flight_server.cpp
    scheduler_engine.cpp
    chunk_to_ipc.cpp)
target_include_directories(flight_sql PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    # repo root, for scheduler/catalog/utility headers the adapter includes
    ${CMAKE_CURRENT_SOURCE_DIR}/../..)
target_link_libraries(flight_sql
    PUBLIC
        flight_sql_core
        flight_sql_ipc
        flight_sql_proto
        flight_sql_flatbuf
        asio-grpc::asio-grpc
        gRPC::grpc++
        protobuf::libprotobuf
    PRIVATE
        scheduler
        catalog)
