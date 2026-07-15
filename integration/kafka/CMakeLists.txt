add_library(kafka_runtime
    kafka_manager.hpp
    detail/kafka_manager.cpp
    detail/kafka_manager_ddl.cpp
    detail/kafka_manager_persistence.cpp
    detail/kafka_manager_stream.cpp
    detail/kafka_const.hpp
    detail/kafka_consumer.hpp
    detail/kafka_consumer.cpp
    detail/kafka_producer.hpp
    detail/kafka_producer.cpp
    detail/kafka_reader.hpp
    detail/kafka_reader.cpp
    detail/kafka_poller.hpp
    detail/kafka_poller.cpp
    detail/kafka_stream.hpp
    detail/kafka_stream.cpp
)

target_link_libraries(kafka_runtime PUBLIC
    kafka_grammar
    otterbrix_local
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    Boost::boost
    Boost::json
    RdKafka::rdkafka++
)

target_include_directories(kafka_runtime PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
