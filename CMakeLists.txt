cmake_minimum_required(VERSION 3.20)
project(ring_allreduce_partner_b LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RING_BUILD_TESTS "Build the mock-transport test suite" ON)
option(RING_BUILD_EXAMPLES "Build example programs" ON)

add_library(ring_collectives
    src/block_layout.cpp
    src/reduction_ops.cpp
    src/ring_collectives.cpp
    src/process_group_api.cpp
)

target_include_directories(ring_collectives
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(ring_collectives PRIVATE
    -Wall -Wextra -Wpedantic
)

add_library(mock_transport
    src/mock_transport.cpp
)

target_include_directories(mock_transport
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(mock_transport PRIVATE
    -Wall -Wextra -Wpedantic
)

find_package(Threads REQUIRED)
target_link_libraries(mock_transport PUBLIC Threads::Threads)

if(RING_BUILD_EXAMPLES)
    add_executable(ring_demo examples/demo.cpp)
    target_link_libraries(ring_demo PRIVATE ring_collectives mock_transport)
endif()

if(RING_BUILD_TESTS)
    enable_testing()

    add_executable(ring_tests
        tests/test_main.cpp
    )

    target_include_directories(ring_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
    )

    target_link_libraries(ring_tests PRIVATE
        ring_collectives
        mock_transport
    )

    add_test(NAME ring_tests COMMAND ring_tests)
endif()
