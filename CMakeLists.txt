cmake_minimum_required(VERSION 3.20)
project(ring_allreduce LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Find system InfiniBand Verbs library
find_library(LIBVERBS_LIB ibverbs REQUIRED)

# Core Ring Collectives Library
add_library(ring_collectives
    src/block_layout.cpp
    src/reduction_ops.cpp
    src/ring_collectives.cpp
    src/process_group_api.cpp
    src/verbs_transport.cpp
)

target_include_directories(ring_collectives
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(ring_collectives PRIVATE
    -Wall -Wextra -Wpedantic
)

# Link InfiniBand Verbs
target_link_libraries(ring_collectives PUBLIC ${LIBVERBS_LIB})

add_executable(test_runner tests/test_runner.cpp)
target_link_libraries(test_runner PRIVATE ring_collectives ibverbs pthread)

# Enable AddressSanitizer and Debug symbols for test_runner
target_compile_options(test_runner PRIVATE -g -fsanitize=address -fno-omit-frame-pointer)
target_link_options(test_runner PRIVATE -fsanitize=address)