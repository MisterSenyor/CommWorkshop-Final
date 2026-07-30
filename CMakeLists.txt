cmake_minimum_required(VERSION 3.20)
project(ring_allreduce LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BUILD_RDMA "Build the real InfiniBand Verbs transport and multi-host runner" ON)
option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(ring_collectives_core
    src/block_layout.cpp
    src/reduction_ops.cpp
    src/ring_collectives.cpp
)
target_include_directories(ring_collectives_core
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(ring_collectives_core PRIVATE -Wall -Wextra -Wpedantic)

add_library(mock_transport tests/mock_transport.cpp)
target_include_directories(mock_transport
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/tests ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_options(mock_transport PRIVATE -Wall -Wextra -Wpedantic)

add_executable(local_collective_tests tests/local_collective_tests.cpp)
target_include_directories(local_collective_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(local_collective_tests PRIVATE
    ring_collectives_core mock_transport pthread
)
target_compile_options(local_collective_tests PRIVATE -Wall -Wextra -Wpedantic)

if(ENABLE_SANITIZERS)
    foreach(target ring_collectives_core mock_transport local_collective_tests)
        target_compile_options(${target} PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer
        )
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endforeach()
endif()

enable_testing()
add_test(NAME local_collective_tests COMMAND local_collective_tests)

if(BUILD_RDMA)
    find_path(INFINIBAND_INCLUDE_DIR infiniband/verbs.h)
    find_library(LIBVERBS_LIB ibverbs)
    if(INFINIBAND_INCLUDE_DIR AND LIBVERBS_LIB)
        add_library(ring_allreduce_rdma
            src/process_group_api.cpp
            src/verbs_transport.cpp
        )
        target_include_directories(ring_allreduce_rdma PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${INFINIBAND_INCLUDE_DIR}
        )
        target_link_libraries(ring_allreduce_rdma PUBLIC
            ring_collectives_core ${LIBVERBS_LIB}
        )
        target_compile_options(ring_allreduce_rdma PRIVATE
            -Wall -Wextra -Wpedantic
        )

        add_executable(rdma_test_runner tests/rdma_test_runner.cpp)
        target_link_libraries(rdma_test_runner PRIVATE
            ring_allreduce_rdma pthread
        )
        target_compile_options(rdma_test_runner PRIVATE
            -Wall -Wextra -Wpedantic
        )
    else()
        message(WARNING
            "libibverbs headers/library were not found. "
            "The local collective tests will still build, but rdma_test_runner is disabled.")
    endif()
endif()
