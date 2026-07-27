#include "block_layout.hpp"
#include "mock_transport.h"
#include "reduction_ops.hpp"
#include "ring_collectives.h"

#include <cmath>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

#define CHECK(condition)                                                                          \
    do {                                                                                          \
        if (!(condition)) {                                                                       \
            std::ostringstream message;                                                           \
            message << "CHECK failed at " << __FILE__ << ':' << __LINE__ << ": " #condition;   \
            throw std::runtime_error(message.str());                                              \
        }                                                                                         \
    } while (false)

void test_block_layout() {
    BlockLayout layout;

    CHECK(compute_block_layout(8, 4, layout) == PG_SUCCESS);
    CHECK((layout.counts == std::vector<std::size_t>{2, 2, 2, 2}));
    CHECK((layout.offsets == std::vector<std::size_t>{0, 2, 4, 6}));

    CHECK(compute_block_layout(10, 4, layout) == PG_SUCCESS);
    CHECK((layout.counts == std::vector<std::size_t>{3, 3, 2, 2}));
    CHECK((layout.offsets == std::vector<std::size_t>{0, 3, 6, 8}));

    CHECK(compute_block_layout(2, 4, layout) == PG_SUCCESS);
    CHECK((layout.counts == std::vector<std::size_t>{1, 1, 0, 0}));
    CHECK((layout.offsets == std::vector<std::size_t>{0, 1, 2, 2}));
}

void test_reduction_ops() {
    std::int32_t sum_destination[] = {1, 2, 3};
    const std::int32_t sum_source[] = {10, 20, 30};
    CHECK(reduce_in_place(sum_destination, sum_source, 3, PG_INT32, PG_SUM) == PG_SUCCESS);
    CHECK(sum_destination[0] == 11);
    CHECK(sum_destination[1] == 22);
    CHECK(sum_destination[2] == 33);

    double max_destination[] = {1.0, 5.0, -2.0};
    const double max_source[] = {2.0, 3.0, -1.0};
    CHECK(reduce_in_place(max_destination, max_source, 3, PG_FLOAT64, PG_MAX) == PG_SUCCESS);
    CHECK(max_destination[0] == 2.0);
    CHECK(max_destination[1] == 5.0);
    CHECK(max_destination[2] == -1.0);
}

template <typename T>
void run_allreduce_case(int world_size,
                        std::size_t count,
                        DATATYPE datatype,
                        OPERATION operation,
                        std::size_t piece_bytes,
                        unsigned int max_inflight,
                        const std::function<T(int, std::size_t)>& make_input,
                        const std::function<T(std::size_t)>& expected,
                        double tolerance = 0.0) {
    pg_transport_t** transports = nullptr;
    CHECK(mock_transport_group_create(world_size, &transports) == 0);

    std::vector<std::vector<T>> inputs(static_cast<std::size_t>(world_size),
                                       std::vector<T>(count));
    std::vector<std::vector<T>> outputs(static_cast<std::size_t>(world_size),
                                        std::vector<T>(count));
    std::vector<int> statuses(static_cast<std::size_t>(world_size), PG_SUCCESS);
    std::vector<std::thread> threads;

    for (int rank = 0; rank < world_size; ++rank) {
        for (std::size_t i = 0; i < count; ++i) {
            inputs[static_cast<std::size_t>(rank)][i] = make_input(rank, i);
        }
    }

    for (int rank = 0; rank < world_size; ++rank) {
        threads.emplace_back([&, rank]() {
            ring_collective_config_t config{};
            config.pipeline_piece_bytes = piece_bytes;
            config.max_inflight = max_inflight;
            config.transfer_mode = PG_TRANSFER_AUTO;

            ring_collective_context_t* context = nullptr;
            int status = ring_collective_create(transports[rank], &config, &context);
            if (status == PG_SUCCESS) {
                status = ring_all_reduce(context,
                                         inputs[static_cast<std::size_t>(rank)].data(),
                                         outputs[static_cast<std::size_t>(rank)].data(),
                                         count,
                                         datatype,
                                         operation);
            }
            statuses[static_cast<std::size_t>(rank)] = status;
            ring_collective_destroy(context);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    for (int rank = 0; rank < world_size; ++rank) {
        CHECK(statuses[static_cast<std::size_t>(rank)] == PG_SUCCESS);
        for (std::size_t i = 0; i < count; ++i) {
            const double actual_value =
                static_cast<double>(outputs[static_cast<std::size_t>(rank)][i]);
            const double expected_value = static_cast<double>(expected(i));
            CHECK(std::fabs(actual_value - expected_value) <= tolerance);
        }
    }

    mock_transport_group_destroy(transports, world_size);
}

void test_two_rank_sum() {
    constexpr int world_size = 2;
    run_allreduce_case<std::int32_t>(
        world_size,
        4,
        PG_INT32,
        PG_SUM,
        64 * 1024,
        4,
        [](int rank, std::size_t index) {
            static const std::int32_t values[2][4] = {
                {1, 2, 3, 4},
                {10, 20, 30, 40},
            };
            return values[rank][index];
        },
        [](std::size_t index) {
            static const std::int32_t expected[] = {11, 22, 33, 44};
            return expected[index];
        });
}

void test_four_rank_uneven_pipelined_sum() {
    constexpr int world_size = 4;
    constexpr std::size_t count = 10;

    run_allreduce_case<std::int32_t>(
        world_size,
        count,
        PG_INT32,
        PG_SUM,
        4, // one int per piece: aggressively exercises piece tags and batches
        2,
        [](int rank, std::size_t index) {
            return static_cast<std::int32_t>((rank + 1) * 100 + index);
        },
        [](std::size_t index) {
            return static_cast<std::int32_t>(1000 + 4 * index);
        });
}

void test_more_ranks_than_elements() {
    constexpr int world_size = 4;
    constexpr std::size_t count = 2;

    run_allreduce_case<std::int32_t>(
        world_size,
        count,
        PG_INT32,
        PG_MAX,
        4,
        2,
        [](int rank, std::size_t index) {
            return static_cast<std::int32_t>(rank * 10 + static_cast<int>(index));
        },
        [](std::size_t index) {
            return static_cast<std::int32_t>(30 + static_cast<int>(index));
        });
}

void test_float_sum() {
    constexpr int world_size = 4;
    constexpr std::size_t count = 17;

    run_allreduce_case<float>(
        world_size,
        count,
        PG_FLOAT32,
        PG_SUM,
        12,
        3,
        [](int rank, std::size_t index) {
            return static_cast<float>(rank + 1) * 0.5F + static_cast<float>(index);
        },
        [](std::size_t index) {
            return 5.0F + 4.0F * static_cast<float>(index);
        },
        1e-5);
}

void test_in_place_and_consecutive_calls() {
    constexpr int world_size = 4;
    constexpr std::size_t count = 9;

    pg_transport_t** transports = nullptr;
    CHECK(mock_transport_group_create(world_size, &transports) == 0);

    std::vector<std::vector<std::int32_t>> buffers(
        world_size, std::vector<std::int32_t>(count));
    std::vector<int> statuses(world_size, PG_SUCCESS);
    std::vector<std::thread> threads;

    for (int rank = 0; rank < world_size; ++rank) {
        for (std::size_t i = 0; i < count; ++i) {
            buffers[rank][i] = rank + 1;
        }
    }

    for (int rank = 0; rank < world_size; ++rank) {
        threads.emplace_back([&, rank]() {
            ring_collective_config_t config{8, 2, PG_TRANSFER_AUTO};
            ring_collective_context_t* context = nullptr;
            int status = ring_collective_create(transports[rank], &config, &context);
            if (status == PG_SUCCESS) {
                status = ring_all_reduce(context,
                                         buffers[rank].data(),
                                         buffers[rank].data(),
                                         count,
                                         PG_INT32,
                                         PG_SUM);
            }

            if (status == PG_SUCCESS) {
                // Every rank now has 10 in every element. Summing these four
                // identical buffers a second time should produce 40.
                status = ring_all_reduce(context,
                                         buffers[rank].data(),
                                         buffers[rank].data(),
                                         count,
                                         PG_INT32,
                                         PG_SUM);
            }

            statuses[rank] = status;
            ring_collective_destroy(context);
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    for (int rank = 0; rank < world_size; ++rank) {
        CHECK(statuses[rank] == PG_SUCCESS);
        for (std::int32_t value : buffers[rank]) {
            CHECK(value == 40);
        }
    }

    mock_transport_group_destroy(transports, world_size);
}

struct TestCase {
    const char* name;
    void (*function)();
};

} // namespace

int main() {
    const std::vector<TestCase> tests = {
        {"block layout", test_block_layout},
        {"reduction operations", test_reduction_ops},
        {"two-rank sum", test_two_rank_sum},
        {"four-rank uneven pipelined sum", test_four_rank_uneven_pipelined_sum},
        {"more ranks than elements", test_more_ranks_than_elements},
        {"floating-point sum", test_float_sum},
        {"in-place and consecutive calls", test_in_place_and_consecutive_calls},
    };

    int failures = 0;
    for (const TestCase& test : tests) {
        try {
            test.function();
            std::cout << "[PASS] " << test.name << '\n';
        } catch (const std::exception& exception) {
            ++failures;
            std::cerr << "[FAIL] " << test.name << ": " << exception.what() << '\n';
        }
    }

    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }

    std::cout << "All tests passed\n";
    return 0;
}
