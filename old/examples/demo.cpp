#include "mock_transport.h"
#include "ring_collectives.h"

#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

int main() {
    constexpr int world_size = 4;
    constexpr std::size_t count = 10;

    pg_transport_t** transports = nullptr;
    if (mock_transport_group_create(world_size, &transports) != 0) {
        std::cerr << "Failed to create mock ring\n";
        return 1;
    }

    std::vector<std::vector<std::int32_t>> inputs(world_size);
    std::vector<std::vector<std::int32_t>> outputs(world_size,
                                                   std::vector<std::int32_t>(count));
    std::vector<int> statuses(world_size, PG_SUCCESS);
    std::vector<std::thread> ranks;

    for (int rank = 0; rank < world_size; ++rank) {
        inputs[rank].resize(count);
        for (std::size_t i = 0; i < count; ++i) {
            inputs[rank][i] = static_cast<std::int32_t>((rank + 1) * 100 + i);
        }
    }

    for (int rank = 0; rank < world_size; ++rank) {
        ranks.emplace_back([&, rank]() {
            ring_collective_config_t config{};
            config.pipeline_piece_bytes = 8; // Force several small pieces in the demo.
            config.max_inflight = 2;
            config.transfer_mode = PG_TRANSFER_AUTO;

            ring_collective_context_t* context = nullptr;
            statuses[rank] = ring_collective_create(transports[rank], &config, &context);
            if (statuses[rank] == PG_SUCCESS) {
                statuses[rank] = ring_all_reduce(context,
                                                 inputs[rank].data(),
                                                 outputs[rank].data(),
                                                 count,
                                                 PG_INT32,
                                                 PG_SUM);
            }
            ring_collective_destroy(context);
        });
    }

    for (std::thread& rank : ranks) {
        rank.join();
    }

    for (int rank = 0; rank < world_size; ++rank) {
        if (statuses[rank] != PG_SUCCESS) {
            std::cerr << "Rank " << rank << " failed with " << statuses[rank] << '\n';
            mock_transport_group_destroy(transports, world_size);
            return 1;
        }

        std::cout << "Rank " << rank << ":";
        for (std::int32_t value : outputs[rank]) {
            std::cout << ' ' << value;
        }
        std::cout << '\n';
    }

    mock_transport_group_destroy(transports, world_size);
    return 0;
}
