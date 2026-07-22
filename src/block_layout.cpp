#include "block_layout.hpp"

#include "ring_collectives.h"

#include <algorithm>

int compute_block_layout(std::size_t count, int world_size, BlockLayout& layout) {
    if (world_size <= 0) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    layout.counts.assign(static_cast<std::size_t>(world_size), 0);
    layout.offsets.assign(static_cast<std::size_t>(world_size), 0);

    const std::size_t base = count / static_cast<std::size_t>(world_size);
    const std::size_t remainder = count % static_cast<std::size_t>(world_size);

    std::size_t running_offset = 0;
    for (int block = 0; block < world_size; ++block) {
        const std::size_t block_count =
            base + (static_cast<std::size_t>(block) < remainder ? 1U : 0U);
        layout.counts[static_cast<std::size_t>(block)] = block_count;
        layout.offsets[static_cast<std::size_t>(block)] = running_offset;
        running_offset += block_count;
    }

    return PG_SUCCESS;
}
