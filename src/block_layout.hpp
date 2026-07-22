#ifndef RING_BLOCK_LAYOUT_HPP
#define RING_BLOCK_LAYOUT_HPP

#include <cstddef>
#include <vector>

struct BlockLayout {
    std::vector<std::size_t> counts;
    std::vector<std::size_t> offsets;
};

int compute_block_layout(std::size_t count, int world_size, BlockLayout& layout);

#endif
