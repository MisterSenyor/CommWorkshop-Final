#ifndef RING_REDUCTION_OPS_HPP
#define RING_REDUCTION_OPS_HPP

#include "ring_collectives.h"

#include <cstddef>

std::size_t datatype_size(DATATYPE datatype);

int reduce_in_place(void* destination,
                    const void* source,
                    std::size_t count,
                    DATATYPE datatype,
                    OPERATION operation);

#endif
