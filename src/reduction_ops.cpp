#include "reduction_ops.hpp"

#include <algorithm>
#include <cstdint>

template <typename T>
int reduce_typed(T* destination,
                 const T* source,
                 std::size_t count,
                 OPERATION operation) {
    if (destination == nullptr || source == nullptr) {
        return count == 0 ? PG_SUCCESS : PG_ERR_INVALID_ARGUMENT;
    }

    switch (operation) {
    case PG_SUM:
        for (std::size_t i = 0; i < count; ++i) {
            destination[i] += source[i];
        }
        return PG_SUCCESS;

    case PG_MIN:
        for (std::size_t i = 0; i < count; ++i) {
            destination[i] = std::min(destination[i], source[i]);
        }
        return PG_SUCCESS;

    case PG_MAX:
        for (std::size_t i = 0; i < count; ++i) {
            destination[i] = std::max(destination[i], source[i]);
        }
        return PG_SUCCESS;

    default:
        return PG_ERR_UNSUPPORTED_OPERATION;
    }
}

std::size_t datatype_size(DATATYPE datatype) {
    switch (datatype) {
    case PG_INT32:
        return sizeof(std::int32_t);
    case PG_FLOAT32:
        return sizeof(float);
    case PG_FLOAT64:
        return sizeof(double);
    default:
        return 0;
    }
}

int reduce_in_place(void* destination,
                    const void* source,
                    std::size_t count,
                    DATATYPE datatype,
                    OPERATION operation) {
    if (count == 0) {
        return PG_SUCCESS;
    }

    switch (datatype) {
    case PG_INT32:
        return reduce_typed(static_cast<std::int32_t*>(destination),
                            static_cast<const std::int32_t*>(source),
                            count,
                            operation);
    case PG_FLOAT32:
        return reduce_typed(static_cast<float*>(destination),
                            static_cast<const float*>(source),
                            count,
                            operation);
    case PG_FLOAT64:
        return reduce_typed(static_cast<double*>(destination),
                            static_cast<const double*>(source),
                            count,
                            operation);
    default:
        return PG_ERR_UNSUPPORTED_DATATYPE;
    }
}
