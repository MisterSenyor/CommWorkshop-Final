#include "process_group_api.h"

#include <new>

namespace {

struct ProcessGroupHandle {
    pg_transport_t* transport = nullptr;
    ring_collective_context_t* collectives = nullptr;
    bool owns_transport = false;
};

} // namespace

extern "C" int pg_create_from_transport(pg_transport_t* transport,
                                         int take_transport_ownership,
                                         const ring_collective_config_t* config,
                                         void** pg_handle) {
    if (transport == nullptr || pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    ProcessGroupHandle* handle = new (std::nothrow) ProcessGroupHandle();
    if (handle == nullptr) {
        return PG_ERR_NO_MEMORY;
    }

    handle->transport = transport;
    handle->owns_transport = take_transport_ownership != 0;

    const int status = ring_collective_create(transport, config, &handle->collectives);
    if (status != PG_SUCCESS) {
        delete handle;
        return status;
    }

    *pg_handle = handle;
    return PG_SUCCESS;
}

extern "C" int pg_all_reduce(void* sendbuf,
                              void* recvbuf,
                              int count,
                              DATATYPE datatype,
                              OPERATION operation,
                              void* pg_handle) {
    if (pg_handle == nullptr || count <= 0) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    ProcessGroupHandle* handle = static_cast<ProcessGroupHandle*>(pg_handle);
    return ring_all_reduce(handle->collectives,
                           sendbuf,
                           recvbuf,
                           static_cast<std::size_t>(count),
                           datatype,
                           operation);
}

extern "C" int pg_close(void* pg_handle) {
    if (pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    ProcessGroupHandle* handle = static_cast<ProcessGroupHandle*>(pg_handle);
    ring_collective_destroy(handle->collectives);
    if (handle->owns_transport) {
        pg_transport_destroy(handle->transport);
    }
    delete handle;
    return PG_SUCCESS;
}
