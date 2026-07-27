#include "mock_transport.h"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <new>
#include <utility>
#include <vector>

namespace {

struct Message {
    pg_tag_t tag = 0;
    std::vector<std::byte> payload;
};

struct MockRing {
    explicit MockRing(int size) : incoming(static_cast<std::size_t>(size)) {}

    std::mutex mutex;
    std::condition_variable condition;
    std::vector<std::deque<Message>> incoming;
};

struct MockEndpoint {
    std::shared_ptr<MockRing> ring;
    int rank = -1;
    int world_size = 0;
    std::uint64_t next_request_id = 1;
};

enum class RequestKind {
    Send,
    Receive,
};

struct RequestState {
    RequestKind kind = RequestKind::Send;
    MockEndpoint* endpoint = nullptr;
    void* receive_buffer = nullptr;
    std::size_t bytes = 0;
    pg_tag_t tag = 0;
    bool completed = false;
    int status = 0;
};

MockEndpoint* endpoint_of(const pg_transport_t* transport) {
    return static_cast<MockEndpoint*>(transport->implementation);
}

int mock_rank(const pg_transport_t* transport) {
    const MockEndpoint* endpoint = endpoint_of(transport);
    return endpoint == nullptr ? -1 : endpoint->rank;
}

int mock_size(const pg_transport_t* transport) {
    const MockEndpoint* endpoint = endpoint_of(transport);
    return endpoint == nullptr ? -1 : endpoint->world_size;
}

bool try_complete_receive_locked(RequestState& request) {
    MockEndpoint* endpoint = request.endpoint;
    auto& queue = endpoint->ring->incoming[static_cast<std::size_t>(endpoint->rank)];

    for (auto iterator = queue.begin(); iterator != queue.end(); ++iterator) {
        if (iterator->tag != request.tag) {
            continue;
        }

        if (iterator->payload.size() != request.bytes) {
            request.status = -1;
            request.completed = true;
            queue.erase(iterator);
            return true;
        }

        if (request.bytes > 0) {
            std::memcpy(request.receive_buffer, iterator->payload.data(), request.bytes);
        }
        queue.erase(iterator);
        request.status = 0;
        request.completed = true;
        return true;
    }

    return false;
}

int mock_post_send_next(pg_transport_t* transport,
                        const void* buffer,
                        std::size_t bytes,
                        pg_tag_t tag,
                        pg_transfer_mode_t mode,
                        pg_request_t* request) {
    (void)mode;
    if (transport == nullptr || request == nullptr || (buffer == nullptr && bytes > 0)) {
        return -1;
    }

    MockEndpoint* endpoint = endpoint_of(transport);
    if (endpoint == nullptr) {
        return -1;
    }

    RequestState* state = new (std::nothrow) RequestState();
    if (state == nullptr) {
        return -1;
    }

    state->kind = RequestKind::Send;
    state->endpoint = endpoint;
    state->completed = true;

    Message message;
    message.tag = tag;
    message.payload.resize(bytes);
    if (bytes > 0) {
        std::memcpy(message.payload.data(), buffer, bytes);
    }

    const int destination = (endpoint->rank + 1) % endpoint->world_size;
    {
        std::lock_guard<std::mutex> lock(endpoint->ring->mutex);
        endpoint->ring->incoming[static_cast<std::size_t>(destination)].push_back(
            std::move(message));
    }
    endpoint->ring->condition.notify_all();

    request->id = endpoint->next_request_id++;
    request->internal = state;
    return 0;
}

int mock_post_recv_prev(pg_transport_t* transport,
                        void* buffer,
                        std::size_t bytes,
                        pg_tag_t tag,
                        pg_transfer_mode_t mode,
                        pg_request_t* request) {
    (void)mode;
    if (transport == nullptr || request == nullptr || (buffer == nullptr && bytes > 0)) {
        return -1;
    }

    MockEndpoint* endpoint = endpoint_of(transport);
    if (endpoint == nullptr) {
        return -1;
    }

    RequestState* state = new (std::nothrow) RequestState();
    if (state == nullptr) {
        return -1;
    }

    state->kind = RequestKind::Receive;
    state->endpoint = endpoint;
    state->receive_buffer = buffer;
    state->bytes = bytes;
    state->tag = tag;

    {
        std::lock_guard<std::mutex> lock(endpoint->ring->mutex);
        try_complete_receive_locked(*state);
    }

    request->id = endpoint->next_request_id++;
    request->internal = state;
    return 0;
}

int mock_test(pg_transport_t* transport, pg_request_t* request, int* completed) {
    (void)transport;
    if (request == nullptr || request->internal == nullptr || completed == nullptr) {
        return -1;
    }

    RequestState* state = static_cast<RequestState*>(request->internal);
    if (!state->completed && state->kind == RequestKind::Receive) {
        std::lock_guard<std::mutex> lock(state->endpoint->ring->mutex);
        try_complete_receive_locked(*state);
    }

    *completed = state->completed ? 1 : 0;
    return state->completed ? state->status : 0;
}

int mock_wait(pg_transport_t* transport, pg_request_t* request) {
    (void)transport;
    if (request == nullptr || request->internal == nullptr) {
        return -1;
    }

    RequestState* state = static_cast<RequestState*>(request->internal);
    if (!state->completed && state->kind == RequestKind::Receive) {
        std::unique_lock<std::mutex> lock(state->endpoint->ring->mutex);
        state->endpoint->ring->condition.wait(lock, [&state]() {
            return try_complete_receive_locked(*state);
        });
    }

    const int status = state->status;
    delete state;
    request->internal = nullptr;
    request->id = 0;
    return status;
}

int mock_progress(pg_transport_t* transport) {
    (void)transport;
    return 0;
}

void mock_destroy(pg_transport_t* transport) {
    if (transport == nullptr) {
        return;
    }
    delete endpoint_of(transport);
    delete transport;
}

const pg_transport_ops_t kMockOperations = {
    mock_rank,
    mock_size,
    mock_post_send_next,
    mock_post_recv_prev,
    mock_test,
    mock_wait,
    mock_progress,
    mock_destroy,
};

} // namespace

extern "C" int mock_transport_group_create(int world_size, pg_transport_t*** transports) {
    if (world_size <= 0 || transports == nullptr) {
        return -1;
    }

    std::shared_ptr<MockRing> ring;
    try {
        ring = std::make_shared<MockRing>(world_size);
    } catch (...) {
        return -1;
    }

    pg_transport_t** created = new (std::nothrow) pg_transport_t*[world_size]{};
    if (created == nullptr) {
        return -1;
    }

    for (int rank = 0; rank < world_size; ++rank) {
        pg_transport_t* transport = new (std::nothrow) pg_transport_t();
        MockEndpoint* endpoint = new (std::nothrow) MockEndpoint();
        if (transport == nullptr || endpoint == nullptr) {
            delete transport;
            delete endpoint;
            mock_transport_group_destroy(created, world_size);
            return -1;
        }

        endpoint->ring = ring;
        endpoint->rank = rank;
        endpoint->world_size = world_size;

        transport->implementation = endpoint;
        transport->operations = &kMockOperations;
        created[rank] = transport;
    }

    *transports = created;
    return 0;
}

extern "C" void mock_transport_group_destroy(pg_transport_t** transports, int world_size) {
    if (transports == nullptr) {
        return;
    }

    for (int rank = 0; rank < world_size; ++rank) {
        if (transports[rank] != nullptr) {
            pg_transport_destroy(transports[rank]);
        }
    }
    delete[] transports;
}
