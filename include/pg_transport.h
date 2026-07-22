#ifndef PG_TRANSPORT_H
#define PG_TRANSPORT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t pg_tag_t;

typedef enum pg_transfer_mode {
    PG_TRANSFER_AUTO = 0,
    PG_TRANSFER_EAGER = 1,
    PG_TRANSFER_RENDEZVOUS = 2
} pg_transfer_mode_t;

typedef struct pg_request {
    uint64_t id;
    void* internal;
} pg_request_t;

typedef struct pg_transport pg_transport_t;

typedef struct pg_transport_ops {
    int (*rank)(const pg_transport_t* transport);
    int (*size)(const pg_transport_t* transport);

    int (*post_send_next)(pg_transport_t* transport,
                          const void* buffer,
                          size_t bytes,
                          pg_tag_t tag,
                          pg_transfer_mode_t mode,
                          pg_request_t* request);

    int (*post_recv_prev)(pg_transport_t* transport,
                          void* buffer,
                          size_t bytes,
                          pg_tag_t tag,
                          pg_transfer_mode_t mode,
                          pg_request_t* request);

    int (*test)(pg_transport_t* transport, pg_request_t* request, int* completed);
    int (*wait)(pg_transport_t* transport, pg_request_t* request);
    int (*progress)(pg_transport_t* transport);
    void (*destroy)(pg_transport_t* transport);
} pg_transport_ops_t;

struct pg_transport {
    void* implementation;
    const pg_transport_ops_t* operations;
};

/*
 * Transport contract
 * ------------------
 * - post_send_next sends to (rank + 1) mod size.
 * - post_recv_prev receives from (rank - 1 + size) mod size.
 * - A send and a receive match by tag.
 * - Posting is asynchronous.
 * - A send buffer cannot be modified until its request completes.
 * - A receive buffer cannot be read until its request completes.
 * - wait() consumes the request: after it returns, request->internal is NULL.
 * - The collective layer is single-threaded per rank. A transport may use
 *   internal threads, but it must not require the collective layer to do so.
 */

static inline int pg_transport_rank(const pg_transport_t* transport) {
    return transport->operations->rank(transport);
}

static inline int pg_transport_size(const pg_transport_t* transport) {
    return transport->operations->size(transport);
}

static inline int pg_transport_post_send_next(pg_transport_t* transport,
                                               const void* buffer,
                                               size_t bytes,
                                               pg_tag_t tag,
                                               pg_transfer_mode_t mode,
                                               pg_request_t* request) {
    return transport->operations->post_send_next(
        transport, buffer, bytes, tag, mode, request);
}

static inline int pg_transport_post_recv_prev(pg_transport_t* transport,
                                               void* buffer,
                                               size_t bytes,
                                               pg_tag_t tag,
                                               pg_transfer_mode_t mode,
                                               pg_request_t* request) {
    return transport->operations->post_recv_prev(
        transport, buffer, bytes, tag, mode, request);
}

static inline int pg_transport_test(pg_transport_t* transport,
                                    pg_request_t* request,
                                    int* completed) {
    return transport->operations->test(transport, request, completed);
}

static inline int pg_transport_wait(pg_transport_t* transport,
                                    pg_request_t* request) {
    return transport->operations->wait(transport, request);
}

static inline int pg_transport_progress(pg_transport_t* transport) {
    return transport->operations->progress(transport);
}

static inline void pg_transport_destroy(pg_transport_t* transport) {
    if (transport != NULL && transport->operations != NULL &&
        transport->operations->destroy != NULL) {
        transport->operations->destroy(transport);
    }
}

#ifdef __cplusplus
}
#endif

#endif
