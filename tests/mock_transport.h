#ifndef MOCK_TRANSPORT_H
#define MOCK_TRANSPORT_H

#include "pg_transport.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Creates one transport endpoint per rank. The returned array has world_size
 * entries and must be destroyed with mock_transport_group_destroy().
 *
 * This implementation is for unit tests only. It emulates ring messages with
 * in-process queues and permits one thread per rank in the test harness.
 */
int mock_transport_group_create(int world_size, pg_transport_t*** transports);

void mock_transport_group_destroy(pg_transport_t** transports, int world_size);

typedef struct mock_transport_stats {
    uint64_t send_requests;
    uint64_t receive_requests;
    uint64_t sent_bytes;
    uint64_t received_bytes;
    uint64_t auto_requests;
    uint64_t eager_requests;
    uint64_t rendezvous_requests;
    uint64_t current_outstanding_requests;
    uint64_t maximum_outstanding_requests;
} mock_transport_stats_t;

/* Test instrumentation. Call only after that rank's worker thread has stopped. */
int mock_transport_get_stats(const pg_transport_t* transport,
                             mock_transport_stats_t* stats);
int mock_transport_reset_stats(pg_transport_t* transport);

#ifdef __cplusplus
}
#endif

#endif
