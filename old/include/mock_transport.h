#ifndef MOCK_TRANSPORT_H
#define MOCK_TRANSPORT_H

#include "pg_transport.h"

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

#ifdef __cplusplus
}
#endif

#endif
