#ifndef RING_COLLECTIVES_H
#define RING_COLLECTIVES_H

#include "pg_transport.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum pg_datatype {
    PG_INT32 = 0,
    PG_FLOAT32 = 1,
    PG_FLOAT64 = 2
} DATATYPE;

typedef enum pg_operation {
    PG_SUM = 0,
    PG_MIN = 1,
    PG_MAX = 2
} OPERATION;

typedef enum pg_status {
    PG_SUCCESS = 0,
    PG_ERR_INVALID_ARGUMENT = -1,
    PG_ERR_NO_MEMORY = -2,
    PG_ERR_TRANSPORT = -3,
    PG_ERR_UNSUPPORTED_DATATYPE = -4,
    PG_ERR_UNSUPPORTED_OPERATION = -5,
    PG_ERR_TAG_RANGE = -6
} pg_status_t;

typedef struct ring_collective_context ring_collective_context_t;

typedef struct ring_collective_config {
    /* Desired payload size of one pipeline piece. Rounded to whole elements. */
    size_t pipeline_piece_bytes;

    /* Maximum number of pieces posted per direction in one batch. */
    unsigned int max_inflight;

    /* AUTO lets Partner A's transport choose eager or rendezvous. */
    pg_transfer_mode_t transfer_mode;
} ring_collective_config_t;

/*
 * The context borrows transport; it does not destroy it.
 * One context represents one rank and must not execute overlapping collectives.
 */
int ring_collective_create(pg_transport_t* transport,
                           const ring_collective_config_t* config,
                           ring_collective_context_t** context);

/*
 * In-place Reduce-Scatter.
 * workbuf initially contains this rank's full input array. After completion,
 * exactly one block is fully reduced on this rank. owned_block identifies it.
 */
int ring_reduce_scatter_inplace(ring_collective_context_t* context,
                                void* workbuf,
                                size_t count,
                                DATATYPE datatype,
                                OPERATION operation,
                                int* owned_block);

/*
 * In-place All-Gather.
 * workbuf initially contains the fully reduced owned_block. After completion,
 * workbuf contains the complete reduced array.
 */
int ring_all_gather_inplace(ring_collective_context_t* context,
                            void* workbuf,
                            size_t count,
                            DATATYPE datatype,
                            int owned_block);

/*
 * Full All-Reduce = Reduce-Scatter followed by All-Gather.
 * sendbuf and recvbuf may be identical.
 */
int ring_all_reduce(ring_collective_context_t* context,
                    const void* sendbuf,
                    void* recvbuf,
                    size_t count,
                    DATATYPE datatype,
                    OPERATION operation);

void ring_collective_destroy(ring_collective_context_t* context);

#ifdef __cplusplus
}
#endif

#endif
