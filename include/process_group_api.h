#ifndef PROCESS_GROUP_API_H
#define PROCESS_GROUP_API_H

#include "pg_transport.h"
#include "ring_collectives.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Required assignment entry point. Partner A should implement the bootstrap
 * and RDMA connection, then call pg_create_from_transport().
 */
int connect_process_group(char* servername, void** pg_handle);

/*
 * Shared integration helper. It creates the opaque process-group handle after
 * Partner A has constructed and connected a transport endpoint.
 */
int pg_create_from_transport(pg_transport_t* transport,
                             int take_transport_ownership,
                             const ring_collective_config_t* config,
                             void** pg_handle);

int pg_all_reduce(void* sendbuf,
                  void* recvbuf,
                  int count,
                  DATATYPE datatype,
                  OPERATION operation,
                  void* pg_handle);

int pg_close(void* pg_handle);

#ifdef __cplusplus
}
#endif

#endif
