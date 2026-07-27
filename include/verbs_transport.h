#ifndef VERBS_TRANSPORT_H
#define VERBS_TRANSPORT_H

#include "pg_transport.h"
#include <infiniband/verbs.h>
#include <string>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <stdexcept>

/* Structure to hold remote memory region information for RDMA operations. */
struct RemoteMR {
    uint64_t vaddr;
    uint32_t rkey;
};

/* VerbsTransport class encapsulates the InfiniBand Verbs API for RDMA communication in a ring topology.
 It provides methods for memory registration, posting RDMA Write with Immediate operations, and polling for completions. */
class VerbsTransport {
public:
    VerbsTransport(int rank, int world_size, struct ibv_context* context, struct ibv_pd* pd);
    ~VerbsTransport();

    struct ibv_mr* register_memory(void* addr, size_t length);
    void unregister_memory(struct ibv_mr* mr);

    void set_ring_qps(struct ibv_qp* qp_next, struct ibv_qp* qp_prev);
    void set_remote_mr_next(const RemoteMR& remote_mr);

    void post_rdma_write_imm(void* local_addr, size_t length, struct ibv_mr* local_mr,
                             uint64_t remote_offset, uint32_t imm_data);

    void post_recv_imm();

    uint32_t poll_completion_imm();

    // Returns a C-style transport structure for interoperability with C code.
    pg_transport_t* get_c_transport() { return &c_transport_; }

    int get_rank() const { return rank_; }
    int get_world_size() const { return world_size_; }

private:
    int rank_; // The rank of the current process in the ring topology.
    int world_size_; // The total number of processes in the ring topology.

    struct ibv_context* context_{nullptr};
    struct ibv_pd* pd_{nullptr}; // Protection Domain for memory registration and QP creation.
    struct ibv_cq* cq_{nullptr}; // Completion Queue for handling work completions.

    struct ibv_qp* qp_next_{nullptr}; // Next Queue Pair in the ring.
    struct ibv_qp* qp_prev_{nullptr}; // Previous Queue Pair in the ring.

    RemoteMR remote_mr_next_{0, 0}; // Remote memory region information for the next node in the ring.

    // C-style transport structure and operations for interoperability with C code.
    pg_transport_t c_transport_;
    pg_transport_ops_t c_ops_;
};

#endif // VERBS_TRANSPORT_H