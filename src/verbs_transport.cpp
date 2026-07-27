#include "verbs_transport.h"
#include <iostream>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <arpa/inet.h>

/* Helper to safely retrieve VerbsTransport instance from C handle implementation pointer */
static VerbsTransport* get_impl(pg_transport_t* transport) {
    if (!transport || !transport->implementation) return nullptr;
    return static_cast<VerbsTransport*>(transport->implementation);
}

static int verbs_rank(const pg_transport_t* transport) {
    if (!transport || !transport->implementation) return -1;
    auto* self = static_cast<const VerbsTransport*>(transport->implementation);
    return self ? self->get_rank() : -1;
}

static int verbs_size(const pg_transport_t* transport) {
    if (!transport || !transport->implementation) return -1;
    auto* self = static_cast<const VerbsTransport*>(transport->implementation);
    return self ? self->get_world_size() : -1;
}

/* Static C-Style Callbacks for pg_transport_ops_t Interoperability */

static int verbs_post_send_next(pg_transport_t* transport, const void* buf, size_t bytes, 
                                pg_tag_t tag, pg_transfer_mode_t mode, pg_request_t* req) {
    (void)mode;
    (void)req;
    VerbsTransport* self = get_impl(transport);
    if (!self) return -1;

    try {
        // Send via RDMA Write with Immediate (tag is sent as imm_data)
        self->post_rdma_write_imm(
            const_cast<void*>(buf), 
            bytes, 
            nullptr, 
            0, 
            static_cast<uint32_t>(tag)
        );
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[VerbsTransport] Error in post_send_next: " << e.what() << std::endl;
        return -1;
    }
}

static int verbs_post_recv_prev(pg_transport_t* transport, void* buf, size_t bytes, 
                                pg_tag_t tag, pg_transfer_mode_t mode, pg_request_t* req) {
    (void)buf;
    (void)bytes;
    (void)tag;
    (void)mode;
    (void)req;
    VerbsTransport* self = get_impl(transport);
    if (!self) return -1;

    try {
        // Post a receive Work Request to qp_prev_ to catch the incoming immediate notification
        self->post_recv_imm();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[VerbsTransport] Error in post_recv_prev: " << e.what() << std::endl;
        return -1;
    }
}

static int verbs_wait(pg_transport_t* transport, pg_request_t* req) {
    (void)req;
    VerbsTransport* self = get_impl(transport);
    if (!self) return -1;

    try {
        // Wait for incoming completion signal from previous node
        self->poll_completion_imm();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[VerbsTransport] Error in wait: " << e.what() << std::endl;
        return -1;
    }
}

/* Constructor - Initializes the VerbsTransport with the given rank, world size, InfiniBand context, and protection domain. It also creates a Completion Queue (CQ) for handling work completions and binds C-style operations. */
VerbsTransport::VerbsTransport(int rank, int world_size, struct ibv_context* context, struct ibv_pd* pd)
    : rank_(rank), world_size_(world_size), context_(context), pd_(pd) {
    if (!context_ || !pd_) {
        throw std::runtime_error("Invalid Verbs context or protection domain provided.");
    }

    // Create a Completion Queue (CQ) capable of holding completion elements
    cq_ = ibv_create_cq(context_, 1024, nullptr, nullptr, 0);
    if (!cq_) {
        throw std::runtime_error("Failed to create IBV Completion Queue (CQ).");
    }

    // Configure operations table for C-style pg_transport_t interface
    std::memset(&c_ops_, 0, sizeof(c_ops_));
    c_ops_.rank = verbs_rank;
    c_ops_.size = verbs_size;
    c_ops_.post_send_next = verbs_post_send_next;
    c_ops_.post_recv_prev = verbs_post_recv_prev;
    c_ops_.wait = verbs_wait;

    // Bind transport instance references to C-style wrapper struct
    std::memset(&c_transport_, 0, sizeof(c_transport_));
    c_transport_.implementation = this;
    c_transport_.operations = &c_ops_;
}

/* Destructor - Cleans up the Completion Queue (CQ) and any other resources allocated by the VerbsTransport. */
VerbsTransport::~VerbsTransport() {
    if (cq_) {
        ibv_destroy_cq(cq_);
    }
}

/* Registers a local memory buffer with the InfiniBand NIC, allowing it to be used for RDMA operations. Returns a pointer to the registered memory region (ibv_mr). */
struct ibv_mr* VerbsTransport::register_memory(void* addr, size_t length) {
    int access_flags = IBV_ACCESS_LOCAL_WRITE | 
                       IBV_ACCESS_REMOTE_WRITE | 
                       IBV_ACCESS_REMOTE_READ;

    struct ibv_mr* mr = ibv_reg_mr(pd_, addr, length, access_flags);
    if (!mr) {
        throw std::runtime_error("ibv_reg_mr failed: Unable to register memory region.");
    }
    return mr;
}

/* Unregisters a previously registered memory region, freeing it from the InfiniBand NIC. */
void VerbsTransport::unregister_memory(struct ibv_mr* mr) {
    if (mr) {
        ibv_dereg_mr(mr);
    }
}

/* Sets the ring topology by configuring the next and previous Queue Pairs (QPs) for RDMA communication. */
void VerbsTransport::set_ring_qps(struct ibv_qp* qp_next, struct ibv_qp* qp_prev) {
    qp_next_ = qp_next;
    qp_prev_ = qp_prev;
}

/* Sets the remote memory region information for the next node in the ring, allowing RDMA writes to target the correct remote buffer. */
void VerbsTransport::set_remote_mr_next(const RemoteMR& remote_mr) {
    remote_mr_next_ = remote_mr;
}

/* Posts a Receive Work Request on the previous Queue Pair to consume the incoming immediate data. */
void VerbsTransport::post_recv_imm() {
    assert(qp_prev_ != nullptr && "qp_prev must be configured before posting Recv");

    struct ibv_recv_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = 2; // Arbitrary receive WR ID
    wr.sg_list = nullptr;
    wr.num_sge = 0;

    struct ibv_recv_wr* bad_wr = nullptr;
    int ret = ibv_post_recv(qp_prev_, &wr, &bad_wr);
    if (ret != 0) {
        throw std::runtime_error("ibv_post_recv failed with error code: " + std::to_string(ret));
    }
}

/* Posts an RDMA Write with Immediate operation to the next node in the ring. The local buffer is written to the remote buffer, and an immediate value is sent to signal completion. */
void VerbsTransport::post_rdma_write_imm(void* local_addr, size_t length, struct ibv_mr* local_mr,
                                          uint64_t remote_offset, uint32_t imm_data) {

    // Ensure that the next Queue Pair (QP) is configured before posting the RDMA Write operation
    assert(qp_next_ != nullptr && "qp_next must be configured before posting RDMA Write");

    // Prepare the Scatter/Gather Element (SGE) for the local buffer
    struct ibv_sge sge;
    std::memset(&sge, 0, sizeof(sge));
    sge.addr = reinterpret_cast<uint64_t>(local_addr);
    sge.length = static_cast<uint32_t>(length);
    sge.lkey = local_mr ? local_mr->lkey : 0;

    // Prepare the Work Request (WR) for the RDMA Write with Immediate operation
    struct ibv_send_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = 1; // Arbitrary work request ID
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM; // Critical for completion signaling
    wr.send_flags = IBV_SEND_SIGNALED;      // Triggers completion entry on local CQ
    wr.imm_data = htonl(imm_data);           // Transmit immediate value in Network Byte Order

    // Set the remote address and remote key for the RDMA Write operation
    wr.wr.rdma.remote_addr = remote_mr_next_.vaddr + remote_offset;
    wr.wr.rdma.rkey = remote_mr_next_.rkey;

    // Post the Work Request to the next Queue Pair (QP) for execution
    struct ibv_send_wr* bad_wr = nullptr;
    int ret = ibv_post_send(qp_next_, &wr, &bad_wr);
    if (ret != 0) {
        throw std::runtime_error("ibv_post_send failed with error code: " + std::to_string(ret));
    }
}

/* Polls the Completion Queue (CQ) for incoming work completions, specifically looking for RDMA Write with Immediate completions. Returns the immediate value which was sent by the previous node in the ring. */
uint32_t VerbsTransport::poll_completion_imm() {

    // Prepare a Work Completion (WC) structure to hold the completion details
    struct ibv_wc wc;
    std::memset(&wc, 0, sizeof(wc));

    // Continuously poll the Completion Queue (CQ) until a valid completion is received
    while (true) {
        
        // Poll the CQ for a single completion entry
        int ne = ibv_poll_cq(cq_, 1, &wc);
        if (ne < 0) {
            throw std::runtime_error("ibv_poll_cq failed while waiting for completion.");
        }
        
        // If a completion entry is received, check its status and opcode
        if (ne > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error("Work Completion failed with status: " + 
                                         std::string(ibv_wc_status_str(wc.status)));
            }

            // If the completion is for an RDMA Write with Immediate, return the immediate value which was sent by the previous node in the ring
            if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                return ntohl(wc.imm_data); // Return chunk identifier or flag
            }

            // If the completion is not for an RDMA Write with Immediate, continue polling for the next completion
            if (wc.opcode == IBV_WC_RDMA_WRITE) {
                continue; 
            }
        }
    }
}