#ifndef VERBS_TRANSPORT_H
#define VERBS_TRANSPORT_H

#include "pg_transport.h"

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <unordered_set>

class VerbsTransport {
public:
    VerbsTransport(int rank,
                   int world_size,
                   struct ibv_context* context,
                   struct ibv_pd* pd);
    ~VerbsTransport();

    VerbsTransport(const VerbsTransport&) = delete;
    VerbsTransport& operator=(const VerbsTransport&) = delete;

    void set_ring_qps(struct ibv_qp* qp_next, struct ibv_qp* qp_prev);
    void set_control_sockets(int next_socket, int previous_socket);

    void post_send_next(const void* buffer,
                        std::size_t bytes,
                        pg_tag_t tag,
                        pg_transfer_mode_t requested_mode,
                        pg_request_t* request);

    void post_receive_previous(void* buffer,
                               std::size_t bytes,
                               pg_tag_t tag,
                               pg_transfer_mode_t requested_mode,
                               pg_request_t* request);

    int test_request(pg_request_t* request, int* completed);
    int wait_request(pg_request_t* request);
    int progress();

    pg_transport_t* get_c_transport() { return &c_transport_; }
    struct ibv_cq* get_cq() const { return cq_; }

    int get_rank() const { return rank_; }
    int get_world_size() const { return world_size_; }

private:
    enum class RequestKind {
        Send,
        Receive,
    };

    struct RequestState {
        RequestKind kind = RequestKind::Send;
        std::uint64_t id = 0;
        pg_tag_t tag = 0;
        pg_transfer_mode_t mode = PG_TRANSFER_AUTO;
        struct ibv_mr* memory_region = nullptr;
        bool completed = false;
        int status = 0;
    };

    pg_transfer_mode_t choose_mode(pg_transfer_mode_t requested_mode,
                                   std::size_t bytes) const;

    struct ibv_mr* register_memory(void* address,
                                   std::size_t length,
                                   int access_flags);

    void poll_one_blocking();
    int poll_available();
    void handle_completion(const struct ibv_wc& completion);
    void consume_request(pg_request_t* request);

    int rank_;
    int world_size_;

    struct ibv_context* context_ = nullptr;
    struct ibv_pd* pd_ = nullptr;
    struct ibv_cq* cq_ = nullptr;

    struct ibv_qp* qp_next_ = nullptr;
    struct ibv_qp* qp_prev_ = nullptr;

    int next_socket_ = -1;
    int previous_socket_ = -1;

    std::uint64_t next_request_id_ = 1;
    std::unordered_set<RequestState*> pending_requests_;

    pg_transport_t c_transport_{};
    pg_transport_ops_t c_operations_{};
};

#endif
