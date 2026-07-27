#include "process_group_api.h"
#include "verbs_transport.h"

#include <new>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdexcept>

namespace {

    /* Structure to manage the process group handle and associated resources */
    struct ProcessGroupHandle {
        pg_transport_t* transport = nullptr;
        ring_collective_context_t* collectives = nullptr;
        bool owns_transport = false;
        struct ibv_qp* qp_next = nullptr;
        struct ibv_qp* qp_prev = nullptr;
    };

    /* Structure to hold data exchanged over TCP socket during ring connection setup */
    struct ExchangeData {
        uint32_t qpn;
        uint16_t lid;
        uint64_t vaddr;
        uint32_t rkey;
    };

    /* Helper function to transition a Queue Pair from RESET -> INIT -> RTR -> RTS */
    int transition_qp_to_rts(struct ibv_qp* qp, uint32_t remote_qpn, uint16_t remote_lid) {
        // 1. Transition to INIT
        struct ibv_qp_attr attr;
        std::memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num = 1;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
        if (ibv_modify_qp(qp, &attr, flags) != 0) return -1;

        // 2. Transition to RTR (Ready to Receive)
        std::memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = IBV_MTU_1024;
        attr.dest_qp_num = remote_qpn;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 12;
        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = remote_lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = 1;

        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        if (ibv_modify_qp(qp, &attr, flags) != 0) return -1;

        // 3. Transition to RTS (Ready to Send)
        std::memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn = 0;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 1;

        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
        if (ibv_modify_qp(qp, &attr, flags) != 0) return -1;

        return 0;
    }

    /* Helper function to create an IBV Queue Pair */
    struct ibv_qp* create_qp(struct ibv_pd* pd, struct ibv_cq* cq) {
        struct ibv_qp_init_attr qp_init_attr;
        std::memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC; // Reliable Connection
        qp_init_attr.cap.max_send_wr = 128;
        qp_init_attr.cap.max_recv_wr = 128;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;

        return ibv_create_qp(pd, &qp_init_attr);
    }

    /* Server socket listener to accept connection from previous neighbor */
    int listen_and_accept(int port) {
        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) return -1;

        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in serv_addr;
        std::memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_addr.s_addr = INADDR_ANY;
        serv_addr.sin_port = htons(port);

        if (bind(listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0 ||
            listen(listen_fd, 1) < 0) {
            close(listen_fd);
            return -1;
        }

        int conn_fd = accept(listen_fd, nullptr, nullptr);
        close(listen_fd);
        return conn_fd;
    }

    /* Client socket to connect to next neighbor */
    int connect_to_host(const char* host, int port) {
        struct addrinfo hints{}, *res = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        std::string port_str = std::to_string(port);

        if (getaddrinfo(host, port_str.c_str(), &hints, &res) != 0) return -1;

        int sock = -1;
        for (int i = 0; i < 50; ++i) { // Retry while server node starts up
            sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
            if (sock >= 0 && connect(sock, res->ai_addr, res->ai_addrlen) == 0) break;
            if (sock >= 0) close(sock);
            sock = -1;
            usleep(100000);
        }

        freeaddrinfo(res);
        return sock;
    }

} // namespace

/* Establishes a connection to the process group server, initializes the VerbsTransport, and creates a process group handle for collective operations. */
extern "C" int connect_process_group(char *servername, void **pg_handle) {
    if (pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    try {
        // 1. Establish InfiniBand context and protection domain
        int num_devices = 0;
        struct ibv_device** dev_list = ibv_get_device_list(&num_devices);
        if (dev_list == nullptr || num_devices == 0) {
            return PG_ERR_TRANSPORT;
        }

        struct ibv_context* context = ibv_open_device(dev_list[0]);
        ibv_free_device_list(dev_list);
        if (context == nullptr) {
            return PG_ERR_TRANSPORT;
        }

        struct ibv_pd* pd = ibv_alloc_pd(context);
        if (pd == nullptr) {
            ibv_close_device(context);
            return PG_ERR_TRANSPORT;
        }

        // Query Local Identifier (LID)
        struct ibv_port_attr port_attr;
        if (ibv_query_port(context, 1, &port_attr) != 0) {
            ibv_dealloc_pd(pd);
            ibv_close_device(context);
            return PG_ERR_TRANSPORT;
        }

        // Parse environment/arguments for rank and world size
        int rank = 0;
        int world_size = 4;
        if (const char* env_rank = std::getenv("RANK")) rank = std::atoi(env_rank);
        if (const char* env_size = std::getenv("WORLD_SIZE")) world_size = std::atoi(env_size);

        // 2. Create VerbsTransport instance for RDMA communication
        VerbsTransport* verbs_transport = new (std::nothrow) VerbsTransport(rank, world_size, context, pd);
        if (verbs_transport == nullptr) {
            ibv_dealloc_pd(pd);
            ibv_close_device(context);
            return PG_ERR_NO_MEMORY;
        }

        // Create dummy registered region to obtain initial remote memory metadata
        uint64_t dummy_buf[64];
        struct ibv_mr* base_mr = verbs_transport->register_memory(dummy_buf, sizeof(dummy_buf));

        // 3. Create Queue Pairs for ring neighbors
        struct ibv_qp* qp_next = create_qp(pd, nullptr);
        struct ibv_qp* qp_prev = create_qp(pd, nullptr);
        if (!qp_next || !qp_prev) {
            verbs_transport->unregister_memory(base_mr);
            delete verbs_transport;
            return PG_ERR_TRANSPORT;
        }

        // 4. Exchange connection info with next and previous ring neighbors via TCP Sockets
        ExchangeData local_next{qp_next->qp_num, port_attr.lid, reinterpret_cast<uint64_t>(dummy_buf), base_mr->rkey};
        ExchangeData local_prev{qp_prev->qp_num, port_attr.lid, reinterpret_cast<uint64_t>(dummy_buf), base_mr->rkey};
        ExchangeData remote_next{}, remote_prev{};

        int base_port = 18000;
        int next_sock = -1, prev_sock = -1;

        // Symmetric ring connection sequence based on rank parity to prevent socket deadlocks
        if (rank % 2 == 0) {
            next_sock = connect_to_host(servername ? servername : "localhost", base_port + rank);
            prev_sock = listen_and_accept(base_port + ((rank - 1 + world_size) % world_size));
        } else {
            prev_sock = listen_and_accept(base_port + ((rank - 1 + world_size) % world_size));
            next_sock = connect_to_host(servername ? servername : "localhost", base_port + rank);
        }

        if (next_sock < 0 || prev_sock < 0) {
            if (next_sock >= 0) close(next_sock);
            if (prev_sock >= 0) close(prev_sock);
            verbs_transport->unregister_memory(base_mr);
            ibv_destroy_qp(qp_next);
            ibv_destroy_qp(qp_prev);
            delete verbs_transport;
            return PG_ERR_TRANSPORT;
        }

        // Perform bidirection struct transfers
        write(next_sock, &local_next, sizeof(local_next));
        read(next_sock, &remote_next, sizeof(remote_next));

        write(prev_sock, &local_prev, sizeof(local_prev));
        read(prev_sock, &remote_prev, sizeof(remote_prev));

        close(next_sock);
        close(prev_sock);

        // 5. Transition QPs to Ready to Send (RTS) and store in VerbsTransport
        if (transition_qp_to_rts(qp_next, remote_next.qpn, remote_next.lid) != 0 ||
            transition_qp_to_rts(qp_prev, remote_prev.qpn, remote_prev.lid) != 0) {
            verbs_transport->unregister_memory(base_mr);
            ibv_destroy_qp(qp_next);
            ibv_destroy_qp(qp_prev);
            delete verbs_transport;
            return PG_ERR_TRANSPORT;
        }

        verbs_transport->set_ring_qps(qp_next, qp_prev);
        verbs_transport->set_remote_mr_next(RemoteMR{remote_next.vaddr, remote_next.rkey});
        verbs_transport->unregister_memory(base_mr);

        // 6. Obtain C-style transport structure and build process group handle
        pg_transport_t* transport = verbs_transport->get_c_transport();
        const int status = pg_create_from_transport(transport, 1, nullptr, pg_handle);
        if (status != PG_SUCCESS) {
            ibv_destroy_qp(qp_next);
            ibv_destroy_qp(qp_prev);
            delete verbs_transport;
            return status;
        }

        return PG_SUCCESS;

    } catch (...) {
        return PG_ERR_TRANSPORT;
    }
}

/* Creates a process group handle from an existing transport instance. */
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

/* Performs an all-reduce operation using the provided process group handle. */
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

/* Closes the process group handle, releasing any associated resources and optionally destroying the transport if owned. */
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