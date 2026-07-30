#include "process_group_api.h"
#include "verbs_transport.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>

namespace {

struct ProcessGroupHandle {
    pg_transport_t* transport = nullptr;
    ring_collective_context_t* collectives = nullptr;
    bool owns_transport = false;
};

struct ExchangeData {
    std::uint32_t qpn;
    std::uint16_t lid;
    std::uint16_t reserved;
};

bool write_all(int socket_fd, const void* source, std::size_t bytes) {
    const auto* current = static_cast<const std::byte*>(source);
    while (bytes > 0U) {
        const ssize_t written = ::write(socket_fd, current, bytes);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return false;
        }
        current += static_cast<std::size_t>(written);
        bytes -= static_cast<std::size_t>(written);
    }
    return true;
}

bool read_all(int socket_fd, void* destination, std::size_t bytes) {
    auto* current = static_cast<std::byte*>(destination);
    while (bytes > 0U) {
        const ssize_t received = ::read(socket_fd, current, bytes);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received <= 0) {
            return false;
        }
        current += static_cast<std::size_t>(received);
        bytes -= static_cast<std::size_t>(received);
    }
    return true;
}

int transition_qp_to_rts(struct ibv_qp* qp,
                         std::uint32_t remote_qpn,
                         std::uint16_t remote_lid) {
    struct ibv_qp_attr attributes{};
    attributes.qp_state = IBV_QPS_INIT;
    attributes.pkey_index = 0;
    attributes.port_num = 1;
    attributes.qp_access_flags =
        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp, &attributes, flags) != 0) {
        return -1;
    }

    attributes = {};
    attributes.qp_state = IBV_QPS_RTR;
    attributes.path_mtu = IBV_MTU_1024;
    attributes.dest_qp_num = remote_qpn;
    attributes.rq_psn = 0;
    attributes.max_dest_rd_atomic = 1;
    attributes.min_rnr_timer = 12;
    attributes.ah_attr.is_global = 0;
    attributes.ah_attr.dlid = remote_lid;
    attributes.ah_attr.sl = 0;
    attributes.ah_attr.src_path_bits = 0;
    attributes.ah_attr.port_num = 1;

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(qp, &attributes, flags) != 0) {
        return -1;
    }

    attributes = {};
    attributes.qp_state = IBV_QPS_RTS;
    attributes.sq_psn = 0;
    attributes.timeout = 14;
    attributes.retry_cnt = 7;
    attributes.rnr_retry = 7; // 7 means infinite RNR retry.
    attributes.max_rd_atomic = 1;

    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
            IBV_QP_MAX_QP_RD_ATOMIC;
    return ibv_modify_qp(qp, &attributes, flags) == 0 ? 0 : -1;
}

struct ibv_qp* create_qp(struct ibv_pd* protection_domain,
                         struct ibv_cq* completion_queue) {
    struct ibv_qp_init_attr attributes{};
    attributes.send_cq = completion_queue;
    attributes.recv_cq = completion_queue;
    attributes.qp_type = IBV_QPT_RC;
    attributes.cap.max_send_wr = 256;
    attributes.cap.max_recv_wr = 256;
    attributes.cap.max_send_sge = 1;
    attributes.cap.max_recv_sge = 1;

    return ibv_create_qp(protection_domain, &attributes);
}

int listen_and_accept(int port) {
    int listen_socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_socket < 0) {
        return -1;
    }

    int enabled = 1;
    setsockopt(listen_socket,
               SOL_SOCKET,
               SO_REUSEADDR,
               &enabled,
               sizeof(enabled));

    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<std::uint16_t>(port));

    if (::bind(listen_socket,
               reinterpret_cast<struct sockaddr*>(&address),
               sizeof(address)) != 0 ||
        ::listen(listen_socket, 1) != 0) {
        ::close(listen_socket);
        return -1;
    }

    const int connection = ::accept(listen_socket, nullptr, nullptr);
    ::close(listen_socket);
    return connection;
}

int connect_to_host(const char* host, int port) {
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* result = nullptr;
    const std::string port_string = std::to_string(port);
    if (getaddrinfo(host, port_string.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int connection = -1;
    for (int attempt = 0; attempt < 100 && connection < 0; ++attempt) {
        for (struct addrinfo* entry = result;
             entry != nullptr && connection < 0;
             entry = entry->ai_next) {
            const int candidate =
                ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
            if (candidate < 0) {
                continue;
            }
            if (::connect(candidate, entry->ai_addr, entry->ai_addrlen) == 0) {
                connection = candidate;
            } else {
                ::close(candidate);
            }
        }
        if (connection < 0) {
            usleep(100000);
        }
    }

    freeaddrinfo(result);
    return connection;
}


int parse_positive_environment_int(const char* name, int default_value) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return default_value;
    }

    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed <= 0 || parsed > std::numeric_limits<int>::max()) {
        throw std::runtime_error(std::string("Invalid ") + name +
                                 " value: " + value);
    }
    return static_cast<int>(parsed);
}

pg_transfer_mode_t parse_transfer_mode_environment() {
    const char* value = std::getenv("PG_TRANSFER_MODE");
    if (value == nullptr || *value == '\0' ||
        std::strcmp(value, "auto") == 0) {
        return PG_TRANSFER_AUTO;
    }
    if (std::strcmp(value, "eager") == 0) {
        return PG_TRANSFER_EAGER;
    }
    if (std::strcmp(value, "rendezvous") == 0) {
        return PG_TRANSFER_RENDEZVOUS;
    }
    throw std::runtime_error(std::string("Invalid PG_TRANSFER_MODE value: ") +
                             value);
}

std::vector<std::string> get_host_list(const char* servername,
                                       int world_size) {
    std::vector<std::string> hosts;
    if (const char* environment_hosts = std::getenv("HOST_LIST")) {
        std::stringstream stream(environment_hosts);
        std::string host;
        while (std::getline(stream, host, ',')) {
            if (!host.empty()) {
                hosts.push_back(host);
            }
        }
    }

    if (hosts.empty()) {
        const std::string fallback =
            servername != nullptr && std::strlen(servername) > 0U
                ? servername
                : "localhost";
        hosts.assign(static_cast<std::size_t>(world_size), fallback);
    }
    return hosts;
}

} // namespace

extern "C" int connect_process_group(char* servername, void** pg_handle) {
    if (pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }
    *pg_handle = nullptr;

    int rank = 0;
    int world_size = 1;
    if (const char* environment_rank = std::getenv("RANK")) {
        rank = std::atoi(environment_rank);
    }
    if (const char* environment_size = std::getenv("WORLD_SIZE")) {
        world_size = std::atoi(environment_size);
    }
    if (rank < 0 || world_size < 2 || rank >= world_size) {
        std::cerr << "Invalid rank/world size: rank=" << rank
                  << " world_size=" << world_size << std::endl;
        return PG_ERR_INVALID_ARGUMENT;
    }

    const std::vector<std::string> hosts =
        get_host_list(servername, world_size);
    if (hosts.size() != static_cast<std::size_t>(world_size)) {
        std::cerr << "HOST_LIST has " << hosts.size()
                  << " hosts, expected " << world_size << std::endl;
        return PG_ERR_INVALID_ARGUMENT;
    }

    int number_of_devices = 0;
    struct ibv_device** devices =
        ibv_get_device_list(&number_of_devices);
    if (devices == nullptr || number_of_devices == 0) {
        std::cerr << "No InfiniBand devices found" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    struct ibv_context* context = ibv_open_device(devices[0]);
    ibv_free_device_list(devices);
    if (context == nullptr) {
        std::cerr << "ibv_open_device failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    struct ibv_pd* protection_domain = ibv_alloc_pd(context);
    if (protection_domain == nullptr) {
        ibv_close_device(context);
        std::cerr << "ibv_alloc_pd failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    struct ibv_port_attr port_attributes{};
    if (ibv_query_port(context, 1, &port_attributes) != 0 ||
        port_attributes.state != IBV_PORT_ACTIVE) {
        ibv_dealloc_pd(protection_domain);
        ibv_close_device(context);
        std::cerr << "InfiniBand port 1 is not active" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    VerbsTransport* verbs_transport = nullptr;
    try {
        verbs_transport =
            new VerbsTransport(rank,
                               world_size,
                               context,
                               protection_domain);
    } catch (const std::exception& error) {
        ibv_dealloc_pd(protection_domain);
        ibv_close_device(context);
        std::cerr << "VerbsTransport creation failed: "
                  << error.what() << std::endl;
        return PG_ERR_TRANSPORT;
    }

    struct ibv_qp* qp_next =
        create_qp(protection_domain, verbs_transport->get_cq());
    struct ibv_qp* qp_previous =
        create_qp(protection_domain, verbs_transport->get_cq());
    if (qp_next == nullptr || qp_previous == nullptr) {
        if (qp_next != nullptr) {
            ibv_destroy_qp(qp_next);
        }
        if (qp_previous != nullptr) {
            ibv_destroy_qp(qp_previous);
        }
        delete verbs_transport;
        std::cerr << "ibv_create_qp failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    // From this point onward the transport owns both QPs.
    verbs_transport->set_ring_qps(qp_next, qp_previous);

    const int next_rank = (rank + 1) % world_size;
    const int previous_rank = (rank - 1 + world_size) % world_size;
    constexpr int base_port = 18000;

    int next_socket = -1;
    int previous_socket = -1;
    const std::string& next_host = hosts[static_cast<std::size_t>(next_rank)];

    std::cerr << "[Rank " << rank << "] connecting next="
              << next_host << ':' << base_port + rank
              << " listening=" << base_port + previous_rank << std::endl;

    if ((rank % 2) == 0) {
        next_socket =
            connect_to_host(next_host.c_str(), base_port + rank);
        previous_socket =
            listen_and_accept(base_port + previous_rank);
    } else {
        previous_socket =
            listen_and_accept(base_port + previous_rank);
        next_socket =
            connect_to_host(next_host.c_str(), base_port + rank);
    }

    if (next_socket < 0 || previous_socket < 0) {
        if (next_socket >= 0) {
            ::close(next_socket);
        }
        if (previous_socket >= 0) {
            ::close(previous_socket);
        }
        delete verbs_transport;
        std::cerr << "TCP ring bootstrap failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    const ExchangeData local_next{
        qp_next->qp_num,
        port_attributes.lid,
        0U,
    };
    const ExchangeData local_previous{
        qp_previous->qp_num,
        port_attributes.lid,
        0U,
    };
    ExchangeData remote_next{};
    ExchangeData remote_previous{};

    // Both ranks send both records first, then receive both. This avoids the
    // next-socket/previous-socket circular read deadlock.
    if (!write_all(next_socket, &local_next, sizeof(local_next)) ||
        !write_all(previous_socket,
                   &local_previous,
                   sizeof(local_previous)) ||
        !read_all(next_socket, &remote_next, sizeof(remote_next)) ||
        !read_all(previous_socket,
                  &remote_previous,
                  sizeof(remote_previous))) {
        ::close(next_socket);
        ::close(previous_socket);
        delete verbs_transport;
        std::cerr << "QP metadata exchange failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    if (transition_qp_to_rts(qp_next,
                             remote_next.qpn,
                             remote_next.lid) != 0 ||
        transition_qp_to_rts(qp_previous,
                             remote_previous.qpn,
                             remote_previous.lid) != 0) {
        ::close(next_socket);
        ::close(previous_socket);
        delete verbs_transport;
        std::cerr << "QP transition to RTS failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    // Neighbor barrier: nobody begins the collective before both QPs on both
    // sides have reached RTS.
    const std::uint8_t ready = 1U;
    std::uint8_t next_ready = 0U;
    std::uint8_t previous_ready = 0U;
    if (!write_all(next_socket, &ready, sizeof(ready)) ||
        !write_all(previous_socket, &ready, sizeof(ready)) ||
        !read_all(next_socket, &next_ready, sizeof(next_ready)) ||
        !read_all(previous_socket,
                  &previous_ready,
                  sizeof(previous_ready)) ||
        next_ready != ready || previous_ready != ready) {
        ::close(next_socket);
        ::close(previous_socket);
        delete verbs_transport;
        std::cerr << "QP ready barrier failed" << std::endl;
        return PG_ERR_TRANSPORT;
    }

    // Keep these sockets open: they carry the small per-transfer rendezvous
    // descriptors. The bulk data still moves through InfiniBand.
    verbs_transport->set_control_sockets(next_socket, previous_socket);

    ring_collective_config_t collective_config{};
    try {
        collective_config.pipeline_piece_bytes =
            static_cast<std::size_t>(parse_positive_environment_int(
                "PG_PIPELINE_PIECE_BYTES", 64 * 1024));
        collective_config.max_inflight =
            static_cast<unsigned int>(parse_positive_environment_int(
                "PG_MAX_INFLIGHT", 4));
        collective_config.transfer_mode = parse_transfer_mode_environment();
    } catch (const std::exception& error) {
        delete verbs_transport;
        std::cerr << "Invalid collective configuration: "
                  << error.what() << std::endl;
        return PG_ERR_INVALID_ARGUMENT;
    }

    std::cerr << "[Rank " << rank << "] collective config: mode="
              << (collective_config.transfer_mode == PG_TRANSFER_EAGER
                      ? "eager"
                      : collective_config.transfer_mode == PG_TRANSFER_RENDEZVOUS
                            ? "rendezvous"
                            : "auto")
              << " piece_bytes=" << collective_config.pipeline_piece_bytes
              << " max_inflight=" << collective_config.max_inflight
              << std::endl;

    pg_transport_t* transport = verbs_transport->get_c_transport();
    const int status =
        pg_create_from_transport(transport, 1, &collective_config, pg_handle);
    if (status != PG_SUCCESS) {
        delete verbs_transport;
        return status;
    }

    std::cerr << "[Rank " << rank << "] process group connected"
              << std::endl;
    return PG_SUCCESS;
}

extern "C" int pg_create_from_transport(
    pg_transport_t* transport,
    int take_transport_ownership,
    const ring_collective_config_t* config,
    void** pg_handle) {
    if (transport == nullptr || pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    auto* handle = new (std::nothrow) ProcessGroupHandle();
    if (handle == nullptr) {
        return PG_ERR_NO_MEMORY;
    }

    handle->transport = transport;
    handle->owns_transport = take_transport_ownership != 0;

    const int status =
        ring_collective_create(transport, config, &handle->collectives);
    if (status != PG_SUCCESS) {
        delete handle;
        return status;
    }

    *pg_handle = handle;
    return PG_SUCCESS;
}

extern "C" int pg_all_reduce(void* sendbuf,
                             void* recvbuf,
                             int count,
                             DATATYPE datatype,
                             OPERATION operation,
                             void* pg_handle) {
    if (pg_handle == nullptr || sendbuf == nullptr ||
        recvbuf == nullptr || count <= 0) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    auto* handle = static_cast<ProcessGroupHandle*>(pg_handle);
    return ring_all_reduce(handle->collectives,
                           sendbuf,
                           recvbuf,
                           static_cast<std::size_t>(count),
                           datatype,
                           operation);
}

extern "C" int pg_close(void* pg_handle) {
    if (pg_handle == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    auto* handle = static_cast<ProcessGroupHandle*>(pg_handle);
    ring_collective_destroy(handle->collectives);
    if (handle->owns_transport) {
        pg_transport_destroy(handle->transport);
    }
    delete handle;
    return PG_SUCCESS;
}
