#include "verbs_transport.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

constexpr std::size_t kEagerThresholdBytes = 4U * 1024U;
constexpr std::uint32_t kControlMagic = 0x52444d41U; // "RDMA"

struct ControlMessage {
    std::uint32_t magic;
    std::uint32_t tag;
    std::uint64_t remote_address;
    std::uint32_t remote_key;
    std::uint32_t bytes;
    std::uint32_t mode;
    std::uint32_t reserved;
};

static_assert(sizeof(ControlMessage) == 32U,
              "ControlMessage must have a stable size");

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

VerbsTransport* implementation_of(pg_transport_t* transport) {
    if (transport == nullptr || transport->implementation == nullptr) {
        return nullptr;
    }
    return static_cast<VerbsTransport*>(transport->implementation);
}

const VerbsTransport* implementation_of(const pg_transport_t* transport) {
    if (transport == nullptr || transport->implementation == nullptr) {
        return nullptr;
    }
    return static_cast<const VerbsTransport*>(transport->implementation);
}

int verbs_rank(const pg_transport_t* transport) {
    const VerbsTransport* implementation = implementation_of(transport);
    return implementation == nullptr ? -1 : implementation->get_rank();
}

int verbs_size(const pg_transport_t* transport) {
    const VerbsTransport* implementation = implementation_of(transport);
    return implementation == nullptr ? -1 : implementation->get_world_size();
}

int verbs_post_send_next(pg_transport_t* transport,
                         const void* buffer,
                         std::size_t bytes,
                         pg_tag_t tag,
                         pg_transfer_mode_t mode,
                         pg_request_t* request) {
    VerbsTransport* implementation = implementation_of(transport);
    if (implementation == nullptr) {
        return -1;
    }

    try {
        implementation->post_send_next(buffer, bytes, tag, mode, request);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[VerbsTransport] post_send_next failed: "
                  << error.what() << std::endl;
        return -1;
    }
}

int verbs_post_recv_prev(pg_transport_t* transport,
                         void* buffer,
                         std::size_t bytes,
                         pg_tag_t tag,
                         pg_transfer_mode_t mode,
                         pg_request_t* request) {
    VerbsTransport* implementation = implementation_of(transport);
    if (implementation == nullptr) {
        return -1;
    }

    try {
        implementation->post_receive_previous(buffer, bytes, tag, mode, request);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[VerbsTransport] post_recv_prev failed: "
                  << error.what() << std::endl;
        return -1;
    }
}

int verbs_test(pg_transport_t* transport,
               pg_request_t* request,
               int* completed) {
    VerbsTransport* implementation = implementation_of(transport);
    if (implementation == nullptr) {
        return -1;
    }

    try {
        return implementation->test_request(request, completed);
    } catch (const std::exception& error) {
        std::cerr << "[VerbsTransport] test failed: "
                  << error.what() << std::endl;
        return -1;
    }
}

int verbs_wait(pg_transport_t* transport, pg_request_t* request) {
    VerbsTransport* implementation = implementation_of(transport);
    if (implementation == nullptr) {
        return -1;
    }

    try {
        return implementation->wait_request(request);
    } catch (const std::exception& error) {
        std::cerr << "[VerbsTransport] wait failed: "
                  << error.what() << std::endl;
        return -1;
    }
}

int verbs_progress(pg_transport_t* transport) {
    VerbsTransport* implementation = implementation_of(transport);
    if (implementation == nullptr) {
        return -1;
    }

    try {
        return implementation->progress();
    } catch (const std::exception& error) {
        std::cerr << "[VerbsTransport] progress failed: "
                  << error.what() << std::endl;
        return -1;
    }
}

void verbs_destroy(pg_transport_t* transport) {
    VerbsTransport* implementation = implementation_of(transport);
    delete implementation;
}

} // namespace

VerbsTransport::VerbsTransport(int rank,
                               int world_size,
                               struct ibv_context* context,
                               struct ibv_pd* pd)
    : rank_(rank),
      world_size_(world_size),
      context_(context),
      pd_(pd) {
    if (rank_ < 0 || world_size_ <= 0 || rank_ >= world_size_ ||
        context_ == nullptr || pd_ == nullptr) {
        throw std::runtime_error("Invalid VerbsTransport constructor arguments");
    }

    cq_ = ibv_create_cq(context_, 2048, nullptr, nullptr, 0);
    if (cq_ == nullptr) {
        throw std::runtime_error("ibv_create_cq failed");
    }

    c_operations_.rank = verbs_rank;
    c_operations_.size = verbs_size;
    c_operations_.post_send_next = verbs_post_send_next;
    c_operations_.post_recv_prev = verbs_post_recv_prev;
    c_operations_.test = verbs_test;
    c_operations_.wait = verbs_wait;
    c_operations_.progress = verbs_progress;
    c_operations_.destroy = verbs_destroy;

    c_transport_.implementation = this;
    c_transport_.operations = &c_operations_;
}

VerbsTransport::~VerbsTransport() {
    if (next_socket_ >= 0) {
        ::close(next_socket_);
        next_socket_ = -1;
    }
    if (previous_socket_ >= 0) {
        ::close(previous_socket_);
        previous_socket_ = -1;
    }

    // Destroy QPs before deregistering memory used by outstanding WRs.
    if (qp_next_ != nullptr) {
        ibv_destroy_qp(qp_next_);
        qp_next_ = nullptr;
    }
    if (qp_prev_ != nullptr) {
        ibv_destroy_qp(qp_prev_);
        qp_prev_ = nullptr;
    }

    for (RequestState* request : pending_requests_) {
        if (request->memory_region != nullptr) {
            ibv_dereg_mr(request->memory_region);
        }
        delete request;
    }
    pending_requests_.clear();

    if (cq_ != nullptr) {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
    if (pd_ != nullptr) {
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
    }
    if (context_ != nullptr) {
        ibv_close_device(context_);
        context_ = nullptr;
    }
}

void VerbsTransport::set_ring_qps(struct ibv_qp* qp_next,
                                  struct ibv_qp* qp_prev) {
    if (qp_next == nullptr || qp_prev == nullptr) {
        throw std::runtime_error("Both ring QPs must be non-null");
    }
    qp_next_ = qp_next;
    qp_prev_ = qp_prev;
}

void VerbsTransport::set_control_sockets(int next_socket,
                                         int previous_socket) {
    if (next_socket < 0 || previous_socket < 0) {
        throw std::runtime_error("Both control sockets must be valid");
    }
    next_socket_ = next_socket;
    previous_socket_ = previous_socket;
}

pg_transfer_mode_t VerbsTransport::choose_mode(
    pg_transfer_mode_t requested_mode,
    std::size_t bytes) const {
    if (requested_mode == PG_TRANSFER_EAGER ||
        requested_mode == PG_TRANSFER_RENDEZVOUS) {
        return requested_mode;
    }
    if (requested_mode != PG_TRANSFER_AUTO) {
        throw std::runtime_error("Unsupported transfer mode");
    }

    return bytes <= kEagerThresholdBytes
               ? PG_TRANSFER_EAGER
               : PG_TRANSFER_RENDEZVOUS;
}

struct ibv_mr* VerbsTransport::register_memory(void* address,
                                                std::size_t length,
                                                int access_flags) {
    if (address == nullptr || length == 0U) {
        throw std::runtime_error("Cannot register an empty buffer");
    }

    struct ibv_mr* memory_region =
        ibv_reg_mr(pd_, address, length, access_flags);
    if (memory_region == nullptr) {
        throw std::runtime_error(
            "ibv_reg_mr failed: " + std::string(std::strerror(errno)));
    }
    return memory_region;
}

void VerbsTransport::post_receive_previous(
    void* buffer,
    std::size_t bytes,
    pg_tag_t tag,
    pg_transfer_mode_t requested_mode,
    pg_request_t* request) {
    if (qp_prev_ == nullptr || previous_socket_ < 0 ||
        buffer == nullptr || bytes == 0U || request == nullptr ||
        request->internal != nullptr) {
        throw std::runtime_error("Invalid receive request");
    }
    if (bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Receive request is too large");
    }

    const pg_transfer_mode_t mode = choose_mode(requested_mode, bytes);
    const int access_flags =
        IBV_ACCESS_LOCAL_WRITE |
        (mode == PG_TRANSFER_RENDEZVOUS ? IBV_ACCESS_REMOTE_WRITE : 0);

    struct ibv_mr* memory_region =
        register_memory(buffer, bytes, access_flags);

    auto* state = new RequestState();
    state->kind = RequestKind::Receive;
    state->id = next_request_id_++;
    state->tag = tag;
    state->mode = mode;
    state->memory_region = memory_region;
    pending_requests_.insert(state);

    struct ibv_sge scatter_gather{};
    struct ibv_recv_wr work_request{};
    work_request.wr_id = reinterpret_cast<std::uint64_t>(state);

    if (mode == PG_TRANSFER_EAGER) {
        scatter_gather.addr = reinterpret_cast<std::uint64_t>(buffer);
        scatter_gather.length = static_cast<std::uint32_t>(bytes);
        scatter_gather.lkey = memory_region->lkey;
        work_request.sg_list = &scatter_gather;
        work_request.num_sge = 1;
    }

    struct ibv_recv_wr* bad_work_request = nullptr;
    const int post_status =
        ibv_post_recv(qp_prev_, &work_request, &bad_work_request);
    if (post_status != 0) {
        pending_requests_.erase(state);
        ibv_dereg_mr(memory_region);
        delete state;
        throw std::runtime_error(
            "ibv_post_recv failed with code " +
            std::to_string(post_status));
    }

    request->id = state->id;
    request->internal = state;

    const ControlMessage control{
        kControlMagic,
        tag,
        reinterpret_cast<std::uint64_t>(buffer),
        memory_region->rkey,
        static_cast<std::uint32_t>(bytes),
        static_cast<std::uint32_t>(mode),
        0U,
    };

    // The previous rank is the sender for this receive request.
    if (!write_all(previous_socket_, &control, sizeof(control))) {
        throw std::runtime_error(
            "Failed to send rendezvous descriptor to previous rank");
    }
}

void VerbsTransport::post_send_next(
    const void* buffer,
    std::size_t bytes,
    pg_tag_t tag,
    pg_transfer_mode_t requested_mode,
    pg_request_t* request) {
    if (qp_next_ == nullptr || next_socket_ < 0 ||
        buffer == nullptr || bytes == 0U || request == nullptr ||
        request->internal != nullptr) {
        throw std::runtime_error("Invalid send request");
    }
    if (bytes > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Send request is too large");
    }

    const pg_transfer_mode_t mode = choose_mode(requested_mode, bytes);

    ControlMessage control{};
    if (!read_all(next_socket_, &control, sizeof(control))) {
        throw std::runtime_error(
            "Failed to receive rendezvous descriptor from next rank");
    }

    if (control.magic != kControlMagic || control.tag != tag ||
        control.bytes != bytes ||
        control.mode != static_cast<std::uint32_t>(mode)) {
        throw std::runtime_error(
            "Control descriptor mismatch: expected tag=" +
            std::to_string(tag) + " bytes=" + std::to_string(bytes) +
            " mode=" + std::to_string(static_cast<unsigned>(mode)) +
            ", received tag=" + std::to_string(control.tag) +
            " bytes=" + std::to_string(control.bytes) +
            " mode=" + std::to_string(control.mode));
    }

    struct ibv_mr* memory_region =
        register_memory(const_cast<void*>(buffer), bytes, 0);

    auto* state = new RequestState();
    state->kind = RequestKind::Send;
    state->id = next_request_id_++;
    state->tag = tag;
    state->mode = mode;
    state->memory_region = memory_region;
    pending_requests_.insert(state);

    struct ibv_sge scatter_gather{};
    scatter_gather.addr = reinterpret_cast<std::uint64_t>(buffer);
    scatter_gather.length = static_cast<std::uint32_t>(bytes);
    scatter_gather.lkey = memory_region->lkey;

    struct ibv_send_wr work_request{};
    work_request.wr_id = reinterpret_cast<std::uint64_t>(state);
    work_request.sg_list = &scatter_gather;
    work_request.num_sge = 1;
    work_request.send_flags = IBV_SEND_SIGNALED;
    work_request.imm_data = htonl(tag);

    if (mode == PG_TRANSFER_EAGER) {
        work_request.opcode = IBV_WR_SEND_WITH_IMM;
    } else {
        work_request.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        work_request.wr.rdma.remote_addr = control.remote_address;
        work_request.wr.rdma.rkey = control.remote_key;
    }

    struct ibv_send_wr* bad_work_request = nullptr;
    const int post_status =
        ibv_post_send(qp_next_, &work_request, &bad_work_request);
    if (post_status != 0) {
        pending_requests_.erase(state);
        ibv_dereg_mr(memory_region);
        delete state;
        throw std::runtime_error(
            "ibv_post_send failed with code " +
            std::to_string(post_status));
    }

    request->id = state->id;
    request->internal = state;
}

void VerbsTransport::handle_completion(const struct ibv_wc& completion) {
    auto* state = reinterpret_cast<RequestState*>(completion.wr_id);
    if (state == nullptr || pending_requests_.find(state) == pending_requests_.end()) {
        throw std::runtime_error("Completion contained an unknown wr_id");
    }

    if (completion.status != IBV_WC_SUCCESS) {
        std::cerr << "[Rank " << rank_ << "] WR id=" << state->id
                  << " tag=" << state->tag
                  << " failed: " << ibv_wc_status_str(completion.status)
                  << " vendor_err=" << completion.vendor_err << std::endl;
        state->status = -1;
        state->completed = true;
        return;
    }

    if (state->kind == RequestKind::Send) {
        const bool valid_opcode =
            (state->mode == PG_TRANSFER_EAGER &&
             completion.opcode == IBV_WC_SEND) ||
            (state->mode == PG_TRANSFER_RENDEZVOUS &&
             completion.opcode == IBV_WC_RDMA_WRITE);
        if (!valid_opcode) {
            state->status = -1;
        }
    } else {
        const bool valid_opcode =
            (state->mode == PG_TRANSFER_EAGER &&
             completion.opcode == IBV_WC_RECV) ||
            (state->mode == PG_TRANSFER_RENDEZVOUS &&
             completion.opcode == IBV_WC_RECV_RDMA_WITH_IMM);

        if (!valid_opcode ||
            (completion.wc_flags & IBV_WC_WITH_IMM) == 0 ||
            ntohl(completion.imm_data) != state->tag) {
            state->status = -1;
        }
    }

    state->completed = true;
}

void VerbsTransport::poll_one_blocking() {
    while (true) {
        struct ibv_wc completion{};
        const int count = ibv_poll_cq(cq_, 1, &completion);
        if (count < 0) {
            throw std::runtime_error("ibv_poll_cq failed");
        }
        if (count == 1) {
            handle_completion(completion);
            return;
        }
        std::this_thread::yield();
    }
}

int VerbsTransport::poll_available() {
    int processed = 0;
    while (true) {
        struct ibv_wc completions[16]{};
        const int count = ibv_poll_cq(cq_, 16, completions);
        if (count < 0) {
            throw std::runtime_error("ibv_poll_cq failed");
        }
        if (count == 0) {
            return processed;
        }
        for (int index = 0; index < count; ++index) {
            handle_completion(completions[index]);
        }
        processed += count;
    }
}

void VerbsTransport::consume_request(pg_request_t* request) {
    auto* state = static_cast<RequestState*>(request->internal);
    if (state->memory_region != nullptr) {
        ibv_dereg_mr(state->memory_region);
        state->memory_region = nullptr;
    }
    pending_requests_.erase(state);
    delete state;
    request->id = 0;
    request->internal = nullptr;
}

int VerbsTransport::test_request(pg_request_t* request, int* completed) {
    if (request == nullptr || request->internal == nullptr || completed == nullptr) {
        return -1;
    }

    auto* state = static_cast<RequestState*>(request->internal);
    if (pending_requests_.find(state) == pending_requests_.end()) {
        return -1;
    }

    if (!state->completed) {
        poll_available();
    }
    *completed = state->completed ? 1 : 0;
    return 0;
}

int VerbsTransport::wait_request(pg_request_t* request) {
    if (request == nullptr || request->internal == nullptr) {
        return -1;
    }

    auto* state = static_cast<RequestState*>(request->internal);
    if (pending_requests_.find(state) == pending_requests_.end()) {
        return -1;
    }

    while (!state->completed) {
        poll_one_blocking();
    }

    const int status = state->status;
    consume_request(request);
    return status;
}

int VerbsTransport::progress() {
    poll_available();
    return 0;
}
