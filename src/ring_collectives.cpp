#include "ring_collectives.h"

#include "block_layout.hpp"
#include "reduction_ops.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

namespace {

constexpr std::size_t kDefaultPieceBytes = 64U * 1024U;
constexpr unsigned int kDefaultMaxInflight = 4U;
constexpr unsigned int kMaxInflightLimit = 1024U;

constexpr std::uint32_t kPieceBits = 12U;
constexpr std::uint32_t kStepBits = 7U;
constexpr std::uint32_t kPhaseBits = 1U;
constexpr std::uint32_t kSequenceBits = 12U;

constexpr std::uint32_t kPieceMask = (1U << kPieceBits) - 1U;
constexpr std::uint32_t kStepMask = (1U << kStepBits) - 1U;
constexpr std::uint32_t kSequenceMask = (1U << kSequenceBits) - 1U;

constexpr std::uint32_t kStepShift = kPieceBits;
constexpr std::uint32_t kPhaseShift = kPieceBits + kStepBits;
constexpr std::uint32_t kSequenceShift = kPieceBits + kStepBits + kPhaseBits;

enum class CollectivePhase : std::uint32_t {
    ReduceScatter = 0U,
    AllGather = 1U,
};

int positive_mod(int value, int modulus) {
    const int result = value % modulus;
    return result < 0 ? result + modulus : result;
}

void* byte_offset(void* buffer, std::size_t bytes) {
    return static_cast<void*>(static_cast<std::byte*>(buffer) + bytes);
}


int make_tag(std::uint32_t sequence,
             CollectivePhase phase,
             int step,
             std::size_t piece,
             pg_tag_t& tag) {
    if (step < 0 || static_cast<std::uint32_t>(step) > kStepMask || piece > kPieceMask) {
        return PG_ERR_TAG_RANGE;
    }

    tag = ((sequence & kSequenceMask) << kSequenceShift) |
          ((static_cast<std::uint32_t>(phase) & 1U) << kPhaseShift) |
          ((static_cast<std::uint32_t>(step) & kStepMask) << kStepShift) |
          (static_cast<std::uint32_t>(piece) & kPieceMask);
    return PG_SUCCESS;
}

std::size_t piece_elements_for(const ring_collective_config_t& config,
                               std::size_t element_size) {
    const std::size_t requested_bytes =
        config.pipeline_piece_bytes == 0 ? kDefaultPieceBytes : config.pipeline_piece_bytes;
    return std::max<std::size_t>(1U, requested_bytes / element_size);
}

std::size_t piece_count_for(std::size_t element_count, std::size_t piece_elements) {
    if (element_count == 0) {
        return 0;
    }
    return (element_count + piece_elements - 1U) / piece_elements;
}

struct PostedRequest {
    pg_request_t request{};
    std::size_t piece_index = 0;
    std::size_t element_offset = 0;
    std::size_t element_count = 0;
};

} // namespace

struct ring_collective_context {
    pg_transport_t* transport = nullptr;
    ring_collective_config_t config{};
    int rank = -1;
    int world_size = 0;
    BlockLayout layout;
    std::vector<std::byte> temporary_buffer;
    std::uint32_t sequence = 0;
};

extern "C" int ring_collective_create(pg_transport_t* transport,
                                       const ring_collective_config_t* config,
                                       ring_collective_context_t** context) {
    if (transport == nullptr || transport->operations == nullptr || context == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    const int rank = pg_transport_rank(transport);
    const int size = pg_transport_size(transport);
    if (rank < 0 || size <= 0 || rank >= size) {
        return PG_ERR_TRANSPORT;
    }

    ring_collective_context* created = new (std::nothrow) ring_collective_context();
    if (created == nullptr) {
        return PG_ERR_NO_MEMORY;
    }

    created->transport = transport;
    created->rank = rank;
    created->world_size = size;
    created->config.pipeline_piece_bytes =
        config == nullptr || config->pipeline_piece_bytes == 0
            ? kDefaultPieceBytes
            : config->pipeline_piece_bytes;
    created->config.max_inflight =
        config == nullptr || config->max_inflight == 0
            ? kDefaultMaxInflight
            : std::min(config->max_inflight, kMaxInflightLimit);
    created->config.transfer_mode =
        config == nullptr ? PG_TRANSFER_AUTO : config->transfer_mode;

    *context = created;
    return PG_SUCCESS;
}

extern "C" int ring_reduce_scatter_inplace(ring_collective_context_t* context,
                                             void* workbuf,
                                             std::size_t count,
                                             DATATYPE datatype,
                                             OPERATION operation,
                                             int* owned_block) {
    if (context == nullptr || workbuf == nullptr || owned_block == nullptr) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    const std::size_t element_size = datatype_size(datatype);
    if (element_size == 0) {
        return PG_ERR_UNSUPPORTED_DATATYPE;
    }

    int status = compute_block_layout(count, context->world_size, context->layout);
    if (status != PG_SUCCESS) {
        return status;
    }

    const std::size_t max_block_count = *std::max_element(
        context->layout.counts.begin(), context->layout.counts.end());
    context->temporary_buffer.resize(max_block_count * element_size);

    const std::size_t piece_elements = piece_elements_for(context->config, element_size);
    const std::size_t batch_capacity = context->config.max_inflight;

    for (int step = 0; step < context->world_size - 1; ++step) {
        const int send_block = positive_mod(context->rank - step, context->world_size);
        const int recv_block = positive_mod(context->rank - step - 1, context->world_size);

        const std::size_t send_count = context->layout.counts[send_block];
        const std::size_t recv_count = context->layout.counts[recv_block];
        const std::size_t send_pieces = piece_count_for(send_count, piece_elements);
        const std::size_t recv_pieces = piece_count_for(recv_count, piece_elements);
        const std::size_t total_batches =
            (std::max(send_pieces, recv_pieces) + batch_capacity - 1U) / batch_capacity;

        for (std::size_t batch = 0; batch < total_batches; ++batch) {
            const std::size_t begin_piece = batch * batch_capacity;
            const std::size_t end_piece = begin_piece + batch_capacity;

            std::vector<PostedRequest> recv_requests;
            std::vector<PostedRequest> send_requests;
            recv_requests.reserve(batch_capacity);
            send_requests.reserve(batch_capacity);

            for (std::size_t piece = begin_piece;
                 piece < std::min(end_piece, recv_pieces);
                 ++piece) {
                const std::size_t element_offset = piece * piece_elements;
                const std::size_t elements =
                    std::min(piece_elements, recv_count - element_offset);
                const std::size_t bytes = elements * element_size;

                pg_tag_t tag = 0;
                status = make_tag(context->sequence,
                                  CollectivePhase::ReduceScatter,
                                  step,
                                  piece,
                                  tag);
                if (status != PG_SUCCESS) {
                    return status;
                }

                PostedRequest posted{};
                posted.piece_index = piece;
                posted.element_offset = element_offset;
                posted.element_count = elements;

                status = pg_transport_post_recv_prev(
                    context->transport,
                    byte_offset(context->temporary_buffer.data(), element_offset * element_size),
                    bytes,
                    tag,
                    context->config.transfer_mode,
                    &posted.request);
                if (status != 0) {
                    return PG_ERR_TRANSPORT;
                }
                recv_requests.push_back(posted);
            }

            for (std::size_t piece = begin_piece;
                 piece < std::min(end_piece, send_pieces);
                 ++piece) {
                const std::size_t element_offset = piece * piece_elements;
                const std::size_t elements =
                    std::min(piece_elements, send_count - element_offset);
                const std::size_t bytes = elements * element_size;

                pg_tag_t tag = 0;
                status = make_tag(context->sequence,
                                  CollectivePhase::ReduceScatter,
                                  step,
                                  piece,
                                  tag);
                if (status != PG_SUCCESS) {
                    return status;
                }

                PostedRequest posted{};
                posted.piece_index = piece;
                posted.element_offset = element_offset;
                posted.element_count = elements;

                const std::size_t block_element_offset =
                    context->layout.offsets[send_block] + element_offset;
                status = pg_transport_post_send_next(
                    context->transport,
                    byte_offset(workbuf, block_element_offset * element_size),
                    bytes,
                    tag,
                    context->config.transfer_mode,
                    &posted.request);
                if (status != 0) {
                    return PG_ERR_TRANSPORT;
                }
                send_requests.push_back(posted);
            }

            for (PostedRequest& posted : recv_requests) {
                if (pg_transport_wait(context->transport, &posted.request) != 0) {
                    return PG_ERR_TRANSPORT;
                }

                const std::size_t destination_element_offset =
                    context->layout.offsets[recv_block] + posted.element_offset;
                status = reduce_in_place(
                    byte_offset(workbuf, destination_element_offset * element_size),
                    byte_offset(context->temporary_buffer.data(),
                                posted.element_offset * element_size),
                    posted.element_count,
                    datatype,
                    operation);
                if (status != PG_SUCCESS) {
                    return status;
                }
            }

            for (PostedRequest& posted : send_requests) {
                if (pg_transport_wait(context->transport, &posted.request) != 0) {
                    return PG_ERR_TRANSPORT;
                }
            }
        }
    }

    *owned_block = positive_mod(context->rank + 1, context->world_size);
    return PG_SUCCESS;
}

extern "C" int ring_all_gather_inplace(ring_collective_context_t* context,
                                         void* workbuf,
                                         std::size_t count,
                                         DATATYPE datatype,
                                         int owned_block) {
    if (context == nullptr || workbuf == nullptr || owned_block < 0 ||
        owned_block >= context->world_size) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    const int expected_owned_block =
        positive_mod(context->rank + 1, context->world_size);
    if (owned_block != expected_owned_block) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    const std::size_t element_size = datatype_size(datatype);
    if (element_size == 0) {
        return PG_ERR_UNSUPPORTED_DATATYPE;
    }

    int status = compute_block_layout(count, context->world_size, context->layout);
    if (status != PG_SUCCESS) {
        return status;
    }

    const std::size_t piece_elements = piece_elements_for(context->config, element_size);
    const std::size_t batch_capacity = context->config.max_inflight;

    for (int step = 0; step < context->world_size - 1; ++step) {
        const int send_block = positive_mod(context->rank - step + 1, context->world_size);
        const int recv_block = positive_mod(context->rank - step, context->world_size);

        const std::size_t send_count = context->layout.counts[send_block];
        const std::size_t recv_count = context->layout.counts[recv_block];
        const std::size_t send_pieces = piece_count_for(send_count, piece_elements);
        const std::size_t recv_pieces = piece_count_for(recv_count, piece_elements);
        const std::size_t total_batches =
            (std::max(send_pieces, recv_pieces) + batch_capacity - 1U) / batch_capacity;

        for (std::size_t batch = 0; batch < total_batches; ++batch) {
            const std::size_t begin_piece = batch * batch_capacity;
            const std::size_t end_piece = begin_piece + batch_capacity;

            std::vector<PostedRequest> recv_requests;
            std::vector<PostedRequest> send_requests;
            recv_requests.reserve(batch_capacity);
            send_requests.reserve(batch_capacity);

            for (std::size_t piece = begin_piece;
                 piece < std::min(end_piece, recv_pieces);
                 ++piece) {
                const std::size_t element_offset = piece * piece_elements;
                const std::size_t elements =
                    std::min(piece_elements, recv_count - element_offset);
                const std::size_t bytes = elements * element_size;
                const std::size_t block_element_offset =
                    context->layout.offsets[recv_block] + element_offset;

                pg_tag_t tag = 0;
                status = make_tag(
                    context->sequence, CollectivePhase::AllGather, step, piece, tag);
                if (status != PG_SUCCESS) {
                    return status;
                }

                PostedRequest posted{};
                posted.piece_index = piece;
                posted.element_offset = element_offset;
                posted.element_count = elements;

                status = pg_transport_post_recv_prev(
                    context->transport,
                    byte_offset(workbuf, block_element_offset * element_size),
                    bytes,
                    tag,
                    context->config.transfer_mode,
                    &posted.request);
                if (status != 0) {
                    return PG_ERR_TRANSPORT;
                }
                recv_requests.push_back(posted);
            }

            for (std::size_t piece = begin_piece;
                 piece < std::min(end_piece, send_pieces);
                 ++piece) {
                const std::size_t element_offset = piece * piece_elements;
                const std::size_t elements =
                    std::min(piece_elements, send_count - element_offset);
                const std::size_t bytes = elements * element_size;
                const std::size_t block_element_offset =
                    context->layout.offsets[send_block] + element_offset;

                pg_tag_t tag = 0;
                status = make_tag(
                    context->sequence, CollectivePhase::AllGather, step, piece, tag);
                if (status != PG_SUCCESS) {
                    return status;
                }

                PostedRequest posted{};
                posted.piece_index = piece;
                posted.element_offset = element_offset;
                posted.element_count = elements;

                status = pg_transport_post_send_next(
                    context->transport,
                    byte_offset(workbuf, block_element_offset * element_size),
                    bytes,
                    tag,
                    context->config.transfer_mode,
                    &posted.request);
                if (status != 0) {
                    return PG_ERR_TRANSPORT;
                }
                send_requests.push_back(posted);
            }

            for (PostedRequest& posted : recv_requests) {
                if (pg_transport_wait(context->transport, &posted.request) != 0) {
                    return PG_ERR_TRANSPORT;
                }
            }

            for (PostedRequest& posted : send_requests) {
                if (pg_transport_wait(context->transport, &posted.request) != 0) {
                    return PG_ERR_TRANSPORT;
                }
            }
        }
    }

    return PG_SUCCESS;
}

extern "C" int ring_all_reduce(ring_collective_context_t* context,
                                const void* sendbuf,
                                void* recvbuf,
                                std::size_t count,
                                DATATYPE datatype,
                                OPERATION operation) {
    if (context == nullptr || sendbuf == nullptr || recvbuf == nullptr || count == 0) {
        return PG_ERR_INVALID_ARGUMENT;
    }

    const std::size_t element_size = datatype_size(datatype);
    if (element_size == 0) {
        return PG_ERR_UNSUPPORTED_DATATYPE;
    }

    if (sendbuf != recvbuf) {
        std::memcpy(recvbuf, sendbuf, count * element_size);
    }

    int owned_block = -1;
    int status = ring_reduce_scatter_inplace(
        context, recvbuf, count, datatype, operation, &owned_block);
    if (status != PG_SUCCESS) {
        return status;
    }

    status = ring_all_gather_inplace(context, recvbuf, count, datatype, owned_block);
    if (status == PG_SUCCESS) {
        ++context->sequence;
    }
    return status;
}

extern "C" void ring_collective_destroy(ring_collective_context_t* context) {
    delete context;
}
