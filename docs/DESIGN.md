# Design decisions

## 1. A transport vtable instead of direct RDMA calls

The collective code depends on `pg_transport_t`, which contains an opaque
implementation pointer and a table of operations. This is the same basic idea
as an interface or abstract base class, but the header remains C-compatible.

Why this choice:

- Partner B can compile without Verbs headers or RDMA hardware.
- The mock and real RDMA implementations can coexist.
- Partner A may write C or C++.
- Unit tests exercise the exact algorithm that will be used in integration.
- There is one explicit ownership and completion contract to review.

The main alternative was to expose functions such as `rdma_post_write()`
directly. That would couple the collective scheduler to QP and MR details and
make independent testing much harder.

## 2. C-compatible public headers with a C++17 implementation

The assignment permits C or C++. The public headers use plain structs, enums,
function pointers, and `extern "C"`. Internally, the project uses C++17 for
vectors, RAII, threads in the mock test harness, and safer allocation.

This gives Partner A a stable C ABI while keeping the test infrastructure small.
It also avoids forcing C++ classes across module boundaries.

## 3. The collective context borrows the transport

`ring_collective_create()` stores a pointer to Partner A's transport but does
not destroy it. The outer process-group object should own both modules and
tear them down in this order:

```text
1. destroy collective context
2. destroy RDMA transport
3. free process-group wrapper
```

This avoids double-free ambiguity and permits multiple algorithmic views over a
transport later, if needed.

## 4. Reduce-Scatter ownership is shifted by one rank

The schedule uses:

```text
Reduce-Scatter step s at rank r:
    send block (r - s) mod P
    receive block (r - s - 1) mod P
```

After `P - 1` steps, rank `r` owns block `(r + 1) mod P`.

The All-Gather schedule deliberately continues from that ownership:

```text
All-Gather step s at rank r:
    send block (r - s + 1) mod P
    receive block (r - s) mod P
```

There is no need to rotate blocks locally between phases.

## 5. Uneven blocks are first-class

For `count = qP + rem`, blocks `0..rem-1` receive `q+1` elements and the rest
receive `q`. This matters because send and receive sizes at one rank may differ
within the same ring step.

The transport API therefore exposes independent byte counts for every send and
receive request. No code assumes `count % P == 0`.

When `count < P`, some blocks are empty. The scheduler emits no requests for an
empty block, while neighboring links carrying non-empty blocks continue.

## 6. Pipeline pieces are aligned to datatype elements

A byte threshold such as 64 KiB may not divide the datatype size cleanly. The
implementation converts it to:

```text
piece_elements = max(1, floor(piece_bytes / element_size))
```

Every request therefore contains complete elements, and reduction never sees a
partial value.

## 7. Bounded batch pipelining

The implementation divides each block into pieces and processes at most
`max_inflight` pieces per direction in a batch:

1. post receives for the batch;
2. post sends for the batch;
3. wait for each receive and immediately reduce it in Reduce-Scatter;
4. wait for each send before reusing source memory;
5. advance to the next batch.

This is intentionally simpler than a fully event-driven cross-step pipeline.
It still overlaps communication of later pieces with reduction of earlier
pieces, but limits work requests and temporary state.

A future optimization can use `pg_transport_test()` and a completion-driven
state machine. That should be attempted only after the RDMA baseline is stable.

## 8. Receive before send

Each batch posts receives before sends. This is a conservative rule that works
well for eager protocols with finite receive slots and avoids circular waiting
if the transport requires receive preparation.

With one-sided rendezvous, Partner A may internally translate the receive post
into metadata publication rather than a hardware receive WR.

## 9. All-Gather receives directly into final output

Reduce-Scatter must receive into temporary memory because the incoming values
must be combined with local values. All-Gather requires no arithmetic, so the
receive target is the final block location in `workbuf`.

This is the interface point that enables zero-copy large-message All-Gather:
Partner A can use RDMA Write or RDMA Read without forcing Partner B to add an
extra copy.

## 10. AUTO/EAGER/RENDEZVOUS is a request, not an implementation detail leak

The collective API passes `PG_TRANSFER_AUTO`, `PG_TRANSFER_EAGER`, or
`PG_TRANSFER_RENDEZVOUS`. It never sees memory keys, QP numbers, or work
requests.

The recommended default is AUTO, decided by Partner A using the actual piece
size and configured eager threshold. Explicit modes are useful for benchmarks.

## 11. 32-bit tags mirror Write-with-Immediate

Tags are 32 bits so they can map naturally to immediate data:

```text
bits 31..20: collective sequence (12 bits)
bits 19:     phase (Reduce-Scatter / All-Gather)
bits 18..12: ring step (7 bits)
bits 11..0:  piece index (12 bits)
```

This supports up to 128 ranks and 4096 pieces per block. The sequence wraps
modulo 4096. Since one context does not overlap collectives and each call fully
completes before returning, wraparound is safe unless stale messages survive
thousands of completed calls—which should already indicate a transport bug.

If the final design needs larger values, use the immediate value as a compact
lookup token and keep a larger local operation ID in transport state.

## 12. Request completion is explicit

Posting never grants permission to reuse a buffer. Only request completion does.
The contract is:

- after send completion, the sender may modify the source memory;
- after receive completion, the collective may read/reduce the destination;
- `wait()` consumes the request object.

This avoids hidden assumptions about whether RDMA posting, local CQ completion,
or remote visibility constitutes completion.

## 13. The mock transport copies eagerly on send

The mock is for correctness, not performance. It copies the send payload into a
per-destination queue and completes the receive when a matching tag appears.
This avoids pointer lifetime races in tests and mimics the semantic boundary of
a completed transport operation.

The test harness uses one thread per rank because all ranks must make progress
concurrently. The production collective code itself remains single-threaded per
process, as required.

## 14. Failure handling is intentionally fail-fast

If a transport post or wait fails, the collective returns immediately. A
production-quality library would need cancellation/draining of already-posted
requests. That policy must be designed jointly with Partner A because only the
transport knows how to safely recover a QP.

For the exercise, a transport failure can be treated as fatal to the process
group. The README calls this out rather than pretending partial recovery exists.

## 15. The assignment-facing handle is only a composition wrapper

`process_group_api.cpp` implements the required `pg_all_reduce()` and
`pg_close()` functions. The opaque handle contains exactly two things: Partner
A's transport and Partner B's collective context.

Partner A retains responsibility for `connect_process_group()`. After creating
and connecting the Verbs transport, that function should call:

```cpp
return pg_create_from_transport(transport, 1, &config, pg_handle);
```

The ownership flag allows the final handle to destroy the transport in
`pg_close()`. Unit tests may pass `0` when a test fixture owns the mock group.
This keeps bootstrap logic out of the collective module while still providing
the exact assignment API at the top level.
