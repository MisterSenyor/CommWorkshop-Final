# Partner B — EOD checklist

## 1. Freeze the boundary with Partner A

- [ ] Partner A accepts `pg_transport.h` or sends concrete requested changes.
- [ ] `post_send_next` means send to `(rank + 1) mod P`.
- [ ] `post_recv_prev` means receive from `(rank - 1 + P) mod P`.
- [ ] Tags are matched exactly.
- [ ] `wait` consumes the request.
- [ ] Send completion defines when the send buffer may be reused.
- [ ] Receive completion defines when the receive buffer may be read.
- [ ] Zero-byte blocks are either supported or skipped by both modules.
- [ ] Partner A confirms whether direct receive into final All-Gather output is possible.

## 2. Run the baseline

```bash
make test
make demo
```

All tests should pass before modifying the algorithm.

## 3. Read the three central files

1. `src/block_layout.cpp`
2. `src/ring_collectives.cpp`
3. `include/pg_transport.h`

Trace one example with four ranks and four blocks on paper.

## 4. Adapt to the shared repository

- [ ] Match the team's datatype and operation enum names.
- [ ] Match the required assignment function signature.
- [ ] Keep `ring_collective_context_t` private.
- [ ] Do not include `<infiniband/verbs.h>` in the collective module.
- [ ] Add Partner A's transport target to CMake.

## 5. Integration tests

- [ ] 2 ranks, `INT32 SUM`, small eager-size payload.
- [ ] 4 ranks, `INT32 SUM`, small eager-size payload.
- [ ] 2 ranks, large rendezvous payload.
- [ ] 4 ranks, large rendezvous payload.
- [ ] Uneven `count`.
- [ ] More ranks than elements.
- [ ] In-place operation.
- [ ] At least two consecutive calls.
- [ ] Compare every output element against a CPU reference.

## 6. Only then tune

- [ ] Sweep pipeline piece sizes.
- [ ] Sweep maximum in-flight pieces.
- [ ] Compare AUTO/EAGER/RENDEZVOUS.
- [ ] Record latency and throughput.
