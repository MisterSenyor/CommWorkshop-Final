## Build and test

```bash
make test
make demo
```

Equivalent CMake commands:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/ring_demo
```

## What is already implemented

The skeleton is deliberately more than empty declarations. It provides a
working baseline that passes against the mock transport:

- `PG_INT32`, `PG_FLOAT32`, and `PG_FLOAT64`;
- `PG_SUM`, `PG_MIN`, and `PG_MAX`;
- in-place and out-of-place All-Reduce;
- counts not divisible by the number of ranks;
- counts smaller than the number of ranks;
- multiple consecutive collectives;
- configurable pipeline piece size and maximum in-flight batch size.

## What remains for integration

1. Partner A implements `pg_transport_t` using Verbs and defines `connect_process_group()`.
2. Partner A calls `pg_create_from_transport()` after ring connection setup.
3. Agree whether `PG_TRANSFER_AUTO` is selected by the collective layer or the
   transport. The current design delegates AUTO to the transport.
4. Register the user/work buffers or introduce a registration/cache API.
5. Decide whether All-Gather receives may write directly into `recvbuf` through
   RDMA Write/Read. The current API explicitly permits this.
6. Add benchmark code for eager versus rendezvous thresholds.
7. Verify completion semantics and QP ordering on the real transport.

## Important API assumption

`pg_transport_wait()` **consumes** its request. Once it returns,
`request.internal` is null and the request object may be reused. Partner A's
implementation should follow the same rule.

The collective context is single-threaded: do not call two collectives
concurrently on the same context.
