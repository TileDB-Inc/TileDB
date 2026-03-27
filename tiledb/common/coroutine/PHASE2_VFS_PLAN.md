# Phase 2: Coroutine-Based Async VFS Layer

## Why We Are Doing This

### Current State (After Phase 1)

Phase 1 introduced coroutine orchestration at the query reader level. The
`read_and_unfilter_tiles_async()` method removes the synchronization barrier
between attributes: each attribute starts unfiltering as soon as its own I/O
completes, rather than waiting for all attributes' I/O to finish.

However, the I/O itself is still blocking. When `FilteredData` queues a read,
it submits a task to `io_tp()` that calls `VFS::read_exactly()`, which blocks
the I/O thread for the entire duration of the read. The call stack is:

```
io_tp thread → VFS::read_exactly() → VFS::read() → VFS::read_impl()
            → S3::read() / Azure::read() / GCS::read() / Posix::read()
            ↑ BLOCKS until all bytes arrive
```

This means:
- **Thread pool saturation**: With 8 I/O threads and 8 concurrent reads, the
  9th read must wait for a thread, even though the I/O is just waiting for
  network data
- **Cloud storage penalty**: S3/Azure/GCS reads have 50-500ms latency per
  request. A blocked thread does nothing useful during that latency
- **Limited concurrency**: The maximum number of in-flight I/O operations
  equals the I/O thread pool size, typically 8-16

### What Phase 2 Changes

Phase 2 makes VFS reads non-blocking by introducing `co_await`able read
operations. Instead of blocking a thread waiting for S3 data, the coroutine
suspends and the thread returns to the pool to do other work. When the data
arrives, the coroutine resumes on any available thread.

```
Before (Phase 1):
  io_thread: submit_read() → vfs.read_exactly() [BLOCKS 200ms] → done

After (Phase 2):
  io_thread: submit_read() → co_await vfs.read_async() → [SUSPENDED, thread freed]
  ... thread does other work for 200ms ...
  io_thread: [RESUMED] → data available → done
```

### Expected Performance Improvements

| Scenario | Phase 1 | Phase 2 (expected) | Why |
|----------|---------|---------------------|-----|
| Local SSD, single attribute | Baseline | ~Same | Local reads are fast (~0.1ms), suspension overhead ≈ read time |
| Local SSD, 10 attributes | Moderate gain | Moderate gain | Local I/O is already fast; Phase 1 overlap is the main win |
| S3, single attribute | Baseline | 2-5x | 100+ concurrent reads with 8 threads instead of 8 max |
| S3, 10 attributes | Good gain | 5-20x | Massive I/O concurrency + attribute-level pipelining |
| S3, large tiles (>10MB) | Baseline | 3-10x | Large reads can be split into concurrent sub-reads |
| Azure/GCS, multi-attribute | Good gain | 5-15x | Same benefits as S3 |

**Key insight**: The gains are proportional to I/O latency. Local filesystem
sees minimal improvement because reads are already fast. Cloud storage sees
dramatic improvement because the ratio of I/O wait time to useful CPU work
is high, and coroutines allow far more concurrent I/O operations than threads.

## Architecture

### Overview

```
Phase 2 Read Pipeline:

  ReaderBase::read_and_unfilter_tiles_async()
  │
  ├─ For each attribute:
  │   ├─ FilteredData(async_tag, ...) kicks off I/O
  │   │   └─ queue_last_block_for_read()
  │   │       └─ co_await vfs.read_async(uri, offset, data, size)  ◄── NEW
  │   │           └─ S3::read_async() / Azure::read_async() / ...  ◄── NEW
  │   │               └─ [SUSPENDS, thread freed to pool]
  │   │               ... network transfer happens ...
  │   │               └─ [RESUMES when data arrives]
  │   │
  │   ├─ co_await ready_event   (attribute I/O done)
  │   └─ unfilter_tiles()
  │
  └─ sync_wait(run_all(tasks))
```

### New VFS API

```cpp
// vfs.h — new async read method
class VFS {
 public:
  // Existing (kept for backward compatibility):
  uint64_t read(const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes);
  void read_exactly(const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes);

  // New async API:
  Task<uint64_t> read_async(const URI& uri, uint64_t offset,
                            void* buffer, uint64_t nbytes);
  Task<void> read_exactly_async(const URI& uri, uint64_t offset,
                                void* buffer, uint64_t nbytes);
};
```

### Backend Changes

Each backend gets an async read method:

```cpp
// s3.h
class S3 {
 public:
  // Existing (blocking):
  uint64_t read(const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes);

  // New (coroutine):
  Task<uint64_t> read_async(const URI& uri, uint64_t offset,
                            void* buffer, uint64_t nbytes);
};
```

**S3 implementation strategy**: The AWS SDK supports async operations via
`GetObjectAsync()` which takes a callback. The coroutine wrapper creates a
`BatchReadyEvent` (or similar), starts the async SDK call, and suspends.
The SDK callback signals the event, resuming the coroutine.

```cpp
Task<uint64_t> S3::read_async(const URI& uri, uint64_t offset,
                               void* buffer, uint64_t nbytes) {
  auto request = make_get_object_request(uri, offset, nbytes);

  // Create a one-shot event for this read.
  IoCompletionEvent completion;

  // Start async SDK call — returns immediately.
  client_->GetObjectAsync(request,
    [&completion, buffer](auto& client, auto& request, auto outcome) {
      // Copy data to buffer, signal completion.
      copy_outcome_to_buffer(outcome, buffer);
      completion.signal();
    });

  // Suspend until the callback fires.
  co_await completion;
  co_return nbytes;
}
```

**Azure implementation strategy**: Azure SDK also supports async operations.
The pattern is similar — start async download, suspend, resume on callback.

**GCS implementation strategy**: The Google Cloud C++ client library supports
async operations via `AsyncClient`. Same coroutine wrapper pattern.

**POSIX implementation strategy**: For local filesystem, two options:
1. **io_uring** (Linux 5.1+): True async I/O. Submit read to io_uring ring,
   suspend coroutine, resume when completion event arrives. This is the
   highest-performance option for local files.
2. **Thread pool fallback**: For platforms without io_uring, keep the current
   blocking `pread()` wrapped in a `ScheduleOn{io_tp}` coroutine. The thread
   blocks during the read, but coroutine callers still benefit from structured
   concurrency.

## Implementation Plan

### Step 1: IoCompletionEvent Primitive

Add a new coroutine primitive similar to `BatchReadyEvent` but designed for
single-completion I/O operations:

```cpp
// tiledb/common/coroutine/io_completion_event.h
class IoCompletionEvent {
 public:
  // co_await to suspend until signaled.
  auto operator co_await();

  // Called from I/O completion callback (any thread).
  void signal(uint64_t bytes_read);
  void signal_error(std::exception_ptr e);
};
```

**Difference from BatchReadyEvent**: IoCompletionEvent is optimized for
single-waiter, single-signal use (no linked list of waiters, can use
lock-free signaling).

### Step 2: VFS Async Read API

Add `read_async()` and `read_exactly_async()` to `VFS`. These are coroutine
methods that internally dispatch to the appropriate backend's async method.

For the initial implementation, backends that don't have native async support
fall back to `ScheduleOn{io_tp}` + blocking read:

```cpp
Task<uint64_t> VFS::read_async(const URI& uri, uint64_t offset,
                                void* buffer, uint64_t nbytes) {
  auto fs = get_fs(uri);
  switch (fs) {
    case Filesystem::S3:
      co_return co_await s3_->read_async(uri, offset, buffer, nbytes);
    case Filesystem::AZURE:
      // Fallback until native async is implemented:
      co_await ScheduleOn{*io_tp_};
      co_return azure_->read(uri, offset, buffer, nbytes);
    // ...
  }
}
```

### Step 3: S3 Native Async Read

Implement `S3::read_async()` using `GetObjectAsync()` from the AWS SDK.
This is the highest-impact change because S3 has the highest latency and
is the most commonly used cloud backend.

The AWS SDK's async operations use an internal thread pool
(`S3ThreadPoolExecutor`) for callbacks. The callback signals the
`IoCompletionEvent`, which resumes the coroutine on whatever thread pool
the coroutine scheduler uses.

### Step 4: FilteredData Integration

Modify `FilteredData::queue_last_block_for_read()` to use the async VFS API
when in async mode:

```cpp
void queue_last_block_for_read(TileType type) {
  if (ready_event_) {
    // Async mode: use coroutine-based I/O.
    pending_reads_.fetch_add(1);
    // Launch a detached coroutine for this block's read.
    auto task = read_block_async(vfs, uri, offset, data, size);
    auto handle = task.detach();
    handle.resume();
  } else {
    // Sync mode: unchanged.
    read_tasks_->push_back(resources_.io_tp().execute([...]{
      vfs.read_exactly(uri, offset, data, size);
      return Status::Ok();
    }));
  }
}
```

### Step 5: Azure Native Async Read

Implement `Azure::read_async()` using the Azure SDK's async download API.

### Step 6: GCS Native Async Read

Implement `GCS::read_async()` using the Google Cloud C++ client's async API.

### Step 7: io_uring for Local Filesystem (Linux)

Implement `Posix::read_async()` using io_uring for Linux. This provides
true kernel-level async I/O for local files, eliminating thread blocking
entirely.

For non-Linux platforms, keep the thread pool fallback.

### Step 8: VFS Parallel Read as Coroutine

Convert `VFS::read()` multi-threaded path to use coroutines instead of
`io_tp_->execute()` + `io_tp_->wait_all()`:

```cpp
Task<uint64_t> VFS::read_async(const URI& uri, uint64_t offset,
                                void* buffer, uint64_t nbytes) {
  uint64_t num_ops = compute_parallelism(uri, nbytes);
  if (num_ops == 1) {
    co_return co_await read_impl_async(uri, offset, buffer, nbytes);
  }

  // Parallel reads via coroutines.
  std::vector<Task<void>> tasks;
  for (uint64_t i = 0; i < num_ops; i++) {
    auto [chunk_offset, chunk_size] = compute_chunk(i, num_ops, offset, nbytes);
    tasks.push_back(read_impl_async(uri, chunk_offset,
                                     (char*)buffer + chunk_offset - offset,
                                     chunk_size));
  }
  co_await when_all(std::move(tasks));
  co_return nbytes;
}
```

## Rollout Strategy

### Feature Flags

```
sm.query.reader.use_coroutine_pipeline = true   (Phase 1 flag, graduate to default)
sm.vfs.experimental.async_reads = false          (Phase 2 flag, new)
```

Phase 2 is gated behind its own flag, independent of Phase 1. When both are
enabled, the full async pipeline is active:
- Phase 1: coroutine orchestration at the reader level
- Phase 2: non-blocking I/O at the VFS level

### Phased Delivery

| Step | Scope | Risk | Impact |
|------|-------|------|--------|
| 1: IoCompletionEvent | New primitive, no existing code changes | Low | Foundation |
| 2: VFS async API | New methods, fallback to blocking | Low | API surface |
| 3: S3 native async | Changes S3 read path | Medium | High (most cloud users) |
| 4: FilteredData integration | Modifies async I/O submission | Medium | Connects the pipeline |
| 5: Azure native async | Changes Azure read path | Medium | Medium |
| 6: GCS native async | Changes GCS read path | Medium | Medium |
| 7: io_uring | Platform-specific, Linux only | Medium | High for local I/O |
| 8: VFS parallel coroutine | Replaces thread-based parallelism | High | Removes thread blocking |

### Backward Compatibility

- All new methods are additive (async variants alongside existing sync methods)
- Feature flag defaults to off; existing behavior unchanged
- Sync VFS methods remain for use by non-query code paths (consolidation,
  fragment listing, metadata loading, etc.)
- Gradual migration: once Phase 2 is stable, Phase 1 + Phase 2 flags can
  be promoted to default-on, and eventually the sync path can be deprecated

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| AWS SDK async API complexity | Callback threading, error handling | Start with S3 only; other backends use thread fallback |
| io_uring kernel version requirement | Linux < 5.1 can't use it | Runtime detection with fallback to thread pool |
| Coroutine frame allocation overhead | Memory pressure with many concurrent reads | Use arena allocator for coroutine frames; pool IoCompletionEvents |
| Exception propagation across async boundaries | Lost errors | IoCompletionEvent supports signal_error(); when_all captures first exception |
| Testing async I/O | Non-deterministic timing | Mock backends for unit tests; real backends for integration tests |

## Measuring Success

### Metrics

1. **Throughput**: Tiles read per second, bytes per second
2. **Latency**: P50/P95/P99 query completion time
3. **Concurrency**: Maximum in-flight I/O operations (should increase 10-100x)
4. **Thread utilization**: Ratio of useful work to idle/blocked time
5. **Memory**: Peak memory during read (should be similar to Phase 1)

### Benchmarks

Use the coroutine benchmark suite (`bench_coroutine_{best,worst,average}_case`)
with cloud storage backends:

```bash
# Local filesystem baseline
./bench_coroutine_best_case run

# S3 with Phase 1 only
TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true \
  BENCH_URI_PREFIX=s3://bucket/bench/ \
  ./bench_coroutine_best_case run

# S3 with Phase 1 + Phase 2
TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true \
  TILEDB_SM_VFS_EXPERIMENTAL_ASYNC_READS=true \
  BENCH_URI_PREFIX=s3://bucket/bench/ \
  ./bench_coroutine_best_case run
```

The expected improvement for S3 multi-attribute reads is 5-20x due to the
elimination of thread blocking during I/O latency windows.
