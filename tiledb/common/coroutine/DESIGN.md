# Coroutine-Based Async Read Pipeline

## Problem: Synchronization Barriers in the Read Path

The TileDB read pipeline processes data in three stages:

1. **I/O** — Read compressed tile data from storage (VFS)
2. **Unfilter** — Decompress/decrypt/checksum the tile data (CPU-bound)
3. **Post-process** — Assemble results for the user

### Previous Architecture

```
read_and_unfilter_attribute_tiles(names, result_tiles)
│
├─ read_attribute_tiles(names, result_tiles)
│   └─ read_tiles(names, result_tiles)
│       ├─ for each attribute:
│       │   ├─ Create FilteredData (kicks off I/O via io_tp)
│       │   └─ Init tiles (set up pointers into FilteredData blocks)
│       │
│       └─ wait_all_status(read_tasks)  ◄── BARRIER 1: ALL attributes' I/O
│
└─ for each attribute:
    └─ unfilter_tiles(name, result_tiles)
        ├─ parallel_for: load_tile_chunk_data   ◄── BARRIER 2a per attribute
        ├─ parallel_for_2d: unfilter_tile       ◄── BARRIER 2b per attribute
        └─ post_process_unfiltered_tile
```

**Barrier 1** is the main bottleneck: all I/O for every attribute must complete
before any unfiltering can start. If attribute A's I/O finishes early but
attribute B's is slow (e.g., large tiles on S3), the compute threads sit idle
waiting for B even though A is ready to unfilter.

**Barriers 2a/2b** are per-attribute `parallel_for` calls that wait for all
tiles of one attribute before moving to the next. These are less impactful but
still sequential across attributes.

A previous attempt to remove these barriers (branch `yt/sc-58287/dont_block_io`)
caused deadlocks. The flat `ProducerConsumerQueue`-based thread pool cannot
handle nested task dependencies: a thread waiting for a subtask via `yield()`
can starve if all threads are similarly blocked.

### Why Coroutines

C++20 stackless coroutines (`co_await`, `co_return`) solve this by *suspending*
without blocking a thread. A coroutine waiting for I/O releases its thread
entirely; when I/O completes, the coroutine resumes on any available thread.
This eliminates the deadlock class because no thread is held while waiting.

## New Architecture

```
read_and_unfilter_tiles_async(names, result_tiles)
│
├─ Phase 1: For each attribute:
│   ├─ Create FilteredData with async_tag
│   │   └─ I/O starts immediately, each attribute gets its own BatchReadyEvent
│   └─ Init tiles (same as before)
│
├─ Phase 2: For each attribute, create a coroutine:
│   ├─ co_await ready_event        (suspend until THIS attribute's I/O is done)
│   ├─ co_await ScheduleOn{compute_tp}  (hop to compute thread pool)
│   └─ unfilter_tiles(name)        (same unfiltering logic as before)
│
└─ Phase 3: sync_wait(run_all(tasks))
    └─ Blocks calling thread until all attribute coroutines complete
```

**Barrier 1 is removed.** Each attribute starts unfiltering as soon as its own
I/O completes, independent of other attributes. If attribute A finishes I/O
first, it immediately begins unfiltering while attribute B's I/O is still in
progress.

**Barriers 2a/2b are preserved** within each attribute (the `parallel_for`
calls inside `unfilter_tiles` are unchanged). Multiple attributes may unfilter
concurrently if their I/O completes close together.

### Feature Flag

The async pipeline is gated behind a config flag (default off):

```
sm.query.reader.use_coroutine_pipeline = false
```

Set via C++ API, C API, or environment variable:
```
TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true
```

Both `read_and_unfilter_attribute_tiles()` and
`read_and_unfilter_coordinate_tiles()` dispatch to the async path when enabled.

## Coroutine Primitives

All primitives are header-only in `tiledb/common/coroutine/`.

### Task\<T\> (`task.h`)

Lazy coroutine return type. The coroutine body does not execute until the Task
is `co_await`ed (initial suspend is `suspend_always`). When the coroutine
finishes, it resumes its continuation (the coroutine that awaited it) via the
`FinalAwaiter`.

```cpp
Task<int> compute() {
  co_return 42;
}

Task<void> caller() {
  int v = co_await compute();  // starts compute() here
}
```

### ScheduleOn (`schedule_on.h`)

Awaitable that moves a coroutine to a different thread pool. On `co_await`, the
coroutine suspends and its handle is scheduled on the target pool via
`ThreadPool::schedule_resume()`.

```cpp
co_await ScheduleOn{compute_tp};
// now running on a compute_tp thread
```

### BatchReadyEvent (`batch_ready_event.h`)

One-shot event for I/O completion notification. Multiple coroutines can
`co_await` the event; all are resumed when `signal()` is called. Supports error
propagation via `signal_error(exception_ptr)`.

Used by `FilteredData` in async mode: each attribute's `FilteredData` owns a
`BatchReadyEvent`. I/O tasks decrement an atomic counter; the last one to finish
signals the event.

**Important:** Do not use temporary lambda coroutines that capture the event by
reference. The closure is destroyed after the Task is created, leaving a
dangling reference. Use free functions or named variables instead.

### AsyncSemaphore (`async_semaphore.h`)

Counting semaphore for coroutines. `co_await sem.acquire()` suspends if the
count is zero. `sem.release()` resumes one waiter or increments the count.

### WhenAll (`when_all.h`)

Awaitable combinator. `co_await when_all(tasks)` suspends until all tasks in the
vector complete. Uses an atomic counter (initialized to count+1 for sentinel
safety). The first exception is captured and rethrown after all tasks finish.

### sync_wait (`sync_wait.h`)

Bridges coroutine world to synchronous code. `sync_wait(task)` blocks the
calling thread (via mutex + condition variable) until the task completes, then
returns the result or rethrows the exception.

## FilteredData Async Mode

`FilteredData` has two construction modes:

1. **Sync mode** (original): Takes a `std::vector<ThreadPool::Task>& read_tasks`
   reference. I/O tasks are pushed to the vector; the caller waits on them with
   `wait_all_status()`.

2. **Async mode** (new): Takes an `AsyncTag` instead of `read_tasks`. Creates a
   `BatchReadyEvent` and an atomic `pending_reads_` counter (initialized to 1
   as a sentinel). Each I/O task decrements the counter on completion; the last
   one signals the event. The sentinel is decremented after construction via
   `finish_async_setup()` to prevent premature signaling.

```cpp
// Sync mode
FilteredData fd(resources, reader, ..., read_tasks, memory_tracker);

// Async mode
FilteredData fd(async_tag, resources, reader, ..., memory_tracker);
co_await fd.ready_event();  // suspends until all I/O for this attribute is done
```

## Thread Pool Integration

`ThreadPool::schedule_resume(coroutine_handle)` was added to schedule coroutine
resumption on the thread pool. It wraps the handle in a `packaged_task` and
pushes it to the task queue, matching the existing `execute()` pattern.

## Key Design Decisions

### Hybrid Architecture

Coroutines orchestrate the pipeline; blocking I/O still runs on the existing
thread pools. This allows incremental adoption without rewriting the VFS layer.
A future phase can make VFS calls `co_await`able for true async I/O (especially
beneficial for S3/cloud storage).

### Per-Attribute Granularity

Each attribute gets one `BatchReadyEvent`. This is the coarsest granularity that
still removes the main barrier. Finer granularity (per-block events) is possible
but adds complexity for diminishing returns in Phase 1.

### Free-Function Coroutines

The per-attribute coroutine (`await_then_unfilter`) is a free function, not a
lambda coroutine. This avoids the closure lifetime issue where a temporary
lambda's closure is destroyed before the coroutine frame finishes using it.
The unfiltering callback is a `std::function<void()>` created inside a member
function (which has access to protected members like `unfilter_tiles`), and
passed as a parameter to the free function (parameters live in the coroutine
frame).

### ScheduleOn After Event

After `co_await event`, the coroutine resumes on whatever thread called
`signal()` — typically the I/O thread that completed the last read. The
`co_await ScheduleOn{compute_tp}` immediately hops to the compute pool, freeing
the I/O thread for other work. The subsequent `unfilter_tiles` call uses
`parallel_for` on the compute pool, so the calling compute thread participates
in work-stealing via `yield()`.

## File Layout

```
tiledb/common/coroutine/
├── task.h                  # Task<T> lazy coroutine type
├── schedule_on.h           # ScheduleOn awaitable for thread pool hopping
├── batch_ready_event.h     # One-shot I/O completion event
├── async_semaphore.h       # Counting semaphore for coroutines
├── when_all.h              # Awaitable combinator for multiple tasks
├── sync_wait.h             # Blocks calling thread until task completes
├── CMakeLists.txt          # Header-only library + test subdirectory
├── DESIGN.md               # This file
└── test/
    ├── CMakeLists.txt
    └── unit_coroutine.cc   # 25 test cases, 59 assertions
```

## Testing

- **Unit tests** (`unit_coroutine.cc`): Cover all primitives — Task creation,
  ScheduleOn, BatchReadyEvent (single/multiple waiters, error propagation),
  AsyncSemaphore, WhenAll, sync_wait. Integration tests cover pipeline I/O →
  compute patterns, semaphore-limited pipelines, and a 500-coroutine stress
  test.

- **Integration tests**: All existing dense (89 tests, 20K+ assertions) and
  sparse (124 tests, 101K+ assertions) tests pass with the coroutine pipeline
  both enabled and disabled.
