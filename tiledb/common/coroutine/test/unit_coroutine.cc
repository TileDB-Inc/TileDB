/**
 * @file tiledb/common/coroutine/test/unit_coroutine.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests for coroutine primitives: Task<T>, ScheduleOn, when_all,
 * AsyncSemaphore, BatchReadyEvent, and sync_wait.
 */

#include <test/support/tdb_catch.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "tiledb/common/coroutine/async_semaphore.h"
#include "tiledb/common/coroutine/batch_ready_event.h"
#include "tiledb/common/coroutine/io_completion_event.h"
#include "tiledb/common/coroutine/schedule_on.h"
#include "tiledb/common/coroutine/sync_wait.h"
#include "tiledb/common/coroutine/task.h"
#include "tiledb/common/coroutine/when_all.h"
#include "tiledb/common/thread_pool/thread_pool.h"

using namespace tiledb::common;

/* ********************************* */
/*            Task<T> Tests          */
/* ********************************* */

TEST_CASE("Task<int>: basic co_return", "[coroutine][task]") {
  auto coro = []() -> Task<int> { co_return 42; };
  auto result = sync_wait(coro());
  CHECK(result == 42);
}

TEST_CASE("Task<void>: basic co_return", "[coroutine][task]") {
  bool executed = false;
  auto coro = [&]() -> Task<void> {
    executed = true;
    co_return;
  };
  sync_wait(coro());
  CHECK(executed);
}

TEST_CASE("Task<string>: non-trivial return type", "[coroutine][task]") {
  auto coro = []() -> Task<std::string> { co_return "hello coroutine"; };
  auto result = sync_wait(coro());
  CHECK(result == "hello coroutine");
}

TEST_CASE("Task: exception propagation", "[coroutine][task]") {
  auto coro = []() -> Task<int> {
    throw std::runtime_error("test error");
    co_return 0;
  };
  CHECK_THROWS_WITH(sync_wait(coro()), "test error");
}

TEST_CASE("Task<void>: exception propagation", "[coroutine][task]") {
  auto coro = []() -> Task<void> {
    throw std::logic_error("void error");
    co_return;
  };
  CHECK_THROWS_AS(sync_wait(coro()), std::logic_error);
}

TEST_CASE("Task: chained co_await", "[coroutine][task]") {
  auto inner = []() -> Task<int> { co_return 10; };
  auto outer = [&]() -> Task<int> {
    int a = co_await inner();
    int b = co_await inner();
    co_return a + b;
  };
  CHECK(sync_wait(outer()) == 20);
}

TEST_CASE("Task: move-only semantics", "[coroutine][task]") {
  auto coro = []() -> Task<int> { co_return 1; };
  Task<int> t = coro();
  CHECK(t.valid());

  Task<int> t2 = std::move(t);
  CHECK(t2.valid());
  CHECK_FALSE(t.valid());
}

/* ********************************* */
/*         ScheduleOn Tests          */
/* ********************************* */

TEST_CASE("ScheduleOn: resumes on thread pool", "[coroutine][schedule_on]") {
  ThreadPool tp(2);
  auto main_id = std::this_thread::get_id();

  auto coro = [&]() -> Task<std::thread::id> {
    co_await ScheduleOn{tp};
    co_return std::this_thread::get_id();
  };

  auto pool_id = sync_wait(coro());
  // The coroutine should have resumed on a pool thread, not the main thread.
  CHECK(pool_id != main_id);
}

TEST_CASE(
    "ScheduleOn: multiple hops between pools", "[coroutine][schedule_on]") {
  ThreadPool pool_a(1);
  ThreadPool pool_b(1);

  auto coro = [&]() -> Task<void> {
    co_await ScheduleOn{pool_a};
    auto id_a = std::this_thread::get_id();

    co_await ScheduleOn{pool_b};
    auto id_b = std::this_thread::get_id();

    // With single-thread pools, we should be on different threads.
    CHECK(id_a != id_b);
    co_return;
  };

  sync_wait(coro());
}

/* ********************************* */
/*          when_all Tests           */
/* ********************************* */

TEST_CASE("when_all: empty vector", "[coroutine][when_all]") {
  auto coro = []() -> Task<void> {
    co_await when_all(std::vector<Task<void>>{});
  };
  sync_wait(coro());
}

TEST_CASE("when_all: multiple tasks complete", "[coroutine][when_all]") {
  std::atomic<int> counter{0};

  auto work = [&]() -> Task<void> {
    counter.fetch_add(1);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(work());
    tasks.push_back(work());
    tasks.push_back(work());
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(counter.load() == 3);
}

TEST_CASE("when_all: tasks on thread pool", "[coroutine][when_all]") {
  ThreadPool tp(4);
  std::atomic<int> counter{0};

  auto work = [&](int val) -> Task<void> {
    co_await ScheduleOn{tp};
    counter.fetch_add(val);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(work(1));
    tasks.push_back(work(10));
    tasks.push_back(work(100));
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(counter.load() == 111);
}

// Explicitly tests the synchronous path where all tasks complete without
// ever suspending. This exercises the symmetric-transfer branch in
// WhenAllAwaitable::await_suspend (counter hits zero during the for-loop,
// continuation is resumed via tail-call rather than inline).
TEST_CASE(
    "when_all: all tasks synchronous (no suspension)",
    "[coroutine][when_all]") {
  std::atomic<int> counter{0};

  // Tasks that complete without any co_await — they run to completion
  // synchronously inside the when_all_task wrappers during await_suspend.
  auto sync_task = [&]() -> Task<void> {
    counter.fetch_add(1);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < 5; ++i) {
      tasks.push_back(sync_task());
    }
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(counter.load() == 5);
}

TEST_CASE("when_all: exception from one task", "[coroutine][when_all]") {
  auto ok_task = []() -> Task<void> { co_return; };
  auto bad_task = []() -> Task<void> {
    throw std::runtime_error("when_all error");
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(ok_task());
    tasks.push_back(bad_task());
    tasks.push_back(ok_task());
    co_await when_all(std::move(tasks));
  };

  CHECK_THROWS_WITH(sync_wait(coro()), "when_all error");
}

/* ********************************* */
/*      AsyncSemaphore Tests         */
/* ********************************* */

TEST_CASE(
    "AsyncSemaphore: acquire succeeds immediately when count > 0",
    "[coroutine][semaphore]") {
  AsyncSemaphore sem(2);

  auto coro = [&]() -> Task<void> {
    co_await sem.acquire();
    co_await sem.acquire();
    // Both should succeed without suspending.
    sem.release();
    sem.release();
    co_return;
  };

  sync_wait(coro());
}

TEST_CASE(
    "AsyncSemaphore: acquire suspends when count is 0",
    "[coroutine][semaphore]") {
  ThreadPool tp(2);
  AsyncSemaphore sem(1);
  std::atomic<int> order{0};

  auto holder = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    co_await sem.acquire();
    order.fetch_add(1);  // step 1
    // Hold the semaphore for a bit.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    order.fetch_add(1);  // step 2
    sem.release();
    co_return;
  };

  auto waiter = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    // Small delay to let holder acquire first.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    co_await sem.acquire();  // Should suspend until holder releases.
    int val = order.load();
    CHECK(val >= 2);  // Should only proceed after holder released.
    sem.release();
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(holder());
    tasks.push_back(waiter());
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
}

/* ********************************* */
/*      BatchReadyEvent Tests        */
/* ********************************* */

TEST_CASE(
    "BatchReadyEvent: signal before await resumes immediately",
    "[coroutine][event]") {
  BatchReadyEvent event;
  event.signal();

  auto coro = [&]() -> Task<void> {
    co_await event;
    // Should not suspend since already signaled.
    co_return;
  };

  sync_wait(coro());
}

TEST_CASE(
    "BatchReadyEvent: await before signal suspends then resumes",
    "[coroutine][event]") {
  ThreadPool tp(2);
  BatchReadyEvent event;
  std::atomic<bool> resumed{false};

  auto waiter = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    co_await event;
    resumed.store(true);
    co_return;
  };

  auto signaler = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(resumed.load());
    event.signal();
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(waiter());
    tasks.push_back(signaler());
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(resumed.load());
}

TEST_CASE(
    "BatchReadyEvent: multiple waiters all resume", "[coroutine][event]") {
  ThreadPool tp(4);
  BatchReadyEvent event;
  std::atomic<int> resumed_count{0};

  auto waiter = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    co_await event;
    resumed_count.fetch_add(1);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < 5; ++i) {
      tasks.push_back(waiter());
    }

    // Start all waiters then signal after a delay.
    auto signaler = [&]() -> Task<void> {
      co_await ScheduleOn{tp};
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      event.signal();
      co_return;
    };
    tasks.push_back(signaler());

    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(resumed_count.load() == 5);
}

TEST_CASE(
    "BatchReadyEvent: signal_error propagates exception",
    "[coroutine][event]") {
  BatchReadyEvent event;

  auto coro = [&]() -> Task<void> {
    // Signal error from another "thread" (inline for simplicity).
    event.signal_error(std::make_exception_ptr(std::runtime_error("io error")));
    co_await event;
    co_return;
  };

  CHECK_THROWS_WITH(sync_wait(coro()), "io error");
}

/* ********************************* */
/*         sync_wait Tests           */
/* ********************************* */

TEST_CASE("sync_wait: blocks until completion", "[coroutine][sync_wait]") {
  ThreadPool tp(1);

  auto coro = [&]() -> Task<int> {
    co_await ScheduleOn{tp};
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    co_return 99;
  };

  auto result = sync_wait(coro());
  CHECK(result == 99);
}

TEST_CASE(
    "sync_wait: rethrows exception from coroutine", "[coroutine][sync_wait]") {
  auto coro = []() -> Task<void> {
    throw std::runtime_error("sync error");
    co_return;
  };

  CHECK_THROWS_WITH(sync_wait(coro()), "sync error");
}

/* ********************************* */
/*     Integration / Stress Tests    */
/* ********************************* */

TEST_CASE(
    "Integration: pipeline I/O -> compute per tile",
    "[coroutine][integration]") {
  ThreadPool io_tp(2);
  ThreadPool compute_tp(2);
  const int num_tiles = 20;
  std::vector<int> results(num_tiles, 0);

  auto process_tile = [&](int tile_idx) -> Task<void> {
    // Simulate I/O phase.
    co_await ScheduleOn{io_tp};
    int data = tile_idx * 10;

    // Simulate compute phase.
    co_await ScheduleOn{compute_tp};
    results[tile_idx] = data + 1;

    co_return;
  };

  auto pipeline = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < num_tiles; ++i) {
      tasks.push_back(process_tile(i));
    }
    co_await when_all(std::move(tasks));
  };

  sync_wait(pipeline());

  for (int i = 0; i < num_tiles; ++i) {
    CHECK(results[i] == i * 10 + 1);
  }
}

TEST_CASE(
    "Integration: semaphore-limited pipeline", "[coroutine][integration]") {
  ThreadPool tp(4);
  const int num_items = 50;
  const int max_in_flight = 3;
  AsyncSemaphore sem(max_in_flight);
  std::atomic<int> concurrent{0};
  std::atomic<int> max_concurrent{0};
  std::atomic<int> completed{0};

  auto work = [&](int) -> Task<void> {
    co_await sem.acquire();
    co_await ScheduleOn{tp};

    int c = concurrent.fetch_add(1) + 1;
    // Track max concurrency.
    int prev_max = max_concurrent.load();
    while (c > prev_max && !max_concurrent.compare_exchange_weak(prev_max, c)) {
    }

    // Simulate work.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    concurrent.fetch_sub(1);
    completed.fetch_add(1);
    sem.release();
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < num_items; ++i) {
      tasks.push_back(work(i));
    }
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());

  CHECK(completed.load() == num_items);
  CHECK(max_concurrent.load() <= max_in_flight);
}

// Free-function coroutines to avoid the lambda-coroutine dangling capture
// issue. When a temporary lambda coroutine captures by reference, the closure
// object may be destroyed before the coroutine completes, leaving dangling
// references in the coroutine frame. Using free functions with parameters
// passed by value (pointers/refs to stable objects) avoids this.
Task<void> batch_tile_worker(
    BatchReadyEvent* event, ThreadPool* compute_tp, int* result_slot) {
  co_await *event;
  co_await ScheduleOn{*compute_tp};
  *result_slot += 1;
}

Task<void> batch_io_simulator(BatchReadyEvent* event, ThreadPool* io_tp) {
  co_await ScheduleOn{*io_tp};
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  event->signal();
}

TEST_CASE(
    "Integration: batch event with per-tile coroutines",
    "[coroutine][integration]") {
  ThreadPool io_tp(2);
  ThreadPool compute_tp(2);
  const int num_batches = 3;
  const int tiles_per_batch = 5;
  std::vector<int> results(num_batches * tiles_per_batch, 0);

  auto pipeline = [&]() -> Task<void> {
    for (int batch = 0; batch < num_batches; ++batch) {
      BatchReadyEvent batch_event;

      std::vector<Task<void>> tile_tasks;
      for (int t = 0; t < tiles_per_batch; ++t) {
        int idx = batch * tiles_per_batch + t;
        // Each result slot starts at idx, worker adds 1.
        results[idx] = idx;
        tile_tasks.push_back(
            batch_tile_worker(&batch_event, &compute_tp, &results[idx]));
      }

      tile_tasks.push_back(batch_io_simulator(&batch_event, &io_tp));

      co_await when_all(std::move(tile_tasks));
    }
    co_return;
  };

  sync_wait(pipeline());

  for (int i = 0; i < num_batches * tiles_per_batch; ++i) {
    CHECK(results[i] == i + 1);
  }
}

TEST_CASE(
    "Stress: many coroutines on small thread pool", "[coroutine][stress]") {
  ThreadPool tp(2);
  const int n = 500;
  std::atomic<int> counter{0};

  auto work = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    counter.fetch_add(1);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < n; ++i) {
      tasks.push_back(work());
    }
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(counter.load() == n);
}

/* ********************************* */
/*    IoCompletionEvent Tests        */
/* ********************************* */

TEST_CASE(
    "IoCompletionEvent: signal before await resumes immediately",
    "[coroutine][io_completion]") {
  IoCompletionEvent event;
  event.signal(42);

  auto coro = [&]() -> Task<uint64_t> {
    auto bytes = co_await event;
    co_return bytes;
  };

  auto result = sync_wait(coro());
  CHECK(result == 42);
}

Task<uint64_t> io_awaiter(IoCompletionEvent* event) {
  auto bytes = co_await *event;
  co_return bytes;
}

TEST_CASE(
    "IoCompletionEvent: await before signal suspends then resumes",
    "[coroutine][io_completion]") {
  ThreadPool tp(2);
  IoCompletionEvent event;
  std::atomic<bool> resumed{false};

  auto waiter = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    auto bytes = co_await event;
    CHECK(bytes == 100);
    resumed.store(true);
    co_return;
  };

  auto signaler = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(resumed.load());
    event.signal(100);
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    tasks.push_back(waiter());
    tasks.push_back(signaler());
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());
  CHECK(resumed.load());
}

TEST_CASE(
    "IoCompletionEvent: signal_error propagates exception",
    "[coroutine][io_completion]") {
  IoCompletionEvent event;

  auto coro = [&]() -> Task<uint64_t> {
    event.signal_error(
        std::make_exception_ptr(std::runtime_error("read failed")));
    auto bytes = co_await event;
    co_return bytes;
  };

  CHECK_THROWS_WITH(sync_wait(coro()), "read failed");
}

TEST_CASE(
    "IoCompletionEvent: signal_error before await rethrows immediately",
    "[coroutine][io_completion]") {
  IoCompletionEvent event;
  event.signal_error(
      std::make_exception_ptr(std::runtime_error("early error")));

  auto coro = [&]() -> Task<uint64_t> {
    auto bytes = co_await event;
    co_return bytes;
  };

  CHECK_THROWS_WITH(sync_wait(coro()), "early error");
}

TEST_CASE(
    "IoCompletionEvent: concurrent signal and await from different threads",
    "[coroutine][io_completion]") {
  ThreadPool tp(2);
  const int num_events = 50;
  std::vector<std::unique_ptr<IoCompletionEvent>> events;
  for (int i = 0; i < num_events; ++i) {
    events.push_back(std::make_unique<IoCompletionEvent>());
  }

  std::atomic<uint64_t> total_bytes{0};

  auto waiter = [&](int idx) -> Task<void> {
    co_await ScheduleOn{tp};
    auto bytes = co_await *events[idx];
    total_bytes.fetch_add(bytes);
    co_return;
  };

  // Signal from separate threads with varying delays.
  auto signaler = [&]() -> Task<void> {
    co_await ScheduleOn{tp};
    for (int i = 0; i < num_events; ++i) {
      events[i]->signal(static_cast<uint64_t>(i + 1));
    }
    co_return;
  };

  auto coro = [&]() -> Task<void> {
    std::vector<Task<void>> tasks;
    for (int i = 0; i < num_events; ++i) {
      tasks.push_back(waiter(i));
    }
    tasks.push_back(signaler());
    co_await when_all(std::move(tasks));
  };

  sync_wait(coro());

  // Sum of 1..50 = 1275.
  uint64_t expected = num_events * (num_events + 1) / 2;
  CHECK(total_bytes.load() == expected);
}

TEST_CASE(
    "IoCompletionEvent: is_signaled reflects state",
    "[coroutine][io_completion]") {
  IoCompletionEvent event;
  CHECK_FALSE(event.is_signaled());
  event.signal(0);
  CHECK(event.is_signaled());
}
