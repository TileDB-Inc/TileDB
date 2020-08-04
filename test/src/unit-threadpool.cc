/**
 * @file   unit-threadpool.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * Tests the `ThreadPool` class.
 */

#include <atomic>
#include <catch.hpp>
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/misc/thread_pool.h"

using namespace tiledb::sm;

TEST_CASE("ThreadPool: Test empty", "[threadpool]") {
  for (int i = 0; i < 10; i++) {
    ThreadPool pool;
    REQUIRE(pool.init(4).ok());
  }
}

TEST_CASE("ThreadPool: Test single thread", "[threadpool]") {
  int result = 0;
  std::vector<std::future<Status>> results;
  ThreadPool pool;
  REQUIRE(pool.init().ok());
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.execute([&result]() {
      result++;
      return Status::Ok();
    }));
  }
  CHECK(pool.wait_all(results).ok());
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test multiple threads", "[threadpool]") {
  std::atomic<int> result(0);
  std::vector<std::future<Status>> results;
  ThreadPool pool;
  REQUIRE(pool.init(4).ok());
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.execute([&result]() {
      result++;
      return Status::Ok();
    }));
  }
  CHECK(pool.wait_all(results).ok());
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test wait status", "[threadpool]") {
  std::atomic<int> result(0);
  std::vector<std::future<Status>> results;
  ThreadPool pool;
  REQUIRE(pool.init(4).ok());
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.execute([&result, i]() {
      result++;
      return i == 50 ? Status::Error("Generic error") : Status::Ok();
    }));
  }
  CHECK(!pool.wait_all(results).ok());
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test no wait", "[threadpool]") {
  {
    ThreadPool pool;
    REQUIRE(pool.init(4).ok());
    std::atomic<int> result(0);
    for (int i = 0; i < 5; i++) {
      std::future<Status> task = pool.execute([&result]() {
        result++;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        return Status::Ok();
      });
      CHECK(task.valid());
    }
    // There may be an error logged when the pool is destroyed if there are
    // outstanding tasks, but everything should still complete.
  }
}

TEST_CASE(
    "ThreadPool: Test pending task cancellation", "[threadpool], [cancel]") {
  SECTION("- No cancellation callback") {
    ThreadPool pool;
    CancelableTasks cancelable_tasks;
    REQUIRE(pool.init(2).ok());
    std::atomic<int> result(0);
    std::vector<std::future<Status>> tasks;

    for (int i = 0; i < 5; i++) {
      tasks.push_back(cancelable_tasks.execute(&pool, [&result]() {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        result++;
        return Status::Ok();
      }));
    }

    // Because the thread pool has 2 threads, the first two will probably be
    // executing at this point, but some will still be queued.
    cancelable_tasks.cancel_all_tasks();

    // The result is the number of threads that returned Ok (were not
    // cancelled).
    std::vector<Status> statuses = pool.wait_all_status(tasks);
    int num_ok = 0;
    for (const auto& st : statuses) {
      num_ok += st.ok() ? 1 : 0;
    }

    CHECK(result == num_ok);
  }

  SECTION("- With cancellation callback") {
    ThreadPool pool;
    CancelableTasks cancelable_tasks;
    REQUIRE(pool.init(2).ok());
    std::atomic<int> result(0), num_cancelled(0);
    std::vector<std::future<Status>> tasks;

    for (int i = 0; i < 5; i++) {
      tasks.push_back(cancelable_tasks.execute(
          &pool,
          [&result]() {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            result++;
            return Status::Ok();
          },
          [&num_cancelled]() { num_cancelled++; }));
    }

    // Because the thread pool has 2 threads, the first two will probably be
    // executing at this point, but some will still be queued.
    cancelable_tasks.cancel_all_tasks();

    // The result is the number of threads that returned Ok (were not
    // cancelled).
    std::vector<Status> statuses = pool.wait_all_status(tasks);
    int num_ok = 0;
    for (const auto& st : statuses) {
      num_ok += st.ok() ? 1 : 0;
    }

    CHECK(result == num_ok);
    CHECK(num_cancelled == (tasks.size() - num_ok));
  }
}

TEST_CASE("ThreadPool: Test execute with empty pool", "[threadpool]") {
  ThreadPool pool;
  std::atomic<int> result(0);
  auto task = pool.execute([&result]() {
    result = 100;
    return Status::Ok();
  });

  CHECK(!task.valid());
  CHECK(result == 0);
}

TEST_CASE("ThreadPool: Test recursion", "[threadpool]") {
  ThreadPool pool;

  SECTION("- One thread") {
    REQUIRE(pool.init(1).ok());
  }

  SECTION("- Two threads") {
    REQUIRE(pool.init(2).ok());
  }

  SECTION("- Ten threads") {
    REQUIRE(pool.init(10).ok());
  }

  std::atomic<int> result(0);

  const size_t num_tasks = 100;
  const size_t num_nested_tasks = 10;
  std::vector<std::future<Status>> tasks;
  for (size_t i = 0; i < num_tasks; ++i) {
    auto task = pool.execute([&]() {
      std::vector<std::future<Status>> inner_tasks;
      for (size_t j = 0; j < num_nested_tasks; ++j) {
        auto inner_task = pool.execute([&result]() {
          ++result;
          return Status::Ok();
        });

        CHECK(inner_task.valid());
        inner_tasks.emplace_back(std::move(inner_task));
      }

      CHECK(pool.wait_all(inner_tasks).ok());
      return Status::Ok();
    });

    CHECK(task.valid());
    tasks.emplace_back(std::move(task));
  }

  CHECK(pool.wait_all(tasks).ok());
  CHECK(result == (num_tasks * num_nested_tasks));
}
