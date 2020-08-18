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
#include <iostream>
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
  std::vector<ThreadPool::Task> results;
  ThreadPool pool;
  REQUIRE(pool.init().ok());
  for (int i = 0; i < 100; i++) {
    ThreadPool::Task task = pool.execute([&result]() {
      result++;
      return Status::Ok();
    });
    REQUIRE(task.valid());
    results.emplace_back(std::move(task));
  }
  CHECK(pool.wait_all(results).ok());
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test multiple threads", "[threadpool]") {
  std::atomic<int> result(0);
  std::vector<ThreadPool::Task> results;
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
  std::vector<ThreadPool::Task> results;
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
      ThreadPool::Task task = pool.execute([&result]() {
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
    std::vector<ThreadPool::Task> tasks;

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
    std::vector<ThreadPool::Task> tasks;

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

  // Test recursive execute-and-wait.
  std::atomic<int> result(0);
  const size_t num_tasks = 100;
  const size_t num_nested_tasks = 10;
  std::vector<ThreadPool::Task> tasks;
  for (size_t i = 0; i < num_tasks; ++i) {
    auto task = pool.execute([&]() {
      std::vector<ThreadPool::Task> inner_tasks;
      for (size_t j = 0; j < num_nested_tasks; ++j) {
        auto inner_task = pool.execute([&]() {
          ++result;
          return Status::Ok();
        });

        inner_tasks.emplace_back(std::move(inner_task));
      }

      pool.wait_all(inner_tasks).ok();
      return Status::Ok();
    });

    CHECK(task.valid());
    tasks.emplace_back(std::move(task));
  }
  CHECK(pool.wait_all(tasks).ok());
  CHECK(result == (num_tasks * num_nested_tasks));

  // Test a top-level execute-and-wait with async-style inner tasks.
  std::condition_variable cv;
  std::mutex cv_mutex;
  tasks.clear();
  for (size_t i = 0; i < num_tasks; ++i) {
    auto task = pool.execute([&]() {
      for (size_t j = 0; j < num_nested_tasks; ++j) {
        pool.execute([&]() {
          std::unique_lock<std::mutex> ul(cv_mutex);
          if (--result == 0) {
            cv.notify_all();
          }
          return Status::Ok();
        });
      }

      return Status::Ok();
    });

    CHECK(task.valid());
    tasks.emplace_back(std::move(task));
  }

  CHECK(pool.wait_all(tasks).ok());

  // Wait all inner tasks to complete.
  std::unique_lock<std::mutex> ul(cv_mutex);
  while (result > 0)
    cv.wait(ul);
}

TEST_CASE("ThreadPool: Test recursion, two pools", "[threadpool]") {
  ThreadPool pool_a;
  ThreadPool pool_b;

  SECTION("- One thread") {
    REQUIRE(pool_a.init(1).ok());
    REQUIRE(pool_b.init(1).ok());
  }

  SECTION("- Two threads") {
    REQUIRE(pool_a.init(2).ok());
    REQUIRE(pool_b.init(2).ok());
  }

  SECTION("- Ten threads") {
    REQUIRE(pool_a.init(10).ok());
    REQUIRE(pool_b.init(2).ok());
  }

  // Test recursive execute-and-wait.
  std::atomic<int> result(0);
  const size_t num_tasks_a = 10;
  const size_t num_tasks_b = 10;
  const size_t num_tasks_c = 10;
  std::vector<ThreadPool::Task> tasks_a;
  for (size_t i = 0; i < num_tasks_a; ++i) {
    auto task_a = pool_a.execute([&]() {
      std::vector<ThreadPool::Task> tasks_b;
      for (size_t j = 0; j < num_tasks_b; ++j) {
        auto task_b = pool_b.execute([&]() {
          std::vector<ThreadPool::Task> tasks_c;
          for (size_t k = 0; k < num_tasks_b; ++k) {
            auto task_c = pool_a.execute([&result]() {
              ++result;
              return Status::Ok();
            });

            tasks_c.emplace_back(std::move(task_c));
          }

          pool_a.wait_all(tasks_c);
          return Status::Ok();
        });

        tasks_b.emplace_back(std::move(task_b));
      }

      pool_b.wait_all(tasks_b).ok();
      return Status::Ok();
    });

    CHECK(task_a.valid());
    tasks_a.emplace_back(std::move(task_a));
  }
  CHECK(pool_a.wait_all(tasks_a).ok());
  CHECK(result == (num_tasks_a * num_tasks_b * num_tasks_c));

  // Test a top-level execute-and-wait with async-style inner tasks.
  std::condition_variable cv;
  std::mutex cv_mutex;
  tasks_a.clear();
  for (size_t i = 0; i < num_tasks_a; ++i) {
    auto task_a = pool_a.execute([&]() {
      std::vector<ThreadPool::Task> tasks_b;
      for (size_t j = 0; j < num_tasks_b; ++j) {
        auto task_b = pool_b.execute([&]() {
          std::vector<ThreadPool::Task> tasks_c;
          for (size_t k = 0; k < num_tasks_b; ++k) {
            auto task_c = pool_a.execute([&]() {
              if (--result == 0) {
                std::unique_lock<std::mutex> ul(cv_mutex);
                cv.notify_all();
              }
              return Status::Ok();
            });

            tasks_c.emplace_back(std::move(task_c));
          }

          pool_a.wait_all(tasks_c);
          return Status::Ok();
        });

        tasks_b.emplace_back(std::move(task_b));
      }

      pool_b.wait_all(tasks_b).ok();
      return Status::Ok();
    });

    CHECK(task_a.valid());
    tasks_a.emplace_back(std::move(task_a));
  }
  CHECK(pool_a.wait_all(tasks_a).ok());

  // Wait all inner tasks to complete.
  std::unique_lock<std::mutex> ul(cv_mutex);
  while (result > 0)
    cv.wait(ul);
}
