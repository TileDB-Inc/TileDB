/**
 * @file   unit-threadpool.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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
#include "tiledb/sm/misc/thread_pool.h"

using namespace tiledb::sm;

TEST_CASE("ThreadPool: Test empty", "[threadpool]") {
  for (int i = 0; i < 10; i++) {
    ThreadPool pool(4);
  }
}

TEST_CASE("ThreadPool: Test single thread", "[threadpool]") {
  int result = 0;
  std::vector<std::future<Status>> results;
  ThreadPool pool;
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.enqueue([&result]() {
      result++;
      return Status::Ok();
    }));
  }
  CHECK(pool.wait_all(results));
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test multiple threads", "[threadpool]") {
  std::atomic<int> result(0);
  std::vector<std::future<Status>> results;
  ThreadPool pool(4);
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.enqueue([&result]() {
      result++;
      return Status::Ok();
    }));
  }
  CHECK(pool.wait_all(results));
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test wait status", "[threadpool]") {
  std::atomic<int> result(0);
  std::vector<std::future<Status>> results;
  ThreadPool pool(4);
  for (int i = 0; i < 100; i++) {
    results.push_back(pool.enqueue([&result, i]() {
      result++;
      return i == 50 ? Status::Error("Generic error") : Status::Ok();
    }));
  }
  CHECK(!pool.wait_all(results));
  CHECK(result == 100);
}

TEST_CASE("ThreadPool: Test no wait", "[threadpool]") {
  {
    ThreadPool pool(4);
    std::atomic<int> result(0);
    for (int i = 0; i < 5; i++) {
      pool.enqueue([&result]() {
        result++;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        return Status::Ok();
      });
    }
    // There may be an error logged when the pool is destroyed if there are
    // outstanding tasks, but everything should still complete.
  }
}