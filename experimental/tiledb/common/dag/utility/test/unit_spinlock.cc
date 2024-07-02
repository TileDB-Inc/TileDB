/**
 * @file unit_spinlock.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Implement tests for spinlocks.
 */

#include <test/support/tdb_catch.h>
#include <future>
#include <mutex>
#include <numeric>
#include <vector>
#include "../spinlock.h"

using namespace tiledb::common;

int a;

template <class lock_type>
void lock_test() {
  lock_type mutex;
  SECTION("construct") {
  }
  SECTION("lock and unlock") {
    mutex.lock();
    mutex.unlock();
  }

  SECTION("lock with std::scoped_lock constructor") {
    std::scoped_lock lock(mutex);
  }
  SECTION("lock with std::unique_lock constructor") {
    std::unique_lock lock(mutex);
  }
  SECTION("lock and unlock with std::unique_lock") {
    std::unique_lock lock(mutex);
    lock.unlock();
    lock.lock();
    lock.unlock();
  }
}

TEST_CASE("Spinlock: Construct spinlock", "[spinlock]") {
  lock_test<spinlock_mutex>();
  lock_test<ttas_bool_spinlock_mutex>();
  lock_test<ttas_flag_spinlock_mutex>();
}

template <class lock_type>
void async_test_no_contention() {
  lock_type mutex;
  std::vector<int> test_vector;
  std::vector<int> ans_vector{1, 2};

  auto fut_b = std::async(std::launch::async, [&]() {
    std::scoped_lock lock(mutex);
    test_vector.push_back(1);
  });

  auto fut_a = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(555));
    std::scoped_lock lock(mutex);
    test_vector.push_back(2);
  });
  fut_b.get();
  fut_a.get();

  CHECK(std::equal(test_vector.begin(), test_vector.end(), ans_vector.begin()));
}

template <class lock_type>
void async_test_possible_contention() {
  lock_type mutex;
  std::vector<int> test_vector;

  const size_t rounds = 8675309;
  size_t i = 0;
  auto fut_b = std::async(std::launch::async, [&]() {
    while (i < rounds) {
      std::scoped_lock lock(mutex);
      test_vector.push_back(i++);
    }
  });

  auto fut_a = std::async(std::launch::async, [&]() {
    while (i < rounds) {
      std::scoped_lock lock(mutex);
      test_vector.push_back(i++);
    }
  });
  fut_b.get();
  fut_a.get();
  std::sort(test_vector.begin(), test_vector.end());
  CHECK((test_vector.size() == rounds || test_vector.size() == rounds + 1));
  CHECK((test_vector.back() == rounds || test_vector.back() == rounds + 1));
}

TEST_CASE("Spinlock: Asynchronous tasks, no contention", "[spinlock]") {
  SECTION("std::mutex") {
    async_test_no_contention<std::mutex>();
  }
  SECTION("spinlock") {
    async_test_no_contention<spinlock_mutex>();
  }
  SECTION("ttas_bool_spinlock") {
    async_test_no_contention<ttas_bool_spinlock_mutex>();
  }
  SECTION("ttas_flag_spinlock") {
    async_test_no_contention<ttas_flag_spinlock_mutex>();
  }
}

TEST_CASE("Spinlock: Asynchronous tasks, possible contention", "[spinlock]") {
  SECTION("std::mutex") {
    async_test_possible_contention<std::mutex>();
  }
  SECTION("spinlock") {
    async_test_possible_contention<spinlock_mutex>();
  }
  SECTION("ttas_bool_spinlock") {
    async_test_possible_contention<ttas_bool_spinlock_mutex>();
  }
  SECTION("ttas_flag_spinlock") {
    async_test_possible_contention<ttas_flag_spinlock_mutex>();
  }
}
