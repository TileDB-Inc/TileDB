/**
 * @file   unit_bounded_buffer.cc
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
 * This file defines some pseudo_node classes for testing ports
 */

#include "experimental/tiledb/common/dag/utility/test/unit_bounded_buffer.h"
#include <future>
#include <numeric>
#include <type_traits>
#include <vector>
#include "experimental/tiledb/common/dag/utility/bounded_buffer.h"

using namespace tiledb::common;

template <class T, class Q>
struct foo {};

template <class Q>
struct bar {};

template <class T>
struct foo<T, BoundedBuffer<T>> {
  static inline BoundedBuffer<T> make(size_t size) {
    return BoundedBuffer<T>{size};
  }
};

template <class T>
struct foo<T, ProducerConsumerQueue<T>> {
  static inline ProducerConsumerQueue<T> make(size_t) {
    return ProducerConsumerQueue<T>{};
  }
};

template <class T>
struct foo<T, BoundedBuffer<T, std::queue<T>>> {
  static inline BoundedBuffer<T, std::queue<T>> make(size_t size) {
    return BoundedBuffer<T, std::queue<T>>{size};
  }
};

template <class T>
struct foo<T, ProducerConsumerQueue<T, std::queue<T>>> {
  static inline ProducerConsumerQueue<T, std::queue<T>> make(size_t) {
    return ProducerConsumerQueue<T, std::queue<T>>{};
  }
};

TEMPLATE_TEST_CASE(
    "BoundedBuffer: BoundedBuffer::try_push",
    "[bounded buffer]",
    BoundedBuffer<size_t>,
    ProducerConsumerQueue<size_t>,
    (BoundedBuffer<size_t, std::queue<size_t>>),
    (ProducerConsumerQueue<size_t, std::queue<size_t>>)) {
  // auto a = BoundedBuffer<size_t>();  // Will fail

  auto a = foo<size_t, TestType>::make(5UL);

  SECTION("Just try_push") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.try_push(num) == true);
      CHECK(a.size() == num);
    }
  }
  SECTION("Just push") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.push(num) == true);
      CHECK(a.size() == num);
    }
  }

  if constexpr (
      std::is_same_v<TestType, BoundedBuffer<size_t>> ||
      std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>>) {
    CHECK(a.try_push(6) == false);
    CHECK(a.try_push(7) == false);
    CHECK(a.size() == 5);
  } else if constexpr (
      std::is_same_v<TestType, ProducerConsumerQueue<size_t>> ||
      std::is_same_v<
          TestType,
          ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
    CHECK(a.try_push(6) == true);
    CHECK(a.try_push(7) == true);
    CHECK(a.size() == 7);
  }
}

TEMPLATE_TEST_CASE(
    "BoundedBuffer: BoundedBuffer::try_pop",
    "[bounded buffer]",
    BoundedBuffer<size_t>,
    ProducerConsumerQueue<size_t>,
    (BoundedBuffer<size_t, std::queue<size_t>>),
    (ProducerConsumerQueue<size_t, std::queue<size_t>>)) {
  auto a = foo<size_t, TestType>::make(5UL);

  for (size_t num = 1; num <= 5; ++num) {
    CHECK(a.try_push(num) == true);
    CHECK(a.size() == num);
  }

  CHECK(a.size() == 5);

  size_t expected_size = 5;

  if constexpr (
      std::is_same_v<TestType, ProducerConsumerQueue<size_t>> ||
      std::is_same_v<
          TestType,
          ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
    expected_size = 7;
  }

  if constexpr (
      std::is_same_v<TestType, BoundedBuffer<size_t>> ||
      std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>>) {
    CHECK(a.try_push(6) == false);
    CHECK(a.try_push(7) == false);
  } else if constexpr (
      std::is_same_v<TestType, ProducerConsumerQueue<size_t>> ||
      std::is_same_v<
          TestType,
          ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
    CHECK(a.try_push(6) == true);
    CHECK(a.try_push(7) == true);
  }

  CHECK(a.size() == expected_size);

  SECTION("Just try_pop") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.size() == expected_size - (num - 1));

      auto x = a.try_pop();
      CHECK(x.has_value() == true);

      if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t>> ||
          std::is_same_v<TestType, ProducerConsumerQueue<size_t>>) {
        CHECK(x.value() == expected_size - (num - 1));
      } else if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>> ||
          std::is_same_v<
              TestType,
              ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
        CHECK(x.value() == num);
      }

      CHECK(a.size() == expected_size - num);
    }
  }

  SECTION("Just pop") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.size() == expected_size - (num - 1));

      auto x = a.pop();
      CHECK(x.has_value() == true);

      if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t>> ||
          std::is_same_v<TestType, ProducerConsumerQueue<size_t>>) {
        CHECK(x.value() == expected_size - (num - 1));
      } else if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>> ||
          std::is_same_v<
              TestType,
              ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
        CHECK(x.value() == num);
      }

      CHECK(a.size() == expected_size - num);
    }
  }
}

TEMPLATE_TEST_CASE(
    "BoundedBuffer: BoundedBuffer::push and BoundedBuffer::pop async",
    "[bounded buffer]",
    BoundedBuffer<size_t>,
    ProducerConsumerQueue<size_t>,
    (BoundedBuffer<size_t, std::queue<size_t>>),
    (ProducerConsumerQueue<size_t, std::queue<size_t>>)) {
  size_t rounds = 517;

  auto a = foo<size_t, TestType>::make(5UL);
  std::vector<size_t> v;
  std::vector<size_t> w(rounds);
  std::iota(w.begin(), w.end(), 0UL);

  auto fut_a = std::async(std::launch::async, [&a, rounds]() {
    for (size_t i = 0; i < rounds; ++i) {
      if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>> ||
          std::is_same_v<TestType, BoundedBuffer<size_t>>) {
        CHECK(a.size() <= 5);
      }

      CHECK(a.push(i) == true);
    }
  });

  auto fut_b = std::async(std::launch::async, [&a, &v, rounds]() {
    for (size_t i = 0; i < rounds; ++i) {
      CHECK(v.size() == i);
      auto b = a.pop();
      CHECK(b.has_value());
      v.push_back(*b);
      CHECK(v.size() == i + 1);
      if constexpr (
          std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>> ||
          std::is_same_v<
              TestType,
              ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
        CHECK(*b == i);
      }
    }
  });

  // Wait for tasks to finish
  fut_a.get();
  fut_b.get();

  CHECK(v.size() == rounds);

  /*
   * Check that items got properly put into queue.  Can't do this for deque
   * since items get pulled in LIFO fashion and so only sections of the vector
   * at a time will be reversed by pushing and popping.
   */
  if constexpr (
      std::is_same_v<TestType, BoundedBuffer<size_t, std::queue<size_t>>> ||
      std::is_same_v<
          TestType,
          ProducerConsumerQueue<size_t, std::queue<size_t>>>) {
    CHECK(std::equal(v.begin(), v.end(), w.begin()));
    // Printf debugging
    //    for (size_t i = 0; i < 5; ++i) {
    //      std::cout << v[i] << " " << w[i] << std::endl;
    //    }
    //    for (size_t i = v.size() - 5; i < v.size(); ++i) {
    //      std::cout << v[i] << " " << w[i] << std::endl;
    //    }
    //    std::cout << std::endl;
  }
}
