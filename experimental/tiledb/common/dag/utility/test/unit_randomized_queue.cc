/**
 * @file   unit_randomized_queue.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include "experimental/tiledb/common/dag/utility/test/unit_randomized_queue.h"
#include <future>
#include <type_traits>
#include <vector>
#include "experimental/tiledb/common/dag/utility/randomized_queue.h"

using namespace tiledb::common;

template <class T, class Q>
struct foo {};

template <class Q>
struct bar {};

template <class T>
struct foo<T, RandomizedQueue<T>> {
  static inline RandomizedQueue<T> make(size_t) {
    return RandomizedQueue<T>{};
  }
};

TEMPLATE_TEST_CASE(
    "RandomizedQueue: RandomizedQueue::try_push",
    "[randomized queue]",
    RandomizedQueue<size_t>) {
  // auto a = RandomizedQueue<size_t>();  // Will fail

  auto a = foo<size_t, TestType>::make(5UL);

  SECTION("Just push") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.push(num) == true);
      CHECK(a.size() == num);
    }
  }
}

TEMPLATE_TEST_CASE(
    "RandomizedQueue: RandomizedQueue::try_pop",
    "[randomized queue]",
    RandomizedQueue<size_t>) {
  auto a = foo<size_t, TestType>::make(5UL);

  for (size_t num = 1; num <= 5; ++num) {
    CHECK(a.try_push(num) == true);
    CHECK(a.size() == num);
  }

  CHECK(a.size() == 5);

  size_t expected_size = 7;

  CHECK(a.try_push(6) == true);
  CHECK(a.try_push(7) == true);

  CHECK(a.size() == expected_size);

  SECTION("Just try_pop") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.size() == expected_size - (num - 1));

      auto x = a.try_pop();
      CHECK(x.has_value() == true);
      CHECK(a.size() == expected_size - num);
    }
  }

  SECTION("Just pop") {
    for (size_t num = 1; num <= 5; ++num) {
      CHECK(a.size() == expected_size - (num - 1));

      auto x = a.pop();
      CHECK(x.has_value() == true);
      CHECK(a.size() == expected_size - num);
    }
  }
}

TEMPLATE_TEST_CASE(
    "RandomizedQueue: Everything pushed will be popped",
    "[randomized queue]",
    RandomizedQueue<size_t>) {
  // auto a = RandomizedQueue<size_t>();  // Will fail

  size_t num_elements = 1337;
  std::vector<size_t> elements(num_elements);
  std::vector<size_t> check(num_elements);
  std::iota(begin(elements), end(elements), 0);

  auto a = foo<size_t, TestType>::make(num_elements);
  for (auto&& e : elements) {
    CHECK(a.push(e) == true);
  }
  for (auto& e : check) {
    auto v = a.pop();
    CHECK(v.has_value());
    e = *v;
  }
  std::sort(begin(check), end(check));
  CHECK(std::equal(begin(check), end(check), begin(elements)));
}

TEMPLATE_TEST_CASE(
    "RandomizedQueue: RandomizedQueue::push and RandomizedQueue::pop async",
    "[randomized queue]",
    RandomizedQueue<size_t>) {
  size_t rounds = 517;

  auto a = foo<size_t, TestType>::make(5UL);
  std::vector<size_t> v;
  std::vector<size_t> w(rounds);
  std::iota(w.begin(), w.end(), 0UL);

  auto fut_a = std::async(std::launch::async, [&a, rounds]() {
    for (size_t i = 0; i < rounds; ++i) {
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
    }
  });

  // Wait for tasks to finish
  fut_a.get();
  fut_b.get();

  CHECK(v.size() == rounds);

  std::sort(begin(v), end(v));
  CHECK(std::equal(begin(v), end(v), begin(w)));
}
