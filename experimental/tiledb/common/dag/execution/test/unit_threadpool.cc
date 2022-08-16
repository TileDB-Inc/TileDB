/**
 * @file unit_threadpool.cc
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
 * Tests the threadpool class.  The basic interface and implementation of the
 * experimental `ThreadPool` is based on the existing TileDB `ThreadPool`.
 * However, the experimental `ThreadPool` extends the existing `ThreadPool`,
 * controlled by three template parameters (as described in threadpool.h);
 *
 * template <
 *   bool WorkStealing = false,
 *   bool MultipleQueues = false,
 *   bool RecursivePush = true>
 * class ThreadPool{};
 *
 *
 */

#include <test/support/tdb_catch.h>
#include "experimental/tiledb/common/dag/execution/threadpool.h"

using namespace tiledb::common;

template <bool A, bool B, bool C>
void test_construct() {
  for (size_t N = 1; N < 8; ++N) {
    auto a = ThreadPool<A, B, C>(N);
    CHECK(a.num_threads() == N);
  }
}

TEST_CASE("Threadpool: Test construct", "[threadpool]") {
  test_construct<false, false, false>();
  test_construct<false, false, true>();
  test_construct<false, true, false>();
  test_construct<false, true, true>();
  test_construct<true, false, false>();
  test_construct<true, false, true>();
  test_construct<true, true, false>();
  test_construct<true, true, true>();
}

template <bool A, bool B, bool C>
void test_simple_job() {
  for (size_t N = 1; N < 8; ++N) {
    auto a = ThreadPool<A, B, C>(N);
    auto fut = a.async([]() { return 8675309; });
    CHECK(fut.get() == 8675309);
  }
}

TEST_CASE("Threadpool: Test run simple job", "[threadpool]") {
  test_simple_job<false, false, false>();
  test_simple_job<false, false, true>();
  test_simple_job<false, true, false>();
  test_simple_job<false, true, true>();
  test_simple_job<true, false, false>();
  test_simple_job<true, false, true>();
  test_simple_job<true, true, false>();
  test_simple_job<true, true, true>();
}

template <bool A, bool B, bool C>
void test_multiple_job() {
  for (size_t N = 1; N < 8; ++N) {
    auto a = ThreadPool<A, B, C>(N);
    auto v = a.async([]() -> size_t { return 0UL; });
    v.get();
    for (size_t i = 1; i < 16; ++i) {
      std::vector<decltype(v)> futs(i);
      futs.clear();
      for (size_t j = 1; j <= i; ++j) {
        futs.emplace_back(a.async([=]() -> size_t { return 8675309 + j + i; }));
      }
      for (size_t j = 1; j <= i; ++j) {
        CHECK(futs[j - 1].get() == (8675309 + j + i));
      }
    }
  }
}

TEST_CASE("Threadpool: Test run multiple job", "[threadpool]") {
  test_multiple_job<false, false, false>();
  test_multiple_job<false, false, true>();
  test_multiple_job<false, true, false>();
  test_multiple_job<false, true, true>();
  test_multiple_job<true, false, false>();
  test_multiple_job<true, false, true>();
  test_multiple_job<true, true, false>();
  test_multiple_job<true, true, true>();
}
