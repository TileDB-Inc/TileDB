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
 * Tests the threadpool class.
 */

#include <tiledb/sm/misc/tdb_catch.h>
#include "experimental/tiledb/common/dag/execution/threadpool.h"

// using namespace tiledb::common;

TEST_CASE("Threadpool: Test construct", "[threadpool]") {
  auto a = ThreadPool<false, false, false>(4);
  CHECK(a.num_threads() == 4);

  auto b = ThreadPool<false, false, true>(4);
  CHECK(a.num_threads() == 4);

  auto c = ThreadPool<false, true, false>(4);
  CHECK(c.num_threads() == 4);

  auto d = ThreadPool<false, true, true>(4);
  CHECK(d.num_threads() == 4);

  auto e = ThreadPool<true, false, false>(4);
  CHECK(e.num_threads() == 4);

  auto f = ThreadPool<true, false, true>(4);
  CHECK(f.num_threads() == 4);

  auto g = ThreadPool<true, true, false>(4);
  CHECK(g.num_threads() == 4);

  auto h = ThreadPool<true, true, true>(4);
  CHECK(g.num_threads() == 4);
}
