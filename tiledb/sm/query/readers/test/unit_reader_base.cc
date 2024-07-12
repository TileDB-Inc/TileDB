/**
 * @file unit_reader_base.cc
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
 * Tests the `ReaderBase` class.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/query/readers/reader_base.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::sm;

TEST_CASE(
    "ReaderBase::compute_chunk_min_max valid inputs",
    "[readerbase][compute_chunk_min_max][valid]") {
  uint64_t num_chunks{0};
  uint64_t num_range_threads{2};
  uint64_t thread_idx{0};
  std::tuple<uint64_t, uint64_t> expected_min_max{0, 0};

  SECTION("Three chunks first thread") {
    num_chunks = 3;
    thread_idx = 0;
    expected_min_max = {0, 2};
  }

  SECTION("Three chunks second thread") {
    num_chunks = 3;
    thread_idx = 1;
    expected_min_max = {2, 3};
  }

  SECTION("0 chunks") {
    num_chunks = 0;
    thread_idx = 1;
    expected_min_max = {0, 0};
  }

  auto res = ReaderBase::compute_chunk_min_max(
      num_chunks, num_range_threads, thread_idx);
  CHECK(res == expected_min_max);
}

TEST_CASE(
    "ReaderBase::compute_chunk_min_max invalid inputs",
    "[readerbase][compute_chunk_min_max][invalid]") {
  SECTION("No range threads") {
    CHECK_THROWS_WITH(
        ReaderBase::compute_chunk_min_max(10, 0, 0),
        "Number of range thread value is 0");
  }

  SECTION("Invalid range thread index") {
    CHECK_THROWS_WITH(
        ReaderBase::compute_chunk_min_max(10, 1, 1),
        "Range thread index is greater than number of range threads");
  }
}
