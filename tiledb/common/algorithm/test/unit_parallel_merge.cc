/**
 * @file   unit_parallel_merge.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
 */

#include "tiledb/common/algorithm/parallel_merge.h"

#include <test/support/tdb_catch.h>

using namespace tiledb::algorithm;
using namespace tiledb::common;

TEST_CASE("parallel merge example", "[algorithm]") {
  std::vector<std::vector<uint64_t>> streambufs = {
      {123, 456, 789, 890, 901},
      {135, 357, 579, 791, 913},
      {24, 246, 468, 680, 802},
      {100, 200, 300, 400, 500}};

  std::vector<std::span<uint64_t>> streams = {
      std::span(streambufs[0]),
      std::span(streambufs[1]),
      std::span(streambufs[2]),
      std::span(streambufs[3]),
  };

  std::vector<uint64_t> output(20);

  ParallelMergeOptions options = {.parallel_factor = 4, .min_merge_items = 4};

  ThreadPool pool(options.parallel_factor);

  std::unique_ptr<ParallelMergeFuture> future =
      parallel_merge(pool, options, streams, &output[0]);

  future->block();

  CHECK(output == std::vector<uint64_t>{24,  100, 123, 135, 200, 246, 300,
                                        357, 400, 456, 468, 500, 579, 680,
                                        789, 791, 802, 890, 901, 913});
}
