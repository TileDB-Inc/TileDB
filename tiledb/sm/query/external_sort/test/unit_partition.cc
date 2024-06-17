/**
 * @file   unit_partition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements unit tests for the alt_var_length_view class.
 */

#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/var_length_util.h"
#include "tiledb/sm/query/external_sort/partition.h"

TEST_CASE("partition: Null test", "[partition][null_test]") {
  REQUIRE(true);
}

TEST_CASE("partition: sized", "[partition]") {
  std::vector<uint64_t> o{8, 6, 7, 5, 3, 0, 9};
  std::vector<uint64_t> p{3, 1, 4, 1, 5, 9, 2};

  REQUIRE(o.size() == p.size());
  size_t num_cells = size(o);
  size_t bin_size = 256;
  auto fixed_bytes_per_cell = 24;

  std::vector<uint64_t> o_bytes{64, 48, 56, 40, 24, 0, 72};
  std::vector<uint64_t> p_bytes{24, 8, 32, 8, 40, 72, 16};
  for (size_t i = 0; i < num_cells; ++i) {
    o_bytes[i] *= 8;
    p_bytes[i] *= 8;
    o[i] *= 8;
    p[i] *= 8;
  }
  std::vector<uint64_t> sum_bytes(num_cells);
  for (size_t i = 0; i < num_cells; ++i) {
    sum_bytes[i] = o_bytes[i] + p_bytes[i] + fixed_bytes_per_cell;
  }
  std::vector<uint64_t> byte_offsets(num_cells + 1);
  lengths_to_offsets(sum_bytes, byte_offsets);
  // {112, 192, /**/ 304, 376, /**/ 464, 560, /**/ 672};
  // {112, 192, /**/ 112, 184, /**/ 88, 184, /**/ 112};

  std::list<std::vector<uint64_t>::iterator> sizes{begin(o), begin(p)};

  auto&& [x, y] =
      bin_partition(bin_size, num_cells, fixed_bytes_per_cell, sizes);
  std::vector<uint64_t> expected_bins{0, 2, 4, 6, 7};
  std::vector<uint64_t> expected_sizes{192, 184, 184, 112};

  CHECK(x == expected_bins);
  CHECK(y == expected_sizes);
}
