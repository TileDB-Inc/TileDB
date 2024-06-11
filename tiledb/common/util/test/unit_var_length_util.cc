/**
 * @file   unit_var_length_util.cc
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
 * This file implements unit tests for the var_length_utils.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <vector>
#include "tiledb/common/util/var_length_util.h"

TEST_CASE("var_length_uti: Null test", "[var_length_util][null_test]") {
  REQUIRE(true);
}

TEST_CASE("var_length_util: Test to from", "[var_length_util]") {
  std::vector<int> lengths{1, 5, 3, 2, 9};
  std::vector<int> tiledb_offsets(size(lengths));
  std::vector<int> arrow_offsets(size(lengths) + 1);

  std::vector<int> expected_lengths{lengths};
  std::vector<int> expected_tiledb_offsets{0, 1, 6, 9, 11};
  std::vector<int> expected_arrow_offsets{0, 1, 6, 9, 11, 20};

  SECTION("to tiledb offsets") {
    lengths_to_offsets(lengths, tiledb_offsets);
    CHECK(tiledb_offsets == expected_tiledb_offsets);
  }
  SECTION("to arrow offsets") {
    lengths_to_offsets(lengths, arrow_offsets);
    CHECK(arrow_offsets == expected_arrow_offsets);
  }
  SECTION("from tiledb offsets") {
    offsets_to_lengths(expected_tiledb_offsets, lengths, 20);
    CHECK(lengths == expected_lengths);
  }
  SECTION("from arrow offsets") {
    offsets_to_lengths(expected_arrow_offsets, lengths);
    CHECK(lengths == expected_lengths);
  }
}
