/**
 * @file
 * /Users/robertbindar/TileDB/TileDB/tiledb/common/test/unit_pairwise_sum.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * This file tests the floats pairwise summation function
 */

#include <test/support/tdb_catch.h>
#include "../pairwise_sum.h"

typedef std::tuple<float, double> ElementType;
TEMPLATE_LIST_TEST_CASE(
    "Pairwise summation for floats", "[pairwise_sum][basic]", ElementType) {
  typedef TestType T;
  std::array<T, 5> a{1.1, 2.2, 3.3, 4.4, 5.5};

  CHECK(tiledb::common::pairwise_sum<T, 5>(std::span(a)) == 16.5);
  CHECK(tiledb::common::pairwise_sum<T, 1>(std::span(a)) == 16.5);
}
#include <iostream>

typedef std::tuple<float, double> ElementType;
TEMPLATE_LIST_TEST_CASE(
    "Test precision for floating point summation",
    "[pairwise_sum][epsilon]",
    ElementType) {
  typedef TestType T;

  auto eps = std::numeric_limits<T>::epsilon();
  constexpr size_t n = 10000;
  std::array<T, n> a;
  std::fill(a.begin(), a.end(), static_cast<T>(1.0) / n);
  REQUIRE(tiledb::common::pairwise_sum<T, 128>(std::span(a)) != 1);

  T sum = tiledb::common::pairwise_sum<T, 128>(std::span(a));

  if constexpr (std::is_same_v<T, float>) {
    float err = std::log2f(n) * eps;
    CHECK(fabsf(1 - sum) <= err);
  } else {
    double err = std::log2l(n) * eps;
    CHECK(fabsl(1 - sum) <= err);
  }
}
