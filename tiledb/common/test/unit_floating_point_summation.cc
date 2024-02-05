/**
 * @file
 * /Users/robertbindar/TileDB/TileDB/tiledb/common/test/unit_floating_point_summation.cc
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
 * This file tests the floating point summation functors
 */

#include <test/support/tdb_catch.h>
#include <catch2/catch_approx.hpp>
#include "../floating_point_summation.h"

using namespace tiledb::common::floating_point_summation;

// Generate a vector of random floating point numbers
// one=true generates a vector filled with 1/Size so that
// sum over all elements equals 1
template <class T, size_t Size = 1>
std::vector<T> generate_data(bool one) {
  std::vector<T> a(Size);
  std::default_random_engine gen;
  std::uniform_real_distribution<T> dist(-1 * Size, Size);
  if (one) {
    std::fill(a.begin(), a.end(), static_cast<T>(1.0) / Size);
  } else {
    for (size_t i = 0; i < a.size(); ++i) {
      a[i] = dist(gen);
    }
  }
  return a;
}

typedef std::tuple<float, double> ElementType;
TEMPLATE_LIST_TEST_CASE(
    "Pairwise summation for floats", "[pairwise_sum][basic]", ElementType) {
  typedef TestType T;

  auto a = generate_data<T, PairwiseBaseSize - 1>(false);
  CHECK(
      PairwiseSum<T>()(std::span(a)) ==
      Catch::Approx(NaiveSum<T>()(std::span(a))));

  a = generate_data<T, PairwiseBaseSize * 2>(false);
  CHECK(
      PairwiseSum<T>()(std::span(a)) ==
      Catch::Approx(NaiveSum<T>()(std::span(a))));
}

typedef std::tuple<float, double> ElementType;
TEMPLATE_LIST_TEST_CASE(
    "Test precision for floating point summation",
    "[pairwise_sum][epsilon]",
    ElementType) {
  typedef TestType T;

  auto eps = std::numeric_limits<T>::epsilon();
  constexpr size_t n = 10000;
  auto a = generate_data<T, n>(true);

  REQUIRE(PairwiseSum<T>()(std::span(a)) != 1);

  T sum = PairwiseSum<T>()(std::span(a));

  if constexpr (std::is_same_v<T, float>) {
    float err = std::log2f(n) * eps;
    CHECK(fabsf(1 - sum) <= err);
  } else {
    double err = std::log2l(n) * eps;
    CHECK(fabsl(1 - sum) <= err);
  }
}

typedef std::tuple<float, double> ElementType;
TEMPLATE_LIST_TEST_CASE(
    "Floating point summation benchmark",
    "[float_summation][benchmark]",
    ElementType) {
  typedef TestType T;

  constexpr size_t n = 8 * 1024 * 1024;
  auto a = generate_data<T, n>(false);

  BENCHMARK("naive_sum") {
    return NaiveSum<T>()(a);
  };

  BENCHMARK("pairwise_sum") {
    return PairwiseSum<T>()(std::span(a));
  };
}
