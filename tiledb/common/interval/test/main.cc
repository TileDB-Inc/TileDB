/**
 * @file main.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines a test `main()` for Interval tests, for independent
 * compilation away from any larger test suite. It also provides tests for
 * support code within `unit_interval.h`.
 */

#include <cmath>
#include "unit_interval.h"

/*
 * The type lists need to be in sorted order for the tests to be valid.
 *
 * When you're not careful about rounding, you can inadvertently create values
 * that look distinct but actually are not.
 */
TEMPLATE_LIST_TEST_CASE(
    "Interval/Test - Sorted, distinct values in test lists",
    "[interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef TestTypeTraits<T> Tr;

  SECTION("outer") {
    std::vector v(Tr::outer);
    REQUIRE(v.size() >= 1);
    size_t n = v.size() - 1;
    for (unsigned int j = 0; j < n; ++j) {
      CHECK(v[j] != v[j + 1]);
      if (v[j] != v[j + 1]) {
        CHECK(v[j] < v[j + 1]);
      }
    }
  }
  SECTION("inner") {
    std::vector v(Tr::inner);
    REQUIRE(v.size() >= 1);
    size_t n = v.size() - 1;
    for (unsigned int j = 0; j < n; ++j) {
      CHECK(v[j] != v[j + 1]);
      if (v[j] != v[j + 1]) {
        CHECK(v[j] < v[j + 1]);
      }
    }
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval/TestTypeTraits - Floating-point not-finite elements",
    "[interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef TestTypeTraits<T> Tr;

  if constexpr (std::is_floating_point_v<T>) {
    // Verify that we've defined non-numeric traits correctly
    CHECK(std::isinf(T(Tr::positive_infinity)));
    CHECK(Tr::positive_infinity > 0);
    CHECK(std::isinf(Tr::negative_infinity));
    CHECK(Tr::negative_infinity < 0);
    CHECK(std::isnan(Tr::NaN));
  }
}
