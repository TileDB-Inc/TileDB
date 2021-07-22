/**
 * @file unit_interval_types.cc
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
 * This file defines test functions for the classes and functions that provide
 * type support for class Interval.
 */

#include "unit_interval.h"

TEMPLATE_LIST_TEST_CASE("TypeTraits", "[interval]", TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef detail::TypeTraits<T> Traits;
  typedef TestTypeTraits<T> Tr;

  std::vector<T> v = GENERATE(choose(2, Tr::inner));
  T i = v[0], j = v[1];
  auto [adjacent, twice_adjacent] = Traits::adjacency(i, j);
  if constexpr (std::is_integral_v<T>) {
    CHECK(!(adjacent && twice_adjacent));
    if (i < j) {
      CHECK(iff(adjacent, i + 1 == j));
      CHECK(iff(twice_adjacent, i + 1 == j - 1));
    } else {
      CHECK(!adjacent);
      CHECK(!twice_adjacent);
    }
  } else if constexpr (std::is_floating_point_v<T>) {
    CHECK_FALSE(adjacent);
    CHECK_FALSE(twice_adjacent);
  } else {
    REQUIRE((false && "unknown type"));
  }
}
