/**
 * @file unit_interval_bound.cc
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
 * This file defines tests for the Interval::Bounds inner class.
 *
 * The comparison tests for bounds do not include the null bound in the tests.
 * The simplest reason to state is that they're excluded by precondition from
 * the comparison functions, so we can't include them. More broadly, though,
 * empty bounds aren't considered ordered, and the empty set is always treated
 * separately in any of the comparison functions.
 */

#include "unit_interval.h"

TEMPLATE_LIST_TEST_CASE(
    "Interval::Bound::compare_as_lower", "[interval]", TypesUnderTest) {
  typedef TestType T;
  typedef WhiteboxInterval<T> WI;
  typedef TestTypeTraits<T> Tr;

  auto inf = WI::infinite_bound();
  SECTION("Bound(-inf,... < Bound(-inf,...") {
    CHECK(inf.compare_as_lower(inf) == 0);
  }
  T i = GENERATE_REF(values(Tr::outer));
  DYNAMIC_SECTION("Bound(-inf,... < Bound(" << i << ",...") {
    auto x = WI::finite_bound(i, false);
    CHECK(inf.compare_as_lower(x) < 0);
  }
  DYNAMIC_SECTION("Bound(-inf,... < Bound[" << i << ",...") {
    auto x = WI::finite_bound(i, true);
    CHECK(inf.compare_as_lower(x) < 0);
  }
  DYNAMIC_SECTION("Bound(" << i << ",... < Bound(-inf,...") {
    auto x = WI::finite_bound(i, false);
    CHECK(x.compare_as_lower(inf) > 0);
  }
  DYNAMIC_SECTION("Bound[" << i << ",... < Bound(-inf,...") {
    auto x = WI::finite_bound(i, true);
    CHECK(x.compare_as_lower(inf) > 0);
  }

  T j = GENERATE_REF(values(Tr::inner));
  DYNAMIC_SECTION("Bound(" << i << ",... < Bound(" << j << ",...") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_lower(y);
    int expected = i == j ? 0 : i < j ? -1 : +1;
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound(" << i << ",... < Bound[" << j << ",...") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_lower(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      if (i < std::numeric_limits<T>::max()) {
        expected = (i + 1 == j) ? 0 : (i + 1 < j) ? -1 : +1;
      } else {
        /*
         * An open lower bound with max value is equivalent to [max+1,... and
         * will overflow. It's also larger than any closed bound.
         */
        expected = +1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      /*
       * When i = j, the closed bound has one extra point. Thus the closed bound
       * is a superset of the open bound. The closed bound is on the right and
       * thus we expect +1.
       */
      expected = i < j ? -1 : +1;
    } else {
      REQUIRE(false && "Unsupported type");
    }
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound[" << i << ",... < Bound(" << j << ",...") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_lower(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      if (j < std::numeric_limits<T>::max()) {
        expected = (i == j + 1) ? 0 : (i < j + 1) ? -1 : +1;
      } else {
        /*
         * An open lower bound with max value is equivalent to [max+1,... and
         * will overflow. It's also larger than any closed bound.
         */
        expected = -1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      /*
       * Just like the above test except that the closed bound is on the left
       * and thus we expect -1 when i == j.
       */
      expected = i <= j ? -1 : +1;
    } else {
      REQUIRE(false && "Unsupported type");
    }
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound[" << i << ",... < Bound[" << j << ",...") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_lower(y);
    int expected = i == j ? 0 : i < j ? -1 : +1;
    CHECK(c == expected);
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval::Bound::compare_as_upper", "[interval]", TypesUnderTest) {
  typedef TestType T;
  typedef WhiteboxInterval<T> WI;
  typedef TestTypeTraits<T> Tr;

  auto inf = WI::infinite_bound();
  SECTION("Bound(-inf,... < Bound(-inf,...") {
    CHECK(inf.compare_as_upper(inf) == 0);
  }

  T i = GENERATE_REF(values(Tr::outer));
  DYNAMIC_SECTION("Bound...,+inf) < Bound...," << i << ")") {
    auto x = WI::finite_bound(i, false);
    CHECK(inf.compare_as_upper(x) > 0);
  }
  DYNAMIC_SECTION("Bound...,+inf) < Bound...," << i << "]") {
    auto x = WI::finite_bound(i, true);
    CHECK(inf.compare_as_upper(x) > 0);
  }
  DYNAMIC_SECTION("Bound...," << i << ") < Bound...,+inf)") {
    auto x = WI::finite_bound(i, false);
    CHECK(x.compare_as_upper(inf) < 0);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound...,+inf)") {
    auto x = WI::finite_bound(i, true);
    CHECK(x.compare_as_upper(inf) < 0);
  }

  T j = GENERATE_REF(values(Tr::inner));
  DYNAMIC_SECTION("Bound...," << i << ") < Bound...," << j << ")") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_upper(y);
    int expected = i == j ? 0 : i < j ? -1 : +1;
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << ") < Bound...," << j << "]") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_upper(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      if (j < std::numeric_limits<T>::max()) {
        expected = (i == j + 1) ? 0 : (i < j + 1) ? -1 : +1;
      } else {
        /*
         * An open upper bound with max value is a superset of every open bound,
         * so the left side is the subset.
         */
        expected = -1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      expected = i <= j ? -1 : +1;
    } else {
      REQUIRE(false && "Unsupported type");
    }
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound...," << j << ")") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_upper(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      if (i < std::numeric_limits<T>::max()) {
        expected = (i + 1 == j) ? 0 : (i + 1 < j) ? -1 : +1;
      } else {
        /*
         * An open upper bound with max value is a superset of every open bound,
         * so the right side is the subset.
         */
        expected = +1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      expected = i < j ? -1 : +1;
    } else {
      REQUIRE(false && "Unsupported type");
    }
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound...," << j << "]") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_upper(y);
    int expected = i == j ? 0 : i < j ? -1 : +1;
    CHECK(c == expected);
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval::Bound::compare_as_mixed", "[interval]", TypesUnderTest) {
  typedef TestType T;
  typedef WhiteboxInterval<T> WI;
  typedef TestTypeTraits<T> Tr;

  auto inf = WI::infinite_bound();
  SECTION("Bound...,+inf) < Bound(-inf,...") {
    CHECK(inf.compare_as_mixed(inf) > 0);
  }

  T i = GENERATE_REF(values(Tr::outer));
  DYNAMIC_SECTION("Bound...,+inf) < Bound(" << i << ",...") {
    auto x = WI::finite_bound(i, false);
    CHECK(inf.compare_as_mixed(x) > 0);
  }
  DYNAMIC_SECTION("Bound...,+inf) < Bound[" << i << ",...") {
    auto x = WI::finite_bound(i, true);
    CHECK(inf.compare_as_mixed(x) > 0);
  }
  DYNAMIC_SECTION("Bound...," << i << ") < Bound(-inf,...") {
    auto x = WI::finite_bound(i, false);
    CHECK(x.compare_as_mixed(inf) > 0);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound(-inf,...") {
    auto x = WI::finite_bound(i, true);
    CHECK(x.compare_as_mixed(inf) > 0);
  }

  T j = GENERATE_REF(values(Tr::inner));
  DYNAMIC_SECTION("Bound...," << i << ") < Bound(" << j << ",...") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_mixed(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      /*
       * Bounds equivalent to ...,i-1] and [j+1,... Adjacency happens when
       * `(i-1)+1==j+1`, that is `i==j+1`
       */
      if (j == std::numeric_limits<T>::max()) {
        // Can't add 1 to 'j', but every `i` is less than `j+1`
        expected = -1;
      } else {
        expected = (i < j + 1) ? -1 : (i == j + 1) ? 0 : +1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      expected = i <= j ? -1 : +1;
    } else {
      REQUIRE((false && "unsupported type"));
    }
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << ") < Bound[" << j << ",...") {
    auto x = WI::finite_bound(i, false);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_mixed(y);
    int expected = (i < j) ? -1 : (i == j) ? 0 : +1;
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound(" << j << ",...") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, false);
    int c = x.compare_as_mixed(y);
    int expected = (i < j) ? -1 : (i == j) ? 0 : +1;
    CHECK(c == expected);
  }
  DYNAMIC_SECTION("Bound...," << i << "] < Bound[" << j << ",...") {
    auto x = WI::finite_bound(i, true);
    auto y = WI::finite_bound(j, true);
    int c = x.compare_as_mixed(y);
    int expected;
    if constexpr (std::is_integral_v<T>) {
      /*
       * Adjacency happens when `i+1==j`
       */
      if (i == std::numeric_limits<T>::max()) {
        // Can't add 1 to 'i', but every `j` is less than `i+1`
        expected = +1;
      } else {
        expected = (i + 1 < j) ? -1 : (i + 1 == j) ? 0 : +1;
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      expected = (i < j) ? -1 : +1;
    } else {
      REQUIRE((false && "unsupported type"));
    }
    CHECK(c == expected);
  }
}
