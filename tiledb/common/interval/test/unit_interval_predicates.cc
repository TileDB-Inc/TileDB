/**
 * @file unit_interval_predicates.cc
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
 * This file defines tests for the predicates of class Interval: interval vs.
 * interval comparison, set membership, point vs. interval comparison.
 */

#include "unit_interval.h"

//=======================================================
// Equality
//=======================================================
/*
 * The tests for `operator==` are incorporated into `check_equality()`, which is
 * used in all the operation tests.
 */

//=======================================================
// Interval comparison
//=======================================================
/*
 * Most of the tests for interval comparison are integrated into the tests for
 * `interval_union` and `cut`.
 *
 * In `check_union`, the union only succeeds if the intervals intersect or
 * adjacent. Union success is correlated against the return value from
 * `compare`. `compare` is anti-symmetrical, so its return values are also
 * checked against the result when the arguments are swapped.
 *
 * In `check_cut`, the cut intervals are tested for adjacency. These tests cover
 * the generic adjacency test, when an upper half-open (resp. closed) interval
 * is adjacent to a lower half-closed (resp. open) one. The tests below for
 * adjacency are confined to the closed vs. closed case, where additional kinds
 * of adjacency may occur.
 */

TEMPLATE_LIST_TEST_CASE(
    "Interval::compare", "[Interval][compare/interval]", TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;
  SECTION("closed A-B compare-to closed C-D") {
    /*
     * The integer test sequences all have at least one choose-4 subsequence
     * that will yield adjacent closed intervals, for example [0,1] and [2,100].
     */
    std::vector<T> v = GENERATE(choose(4, Tr::outer));
    const auto a = v[0], b = v[1], c = v[2], d = v[3];

    DYNAMIC_SECTION(
        "[" << a << "," << b << "] compare-to [" << c << "," << d << "]") {
      bool expected_adj = is_adjacent(b, c);
      I x(I::closed, a, b, I::closed), y(I::closed, c, d, I::closed);
      auto [cmp, adj] = x.compare(y);
      CHECK(cmp < 0);
      CHECK(adj == expected_adj);
      auto [cmp2, adj2] = y.compare(x);
      CHECK(cmp2 > 0);
      CHECK(adj2 == expected_adj);
    }
  }
}

//=======================================================
// Point comparison
//=======================================================

template <class T>
void check_point_below(T x, Interval<T> y) {
  CHECK(!y.is_member(x));
  if (!y.is_empty()) {
    CHECK(y.compare(x) < 0);
  }
}

template <class T>
void check_point_inside(T x, Interval<T> y) {
  if (!y.is_empty()) {
    CHECK(y.is_member(x));
    CHECK(y.compare(x) == 0);
  } else {
    CHECK(!y.is_member(x));
  }
}

template <class T>
void check_point_above(T x, Interval<T> y) {
  CHECK(!y.is_member(x));
  if (!y.is_empty()) {
    CHECK(y.compare(x) > 0);
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval::is_member",
    "[Interval][is_member][compare/point]",
    TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;

  SECTION("Three unique points") {
    SECTION("A member of B-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(a << " is-member-of (" << b << "," << c << ")") {
        check_point_below(a, I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(a << " is-member-of (" << b << "," << c << "]") {
        check_point_below(a, I(I::open, b, c, I::closed));
      }
      DYNAMIC_SECTION(a << " is-member-of [" << b << "," << c << ")") {
        check_point_below(a, I(I::closed, b, c, I::open));
      }
      DYNAMIC_SECTION(a << " is-member-of [" << b << "," << c << "]") {
        check_point_below(a, I(I::closed, b, c, I::closed));
      }
    }
    SECTION("B member of A-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(b << " is-member-of (" << a << "," << c << ")") {
        check_point_inside(b, I(I::open, a, c, I::open));
      }
      DYNAMIC_SECTION(b << " is-member-of (" << a << "," << c << "]") {
        check_point_inside(b, I(I::open, a, c, I::closed));
      }
      DYNAMIC_SECTION(b << " is-member-of [" << a << "," << c << ")") {
        check_point_inside(b, I(I::closed, a, c, I::open));
      }
      DYNAMIC_SECTION(b << " is-member-of [" << a << "," << c << "]") {
        check_point_inside(b, I(I::closed, a, c, I::closed));
      }
    }
    SECTION("C member of A-B") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(c << " is-member-of (" << a << "," << b << ")") {
        check_point_above(c, I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(c << " is-member-of (" << a << "," << b << "]") {
        check_point_above(c, I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(c << " is-member-of [" << a << "," << b << ")") {
        check_point_above(c, I(I::closed, a, b, I::open));
      }
      DYNAMIC_SECTION(c << " is-member-of [" << a << "," << b << "]") {
        check_point_above(c, I(I::closed, a, b, I::closed));
      }
    }
  }
  SECTION("Two unique points") {
    SECTION("A member of A-B") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(a << " is-member-of (" << a << "," << b << ")") {
        check_point_below(a, I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(a << " is-member-of (" << a << "," << b << "]") {
        check_point_below(a, I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(a << " is-member-of [" << a << "," << b << ")") {
        check_point_inside(a, I(I::closed, a, b, I::open));
      }
      DYNAMIC_SECTION(a << " is-member-of [" << a << "," << b << "]") {
        check_point_inside(a, I(I::closed, a, b, I::closed));
      }
    }
    SECTION("B member of A-B") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(b << " is-member-of (" << a << "," << b << ")") {
        check_point_above(b, I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(b << " is-member-of (" << a << "," << b << "]") {
        check_point_inside(b, I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(b << " is-member-of [" << a << "," << b << ")") {
        check_point_above(b, I(I::closed, a, b, I::open));
      }
      DYNAMIC_SECTION(b << " is-member-of [" << a << "," << b << "]") {
        check_point_inside(b, I(I::closed, a, b, I::closed));
      }
    }
  }
  SECTION("One unique point") {
    SECTION("A member of A-A") {
      T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(a << " is-member-of [" << a << "," << a << "]") {
        check_point_inside(a, I(I::closed, a, a, I::closed));
      }
    }
  }
}
