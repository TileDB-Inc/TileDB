/**
 * @file unit_interval_operations.cc
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
 * This file defines tests for the operations of class Interval: intersection,
 * union, and cut.
 *
 * @section Test Generators
 *
 * Testing interval operations requires two intervals, requiring four points for
 * the bounds. The "choose(4,...)" generator chooses 4 elements from a list.
 * More accurately, it enumerates all the ways of choosing 4 elements, and the
 * number of elements it enumerates is a fourth-degree polynomial in the length
 * of the list. (It's the binomial coefficient N-choose-4.) At 8 elements it
 * yields 70 elements; at 9 it's 136. It's enough to exhaustively test
 * everything without going completely overboard.
 */

#include "unit_interval.h"

/* ************************************** */
/*   Tests for Interval::intersection()   */
/* ************************************** */

template <class T>
void check_intersection(
    Interval<T> left, Interval<T> right, Interval<T> expected) {
  typedef WhiteboxInterval<T> WI;
  WI z = WI(left).intersection(WI(right));
  z.check_equality(expected);
}

TEMPLATE_LIST_TEST_CASE(
    "Interval::intersection", "[interval][intersection]", TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;
  const I empty(I::empty_set);

  SECTION("Four unique endpoints") {
    // Order A-B-C-D
    SECTION("A-B and C-D: Disjoint") {
      /*
       * The bounds of A and D not likely to trigger faults here, so we only
       * test open and closed intervals. This means we have four sections
       * instead of 16.
       */
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << c << "," << d << ")") {
        check_intersection(
            I(I::open, a, b, I::open), I(I::open, c, d, I::open), empty);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << c << "," << d << "]") {
        check_intersection(
            I(I::open, a, b, I::open), I(I::closed, c, d, I::closed), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << c << "," << d << ")") {
        check_intersection(
            I(I::closed, a, b, I::closed), I(I::open, c, d, I::open), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << c << "," << d << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, c, d, I::closed),
            empty);
      }
    }
    SECTION("Inf-B and C-D: Disjoint") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto b = v[0], c = v[1], d = v[2];
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (" << c << "," << d << ")") {
        check_intersection(
            I(I::minus_infinity, b, I::open), I(I::open, c, d, I::open), empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect [" << c << "," << d << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::closed, c, d, I::closed),
            empty);
      }
    }
    SECTION("A-B and C-Inf: Disjoint") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << c << ",+infinity)") {
        check_intersection(
            I(I::open, a, b, I::open), I(I::open, c, I::plus_infinity), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << c << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::open, c, I::plus_infinity),
            empty);
      }
    }
    SECTION("Inf-B and C-Inf: Disjoint") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const auto b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (" << c << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::open, c, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "[-infinity," << b << "] intersect (" << c << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::open, c, I::plus_infinity),
            empty);
      }
    }

    // Order A-C-B-D
    SECTION("A-C and B-D: Overlap") {
      /*
       * The bounds of A and D not likely to trigger faults here, so we only
       * test open and closed intervals. This means we have four sections
       * instead of 16.
       */
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect (" << b << "," << d << ")") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::open, b, d, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect [" << b << "," << d << "]") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::closed, b, d, I::closed),
            I(I::closed, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect (" << b << "," << d << ")") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::open, b, d, I::open),
            I(I::open, b, c, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect [" << b << "," << d << "]") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, d, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("Inf-C and B-D: Overlap") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto b = v[0], c = v[1], d = v[2];
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect (" << b << "," << d << ")") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::open, b, d, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect [" << b << "," << d << "]") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::closed, b, d, I::closed),
            I(I::closed, b, c, I::open));
      }
    }
    SECTION("A-C and B-Inf: Overlap") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, c, I::closed));
      }
    }
    SECTION("Inf-C and B-Inf: Overlap") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << "] intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, c, I::closed),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, c, I::closed));
      }
    }

    // Order A-D-B-C
    SECTION("A-D and B-C: Surround") {
      /*
       * These tests simultaneously exercise A vs. B bounds and C vs. D bounds.
       * Any defects here are unlikely to be correlated, so we don't exercise
       * them independently. This means we have four sections instead of 16.
       */
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << d << ") intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::open, a, d, I::open),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }

      DYNAMIC_SECTION(
          "(" << a << "," << d << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::open, a, d, I::open),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }

      DYNAMIC_SECTION(
          "[" << a << "," << d << "] intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::closed, a, d, I::closed),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }

      DYNAMIC_SECTION(
          "[" << a << "," << d << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, d, I::closed),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("Inf-D and B-C: Surround") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto b = v[0], c = v[1], d = v[2];
      DYNAMIC_SECTION(
          "(-infinity," << d << ") intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::minus_infinity, d, I::open),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }

      DYNAMIC_SECTION(
          "(-infinity," << d << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, d, I::open),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("A-Inf and B-C: Surround") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("Inf-Inf and B-C: Surround") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const auto b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity,+infinity) intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity,+infinity) intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
  }
  SECTION("Three unique endpoints") {
    SECTION("A-A and B-C: Disjoint") {
      /*
       * The open interval (a,a) is empty, so we don't use it here.
       *
       * We test A vs. B on the lower and A vs. C on the upper. Correlated
       * defects are unlikely here, so we don't test them independently. This
       * means we have two cases instead of four.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::closed, a, a, I::closed), I(I::open, b, c, I::open), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, a, I::closed),
            I(I::closed, b, c, I::closed),
            empty);
      }
    }
    // No sections for A=-inf, since (-inf, -inf) is the empty set
    SECTION("A-A and B-Inf: Disjoint") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, a, I::closed),
            I(I::open, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, a, I::closed),
            I(I::closed, b, I::plus_infinity),
            empty);
      }
    }

    SECTION("A-B and A-C: Surround, Degenerate Lower") {
      /*
       * We test A vs. A and  B vs. C in all four cases each. Correlated
       * failure is unlikely, so we don't test them independently. This yields
       * four sections instead of 16.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << a << "," << c << ")") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::open, a, c, I::open),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << a << "," << c << "]") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::closed, a, c, I::closed),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << a << "," << c << ")") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::open, a, c, I::open),
            I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << a << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, a, c, I::closed),
            I(I::closed, a, b, I::closed));
      }
    }
    SECTION("Inf-B and Inf-C: Surround, Degenerate Lower") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (-infinity," << c << ")") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, c, I::open),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (-infinity," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, c, I::closed),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (-infinity," << c << ")") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, c, I::open),
            I(I::minus_infinity, b, I::closed));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (-infinity," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, c, I::closed),
            I(I::minus_infinity, b, I::closed));
      }
    }
    SECTION("A-B and A-Inf: Surround, Degenerate Lower") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << a << ",+infinity)") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::open, a, I::plus_infinity),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << a << ",+infinity)") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::closed, a, I::plus_infinity),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << a << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::open, a, I::plus_infinity),
            I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << a << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, a, I::plus_infinity),
            I(I::closed, a, b, I::closed));
      }
    }
    SECTION("Inf-B and Inf-Inf: Surround, Degenerate Lower") {
      T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (-infinity,+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, I::plus_infinity),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (-infinity,+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, I::plus_infinity),
            I(I::minus_infinity, b, I::closed));
      }
    }

    SECTION("A-B and B-C: Adjacent") {
      /*
       * Bounds at A and C are unlikely to trigger faults, so we don't
       * generate all possible permutations. We test all four B vs. B bounds
       * with A and C always closed to avoid empty operands. This yields four
       * sections instead of 16.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") intersect (" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::open), I(I::open, b, c, I::closed), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::open), I(I::closed, b, c, I::closed), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed), I(I::open, b, c, I::closed), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, b, c, I::closed),
            I(I::single_point, b));
      }
    }
    SECTION("Inf-B and B-C: Adjacent") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::open, b, c, I::closed),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::closed, b, c, I::closed),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::open, b, c, I::closed),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::closed, b, c, I::closed),
            I(I::single_point, b));
      }
    }
    SECTION("A-B and B-Inf: Adjacent") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::open),
            I(I::open, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::open),
            I(I::closed, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::open, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, b, I::plus_infinity),
            I(I::single_point, b));
      }
    }
    SECTION("Inf-B and B-Inf: Adjacent") {
      const T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::open, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::closed, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::open, b, I::plus_infinity),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::closed, b, I::plus_infinity),
            I(I::single_point, b));
      }
    }

    SECTION("A-B and C-C: Disjoint") {
      /*
       * See "Disjoint: A-A and B-C". This section is the same after swapping
       * low and high.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << c << "," << c << "]") {
        check_intersection(
            I(I::open, a, b, I::open), I(I::closed, c, c, I::closed), empty);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << c << "," << c << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, c, c, I::closed),
            empty);
      }
    }
    // No sections for C=inf, since (+inf, +inf) is the empty set
    SECTION("Inf-B and C-C: Disjoint") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect [" << c << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::closed, c, c, I::closed),
            empty);
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect [" << c << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::closed, c, c, I::closed),
            empty);
      }
    }

    SECTION("A-C and B-B: Surround") {
      /*
       * Like the disjoint cases, we only test with [b,b].
       *
       * Correlated faults with lower A vs. B and upper B vs. C bounds are
       * unlikely, so we exercise A-B with only open and closed intervals.
       * This yields two sections instead of four.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
    }
    SECTION("Inf-C and B-B: Surround") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << "] intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::minus_infinity, c, I::closed),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
    }
    SECTION("A-Inf and B-B: Surround") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << ",+infinity) intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::closed, a, I::plus_infinity),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
    }
    SECTION("Inf-Inf and B-B: Surround") {
      T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(-infinity,+infinity) intersect [" << b << "," << b << "]") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::closed, b, b, I::closed),
            I(I::closed, b, b, I::closed));
      }
    }

    SECTION("A-C and B-C: Surround, Degenerate Upper") {
      /*
       * See "Surround, Degenerate Lower: A-B and A-C". This section is the
       * same after swapping low and high.
       */
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const T a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::open, a, c, I::open),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("Inf-C and B-C: Surround, Degenerate Upper") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T b = v[0], c = v[1];
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << ") intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, c, I::open),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << "] intersect (" << b << "," << c << ")") {
        check_intersection(
            I(I::minus_infinity, c, I::closed),
            I(I::open, b, c, I::open),
            I(I::open, b, c, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << c << "] intersect [" << b << "," << c << "]") {
        check_intersection(
            I(I::minus_infinity, c, I::closed),
            I(I::closed, b, c, I::closed),
            I(I::closed, b, c, I::closed));
      }
    }
    SECTION("A-Inf and B-Inf: Surround, Degenerate Upper") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::closed, b, I::plus_infinity),
            I(I::closed, b, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "[" << a << ",+infinity) intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, I::plus_infinity),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "[" << a << ",+infinity) intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::closed, a, I::plus_infinity),
            I(I::closed, b, I::plus_infinity),
            I(I::closed, b, I::plus_infinity));
      }
    }
    SECTION("Inf-Inf and B-Inf: Surround, Degenerate Upper") {
      const T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(-infinity,+infinity) intersect (" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::open, b, I::plus_infinity),
            I(I::open, b, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "(-infinity,+infinity) intersect [" << b << ",+infinity)") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::closed, b, I::plus_infinity),
            I(I::closed, b, I::plus_infinity));
      }
    }
  }

  SECTION("Two unique endpoints") {
    SECTION("A-B and A-B: Identical") {
      /*
       * We test the open interval and the closed interval against each of the
       * others.
       */
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << a << "," << b << ")") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::open, a, b, I::open),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect (" << a << "," << b << "]") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::open, a, b, I::closed),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << a << "," << b << ")") {
        check_intersection(
            I(I::open, a, b, I::closed),
            I(I::closed, a, b, I::open),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") intersect [" << a << "," << b << "]") {
        check_intersection(
            I(I::open, a, b, I::open),
            I(I::closed, a, b, I::closed),
            I(I::open, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << "] intersect [" << a << "," << b << "]") {
        check_intersection(
            I(I::open, a, b, I::closed),
            I(I::closed, a, b, I::closed),
            I(I::open, a, b, I::closed));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") intersect [" << a << "," << b << "]") {
        check_intersection(
            I(I::closed, a, b, I::open),
            I(I::closed, a, b, I::closed),
            I(I::closed, a, b, I::open));
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] intersect [" << a << "," << b << "]") {
        check_intersection(
            I(I::closed, a, b, I::closed),
            I(I::closed, a, b, I::closed),
            I(I::closed, a, b, I::closed));
      }
    }
    SECTION("Inf-B and Inf-B: Identical") {
      const T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (-infinity," << b << ")") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << ") intersect (-infinity," << b << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (-infinity," << b << ")") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, b, I::open),
            I(I::minus_infinity, b, I::open));
      }
      DYNAMIC_SECTION(
          "(-infinity," << b << "] intersect (-infinity," << b << "]") {
        check_intersection(
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, b, I::closed),
            I(I::minus_infinity, b, I::closed));
      }
    }
    SECTION("A-Inf and A-Inf: Identical") {
      const T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect (" << a << ",+infinity)") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::open, a, I::plus_infinity),
            I(I::open, a, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "(" << a << ",+infinity) intersect [" << a << ",+infinity)") {
        check_intersection(
            I(I::open, a, I::plus_infinity),
            I(I::closed, a, I::plus_infinity),
            I(I::open, a, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "[" << a << ",+infinity) intersect (" << a << ",+infinity)") {
        check_intersection(
            I(I::closed, a, I::plus_infinity),
            I(I::open, a, I::plus_infinity),
            I(I::open, a, I::plus_infinity));
      }
      DYNAMIC_SECTION(
          "[" << a << ",+infinity) intersect [" << a << ",+infinity)") {
        check_intersection(
            I(I::closed, a, I::plus_infinity),
            I(I::closed, a, I::plus_infinity),
            I(I::closed, a, I::plus_infinity));
      }
    }
    SECTION("Inf-Inf and Inf-Inf: Identical") {
      DYNAMIC_SECTION("(-infinity,+infinity) intersect (-infinity,+infinity)") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity),
            I(I::minus_infinity, I::plus_infinity),
            I(I::minus_infinity, I::plus_infinity));
      }
    }

    SECTION("A-B and Empty") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION("(" << a << "," << b << ") intersect empty") {
        check_intersection(I(I::open, a, b, I::open), empty, empty);
      }
      DYNAMIC_SECTION("(" << a << "," << b << "] intersect empty") {
        check_intersection(I(I::open, a, b, I::closed), empty, empty);
      }
      DYNAMIC_SECTION("[" << a << "," << b << ") intersect empty") {
        check_intersection(I(I::closed, a, b, I::open), empty, empty);
      }
      DYNAMIC_SECTION("[" << a << "," << b << "] intersect empty") {
        check_intersection(I(I::closed, a, b, I::closed), empty, empty);
      }
    }
    SECTION("Inf-B and Empty") {
      const T b = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION("(-infinity," << b << ") intersect empty") {
        check_intersection(I(I::minus_infinity, b, I::open), empty, empty);
      }
      DYNAMIC_SECTION("(-infinity," << b << "] intersect empty") {
        check_intersection(I(I::minus_infinity, b, I::closed), empty, empty);
      }
    }
    SECTION("A-Inf and Empty") {
      const T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION("(" << a << ",+infinity,) intersect empty") {
        check_intersection(I(I::open, a, I::plus_infinity), empty, empty);
      }
      DYNAMIC_SECTION("[" << a << ",+infinity,) intersect empty") {
        check_intersection(I(I::closed, a, I::plus_infinity), empty, empty);
      }
    }
    SECTION("Inf-Inf and Empty") {
      DYNAMIC_SECTION("(-infinity,+infinity,) intersect empty") {
        check_intersection(
            I(I::minus_infinity, I::plus_infinity), empty, empty);
      }
    }

    SECTION("A-A and B-B: Disjoint") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const T a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] intersect [" << b << "," << b << "]") {
        check_intersection(I(I::single_point, a), I(I::single_point, b), empty);
      }
    }
  }

  SECTION("One unique endpoint") {
    SECTION("A-A and A-A: Identical") {
      const T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION("(" << a << ",+infinity,) intersect empty") {
        I x(I::single_point, a);
        check_intersection(x, x, x);
      }
    }
    SECTION("A-A and Empty") {
      const T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION("(" << a << ",+infinity,) intersect empty") {
        check_intersection(I(I::single_point, a), empty, empty);
      }
    }
  }
}

/* ******************************** */
/*    Tests for Interval::union()   */
/* ******************************** */

/**
 * Check that a union produces the expected result, except if either of the
 * operands is empty, in which case it cheats and overrides the expected result
 * internally.
 */
template <class T>
void check_union(
    Interval<T> left,
    Interval<T> right,
    std::tuple<bool, std::optional<Interval<T>>> expected) {
  typedef WhiteboxInterval<T> WI;
  if (left.is_empty()) {
    expected = {true, right};
  } else if (right.is_empty()) {
    expected = {true, left};
  }
  auto [b, z] = WI(left).interval_union(WI(right));
  bool expected_b = std::get<0>(expected);
  if (!expected_b) {
    CHECK(!b);
  } else if (!b) {
    CHECK(!expected_b);
  } else {
    // Assert: b and expected_b
    Interval<T> expected_z = std::get<1>(expected).value();
    WI(z.value()).check_equality(expected_z);
  }
  /*
   * Verify that comparing the intervals matches the expected union outcome.
   *
   * By construction, if the intervals are disjoint then the left one is
   * less than the right one.
   */
  if (left.is_empty() || right.is_empty()) {
    return;
  }
  auto [c1, b1] = left.compare(right);
  auto [c2, b2] = right.compare(left);
  if (b) {
    // the intervals should be either intersecting or adjacent
    CHECK((c1 == 0 || b1));
    CHECK((c2 == 0 || b2));
  } else {
    // the intervals should be disjoint and not adjacent
    CHECK(c1 < 0);
    CHECK(!b1);
    CHECK(c2 > 0);
    CHECK(!b2);
  }
  if (c1 < 0) {
    CHECK(c2 > 0);
    CHECK(b1 == b2);
  } else if (c1 == 0) {
    CHECK(c2 == 0);
    CHECK(!b1);
    CHECK(!b2);
  } else {
    CHECK(c2 <= 0);  // Will fail; test case is malformed.
  }
}

TEMPLATE_LIST_TEST_CASE(
    "Interval::interval_union",
    "[interval][union][compare/interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;

  // TODO: Tests with infinite bounds

  SECTION("Four unique endpoints") {
    SECTION("Disjoint: A-B and C-D") {
      /*
       * The intervals in this section are all disjoint by construction. If one
       * of the operands is empty, though, the union will be defined.
       */
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") union (" << c << "," << d << ")") {
        check_union(
            I(I::open, a, b, I::open),
            I(I::open, c, d, I::open),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") union [" << c << "," << d << "]") {
        check_union(
            I(I::open, a, b, I::open),
            I(I::closed, c, d, I::closed),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union (" << c << "," << d << ")") {
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::open, c, d, I::open),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union [" << c << "," << d << "]") {
        bool adj = is_adjacent(b, c);
        std::optional<I> expected(
            adj ? std::optional(I(I::closed, a, d, I::closed)) : std::nullopt);
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::closed, c, d, I::closed),
            {adj, expected});
      }
    }
    SECTION("Overlap: A-C and B-D") {
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") union (" << b << "," << d << ")") {
        check_union(
            I(I::open, a, c, I::open),
            I(I::open, b, d, I::open),
            {true, I(I::open, a, d, I::open)});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") union [" << b << "," << d << "]") {
        // If a is adjacent to b, the result will have a closed lower bound
        bool adj = is_adjacent(a, b);
        I expected =
            adj ? I(I::closed, b, d, I::closed) : I(I::open, a, d, I::closed);
        check_union(
            I(I::open, a, c, I::open),
            I(I::closed, b, d, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] union (" << b << "," << d << ")") {
        // If c is adjacent to d, the result will have a closed upper bound
        bool adj = is_adjacent(c, d);
        I expected =
            adj ? I(I::closed, a, c, I::closed) : I(I::closed, a, d, I::open);
        check_union(
            I(I::closed, a, c, I::closed),
            I(I::open, b, d, I::open),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] union [" << b << "," << d << "]") {
        check_union(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, d, I::closed),
            {true, I(I::closed, a, d, I::closed)});
      }
    }
    SECTION("Surround: A-D and B-C") {
      std::vector<T> v = GENERATE(choose(4, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2], d = v[3];
      DYNAMIC_SECTION(
          "(" << a << "," << d << ") union (" << b << "," << c << ")") {
        check_union(
            I(I::open, a, d, I::open),
            I(I::open, b, c, I::open),
            {true, I(I::open, a, d, I::open)});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << d << ") union [" << b << "," << c << "]") {
        // If a is adjacent to b, the result will have a closed lower bound
        // If c is adjacent to d, the result will have a closed upper bound
        bool adj_lower = is_adjacent(a, b);
        bool adj_upper = is_adjacent(c, d);
        I expected = adj_lower ? (adj_upper ? I(I::closed, b, c, I::closed) :
                                              I(I::closed, b, d, I::open)) :
                                 (adj_upper ? I(I::open, a, c, I::closed) :
                                              I(I::open, a, d, I::open));
        check_union(
            I(I::open, a, d, I::open),
            I(I::closed, b, c, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << d << "] union (" << b << "," << c << ")") {
        check_union(
            I(I::closed, a, d, I::closed),
            I(I::open, b, c, I::open),
            {true, I(I::closed, a, d, I::closed)});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << d << "] union [" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, d, I::closed),
            I(I::closed, b, c, I::closed),
            {true, I(I::closed, a, d, I::closed)});
      }
    }
  }
  SECTION("Three unique endpoints") {
    SECTION("Disjoint: A-A and B-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] union (" << b << "," << c << ")") {
        check_union(
            I(I::closed, a, a, I::closed),
            I(I::open, b, c, I::open),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] union [" << b << "," << c << "]") {
        // If `a` is adjacent to `b`, then the union is defined.
        bool adj = is_adjacent(a, b);
        std::optional<I> expected =
            adj ? std::optional(I(I::closed, a, c, I::closed)) : std::nullopt;
        check_union(
            I(I::closed, a, a, I::closed),
            I(I::closed, b, c, I::closed),
            {adj, expected});
      }
    }
    SECTION("Surround, Degenerate Lower: A-B and A-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") union (" << a << "," << c << ")") {
        check_union(
            I(I::open, a, b, I::open),
            I(I::open, a, c, I::open),
            {true, I(I::open, a, c, I::open)});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") union [" << a << "," << c << "]") {
        check_union(
            I(I::open, a, b, I::open),
            I(I::closed, a, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union (" << a << "," << c << ")") {
        // If b and c are adjacent, the result will have a closed upper bound
        bool adj = is_adjacent(b, c);
        I expected =
            adj ? I(I::closed, a, b, I::closed) : I(I::closed, a, c, I::open);
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::open, a, c, I::open),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union [" << a << "," << c << "]") {
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::closed, a, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
    }
    SECTION("Adjacent: A-B and B-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") union (" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, b, I::open),
            I(I::open, b, c, I::closed),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") union [" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, b, I::open),
            I(I::closed, b, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union (" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::open, b, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union [" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::closed, b, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
    }
    SECTION("Disjoint: A-B and C-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") union [" << c << "," << c << "]") {
        check_union(
            I(I::open, a, b, I::open),
            I(I::closed, c, c, I::closed),
            {false, std::nullopt});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] union [" << c << "," << c << "]") {
        // If b and c are adjacent, the union is an interval.
        bool adj = is_adjacent(b, c);
        std::optional<I> expected =
            adj ? std::optional(I(I::closed, a, c, I::closed)) : std::nullopt;
        check_union(
            I(I::closed, a, b, I::closed),
            I(I::closed, c, c, I::closed),
            {adj, expected});
      }
    }
    SECTION("Surround: A-C and B-B") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") union [" << b << "," << b << "]") {
        // If `a` is adjacent to `b`, the result will have a closed lower bound.
        // If `b` is adjacent to `c`, the result will have a closed upper bound.
        bool adj_lower = is_adjacent(a, b);
        bool adj_upper = is_adjacent(b, c);
        I expected = adj_lower ? (adj_upper ? I(I::closed, b, b, I::closed) :
                                              I(I::closed, b, c, I::open)) :
                                 (adj_upper ? I(I::open, a, b, I::closed) :
                                              I(I::open, a, c, I::open));
        check_union(
            I(I::open, a, c, I::open),
            I(I::closed, b, b, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << "] union [" << b << "," << b << "]") {
        // If `a` is adjacent to `b`, the result will have a closed lower bound.
        bool adj = is_adjacent(a, b);
        I expected =
            adj ? I(I::closed, b, c, I::closed) : I(I::open, a, c, I::closed);
        check_union(
            I(I::open, a, c, I::closed),
            I(I::closed, b, b, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << ") union [" << b << "," << b << "]") {
        // If `b` is adjacent to `c`, the result will have a closed upper bound.
        bool adj = is_adjacent(b, c);
        I expected =
            adj ? I(I::closed, a, b, I::closed) : I(I::closed, a, c, I::open);
        check_union(
            I(I::closed, a, c, I::open),
            I(I::closed, b, b, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] union [" << b << "," << b << "]") {
        check_union(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, b, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
    }
    SECTION("Surround, Degenerate Upper: A-C and B-C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") union (" << b << "," << c << ")") {
        check_union(
            I(I::open, a, c, I::open),
            I(I::open, b, c, I::open),
            {true, I(I::open, a, c, I::open)});
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") union [" << b << "," << c << "]") {
        // If `a` is adjacent to `b`, the result will have a closed lower bound.
        bool adj = is_adjacent(a, b);
        I expected =
            adj ? I(I::closed, b, c, I::closed) : I(I::open, a, c, I::closed);
        check_union(
            I(I::open, a, c, I::open),
            I(I::closed, b, c, I::closed),
            {true, expected});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] union (" << b << "," << c << ")") {
        check_union(
            I(I::closed, a, c, I::closed),
            I(I::open, b, c, I::open),
            {true, I(I::closed, a, c, I::closed)});
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] union [" << b << "," << c << "]") {
        check_union(
            I(I::closed, a, c, I::closed),
            I(I::closed, b, c, I::closed),
            {true, I(I::closed, a, c, I::closed)});
      }
    }
  }
}

/* ******************************** */
/*     Tests for Interval::cut()    */
/* ******************************** */

template <class T>
void check_cut(
    Interval<T> x,
    T y,
    Interval<T> expected_z_below,
    Interval<T> expected_z_above,
    bool is_result_lower_open) {
  typedef WhiteboxInterval<T> WI;
  auto [z_below, z_above] = x.cut(y, is_result_lower_open);
  WI(z_below).check_equality(expected_z_below);
  WI(z_above).check_equality(expected_z_above);
  /*
   * Verify that we recover the original interval as the union of the two
   * cut pieces.
   *
   * We don't use check_equality here because a reunion after a cut is not
   * required to have the same representation as the original, merely to
   * represent the same set.
   */
  auto [x_reunited, x_reunion] = z_below.interval_union(z_above);
  REQUIRE(x_reunited);
  CHECK(x == x_reunion.value());
  /*
   * Verify that compare() shows that the two cut pieces are adjacent.
   */
  if (!z_below.is_empty() && !z_above.is_empty()) {
    auto [c, adj] = z_below.compare(z_above);
    CHECK(c < 0);
    CHECK(adj);
  }
}

/*
 * These tests are tagged with `union` and `compare(interval)` because they're
 * also tested in `check_cut`. It checks that adjacent interval reunite and
 * that they're seen as adjacent.
 */
TEMPLATE_LIST_TEST_CASE(
    "Interval::cut",
    "[interval][cut][union][compare/interval]",
    TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;
  I empty(I::empty_set);

  // TODO: Tests with infinite bounds

  SECTION("Three unique points") {
    SECTION("Trivial: cut A-B at C") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << c << ", lower open") {
        I x(I::open, a, b, I::open);
        check_cut(x, c, x, empty, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << c << ", lower closed") {
        I x(I::open, a, b, I::open);
        check_cut(x, c, x, empty, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << c << ", lower open") {
        I x(I::closed, a, b, I::closed);
        check_cut(x, c, x, empty, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << c << ", lower closed") {
        I x(I::closed, a, b, I::closed);
        check_cut(x, c, x, empty, false);
      }
    }
    SECTION("Ordinary: cut A-C at B") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") cut at " << b << ", lower open") {
        I x(I::open, a, c, I::open);
        I z0(I::open, a, b, I::open), z1(I::closed, b, c, I::open);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << ") cut at " << b << ", lower closed") {
        I x(I::open, a, c, I::open);
        I z0(I::open, a, b, I::closed), z1(I::open, b, c, I::open);
        check_cut(x, b, z0, z1, false);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << "] cut at " << b << ", lower open") {
        I x(I::open, a, c, I::closed);
        I z0(I::open, a, b, I::open), z1(I::closed, b, c, I::closed);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << c << "] cut at " << b << ", lower closed") {
        I x(I::open, a, c, I::closed);
        I z0(I::open, a, b, I::closed), z1(I::open, b, c, I::closed);
        check_cut(x, b, z0, z1, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << ") cut at " << b << ", lower open") {
        I x(I::closed, a, c, I::open);
        I z0(I::closed, a, b, I::open), z1(I::closed, b, c, I::open);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << ") cut at " << b << ", lower closed") {
        I x(I::closed, a, c, I::open);
        I z0(I::closed, a, b, I::closed), z1(I::open, b, c, I::open);
        check_cut(x, b, z0, z1, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] cut at " << b << ", lower open") {
        I x(I::closed, a, c, I::closed);
        I z0(I::closed, a, b, I::open), z1(I::closed, b, c, I::closed);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << c << "] cut at " << b << ", lower closed") {
        I x(I::closed, a, c, I::closed);
        I z0(I::closed, a, b, I::closed), z1(I::open, b, c, I::closed);
        check_cut(x, b, z0, z1, false);
      }
    }
    SECTION("Trivial: cut B-C at A") {
      std::vector<T> v = GENERATE(choose(3, Tr::outer));
      const auto a = v[0], b = v[1], c = v[2];
      DYNAMIC_SECTION(
          "(" << b << "," << c << ") cut at " << a << ", lower open") {
        I x(I::open, b, c, I::open);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "(" << b << "," << c << ") cut at " << a << ", lower closed") {
        I x(I::open, b, c, I::open);
        check_cut(x, a, empty, x, false);
      }
      DYNAMIC_SECTION(
          "[" << b << "," << c << "] cut at " << a << ", lower open") {
        I x(I::closed, b, c, I::closed);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "[" << b << "," << c << "] cut at " << a << ", lower closed") {
        I x(I::closed, b, c, I::closed);
        check_cut(x, a, empty, x, false);
      }
    }
  }
  SECTION("Two unique points") {
    SECTION("Lower: cut A-B at A") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const auto a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << a << ", lower open") {
        I x(I::open, a, b, I::open);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << a << ", lower closed") {
        I x(I::open, a, b, I::open);
        check_cut(x, a, empty, x, false);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << "] cut at " << a << ", lower open") {
        I x(I::open, a, b, I::closed);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << "] cut at " << a << ", lower closed") {
        I x(I::open, a, b, I::closed);
        check_cut(x, a, empty, x, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") cut at " << a << ", lower open") {
        I x(I::closed, a, b, I::open);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") cut at " << a << ", lower closed") {
        I x(I::closed, a, b, I::open);
        I z0(I::single_point, a), z1(I::open, a, b, I::open);
        check_cut(x, a, z0, z1, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << a << ", lower open") {
        I x(I::closed, a, b, I::closed);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << a << ", lower closed") {
        I x(I::closed, a, b, I::closed);
        I z0(I::single_point, a), z1(I::open, a, b, I::closed);
        check_cut(x, a, z0, z1, false);
      }
    }
    SECTION("Upper: cut A-B at B") {
      std::vector<T> v = GENERATE(choose(2, Tr::outer));
      const auto a = v[0], b = v[1];
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << b << ", lower open") {
        I x(I::open, a, b, I::open);
        check_cut(x, b, x, empty, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << ") cut at " << b << ", lower closed") {
        I x(I::open, a, b, I::open);
        check_cut(x, b, x, empty, false);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << "] cut at " << b << ", lower open") {
        I x(I::open, a, b, I::closed);
        I z0(I::open, a, b, I::open), z1(I::single_point, b);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "(" << a << "," << b << "] cut at " << b << ", lower closed") {
        I x(I::open, a, b, I::closed);
        check_cut(x, b, x, empty, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") cut at " << b << ", lower open") {
        I x(I::closed, a, b, I::open);
        check_cut(x, b, x, empty, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << ") cut at " << b << ", lower closed") {
        I x(I::closed, a, b, I::open);
        check_cut(x, b, x, empty, false);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << b << ", lower open") {
        I x(I::closed, a, b, I::closed);
        I z0(I::closed, a, b, I::open), z1(I::single_point, b);
        check_cut(x, b, z0, z1, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << b << "] cut at " << b << ", lower closed") {
        I x(I::closed, a, b, I::closed);
        check_cut(x, b, x, empty, false);
      }
    }
  }
  SECTION("One unique point") {
    SECTION("Cut A-A at A") {
      T a = GENERATE(values(Tr::outer));
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] cut at " << a << ", lower open") {
        I x(I::closed, a, a, I::closed);
        check_cut(x, a, empty, x, true);
      }
      DYNAMIC_SECTION(
          "[" << a << "," << a << "] cut at " << a << ", lower closed") {
        I x(I::closed, a, a, I::closed);
        check_cut(x, a, x, empty, false);
      }
    }
  }
}
