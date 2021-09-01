/**
 * @file unit_interval_constructors.cc
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
 * This file defines tests for the constructors of class Interval.
 */

#include <initializer_list>
#include "unit_interval.h"

TEMPLATE_LIST_TEST_CASE(
    "Interval::Interval", "[interval][constructor]", TypesUnderTest) {
  typedef TestType T;
  typedef Interval<T> I;
  typedef TestTypeTraits<T> Tr;

  SECTION("Zero arguments") {
    SECTION("Empty set") {
      I x(I::empty_set);
      test_interval_invariants(x);
      CHECK(x.is_empty());
    }
    SECTION("(-infinity,+infinity)") {
      I x(I::minus_infinity, I::plus_infinity);
      test_interval_invariants(x);
      CHECK(!x.is_empty());
      CHECK(!x.has_single_point());
      CHECK(x.is_lower_bound_infinite());
      CHECK(x.is_upper_bound_infinite());
    }
  }
  SECTION("One argument") {
    T i = GENERATE_REF(values(Tr::outer));
    DYNAMIC_SECTION("single point [" << i << "," << i << "]") {
      I x(I::single_point, i);
      test_interval_invariants(x);
      CHECK(x.has_single_point());
    }
    DYNAMIC_SECTION("(-infinity," << i << ")") {
      I x(I::minus_infinity, i, I::open);
      test_interval_invariants(x);
      CHECK(x.is_lower_bound_infinite());
      CHECK(x.is_upper_bound_open());
    }
    DYNAMIC_SECTION("(-infinity," << i << "]") {
      I x(I::minus_infinity, i, I::closed);
      test_interval_invariants(x);
      CHECK(x.is_lower_bound_infinite());
      CHECK(x.is_upper_bound_closed());
    }
    DYNAMIC_SECTION("(" << i << ",+infinity)") {
      I x(I::open, i, I::plus_infinity);
      test_interval_invariants(x);
      CHECK(x.is_lower_bound_open());
      CHECK(x.is_upper_bound_infinite());
    }
    DYNAMIC_SECTION("[" << i << ",+infinity)") {
      I x(I::closed, i, I::plus_infinity);
      CHECK(x.is_lower_bound_closed());
      CHECK(x.is_upper_bound_infinite());
      test_interval_invariants(x);
    }
  }
  if constexpr (std::is_floating_point_v<T>) {
    SECTION("One argument with infinite values") {
      SECTION("single point [+infinite_value,+infinite_value]") {
        I x(I::single_point, Tr::positive_infinity);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
      SECTION("single point [-infinite_value,-infinite_value]") {
        I x(I::single_point, Tr::negative_infinity);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
      SECTION("(-infinity,+infinite_value)") {
        I x(I::minus_infinity, Tr::positive_infinity, I::open);
        test_interval_invariants(x);
        CHECK(x.is_lower_bound_infinite());
        CHECK(x.is_upper_bound_infinite());
      }
      SECTION("(-infinity,-infinite_value)") {
        I x(I::minus_infinity, Tr::negative_infinity, I::open);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
      SECTION("(-infinity,+infinite_value]") {
        I x(I::minus_infinity, Tr::positive_infinity, I::closed);
        test_interval_invariants(x);
        CHECK(x.is_lower_bound_infinite());
        CHECK(x.is_upper_bound_infinite());
      }
      SECTION("(-infinity,-infinite_value]") {
        I x(I::minus_infinity, Tr::negative_infinity, I::closed);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
      SECTION("(-infinite_value,+infinity)") {
        I x(I::open, Tr::negative_infinity, I::plus_infinity);
        test_interval_invariants(x);
        CHECK(x.is_lower_bound_infinite());
        CHECK(x.is_upper_bound_infinite());
      }
      SECTION("(+infinite_value,+infinity)") {
        I x(I::open, Tr::positive_infinity, I::plus_infinity);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
      SECTION("[-infinite_value,+infinity)") {
        I x(I::closed, Tr::negative_infinity, I::plus_infinity);
        test_interval_invariants(x);
        CHECK(x.is_lower_bound_infinite());
        CHECK(x.is_upper_bound_infinite());
      }
      SECTION("[+infinite_value,+infinity)") {
        I x(I::closed, Tr::positive_infinity, I::plus_infinity);
        test_interval_invariants(x);
        CHECK(x.is_empty());
      }
    }
  } //is_floating_point_v()
  SECTION("Two arguments") {
    T i = GENERATE_REF(values(Tr::outer));
    T j = GENERATE_REF(values(Tr::inner));
    DYNAMIC_SECTION("(" << i << "," << j << ")") {
      I x(I::open, i, j, I::open);
      test_interval_invariants(x);
      CHECK(implies(!x.is_empty(), x.is_lower_bound_open()));
      CHECK(implies(!x.is_empty(), x.is_upper_bound_open()));
      if constexpr (std::is_integral_v<T>) {
        // Initial i < j guards against overflow
        CHECK(implies(i < j && i + 1 < j, !x.is_empty()));
        CHECK(implies(i < j && i + 1 == j - 1, x.has_single_point()));
      } else if constexpr (std::is_floating_point_v<T>) {
        CHECK(implies(i < j, !x.is_empty()));
        CHECK(!x.has_single_point());
      } else if constexpr (std::is_base_of<std::string, T>::value) {
        detail::TypeTraits<std::string> tts;
        auto [adjacenct, twice_adjacent] = tts.adjacency(i, j);
        // TBD: simple vers of... CHECK(implies(i < j && i + 1 < j, !x.is_empty()));
        CHECK(implies(twice_adjacent, x.has_single_point()));
      } else if constexpr (std::is_base_of<std::string_view, T>::value) {
        detail::TypeTraits<std::string_view> ttsv;
        auto [adjacenct, twice_adjacent] = ttsv.adjacency(i, j);
        CHECK(implies(twice_adjacent, x.has_single_point()));
      } else {
        REQUIRE((false && "unexpected type"));
      }
    }
    DYNAMIC_SECTION("(" << i << "," << j << "]") {
      I x(I::open, i, j, I::closed);
      test_interval_invariants(x);
      CHECK(implies(!x.is_empty(), x.is_lower_bound_open()));
      CHECK(implies(!x.is_empty(), x.is_upper_bound_closed()));
      CHECK(implies(i < j, !x.is_empty()));
      if constexpr (std::is_integral_v<T>) {
        CHECK(implies(i < j && i == j - 1, x.has_single_point()));
      } else if constexpr (std::is_floating_point_v<T>) {
        CHECK(!x.has_single_point());
      } else if constexpr (std::is_base_of<std::string, T>::value) {
        detail::TypeTraits<std::string> tts;
        auto [adjacent, twice_adjacent] = tts.adjacency(i, j);
        CHECK(implies(adjacent, x.has_single_point()));
      } else if constexpr (std::is_base_of<std::string_view, T>::value) {
        detail::TypeTraits<std::string_view> ttsv;
        auto [adjacent, twice_adjacent] = ttsv.adjacency(i, j);
        CHECK(implies(adjacent, x.has_single_point()));
      } else {
        REQUIRE((false && "unexpected type"));
      }

    }
    DYNAMIC_SECTION("[" << i << "," << j << ")") {
      I x(I::closed, i, j, I::open);
      test_interval_invariants(x);
      CHECK(implies(!x.is_empty(), x.is_lower_bound_closed()));
      CHECK(implies(!x.is_empty(), x.is_upper_bound_open()));
      CHECK(implies(i < j, !x.is_empty()));
      if constexpr (std::is_integral_v<T>) {
        CHECK(implies(i < j && i == j - 1, x.has_single_point()));
      } else if constexpr (std::is_floating_point_v<T>) {
        CHECK(!x.has_single_point());
      } else if constexpr (std::is_base_of<std::string, T>::value) {
        detail::TypeTraits<std::string> tts;
        auto [adjacent, twice_adjacent] = tts.adjacency(i, j);
        CHECK(implies(adjacent, x.has_single_point()));
      } else if constexpr (std::is_base_of<std::string_view, T>::value) {
        detail::TypeTraits<std::string_view> ttsv;
        auto [adjacent, twice_adjacent] = ttsv.adjacency(i, j);
        CHECK(implies(adjacent, x.has_single_point()));
      } else {
        REQUIRE((false && "unexpected type"));
      }
    }
    DYNAMIC_SECTION("[" << i << "," << j << "]") {
      I x(I::closed, i, j, I::closed);
      test_interval_invariants(x);
      CHECK(implies(!x.is_empty(), x.is_lower_bound_closed()));
      CHECK(implies(!x.is_empty(), x.is_upper_bound_closed()));
      CHECK(implies(i <= j, !x.is_empty()));
      CHECK(implies(i == j, x.has_single_point()));
    }
  }
}

/*
 * However odd these tests might look, they're here because IEEE754 defines a
 * totalOrder function that puts -0.0 before +0.0, which makes totalOrder
 * something other than a mathematical total ordering. These tests detect
 * whether a floating point environment is following that standard with an
 * abundance of zeal.
 */
TEST_CASE("Interval::Interval - Floating-point Zero", "[interval]") {
  CHECK(std::signbit(-0.0f));
  CHECK(std::signbit(+0.0f / -1.0f));
  CHECK(std::signbit(-0.0f / 1.0f));
  CHECK(!std::signbit(+0.0f));
  CHECK(!std::signbit(+0.0f / +1.0f));
  CHECK(!std::signbit(-0.0f / -1.0f));
  CHECK(-0.0f == +0.0f);
  CHECK(!(-0.0f < +0.0f));
  CHECK(!(+0.0f < -0.0f));
  typedef float T;
  typedef Interval<T> I;
  {
    INFO("(-0.0,+0.0)");
    I x(I::open, -0.0, +0.0, I::open);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("(-0.0,+0.0]");
    I x(I::open, -0.0, +0.0, I::closed);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("[-0.0,+0.0)");
    I x(I::closed, -0.0, +0.0, I::open);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("[-0.0,+0.0]");
    I x(I::closed, -0.0, +0.0, I::closed);
    test_interval_invariants(x);
    CHECK(!x.is_empty());
    CHECK(x.has_single_point());
  }
  {
    INFO("(+0.0,-0.0)");
    I x(I::open, +0.0, -0.0, I::open);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("(+0.0,-0.0]");
    I x(I::open, +0.0, -0.0, I::closed);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("[+0.0,-0.0)");
    I x(I::closed, +0.0, -0.0, I::open);
    test_interval_invariants(x);
    CHECK(x.is_empty());
    CHECK(!x.has_single_point());
  }
  {
    INFO("[+0.0,-0.0]");
    I x(I::closed, +0.0, -0.0, I::closed);
    test_interval_invariants(x);
    CHECK(!x.is_empty());
    CHECK(x.has_single_point());
  }
}
