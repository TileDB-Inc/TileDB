/**
 * @file   unit_interval.h
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
 * This file defines common test functions for class Interval and its supporting
 * classes.
 */

#ifndef TILEDB_UNIT_INTERVAL_H
#define TILEDB_UNIT_INTERVAL_H

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <cmath>
#include <optional>

#include "../interval.h"
using namespace tiledb::common;

inline bool iff(bool x, bool y) {
  return (x && y) || (!x && !y);
}

inline bool implies(bool x, bool y) {
  return !x || y;
}

/* ******************************** */
/*       class WhiteboxInterval     */
/* ******************************** */

// forward declaration
template <class T>
void test_interval_invariants(const Interval<T>&);

namespace tiledb::common {

template <class T>
class WhiteboxInterval : public Interval<T> {
  typedef Interval<T> Base;

 public:
  typedef typename Base::Bound Bound;

  explicit WhiteboxInterval(Interval<T> x)
      : Base(x) {
    /*
     * We could check the class invariants here, but we'll leave that to the
     * constructor tests.
     */
  }

  explicit operator Interval<T>() {
    return *this;
  }

  void check_equality(Interval<T> expected) {
    if (expected.is_empty()) {
      CHECK(Base::is_empty_);
    } else if (Base::is_empty_) {
      CHECK(expected.Base::is_empty_);  // will fail
    } else {
      // Both z and z1 are not empty
      CHECK(*this == expected);
      CHECK(iff(Base::has_single_point_, expected.Base::has_single_point_));
      // Lower
      CHECK(iff(Base::is_lower_infinite_, expected.Base::is_lower_infinite_));
      CHECK(iff(Base::is_lower_open_, expected.Base::is_lower_open_));
      CHECK(iff(Base::is_lower_closed_, expected.Base::is_lower_closed_));
      if (!Base::is_lower_infinite_ && !expected.Base::is_lower_infinite_) {
        CHECK(
            Base::lower_bound_.value() == expected.Base::lower_bound_.value());
      }
      // Upper
      CHECK(iff(Base::is_upper_infinite_, expected.Base::is_upper_infinite_));
      CHECK(iff(Base::is_upper_open_, expected.Base::is_upper_open_));
      CHECK(iff(Base::is_upper_closed_, expected.Base::is_upper_closed_));
      if (!Base::is_upper_infinite_ && !expected.Base::is_upper_infinite_) {
        CHECK(
            Base::upper_bound_.value() == expected.Base::upper_bound_.value());
      }
    }
  }

  WhiteboxInterval<T> intersection(WhiteboxInterval<T> y) {
    auto z = WhiteboxInterval<T>(Base::intersection(y));
    auto z1 = y.Base::intersection(*this);
    z.check_equality(z1);
    return z;
  }

  tuple<bool, optional<WhiteboxInterval<T>>> interval_union(
      WhiteboxInterval<T> y) {
    auto [b, z] = Base::interval_union(y);
    auto [b1, z1] = y.Base::interval_union(*this);
    CHECK(iff(b, b1));
    if (b && b1) {
      WhiteboxInterval<T> zv(z.value());
      zv.check_equality(z1.value());
      return {true, zv};
    } else {
      return {false, nullopt};
    }
  }

  static Bound finite_bound(T x, bool is_closed) {
    return Bound(x, is_closed);
  }
  static Bound infinite_bound() {
    return Base::BoundInfinity();
  }
  static Bound emptyset_bound() {
    return Base::BoundNull;
  }
};

}  // namespace tiledb::common

/* ******************************** */
/*          Types under rest        */
/* ******************************** */

/**
 * List of types used to instantiate generic test cases
 */
typedef tuple<uint16_t, uint64_t, int16_t, double> TypesUnderTest;

/**
 * Test type traits allow generic instantiation by Catch from a list of types.
 */
template <class T, class Enable = T>
class TestTypeTraits;

/**
 * Test traits for unsigned integral values
 */
template <class T>
class TestTypeTraits<
    T,
    typename std::enable_if<std::is_integral_v<T> && std::is_unsigned_v<T>, T>::
        type> {
  static constexpr auto max = std::numeric_limits<T>::max();

 public:
  static constexpr std::initializer_list<T> outer = {
      0, 1, 2, 100, max - 1, max};
  static constexpr std::initializer_list<T> inner = {
      0, 1, 2, 3, 99, 100, 101, max - 1, max};
};

/**
 * Test traits for signed integral values
 */
template <class T>
class TestTypeTraits<
    T,
    typename std::enable_if<std::is_integral_v<T> && std::is_signed_v<T>, T>::
        type> {
  static constexpr auto min = std::numeric_limits<T>::min();
  static constexpr auto max = std::numeric_limits<T>::max();

 public:
  static constexpr std::initializer_list<T> outer = {
      min, min + 1, -100, 0, 1, 2, 100, max - 1, max};
  static constexpr std::initializer_list<T> inner = {
      min, min + 1, -101, -100, -99, -2, -1, 0, 1, 2, 3, max - 1, max};
};

/**
 * Test traits for unsigned integral values
 */
template <class T>
class TestTypeTraits<
    T,
    typename std::enable_if<std::is_floating_point_v<T>, T>::type> {
  /*
   * We can't just add or subract 1.0 to extreme limits because it's small
   * enough to disappear after rounding. Instead, `almost_one` is the largest
   * mantissa that's less than one.
   *
   * We might have used std::nextafter(), but it's not declared constexpr and
   * thus can't be evaluated within a static constexpr initializer.
   */
  static constexpr T almost_one = 1.0 - std::numeric_limits<T>::epsilon();
  static constexpr T min = std::numeric_limits<T>::lowest();
  static constexpr T almost_min = min * almost_one;
  static constexpr T max = std::numeric_limits<T>::max();
  static constexpr T almost_max = max * almost_one;

 public:
  static constexpr std::initializer_list<T> outer = {
      min, almost_min, -100.0, 0.0, 1.0, 2.0, 100.0, almost_max, max};
  static constexpr std::initializer_list<T> inner = {
      min,
      almost_min,
      -100.01,
      -100.0,
      -99.99,
      -2.0,
      -1.0,
      0,
      0.9,
      1.0,
      1.1,
      almost_max,
      max};

  static constexpr T positive_infinity = std::numeric_limits<T>::infinity();
  static constexpr T negative_infinity = -std::numeric_limits<T>::infinity();
  static constexpr T NaN = std::numeric_limits<T>::quiet_NaN();
};

/* ******************************** */
/*             is_adjacent          */
/* ******************************** */

/**
 * Independent access to adjacency testing
 */

template <class T>
bool is_adjacent([[maybe_unused]] T x, [[maybe_unused]] T y) {
  if constexpr (std::is_integral_v<T>) {
    return (x < std::numeric_limits<T>::max()) && x + 1 == y;
  } else if constexpr (std::is_floating_point_v<T>) {
    return false;
  } else {
    REQUIRE((false && "unsupported type"));
  }
}

/* ******************************** */
/*   Choose generator for Catch     */
/* ******************************** */

/**
 * A generator for Catch. Given a list L and a number K, it generates every way
 * of choosing K elements from L. Each generated subset of L is in the same
 * order as the elements appear in L.
 *
 * @tparam T The underlying type that we're choosing from.
 */
template <class T>
class ChooseGenerator : public Catch::Generators::IGenerator<std::vector<T>> {
  unsigned int k_;
  const std::vector<T> list_;
  bool finished;
  /// The length of the list to draw from.
  size_t n_;
  /**
   * Return value for the generator. Catch needs a generator to return
   * references, so we need something to reference.
   */
  mutable std::vector<T> rv;

  /**
   * A list of iterators of length `k_ + 1`. The last entry in the list always
   * contains `list_.cend()`. The iteration preserves list order because the
   * iterators always remain in sorted order.
   *
   * @invariant i < j \implies it[i] is prior to it[j]
   */
  std::vector<typename decltype(list_)::const_iterator> it;

 public:
  typedef std::vector<T> value_type;

  ChooseGenerator(unsigned int k, std::initializer_list<T> list)
      : k_(k)
      , list_(list)
      , finished(false)
      , n_(list.size())
      , rv(k_)
      , it(k_ + 1) {
    if (n_ < k) {
      // We can't choose more elements than we have.
      throw std::invalid_argument(
          "ChooseGenerator - "
          "asking for more choices than there are possibilities");
    }
    // Initialize the iterators
    it[0] = list_.cbegin();
    for (unsigned int j = 1; j < k_; ++j) {
      it[j] = it[j - 1];
      ++it[j];
    }
    it[k_] = list_.cend();
  }

  bool next() override {
    unsigned int j;
    /*
     * Try incrementing from the back end until we can increment without bumping
     * up against the next one in line.
     */
    for (j = k_; j > 0; --j) {
      auto x = it[j - 1];
      ++x;
      if (x != it[j]) {
        it[j - 1] = x;
        break;
      }
    }
    if (j == 0) {
      // We can't move forward any more
      return false;
    }
    /*
     * Reinitialize the starting sequence starting with the one after the
     * successful increment.
     */
    for (; j < k_; ++j) {
      it[j] = it[j - 1];
      ++it[j];
    }
    return true;
  }

  value_type const& get() const override {
    if constexpr (true) {
      for (unsigned int j = 0; j < k_; ++j) {
        rv[j] = T();
      }
    }
    for (unsigned int j = 0; j < k_; ++j) {
      rv[j] = *it[j];
    }
    return rv;
  }
};

/**
 * Factory for `ChooseGenerator` as the Catch framework requires to consume it.
 * @tparam T The underlying type that we're choosing from.
 * @param k The number of element of `list` to choose from.
 * @param list The list from which elements are taken.
 * @return An instance of `ChooseGenerator` wrapped as Catch requires.
 */
template <class T>
Catch::Generators::GeneratorWrapper<typename ChooseGenerator<T>::value_type>
choose(unsigned int k, std::initializer_list<T> list) {
  return Catch::Generators::GeneratorWrapper<
      typename ChooseGenerator<T>::value_type>(
      Catch::Detail::make_unique<ChooseGenerator<T>>(k, list));
}

/* ************************************ */
/*   Verification of class invariants   */
/* ************************************ */

/**
 * Checks that all the class invariants of an Interval object are satisfied.
 *
 * Note that the checks here are made in the same order that they're documented
 * in the code. This is for ease of auditing that all the invariants are checked
 * in this function.
 *
 * @tparam T Type for Interval<T>
 * @param x The object under test
 */
template <class T>
void test_interval_invariants(const Interval<T>& x) {
  typedef detail::TypeTraits<T> Traits;

  CHECK(
      iff(x.lower_bound_has_value(),
          x.is_lower_bound_open() || x.is_lower_bound_closed()));

  CHECK(
      iff(x.upper_bound_has_value(),
          x.is_upper_bound_open() || x.is_upper_bound_closed()));

  if (x.is_empty()) {
    CHECK(!x.has_single_point());
    CHECK(!x.is_lower_bound_open());
    CHECK(!x.is_lower_bound_closed());
    CHECK(!x.is_lower_bound_infinite());
    CHECK(!x.is_upper_bound_open());
    CHECK(!x.is_upper_bound_closed());
    CHECK(!x.is_upper_bound_infinite());
    CHECK(!x.lower_bound_has_value());
    CHECK(!x.upper_bound_has_value());
  }

  if (x.has_single_point()) {
    CHECK(!x.is_empty());
    CHECK(!x.is_lower_bound_infinite());
    REQUIRE(x.lower_bound_has_value());
    CHECK(!x.is_upper_bound_infinite());
    REQUIRE(x.upper_bound_has_value());
    T a = x.lower_bound(), b = x.upper_bound();
    if (x.is_lower_bound_closed() && x.is_upper_bound_closed()) {
      REQUIRE(a == b);
    } else if (
        (x.is_lower_bound_open() && x.is_upper_bound_closed()) ||
        (x.is_lower_bound_closed() && x.is_upper_bound_open())) {
      if constexpr (std::is_integral_v<T>) {
        REQUIRE(a < b);
        CHECK(a + 1 == b);
        CHECK(Traits::adjacent(a, b));
      } else if constexpr (std::is_floating_point_v<T>) {
        REQUIRE(
            (false && "single-point intervals for floating point are closed"));
      } else {
        REQUIRE((false && "unknown type"));
      }
    } else if (x.is_lower_bound_open() && x.is_upper_bound_open()) {
      if constexpr (std::is_integral_v<T>) {
        auto [adj1, adj2] = Traits::adjacency(a, b);
        REQUIRE(a < b);
        CHECK(a + 1 == b - 1);
        CHECK(adj2);
        CHECK_FALSE(adj1);
      } else if constexpr (std::is_floating_point_v<T>) {
        REQUIRE(
            (false && "single-point intervals for floating point are closed"));
      } else {
        REQUIRE((false && "unknown type"));
      }
    } else {
      REQUIRE(false);  // logic error
    }
  }

  if (x.is_lower_bound_open()) {
    CHECK(!x.is_lower_bound_closed());
    CHECK(!x.is_lower_bound_infinite());
    CHECK(x.lower_bound_has_value());
  }

  if (x.is_lower_bound_closed()) {
    CHECK(!x.is_lower_bound_open());
    CHECK(!x.is_lower_bound_infinite());
    CHECK(x.lower_bound_has_value());
  }

  if (x.is_lower_bound_infinite()) {
    CHECK(!x.is_lower_bound_open());
    CHECK(!x.is_lower_bound_closed());
    CHECK(!x.lower_bound_has_value());
  }

  if (x.is_upper_bound_open()) {
    CHECK(!x.is_upper_bound_closed());
    CHECK(!x.is_upper_bound_infinite());
    CHECK(x.upper_bound_has_value());
  }

  if (x.is_upper_bound_closed()) {
    CHECK(!x.is_upper_bound_open());
    CHECK(!x.is_upper_bound_infinite());
    CHECK(x.upper_bound_has_value());
  }

  if (x.is_upper_bound_infinite()) {
    CHECK(!x.is_upper_bound_open());
    CHECK(!x.is_upper_bound_closed());
    CHECK(!x.upper_bound_has_value());
  }

  if (x.lower_bound_has_value() && x.upper_bound_has_value()) {
    T a = x.lower_bound(), b = x.upper_bound();
    CHECK(a <= b);
  }
}

#endif  // TILEDB_UNIT_INTERVAL_H
