/**
 * @file
 * stdx/synchronized_optional/test/unit_synchronized_optional_comparisons.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 */

#include "../synchronized_optional.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

/*
 * Comparison tests
 *
 * There are lots of comparisons, and even more comparison tests. The primary
 * subject of testing is the correct forwarding of the comparisons from
 * `std::optional`. As a result there's no enumeration by type, but there is by
 * value. For clarity, we use `int` as the base type and only three `int`
 * values: -1, 0, and +1.
 */
using soint = stdx::synchronized_optional<int>;

template <class T, class U>
void checkers(
    std::string a,
    std::string b,
    T& x,
    U& y,
    bool less,
    bool equal,
    bool greater) {
  REQUIRE(
      ((less && !equal && !greater) || (equal && !less && !greater) ||
       (greater && !less && !equal)));
  DYNAMIC_SECTION(a << "==" << b) {
    CHECK((x == y) == equal);
  }
  DYNAMIC_SECTION(a << "!=" << b) {
    CHECK((x != y) != equal);
  }
  DYNAMIC_SECTION(a << "<" << b) {
    CHECK((x < y) == less);
  }
  DYNAMIC_SECTION(a << "<=" << b) {
    CHECK((x <= y) != greater);
  }
  DYNAMIC_SECTION(a << ">" << b) {
    CHECK((x > y) == greater);
  }
  DYNAMIC_SECTION(a << ">=" << b) {
    CHECK((x >= y) != less);
  }
}

/**
 * Exercise the operators when passed the same object on left and right sides.
 *
 * The test here includes ensuring that the operator function does not hang from
 * trying to lock the same object twice.
 */
TEST_CASE("synchronized_optional comparison - reflexive") {
  const soint x{std::in_place, 0};
  CHECK(x == x);
  CHECK(x <= x);
  CHECK(x >= x);
  CHECK_FALSE(x != x);
  CHECK_FALSE(x < x);
  CHECK_FALSE(x > x);
}

/*
 * Direct checks for `synchronized_optional` vs. itself. These are duplicated in
 * the generated checks below; they're explicit here to validate the
 * `checkers()` function.
 */
TEST_CASE("synchronized_optional comparison - 0, 0") {
  const soint x{std::in_place, 0};
  const soint y{std::in_place, 0};
  checkers("0", "0", x, y, false, true, false);
}

TEST_CASE("synchronized_optional comparison - 0, -1") {
  const soint x{std::in_place, 0};
  const soint y{std::in_place, -1};
  checkers("0", "-1", x, y, false, false, true);
}

TEST_CASE("synchronized_optional comparison - 0, +1") {
  const soint x{std::in_place, 0};
  const soint y{std::in_place, 1};
  checkers("0", "+1", x, y, true, false, false);
}

/**
 * Comparison vs. explicit `nullopt`. `nullopt` is similar to negative infinity;
 * it's ordered before every other element (even the negative infinity of a
 * floating point type).
 */
TEST_CASE("synchronized_optional comparison - vs. explicit nullopt") {
  const std::string sn{"explicit nullopt"};
  SECTION("non-nullopt") {
    const soint x{std::in_place, 0};
    checkers("0", "explicit nullopt", x, nullopt, false, false, true);
    checkers("explicit nullopt", "0", nullopt, x, true, false, false);
  }
  SECTION("nullopt") {
    const soint y{nullopt};
    checkers("nullopt", "explicit nullopt", y, nullopt, false, true, false);
    checkers("explicit nullopt", "nullopt", nullopt, y, false, true, false);
  }
}

/**
 * Comparison vs. implicit `nullopt` through a `synchronized_optional`
 * containing no value.
 */
TEST_CASE("synchronized_optional comparison - vs. implicit nullopt") {
  const soint n{nullopt};
  const std::string sn{"implicit nullopt"};
  SECTION("synchronized_optional value") {
    const soint x{std::in_place, 0};
    checkers("0", sn, x, n, false, false, true);
    checkers(sn, "0", n, x, true, false, false);
  }
  SECTION("synchronized_optional nullopt") {
    const soint y{nullopt};
    checkers("nullopt", sn, y, n, false, true, false);
    checkers(sn, "nullopt", n, y, false, true, false);
  }
  SECTION("explicit value") {
    const int z{0};
    checkers("0", sn, z, n, false, false, true);
    checkers(sn, "0", n, z, true, false, false);
  }
}

TEST_CASE("synchronized_optional comparison - generated value") {
  const int x{0};
  const std::string sx{"0"};
  const soint n{nullopt};
  const std::string sn{"nullopt"};
  const int y = GENERATE(-1, 0, +1);
  const std::string sy{std::to_string(y)};
  DYNAMIC_SECTION(sx << " vs. " << sy) {
    const bool less{x < y};
    const bool equal{x == y};
    const bool greater{x > y};
    const soint a{std::in_place, x};
    const soint b{std::in_place, y};
    SECTION("synchronized_optional vs. synchronized_optional") {
      checkers(sx, sy, a, b, less, equal, greater);
    }
    SECTION("synchronized_optional value vs. explicit value") {
      checkers(sx, sy, a, y, less, equal, greater);
    }
    SECTION("explict value vs. synchronized_optional value") {
      checkers(sx, sy, x, b, less, equal, greater);
    }
  }
}
