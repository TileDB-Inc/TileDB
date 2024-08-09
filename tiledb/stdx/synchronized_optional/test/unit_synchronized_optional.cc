/**
 * @file tiledb/stdx/synchronized_optional/test/unit_synchronized_optional.cc
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

using stdx::synchronized_optional;
using soint = synchronized_optional<int>;

TEST_CASE("synchronized_optional - default construct") {
  soint x;
  REQUIRE(!x.has_value());
  CHECK(!x);
}

TEST_CASE("synchronized_optional - construct null") {
  soint x{nullopt};
  REQUIRE(!x.has_value());
}

TEST_CASE("synchronized_optional - construct null then reset") {
  soint x{nullopt};
  CHECK(!x);
  x.reset();
  CHECK(!x);
}

TEST_CASE("synchronized_optional - construct in place") {
  soint x{std::in_place, 5};
  REQUIRE(x.has_value());
  CHECK(x);
  CHECK(x == 5);
}

TEST_CASE("synchronized_optional - construct in place then reset") {
  soint x{std::in_place, 5};
  REQUIRE(x.has_value());
  x.reset();
  REQUIRE(!x.has_value());
}

TEST_CASE("synchronized_optional - copy construct") {
  soint x{std::in_place, 6};
  soint y{soint{x}};  // explicit copy-constructor, direct initialization
  REQUIRE(y);
  CHECK(y == 6);
}

TEST_CASE("synchronized_optional - copy construct to const") {
  soint x{std::in_place, 6};
  const soint y{soint{x}};
  REQUIRE(y);
  CHECK(y == 6);
}

TEST_CASE("synchronized_optional - copy construct from const") {
  const soint x{std::in_place, 6};
  soint y{soint{x}};
  REQUIRE(y);
  CHECK(y == 6);
}

TEST_CASE("synchronized_optional - copy construct to/from const") {
  const soint x{std::in_place, 6};
  const soint y{soint{x}};
  REQUIRE(y);
  CHECK(y == 6);
}

TEST_CASE("synchronized_optional - copy construct from empty") {
  soint x{nullopt};
  REQUIRE(!x);
  soint y{soint{x}};
  REQUIRE(!y);
}

TEST_CASE("synchronized_optional - copy construct from empty to const") {
  soint x{nullopt};
  REQUIRE(!x);
  const soint y{soint{x}};
  REQUIRE(!y);
}

TEST_CASE("synchronized_optional - copy construct from const empty") {
  const soint x{nullopt};
  REQUIRE(!x);
  soint y{soint{x}};
  REQUIRE(!y);
}

TEST_CASE("synchronized_optional - copy construct from const empty to const") {
  const soint x{nullopt};
  REQUIRE(!x);
  const soint y{soint{x}};
  REQUIRE(!y);
}

TEST_CASE("synchronized_optional - move construct") {
  auto y{soint{std::in_place, 7}};
  REQUIRE(y);
  CHECK(y == 7);
}

TEST_CASE("synchronized_optional - move construct to const") {
  const auto y{soint{std::in_place, 7}};
  REQUIRE(y);
  CHECK(y == 7);
}

TEST_CASE("synchronized_optional - move construct from empty") {
  auto y{soint{}};
  REQUIRE(!y.has_value());
}

TEST_CASE("synchronized_optional - move construct from empty to const") {
  const auto y{soint{}};
  REQUIRE(!y.has_value());
}

TEST_CASE("synchronized_optional - null assignment") {
  soint x{std::in_place, 7};
  REQUIRE(x.has_value());
  x = nullopt;
  REQUIRE(!x.has_value());
}

TEST_CASE("synchronized_optional - copy assignment") {
  soint x{std::in_place, 7};
  soint y{std::in_place, 8};
  REQUIRE(x.has_value());
  REQUIRE(x == 7);
  REQUIRE(y == 8);
  x = y;
  REQUIRE(x);
  REQUIRE(y);
  CHECK(x == 8);
  CHECK(y == 8);
}

TEST_CASE("synchronized_optional - move assignment") {
  soint x{std::in_place, 7};
  REQUIRE(x.has_value());
  REQUIRE(x == 7);
  // explicit call with temporary argument must resolve to T&& argument
  (void)x.operator=(soint{std::in_place, 8});
  REQUIRE(x);
  CHECK(x == 8);
  // implicit call
  x = soint{std::in_place, 7};
  REQUIRE(x);
  CHECK(x == 7);
}

TEST_CASE("synchronized_optional - emplace") {
  soint x;
  x.emplace(7);
  REQUIRE(x.has_value());
  CHECK(x == 7);
}

TEST_CASE("synchronized_optional - emplace then reset") {
  soint x;
  x.emplace(7);
  REQUIRE(x.has_value());
  x.reset();
  REQUIRE(!x.has_value());
}

TEST_CASE("synchronized_optional - operator*") {
  soint x{std::in_place, 8};
  REQUIRE(x);
  SECTION("to write handle") {
    soint::reference_type y{*x};
    CHECK(y == 8);
  }
  SECTION("to read handle") {
    // This syntax ensures that a read reference is returned
    const soint::const_reference_type y{*const_cast<const soint&>(x)};
    CHECK(y == 8);
  }
  SECTION("through write handle to underlying") {
    const int y{*x};
    CHECK(y == 8);
  }
  SECTION("through read handle to underlying") {
    const int y{*const_cast<const soint&>(x)};
    CHECK(y == 8);
  }
}

TEST_CASE("synchronized_optional - operator* on const") {
  const soint x{std::in_place, 8};
  REQUIRE(x);
  SECTION("to read handle") {
    const soint::const_reference_type y{*x};
    CHECK(y == 8);
  }
  SECTION("through read handle to underlying") {
    int y{*x};
    CHECK(y == 8);
  }
}

class intplus {
  int x_;

 public:
  intplus() = delete;
  explicit intplus(int x)
      : x_(x) {
  }
  int plus(int y) {
    x_ += y;
    return x_;
  }
  // Non-standard name to ensure resolution is to this function
  [[nodiscard]] int intvalue() const {
    return x_;
  }
  // NOLINTNEXTLINE(google-explicit-constructor)
  operator int() const {
    return x_;
  }
};
using sointplus = synchronized_optional<intplus>;

TEST_CASE("synchronized_optional - simultaneous retrieval") {
  const soint x{std::in_place, 9};
  REQUIRE(x.has_value());
  const soint::const_reference_type y{*x};
  const soint::const_reference_type z{*x};
  CHECK(y == 9);
  CHECK(z == 9);
}

TEST_CASE("synchronized_optional - value") {
  sointplus x{std::in_place, 10};
  REQUIRE(x);
  SECTION("to write handle") {
    const sointplus::reference_type y{x.value()};
    CHECK(*y == 10);
  }
  SECTION("to read handle") {
    // This syntax ensures that a read reference is returned
    const sointplus::const_reference_type y{
        const_cast<const sointplus&>(x).value()};
    CHECK(*y == 10);
  }
  SECTION("through write handle to underlying") {
    const int y{*x.value()};
    CHECK(y == 10);
  }
  SECTION("through read handle to underlying") {
    const int y{*const_cast<const sointplus&>(x).value()};
    CHECK(y == 10);
  }
}

TEST_CASE("synchronized_optional - value on const") {
  const soint x{std::in_place, 11};
  REQUIRE(x);
  SECTION("to read handle") {
    const soint::const_reference_type y{*x};
    CHECK(y == 11);
  }
  SECTION("through read handle to underlying") {
    int y{*x};
    CHECK(y == 11);
  }
}

TEST_CASE("synchronized_optional - const_value") {
  soint x{std::in_place, 12};
  REQUIRE(x);
  SECTION("to read handle") {
    const soint::const_reference_type y{x.const_value()};
    CHECK(y == 12);
  }
  SECTION("through read handle to underlying") {
    int y{x.const_value()};
    CHECK(y == 12);
  }
}

soint soint_rvalue(int k) {
  return soint{std::in_place, k};
}

TEST_CASE("synchronized_optional - const_value from rvalue") {
  CHECK(soint_rvalue(13).const_value() == 13);
}

const soint soint_const_rvalue(int k) {
  return soint{std::in_place, k};
}

TEST_CASE("synchronized_optional - value from const rvalue") {
  CHECK(soint_const_rvalue(14).value() == 14);
}
