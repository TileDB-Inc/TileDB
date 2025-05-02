/**
 * @file unit_arithmetic.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file tests arithmetic functions defined in `arithmetic.h`.
 */

#include "tiledb/common/arithmetic.h"

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

using namespace tiledb::common;

TEST_CASE(
    "Arithmetic checked_arithmetic uint32_t add", "[arithmetic][rapidcheck]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<uint32_t>::add(0, 0) == 0);
  }

  SECTION("Boundary") {
    const uint32_t max = std::numeric_limits<uint32_t>::max();
    CHECK(checked_arithmetic<uint32_t>::add(0, max) == max);
    CHECK(checked_arithmetic<uint32_t>::add(max, 0) == max);
    CHECK(checked_arithmetic<uint32_t>::add(10, max - 10) == max);
    CHECK(checked_arithmetic<uint32_t>::add(max - 10, 10) == max);
  }

  SECTION("Overflow") {
    const uint32_t max = std::numeric_limits<uint32_t>::max();
    CHECK(checked_arithmetic<uint32_t>::add(1, max) == std::nullopt);
    CHECK(checked_arithmetic<uint32_t>::add(max, 1) == std::nullopt);
    CHECK(checked_arithmetic<uint32_t>::add(10, max - 9) == std::nullopt);
    CHECK(checked_arithmetic<uint32_t>::add(max - 9, 10) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic int32_t add", "[arithmetic]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<int32_t>::add(0, 0) == 0);
    CHECK(checked_arithmetic<int32_t>::add(0, 1) == 1);
    CHECK(checked_arithmetic<int32_t>::add(-1, 0) == -1);
  }

  const int32_t max = std::numeric_limits<int32_t>::max();
  const int32_t min = std::numeric_limits<int32_t>::min();

  SECTION("Boundary") {
    CHECK(checked_arithmetic<int32_t>::add(max, min) == -1);
    CHECK(checked_arithmetic<int32_t>::add(max, 0) == max);
    CHECK(checked_arithmetic<int32_t>::add(0, max) == max);
    CHECK(checked_arithmetic<int32_t>::add(min, 0) == min);
    CHECK(checked_arithmetic<int32_t>::add(0, min) == min);
    CHECK(checked_arithmetic<int32_t>::add(max - 10, 10) == max);
    CHECK(checked_arithmetic<int32_t>::add(10, max - 10) == max);
    CHECK(checked_arithmetic<int32_t>::add(min + 10, -10) == min);
    CHECK(checked_arithmetic<int32_t>::add(-10, min + 10) == min);
  }

  SECTION("Overflow") {
    CHECK(checked_arithmetic<int32_t>::add(max, 1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(1, max) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(min, -1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(-1, min) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(max - 9, 10) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(10, max - 9) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(min + 9, -10) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(-10, min + 9) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic uint64_t add", "[arithmetic]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<uint64_t>::add(0, 0) == 0);
  }

  SECTION("Boundary") {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    CHECK(checked_arithmetic<uint64_t>::add(0, max) == max);
    CHECK(checked_arithmetic<uint64_t>::add(max, 0) == max);
    CHECK(checked_arithmetic<uint64_t>::add(10, max - 10) == max);
    CHECK(checked_arithmetic<uint64_t>::add(max - 10, 10) == max);
  }

  SECTION("Overflow") {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    CHECK(checked_arithmetic<uint64_t>::add(1, max) == std::nullopt);
    CHECK(checked_arithmetic<uint64_t>::add(max, 1) == std::nullopt);
    CHECK(checked_arithmetic<uint64_t>::add(10, max - 9) == std::nullopt);
    CHECK(checked_arithmetic<uint64_t>::add(max - 9, 10) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic int64_t add", "[arithmetic]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<int32_t>::add(0, 0) == 0);
    CHECK(checked_arithmetic<int32_t>::add(0, 1) == 1);
    CHECK(checked_arithmetic<int32_t>::add(-1, 0) == -1);
  }

  const int32_t max = std::numeric_limits<int32_t>::max();
  const int32_t min = std::numeric_limits<int32_t>::min();

  SECTION("Boundary") {
    CHECK(checked_arithmetic<int32_t>::add(max, min) == -1);
    CHECK(checked_arithmetic<int32_t>::add(max, 0) == max);
    CHECK(checked_arithmetic<int32_t>::add(0, max) == max);
    CHECK(checked_arithmetic<int32_t>::add(min, 0) == min);
    CHECK(checked_arithmetic<int32_t>::add(0, min) == min);
    CHECK(checked_arithmetic<int32_t>::add(max - 10, 10) == max);
    CHECK(checked_arithmetic<int32_t>::add(10, max - 10) == max);
    CHECK(checked_arithmetic<int32_t>::add(min + 10, -10) == min);
    CHECK(checked_arithmetic<int32_t>::add(-10, min + 10) == min);
  }

  SECTION("Overflow") {
    CHECK(checked_arithmetic<int32_t>::add(max, 1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(1, max) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(min, -1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(-1, min) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(max - 9, 10) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(10, max - 9) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(min + 9, -10) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::add(-10, min + 9) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic uint32_t sub", "[arithmetic]") {
  rc::prop("a < b", [](uint32_t a) {
    const uint32_t max = std::numeric_limits<uint32_t>::max();
    RC_PRE(a != max);
    const uint32_t b = *rc::gen::inRange<uint32_t>(a + 1, max);
    const std::optional<uint32_t> c = checked_arithmetic<uint32_t>::sub(a, b);
    RC_ASSERT(c == std::nullopt);
  });

  rc::prop("a >= b", [](int32_t a) {
    const uint32_t b = *rc::gen::inRange<uint32_t>(0, a);
    const std::optional<uint32_t> c = checked_arithmetic<uint32_t>::sub(a, b);
    RC_ASSERT(c == a - b);
  });
}

TEST_CASE("Arithmetic checked_arithmetic int32_t sub", "[arithmetic]") {
  const int32_t max = std::numeric_limits<int32_t>::max();
  const int32_t min = std::numeric_limits<int32_t>::min();

  SECTION("Example") {
    CHECK(checked_arithmetic<int32_t>::sub(max, 0) == max);
    CHECK(checked_arithmetic<int32_t>::sub(0, max) == min + 1);
    CHECK(checked_arithmetic<int32_t>::sub(0, min + 1) == max);
  }

  SECTION("Overflow") {
    CHECK(checked_arithmetic<int32_t>::sub(max, -1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::sub(1, -max) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::sub(min, 1) == std::nullopt);
    CHECK(checked_arithmetic<int32_t>::sub(0, min) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic uint64_t sub", "[arithmetic]") {
  rc::prop("a < b", [](uint64_t a) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    RC_PRE(a != max);
    const uint64_t b = *rc::gen::inRange<uint64_t>(a + 1, max);
    const std::optional<uint64_t> c = checked_arithmetic<uint64_t>::sub(a, b);
    RC_ASSERT(c == std::nullopt);
  });

  rc::prop("a >= b", [](int32_t a) {
    const uint64_t b = *rc::gen::inRange<uint64_t>(0, a);
    const std::optional<uint64_t> c = checked_arithmetic<uint64_t>::sub(a, b);
    RC_ASSERT(c == a - b);
  });
}

TEST_CASE("Arithmetic checked_arithmetic uint64_t sub_signed", "[arithmetic]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<uint64_t>::sub_signed(0, 0) == 0);
    CHECK(checked_arithmetic<uint64_t>::sub_signed(0, 1) == -1);
  }

  SECTION("Boundary") {
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(0, 0x7FFFFFFFFFFFFFFF) ==
        0x8000000000000001);
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(0, 0x8000000000000000) ==
        0x8000000000000000);
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(
            0xFFFFFFFFFFFFFFFF, 0x8000000000000000) == 0x7FFFFFFFFFFFFFFF);
  }

  SECTION("Overflow") {
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(0, 0x8000000000000001) ==
        std::nullopt);
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(0xFFFFFFFFFFFFFFFF, 0) ==
        std::nullopt);
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(
            0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFE) == std::nullopt);
    CHECK(
        checked_arithmetic<uint64_t>::sub_signed(
            0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF) == std::nullopt);
  }
}

TEST_CASE("Arithmetic checked_arithmetic int64_t sub", "[arithmetic]") {
  SECTION("Example") {
    CHECK(checked_arithmetic<int64_t>::sub(0, 0) == 0);
    CHECK(checked_arithmetic<int64_t>::sub(0, 1) == -1);
    CHECK(checked_arithmetic<int64_t>::sub(-1, 0) == -1);
  }

  SECTION("Boundary") {
    CHECK(
        checked_arithmetic<int64_t>::sub(0, 0x7FFFFFFFFFFFFFFF) ==
        0x8000000000000001);
    CHECK(
        checked_arithmetic<int64_t>::sub(0, 0x8000000000000001) ==
        0x7FFFFFFFFFFFFFFF);
    CHECK(
        checked_arithmetic<int64_t>::sub(-1, 0x7FFFFFFFFFFFFFFE) ==
        0x8000000000000001);
    CHECK(
        checked_arithmetic<int64_t>::sub(-1, 0x7FFFFFFFFFFFFFFF) ==
        0x8000000000000000);
    CHECK(
        checked_arithmetic<int64_t>::sub(-1, 0x8000000000000000) ==
        0x7FFFFFFFFFFFFFFF);
    CHECK(
        checked_arithmetic<int64_t>::sub(0x7FFFFFFFFFFFFFFF, 0) ==
        0x7FFFFFFFFFFFFFFF);
  }

  SECTION("Overflow") {
    CHECK(
        checked_arithmetic<int64_t>::sub(0, 0x8000000000000000) ==
        std::nullopt);
    CHECK(
        checked_arithmetic<int64_t>::sub(0x7FFFFFFFFFFFFFFF, -1) ==
        std::nullopt);
  }
}
