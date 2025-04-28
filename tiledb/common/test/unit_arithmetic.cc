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

#include "build/test/support/oxidize/arithmetic.h"
#include "tiledb/common/arithmetic.h"

#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include <test/support/tdb_rapidcheck.h>

using namespace tiledb::common;

TEST_CASE(
    "Arithmetic checked_arithmetic uint32_t add", "[arithmetic][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint32_t a, uint32_t b)
                  ->std::optional<uint32_t> {
    const auto cpp = checked_arithmetic<uint32_t>::add(a, b);
    std::optional<uint32_t> rs;
    {
      uint32_t value;
      if (rust::std::u32_checked_add(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop(
        "checked_arithmetic<uint32_t>::add", [doit](uint32_t a, uint32_t b) {
          doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
        });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic int32_t add", "[arithmetic][rapidcheck]") {
  auto doit =
      []<typename Asserter = tiledb::test::AsserterCatch>(int32_t a, int32_t b)
          ->std::optional<int32_t> {
    const auto cpp = checked_arithmetic<int32_t>::add(a, b);
    std::optional<int32_t> rs;
    {
      int32_t value;
      if (rust::std::i32_checked_add(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop("checked_arithmetic<int32_t>::add", [doit](int32_t a, int32_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic uint64_t add", "[arithmetic][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint64_t a, uint64_t b)
                  ->std::optional<uint64_t> {
    const auto cpp = checked_arithmetic<uint64_t>::add(a, b);
    std::optional<uint64_t> rs;
    {
      uint64_t value;
      if (rust::std::u64_checked_add(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop(
        "checked_arithmetic<uint64_t>::add", [doit](uint64_t a, uint64_t b) {
          doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
        });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic int64_t add", "[arithmetic][rapidcheck]") {
  auto doit =
      []<typename Asserter = tiledb::test::AsserterCatch>(int64_t a, int64_t b)
          ->std::optional<int64_t> {
    const auto cpp = checked_arithmetic<int64_t>::add(a, b);
    std::optional<int64_t> rs;
    {
      int64_t value;
      if (rust::std::i64_checked_add(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop("checked_arithmetic<int64_t>::add", [doit](int64_t a, int64_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic uint32_t sub", "[arithmetic][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint32_t a, uint32_t b)
                  ->std::optional<uint32_t> {
    const std::optional<uint32_t> cpp = checked_arithmetic<uint32_t>::sub(a, b);

    std::optional<uint32_t> rs;
    {
      uint32_t value;
      if (rust::std::u32_checked_sub(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop(
        "checked_arithmetic<uint32_t>::sub", [doit](uint32_t a, uint32_t b) {
          doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
        });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic int32_t sub", "[arithmetic][rapidcheck]") {
  auto doit =
      []<typename Asserter = tiledb::test::AsserterCatch>(int32_t a, int32_t b)
          ->std::optional<int32_t> {
    const std::optional<int32_t> cpp = checked_arithmetic<int32_t>::sub(a, b);

    std::optional<int32_t> rs;
    {
      int32_t value;
      if (rust::std::i32_checked_sub(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop("checked_arithmetic<int32_t>::sub", [doit](int32_t a, int32_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic uint64_t sub", "[arithmetic][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint64_t a, uint64_t b)
                  ->std::optional<int64_t> {
    const std::optional<int64_t> cpp = checked_arithmetic<uint64_t>::sub(a, b);

    std::optional<uint64_t> rs;
    {
      uint64_t value;
      if (rust::std::u64_checked_sub(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Rapidcheck") {
    rc::prop(
        "checked_arithmetic<uint64_t>::sub", [doit](uint64_t a, uint64_t b) {
          doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
        });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic uint64_t sub_signed",
    "[arithmetic][rapidcheck]") {
  auto doit = []<typename Asserter = tiledb::test::AsserterCatch>(
                  uint64_t a, uint64_t b)
                  ->std::optional<int64_t> {
    const std::optional<int64_t> cpp =
        checked_arithmetic<uint64_t>::sub_signed(a, b);

    std::optional<int64_t> rs;
    {
      int64_t value;
      if (rust::std::u64_checked_sub_signed(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Example") {
    CHECK(doit(0, 0) == 0);
    CHECK(doit(0, 1) == -1);
    CHECK(doit(0, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000001);
    CHECK(doit(0, 0x8000000000000000) == 0x8000000000000000);
    CHECK(doit(0, 0x8000000000000001) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFE) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF) == std::nullopt);
    CHECK(doit(0xFFFFFFFFFFFFFFFF, 0x8000000000000000) == 0x7FFFFFFFFFFFFFFF);
  }

  SECTION("Rapidcheck") {
    rc::prop(
        "checked_arithmetic<uint64_t>::sub_signed",
        [doit](uint64_t a, uint64_t b) {
          doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
        });
  }
}

TEST_CASE(
    "Arithmetic checked_arithmetic int64_t sub", "[arithmetic][rapidcheck]") {
  auto doit =
      []<typename Asserter = tiledb::test::AsserterCatch>(int64_t a, int64_t b)
          ->std::optional<int64_t> {
    const std::optional<int64_t> cpp = checked_arithmetic<int64_t>::sub(a, b);

    std::optional<int64_t> rs;
    {
      int64_t value;
      if (rust::std::i64_checked_sub(a, b, value)) {
        rs.emplace(value);
      }
    }
    ASSERTER(cpp == rs);

    return cpp;
  };

  SECTION("Example") {
    CHECK(doit(0, 0) == 0);
    CHECK(doit(0, 1) == -1);
    CHECK(doit(0, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000001);
    CHECK(doit(0, 0x8000000000000000) == std::nullopt);
    CHECK(doit(0, 0x8000000000000001) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(-1, 0) == -1);
    CHECK(doit(-1, 0x7FFFFFFFFFFFFFFE) == 0x8000000000000001);
    CHECK(doit(-1, 0x7FFFFFFFFFFFFFFF) == 0x8000000000000000);
    CHECK(doit(-1, 0x8000000000000000) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(0x7FFFFFFFFFFFFFFF, 0) == 0x7FFFFFFFFFFFFFFF);
    CHECK(doit(0x7FFFFFFFFFFFFFFF, -1) == std::nullopt);
  }

  SECTION("Rapidcheck") {
    rc::prop("checked_arithmetic<int64_t>::sub", [doit](int64_t a, int64_t b) {
      doit.operator()<tiledb::test::AsserterRapidcheck>(a, b);
    });
  }
}
