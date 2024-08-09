/**
 * @file   tiledb/common/exception/test/unit_exception.cc
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

#include <catch2/catch_test_macros.hpp>
#include "../exception.h"

using namespace tiledb::common;

TEST_CASE("Exception - construct directly") {
  try {
    throw StatusException("where", "what");
  } catch (const StatusException& e) {
    CHECK(std::string(e.what()) == std::string("where: what"));
  } catch (...) {
    FAIL("threw something other than StatusException");
  }
}

TEST_CASE("Exception - construct from error status") {
  try {
    Status st{"where", "what"};
    throw StatusException(st);
  } catch (const StatusException& e) {
    CHECK(std::string(e.what()) == std::string("where: what"));
  }
}

TEST_CASE("Exception - construct from invalid status") {
  Status st{};
  REQUIRE_THROWS(StatusException{st});
  CHECK_THROWS_AS(StatusException{st}, std::invalid_argument);
}

TEST_CASE("throw_if_not_ok - nothrow on OK status") {
  Status st{};
  REQUIRE_NOTHROW(throw_if_not_ok(st));
}

TEST_CASE("throw_if_not_ok - throw on error status") {
  Status st{"where", "what"};
  REQUIRE_THROWS(throw_if_not_ok(st));
  REQUIRE_THROWS_AS(throw_if_not_ok(st), StatusException);
}
