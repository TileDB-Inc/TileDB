/**
 * @file unit_parse_argument.cc
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
 * Tests for useful (global) functions.
 */

#include "catch.hpp"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/parse_argument.h"

using namespace tiledb::sm::utils::parse;
using namespace tiledb::sm;

TEST_CASE("Test to_str function for integers", "[to_str][integer]") {
  int8_t int8_value = -10;
  uint8_t uint8_value = 10;

  REQUIRE(to_str(&int8_value, Datatype::INT8) == "-10");
  REQUIRE(to_str(&uint8_value, Datatype::UINT8) == "10");

  int16_t int16_value = -10;
  uint16_t uint16_value = 10;

  REQUIRE(to_str(&int16_value, Datatype::INT16) == "-10");
  REQUIRE(to_str(&uint16_value, Datatype::UINT16) == "10");

  int32_t int32_value = -10;
  uint32_t uint32_value = 10;

  REQUIRE(to_str(&int32_value, Datatype::INT32) == "-10");
  REQUIRE(to_str(&uint32_value, Datatype::UINT32) == "10");

  int64_t int64_value = -10;
  uint64_t uint64_value = 10;

  REQUIRE(to_str(&int64_value, Datatype::INT64) == "-10");
  REQUIRE(to_str(&uint64_value, Datatype::UINT64) == "10");
}
