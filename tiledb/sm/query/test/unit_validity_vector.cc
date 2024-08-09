/**
 * @file unit_validity_vector.cc
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
 *
 * @section DESCRIPTION
 *
 * Tests the `ValidityVector` class.
 */

#include "tiledb/sm/query/validity_vector.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace std;
using namespace tiledb::sm;

TEST_CASE(
    "ValidityVector: Test default constructor",
    "[ValidityVector][default_constructor]") {
  ValidityVector validity_vector;
  REQUIRE(validity_vector.bytemap() == nullptr);
  REQUIRE(validity_vector.bytemap_size() == nullptr);
  REQUIRE(validity_vector.buffer() == nullptr);
  REQUIRE(validity_vector.buffer_size() == nullptr);
}

TEST_CASE(
    "ValidityVector: Test move constructor",
    "[ValidityVector][move_constructor]") {
  uint8_t bytemap[10];
  uint64_t bytemap_size = sizeof(bytemap);
  for (uint64_t i = 0; i < bytemap_size; ++i)
    bytemap[i] = i % 2;

  ValidityVector validity_vector1{bytemap, &bytemap_size};

  ValidityVector validity_vector2(std::move(validity_vector1));

  REQUIRE(validity_vector2.bytemap() == bytemap);
  REQUIRE(validity_vector2.bytemap_size() == &bytemap_size);
  REQUIRE(validity_vector2.buffer() == bytemap);
  REQUIRE(validity_vector2.buffer_size() == &bytemap_size);
}

TEST_CASE(
    "ValidityVector: Test move-assignment",
    "[ValidityVector][move_assignment]") {
  uint8_t bytemap[10];
  uint64_t bytemap_size = sizeof(bytemap);
  for (uint64_t i = 0; i < bytemap_size; ++i)
    bytemap[i] = i % 2;

  ValidityVector validity_vector1{bytemap, &bytemap_size};

  ValidityVector validity_vector2 = std::move(validity_vector1);

  REQUIRE(validity_vector2.bytemap() == bytemap);
  REQUIRE(validity_vector2.bytemap_size() == &bytemap_size);
  REQUIRE(validity_vector2.buffer() == bytemap);
  REQUIRE(validity_vector2.buffer_size() == &bytemap_size);
}
