/**
 * @file unit_bytevecvalue.cc
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
 * Tests the `ByteVecValue' class.
 */

#include <cstring>
#include <numeric>

#include "test/support/src/ast_helpers.h"
#include "tiledb/sm/misc/types.h"

#include <catch2/catch_test_macros.hpp>

using namespace tiledb::sm;

TEST_CASE("ByteVecValue Constructors", "[bytevecvalue]") {
  // Default constructor
  ByteVecValue b;
  REQUIRE(b.size() == 0);
  REQUIRE(!b);

  // Size constructor
  ByteVecValue c(5);
  REQUIRE(c.size() == 5);
  REQUIRE(c);

  // Vector constructor
  std::vector<uint8_t> vec(256);
  std::iota(vec.begin(), vec.end(), 0);
  ByteVecValue d(std::move(vec));
  std::string hex_str =
      "00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f "
      "10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f "
      "20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f "
      "30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f "
      "40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f "
      "50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f "
      "60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f "
      "70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f "
      "80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f "
      "90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f "
      "a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af "
      "b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf "
      "c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf "
      "d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df "
      "e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef "
      "f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff";

  REQUIRE(d.size() == 256);
  REQUIRE(d);
  REQUIRE(tiledb::test::bbv_to_hex_str(d) == hex_str);
}

template <class T>
void test_case(T val) {
  ByteVecValue vec;
  vec.resize(sizeof(T));
  memcpy(vec.data(), &val, sizeof(T));
  REQUIRE(
      tiledb::test::bbv_to_hex_str(vec) ==
      tiledb::test::ptr_to_hex_str(&val, sizeof(T)));
}

TEST_CASE("ByteVecValue from pointers", "[bytevecvalue]") {
  test_case<int8_t>(49);
  test_case<uint8_t>(50);
  test_case<int16_t>(1000);
  test_case<uint16_t>(2022);
  test_case<int32_t>(985761475);
  test_case<uint32_t>(1985761475);
  test_case<int64_t>(981934736546381904);
  test_case<uint64_t>(93472336546381904);
  test_case<float>(10.472F);
  test_case<double>(239347.47521L);
  std::string s = "supercalifragilisticexpialidocious";
  test_case<char*>(const_cast<char*>(s.c_str()));
}
