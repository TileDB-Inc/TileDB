/**
* @file   unit_linalg.cc
*
* @section LICENSE
*
* The MIT License
*
* @copyright Copyright (c) 2023 TileDB, Inc.
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
*/

#include <catch2/catch_all.hpp>
#include "../linalg.h"

using namespace tiledb::common;

TEST_CASE("linalg: test test", "[linalg]") {
  REQUIRE(true);
}

using TestTypes = std::tuple<float, double, int, char, size_t, uint32_t>;
TEMPLATE_LIST_TEST_CASE("linalg: test Matrix constructor, row oriented", "[linalg]", TestTypes) {
  auto a = Matrix<TestType, Kokkos::layout_right>(3, 2);
  SECTION("dims") {
    CHECK(a.num_rows() == 3);
    CHECK(a.num_cols() == 2);
  }
  SECTION("values") {
    auto v = a.data();
    std::iota(v, v + 6, 1);
    CHECK(a(0, 0) == 1);
    CHECK(a(0, 1) == 2);
    CHECK(a(1, 0) == 3);
    CHECK(a(1, 1) == 4);
    CHECK(a(2, 0) == 5);
    CHECK(a(2, 1) == 6);
  }
}

using TestTypes = std::tuple<float, double, int, char, size_t, uint32_t>;
TEMPLATE_LIST_TEST_CASE("linalg: test Matrix constructor, column oriented", "[linalg]", TestTypes) {
  auto a = Matrix<TestType, Kokkos::layout_left>(3, 2);
  SECTION("dims") {
    CHECK(a.num_rows() == 3);
    CHECK(a.num_cols() == 2);
  }
  SECTION("values") {
    auto v = a.data();
    std::iota(v, v + 6, 1);
    CHECK(a(0, 0) == 1);
    CHECK(a(0, 1) == 4);
    CHECK(a(1, 0) == 2);
    CHECK(a(1, 1) == 5);
    CHECK(a(2, 0) == 3);
    CHECK(a(2, 1) == 6);
  }
}