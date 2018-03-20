/**
 * @file   unit-cppapi-util.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Util Tests for C++ API.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/utils.h"

using namespace tiledb;

TEST_CASE("C++ API: Utils", "[cppapi]") {
  std::vector<char> buf = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'};
  std::vector<uint64_t> offsets = {0, 5};

  SECTION("Group by cell, offset") {
    auto ret = group_by_cell<char, std::string>(offsets, buf);
    REQUIRE(ret.size() == 2);
    CHECK(ret[0] == "abcde");
    CHECK(ret[1] == "fghi");
  }

  SECTION("Group by cell, fixed") {
    CHECK_THROWS(group_by_cell<2>(buf));
    auto ret = group_by_cell<3>(buf);
    CHECK(ret.size() == 3);
    CHECK(std::string(ret[0].begin(), ret[0].end()) == "abc");
    CHECK(std::string(ret[1].begin(), ret[1].end()) == "def");
    CHECK(std::string(ret[2].begin(), ret[2].end()) == "ghi");
  }

  SECTION("Group by cell, dynamic") {
    CHECK_THROWS(group_by_cell(buf, 2));
    auto ret = group_by_cell(buf, 3);
    CHECK(ret.size() == 3);
    CHECK(std::string(ret[0].begin(), ret[0].end()) == "abc");
    CHECK(std::string(ret[1].begin(), ret[1].end()) == "def");
    CHECK(std::string(ret[2].begin(), ret[2].end()) == "ghi");
  }

  SECTION("Unpack var buffer") {
    auto grouped = group_by_cell(buf, 3);
    auto ungrouped = ungroup_var_buffer(grouped);
    CHECK(ungrouped.first.size() == 3);
    CHECK(ungrouped.second.size() == 9);
  }

  SECTION("Flatten") {
    std::vector<std::string> v = {"a", "bb", "ccc"};
    auto f = flatten(v);
    CHECK(f.size() == 6);
    CHECK(std::string(f.begin(), f.end()) == "abbccc");

    std::vector<std::vector<double>> d = {{1.2, 2.1}, {2.3, 3.2}, {3.4, 4.3}};
    auto f2 = flatten(d);
    CHECK(f2.size() == 6);
    CHECK(f2[0] == 1.2);
    CHECK(f2[1] == 2.1);
    CHECK(f2[2] == 2.3);
    CHECK(f2[3] == 3.2);
    CHECK(f2[4] == 3.4);
    CHECK(f2[5] == 4.3);
  }
}
