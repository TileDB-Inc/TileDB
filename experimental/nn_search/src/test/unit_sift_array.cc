/**
 * @file   unit_sift_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
#include "../sift_array.h"

TEST_CASE("sift_array: test test", "[sift_db]") {
  REQUIRE(true);
}

TEST_CASE("sift_array: test exceptions", "[sift_array]") {
  SECTION("file does not exist") {
    REQUIRE_THROWS_WITH(
        sift_array<float>("no_such_file"),
        "[TileDB::Array] Error: Cannot open array; Array does not exist.");
  }
}

TEST_CASE("sift_array: open files", "[sift_array]") {
  SECTION("Default") {
    auto base = sift_array<float>("s3://tiledb-lums/sift/siftsmall_base");
    CHECK(base.size() == 10'000);

    auto query = sift_array<float>("s3://tiledb-lums/sift/siftsmall_query");
    CHECK(query.size() == 100);

    auto truth = sift_array<int>("s3://tiledb-lums/sift/siftsmall_groundtruth");
    CHECK(truth.size() == 100);
  }

  SECTION("Zero size") {
    auto base = sift_array<float>("s3://tiledb-lums/sift/siftsmall_base", 0);
    CHECK(base.size() == 10'000);

    auto query = sift_array<float>("s3://tiledb-lums/sift/siftsmall_query", 0);
    CHECK(query.size() == 100);

    auto truth =
        sift_array<int>("s3://tiledb-lums/sift/siftsmall_groundtruth", 0);
    CHECK(truth.size() == 100);
  }

  SECTION("Size 10") {
    auto base = sift_array<float>("s3://tiledb-lums/sift/siftsmall_base", 10);
    CHECK(base.size() == 10);

    auto query = sift_array<float>("s3://tiledb-lums/sift/siftsmall_query", 10);
    CHECK(query.size() == 10);

    auto truth =
        sift_array<int>("s3://tiledb-lums/sift/siftsmall_groundtruth", 10);
    CHECK(truth.size() == 10);
  }
}
