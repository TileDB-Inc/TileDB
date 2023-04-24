/**
 * @file   unit_sift_db.cc
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
#include <filesystem>
#include <iostream>
#include "../sift_db.h"
#include "./config.h"

TEST_CASE("sift_db: test test", "[sift_db]") {
#include <filesystem>
#include <iostream>

  std::filesystem::path currentDir = std::filesystem::current_path();
  std::cout << "Current working directory is: " << currentDir << std::endl;

  CHECK(true);
}

TEST_CASE("sift_db: test exceptions", "[sift_db]") {
  SECTION("file does not exist") {
    CHECK_THROWS_WITH(
        sift_db<float>("no_such_file", 128),
        "file no_such_file does not exist");
  }
}

TEST_CASE("sift_db: open files", "[sift_db]") {
  std::filesystem::path path(SIFT_TEST_DIR + std::string("/siftsmall"));
  std::filesystem::path base_file("siftsmall_base.fvecs");
  std::filesystem::path query_file("siftsmall_query.fvecs");
  std::filesystem::path groundtruth_file("siftsmall_groundtruth.ivecs");

  SECTION("Default size") {
    auto base_db = sift_db<float>((path / base_file).string());
    CHECK(base_db.size() == 10'000);

    auto query_db = sift_db<float>((path / query_file).string());
    CHECK(query_db.size() == 100);

    auto truth_db = sift_db<float>((path / groundtruth_file).string());
    CHECK(truth_db.size() == 100);
  }

  SECTION("Zero size") {
    auto base_db = sift_db<float>((path / base_file).string(), 0);
    CHECK(base_db.size() == 10'000);

    auto query_db = sift_db<float>((path / query_file).string(), 0);
    CHECK(query_db.size() == 100);

    auto truth_db = sift_db<float>((path / groundtruth_file).string(), 0);
    CHECK(truth_db.size() == 100);
  }

  SECTION("Size 10") {
    auto base_db = sift_db<float>((path / base_file).string(), 10);
    CHECK(base_db.size() == 10);

    auto query_db = sift_db<float>((path / query_file).string(), 10);
    CHECK(query_db.size() == 10);

    auto truth_db = sift_db<float>((path / groundtruth_file).string(), 10);
    CHECK(truth_db.size() == 10);
  }
}
