/**
 * @file unit-Consolidator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * Tests the `Subarray` class.
 */

#include <catch.hpp>
#include <iostream>
#include "test/src/helpers.h"
#include "tiledb/sm/storage_manager/consolidator.h"

using namespace tiledb::sm;

TEST_CASE(
    "Consolidator: Remove consolidate fragment URIs, remove none",
    "[remove-consolidated-URIs][remove-none]") {
  std::vector<TimestampedURI> uris;
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 1));
  Consolidator::remove_consolidated_fragment_uris(&uris);
  CHECK(uris.size() == 1);
  CHECK(uris[0].timestamp_range_ == std::pair<uint64_t, uint64_t>(0, 1));

  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 3));
  Consolidator::remove_consolidated_fragment_uris(&uris);
  CHECK(uris.size() == 2);
  CHECK(uris[0].timestamp_range_ == std::pair<uint64_t, uint64_t>(0, 1));
  CHECK(uris[1].timestamp_range_ == std::pair<uint64_t, uint64_t>(2, 3));
}

TEST_CASE(
    "Consolidator: Remove consolidate fragment URIs, remove one level at start",
    "[remove-consolidated-URIs][remove-one-level-start]") {
  std::vector<TimestampedURI> uris;
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 0));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 1));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(1, 1));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 2));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(3, 3));
  Consolidator::remove_consolidated_fragment_uris(&uris);
  CHECK(uris.size() == 3);
  CHECK(uris[0].timestamp_range_ == std::pair<uint64_t, uint64_t>(0, 1));
  CHECK(uris[1].timestamp_range_ == std::pair<uint64_t, uint64_t>(2, 2));
  CHECK(uris[2].timestamp_range_ == std::pair<uint64_t, uint64_t>(3, 3));
}

TEST_CASE(
    "Consolidator: Remove consolidate fragment URIs, remove one level at "
    "middle",
    "[remove-consolidated-URIs][remove-one-level-middle]") {
  std::vector<TimestampedURI> uris;
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 0));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(1, 1));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 2));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 3));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(3, 3));
  Consolidator::remove_consolidated_fragment_uris(&uris);
  CHECK(uris.size() == 3);
  CHECK(uris[0].timestamp_range_ == std::pair<uint64_t, uint64_t>(0, 0));
  CHECK(uris[1].timestamp_range_ == std::pair<uint64_t, uint64_t>(1, 1));
  CHECK(uris[2].timestamp_range_ == std::pair<uint64_t, uint64_t>(2, 3));
}

TEST_CASE(
    "Consolidator: Remove consolidate fragment URIs, remove two levels",
    "[remove-consolidated-URIs][remove-two-levels]") {
  std::vector<TimestampedURI> uris;
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 0));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 1));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(0, 3));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(1, 1));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 2));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(2, 3));
  uris.emplace_back(URI(), std::pair<uint64_t, uint64_t>(3, 3));
  Consolidator::remove_consolidated_fragment_uris(&uris);
  CHECK(uris.size() == 1);
  CHECK(uris[0].timestamp_range_ == std::pair<uint64_t, uint64_t>(0, 3));
}