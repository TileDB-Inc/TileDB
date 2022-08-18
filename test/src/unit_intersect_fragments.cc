/**
 * @file unit_intersect_fragments.cc
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
 * This file tests the ``intersect_fragments`` function.
 */

#include <set>
#include <utility>
#include <vector>

#include <catch.hpp>
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/storage_format/uri/generate_uri.h"

using namespace tiledb::common;
using namespace tiledb::sm;

const uint32_t test_format_version{16};

/**
 * Create example fragment timestamps.
 *
 * Note: uses subsequent timestamps to guarantee order of URIs.
 */
tuple<std::vector<std::string>, std::vector<std::pair<uint64_t, uint64_t>>>
fragment_uri_components(uint64_t num_components) {
  std::vector<std::string> frag_names;
  std::vector<std::pair<uint64_t, uint64_t>> times;
  for (uint64_t ii{1}; ii <= num_components; ++ii) {
    frag_names.push_back(tiledb::storage_format::generate_fragment_name(
        ii, test_format_version));
    uint64_t timestamp_start{ii};
    uint64_t timestamp_stop{ii};
    times.emplace_back(std::move(timestamp_start), std::move(timestamp_stop));
  }
  return {frag_names, times};
}

TEST_CASE("Intersect fragments", "[uri]") {
  // Initialize fragment lists.
  std::vector<TimestampedURI> list1;
  std::vector<TimestampedURI> list2;
  std::set<URI> expected_values;

  SECTION("Both lists empty") {
    // No data to add.
  }

  SECTION("Comparison list is empty") {
    // Add timestamped URIs to list 1 only.
    auto path2 = URI("base2", false);
    auto&& [names, times] = fragment_uri_components(2);
    list2.emplace_back(path2.join_path(names[0]), times[0]);
    list2.emplace_back(path2.join_path(names[1]), times[1]);

    // No data in result - expect empty vector.
  }

  SECTION("Fragment list is empty") {
    // Add timestamped URIs to list 2 only.
    auto path1 = URI("base1", false);
    auto&& [names, times] = fragment_uri_components(2);
    list1.emplace_back(path1.join_path(names[0]), times[0]);
    list1.emplace_back(path1.join_path(names[1]), times[1]);

    // No data in result - expect empty vector.
  }

  SECTION("Disjoint fragment lists") {
    // Create fragment names.
    auto&& [names, times] = fragment_uri_components(6);

    // Add fragment URIs to fragment list 1.
    auto path1 = URI("base1", false);
    list1.emplace_back(path1.join_path(names[0]), times[0]);
    list1.emplace_back(path1.join_path(names[4]), times[4]);

    // Add fragment URIS to fragment list 2.
    auto path2 = URI("base2", false);
    list2.emplace_back(path2.join_path(names[1]), times[1]);
    list2.emplace_back(path2.join_path(names[2]), times[2]);
    list2.emplace_back(path2.join_path(names[3]), times[3]);
    list2.emplace_back(path2.join_path(names[5]), times[5]);

    // No data in result - expect empty vector.
  }

  SECTION("Indentical fragment lists") {
    // Create fragment names.
    auto&& [names, times] = fragment_uri_components(3);

    // Add fragment URIs to fragment list 1.
    auto path1 = URI("base1", false);
    list1.emplace_back(path1.join_path(names[0]), times[0]);
    list1.emplace_back(path1.join_path(names[1]), times[1]);
    list1.emplace_back(path1.join_path(names[2]), times[2]);

    // Add fragment URIS to fragment list 2.
    auto path2 = URI("base2", false);
    list2.emplace_back(path2.join_path(names[2]), times[2]);
    list2.emplace_back(path2.join_path(names[1]), times[1]);
    list2.emplace_back(path2.join_path(names[0]), times[0]);

    // Update expected result names.
    expected_values.insert(path2.join_path(names[0]));
    expected_values.insert(path2.join_path(names[1]));
    expected_values.insert(path2.join_path(names[2]));
  }

  SECTION("Extra values in comparison list") {
    // Create fragment names.
    auto&& [names, times] = fragment_uri_components(6);

    // Add fragment URIs to fragment list 1.
    auto path1 = URI("base1", false);
    list1.emplace_back(path1.join_path(names[0]), times[0]);
    list1.emplace_back(path1.join_path(names[1]), times[1]);
    list1.emplace_back(path1.join_path(names[2]), times[2]);
    list1.emplace_back(path1.join_path(names[3]), times[3]);
    list1.emplace_back(path1.join_path(names[4]), times[4]);
    list1.emplace_back(path1.join_path(names[5]), times[5]);

    // Add fragment URIS to fragment list 2.
    auto path2 = URI("base2", false);
    list2.emplace_back(path2.join_path(names[4]), times[4]);
    list2.emplace_back(path2.join_path(names[2]), times[2]);
    list2.emplace_back(path2.join_path(names[1]), times[1]);

    // Update expected result names.
    expected_values.insert(path2.join_path(names[1]));
    expected_values.insert(path2.join_path(names[2]));
    expected_values.insert(path2.join_path(names[4]));
  }

  SECTION("Extra values in fragment list") {
    // Create fragment names.
    auto&& [names, times] = fragment_uri_components(6);

    // Add fragment URIs to fragment list 1.
    auto path1 = URI("base1", false);
    list1.emplace_back(path1.join_path(names[4]), times[4]);
    list1.emplace_back(path1.join_path(names[2]), times[2]);
    list1.emplace_back(path1.join_path(names[1]), times[1]);

    // Add fragment URIS to fragment list 2.
    auto path2 = URI("base2", false);
    list2.emplace_back(path2.join_path(names[0]), times[0]);
    list2.emplace_back(path2.join_path(names[1]), times[1]);
    list2.emplace_back(path2.join_path(names[2]), times[2]);
    list2.emplace_back(path2.join_path(names[3]), times[3]);
    list2.emplace_back(path2.join_path(names[4]), times[4]);
    list2.emplace_back(path2.join_path(names[5]), times[5]);

    // Update expected result names.
    expected_values.insert(path2.join_path(names[1]));
    expected_values.insert(path2.join_path(names[2]));
    expected_values.insert(path2.join_path(names[4]));
  }

  SECTION("Extra values in both lists") {
    // Create fragment names.
    auto&& [names, times] = fragment_uri_components(8);

    // Add fragment URIs to fragment list 1.
    auto path1 = URI("base1", false);
    list1.emplace_back(path1.join_path(names[4]), times[4]);
    list1.emplace_back(path1.join_path(names[2]), times[2]);
    list1.emplace_back(path1.join_path(names[1]), times[1]);
    list1.emplace_back(path1.join_path(names[5]), times[5]);
    list1.emplace_back(path1.join_path(names[7]), times[7]);

    // Add fragment URIS to fragment list 2.
    auto path2 = URI("base2", false);
    list2.emplace_back(path2.join_path(names[0]), times[0]);
    list2.emplace_back(path2.join_path(names[1]), times[1]);
    list2.emplace_back(path2.join_path(names[2]), times[2]);
    list2.emplace_back(path2.join_path(names[3]), times[3]);
    list2.emplace_back(path2.join_path(names[4]), times[4]);
    list2.emplace_back(path2.join_path(names[6]), times[6]);

    // Update expected result names.
    expected_values.insert(path2.join_path(names[1]));
    expected_values.insert(path2.join_path(names[2]));
    expected_values.insert(path2.join_path(names[4]));
  }

  // Remove fragments.
  intersect_fragments(list1, list2);

  // Check fragments in list2.
  {
    INFO(
        "List2 missing fragments. Expected " +
        std::to_string(expected_values.size()) + "fragments, but found " +
        std::to_string(list2.size()) + " framgents.");
    CHECK(list2.size() >= expected_values.size());
  }
  for (const auto& frag_uri : list2) {
    auto search = expected_values.find(frag_uri.uri_);
    INFO("Extra fragment found in list2: " + frag_uri.uri_.to_string());
    CHECK(search != expected_values.end());
  }
}
