/**
 * @file tiledb/storage_format/uri/test/unit_uri_format.cc
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
 * Tests for the storage format's uri manipulation APIs.
 */

#include <test/support/tdb_catch.h>

#include "../fragment_name.h"
#include "../parse_uri.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::storage_format;
using namespace tiledb::sm::utils::parse;

TEST_CASE(
    "StorageFormat: Fragment: compute_new_fragment_name",
    "[storage_format][fragment][compute_new_fragment_name]") {
  // Create fragments at timestamps 1 and 2
  auto start = tiledb::sm::URI{
      "file:///array_name/__fragments/__1_1_44318efd44f546b18db13edc8d10805b"};
  auto end = tiledb::sm::URI{
      "file:///array_name/__fragments/__2_2_44318efd44f546b18db13edc8d10805b"};

  SECTION("valid timestamps") {
    // Compute new fragment name for fragments at timestamps 1, 2
    auto name{compute_new_fragment_name(start, end, constants::format_version)};

    // Check new fragment name timestamp range
    auto name_substr{name.substr(name.find_last_of("/"), 6)};
    CHECK(name_substr == "/__1_2");
  }

  SECTION("invalid timestamps") {
    // Try to compute new fragment name for fragments at timestamps 2, 1
    REQUIRE_THROWS_WITH(
        compute_new_fragment_name(end, start, constants::format_version),
        Catch::Matchers::ContainsSubstring(
            "start timestamp cannot be after end timestamp"));
  }
}

TEST_CASE(
    "StorageFormat: Fragment: generate_fragment_name",
    "[storage_format][fragment][generate_fragment_name]") {
  // Note: all format versions will generate the same format of fragment name
  uint64_t timestamp = 1;
  format_version_t format_version = 5;

  // Generate new fragment name
  auto name{generate_fragment_name(timestamp, format_version)};

  // Check new fragment name's timestamp range
  auto name_timestamps{name.substr(0, 6)};
  CHECK(name_timestamps == "/__1_1");

  // Check new fragment name's format version
  auto name_version{name.substr(name.find_last_of("_"))};
  CHECK(name_version == "_5");
}

TEST_CASE(
    "StorageFormat: Parse: get_timestamp_range",
    "[storage_format][parse][get_timestamp_range]") {
  std::pair<uint64_t, uint64_t> range;

  SECTION("name version 1") {
    // Note that this version sets both range values to the end timestamp.
    // As such, if the end_timestamp < start_timestamp, no error is thrown.
    SECTION("valid timestamps") {
      // Create fragment at timestamp 1-2
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__44318efd44f546b18db13edc8d10805b_1{_2}"};

      // Check timestamp range
      REQUIRE(get_timestamp_range(frag, &range).ok());
      CHECK(range.first == 2);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__44318efd44f546b18db13edc8d10805b_2{_1}"};

      // Check timestamp range
      REQUIRE(get_timestamp_range(frag, &range).ok());
      CHECK(range.first == 1);
      CHECK(range.second == 1);
    }
  }

  SECTION("name version 2") {
    SECTION("valid timestamps") {
      // Create fragment at timestamp 1-2
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__1_2_44318efd44f546b18db13edc8d10805b"};

      // Check timestamp range
      REQUIRE(get_timestamp_range(frag, &range).ok());
      CHECK(range.first == 1);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__2_1_44318efd44f546b18db13edc8d10805b"};

      // Try to get timestamp range
      REQUIRE_THROWS_WITH(
          get_timestamp_range(frag, &range),
          Catch::Matchers::ContainsSubstring(
              "start timestamp cannot be after end timestamp"));
    }
  }

  SECTION("name version 3") {
    SECTION("valid timestamps") {
      // Create fragment at timestamp 1-2
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__1_2_44318efd44f546b18db13edc8d10805b_5"};

      // Check timestamp range
      REQUIRE(get_timestamp_range(frag, &range).ok());
      CHECK(range.first == 1);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI{
          "file:///array_name/__fragments/"
          "__2_1_44318efd44f546b18db13edc8d10805b_5"};

      // Try to get timestamp range
      REQUIRE_THROWS_WITH(
          get_timestamp_range(frag, &range),
          Catch::Matchers::ContainsSubstring(
              "start timestamp cannot be after end timestamp"));
    }
  }
}
