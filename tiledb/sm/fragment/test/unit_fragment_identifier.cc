/**
 * @file tiledb/sm/fragment/test/unit_fragment_identifier.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file tests the FragmentID class
 */

#include <test/support/tdb_catch.h>
#include <iostream>

#include "tiledb/sm/fragment/fragment_identifier.h"

using namespace tiledb::sm;

TEST_CASE(
    "FragmentID: get_timestamp_range", "[fragment_id][get_timestamp_range]") {
  // #TODO use table-driven tests to reduce copypasta
  std::pair<uint64_t, uint64_t> range;

  SECTION("name version 1") {
    // Note: the end timestamp may or may not be present.
    // If present, this version sets both range values to the end timestamp.
    // As such, if the end_timestamp < start_timestamp, no error is thrown.
    SECTION("valid timestamps") {
      tiledb::sm::URI frag;

      SECTION("with end timestamp") {
        // Create fragment at timestamp 1-2
        frag = tiledb::sm::URI(
            "file:///array_name/__fragments/"
            "__44318efd44f546b18db13edc8d10805b_1_2");
      }

      SECTION("without end timestamp") {
        // Create fragment at timestamp 2
        frag = tiledb::sm::URI(
            "file:///array_name/__fragments/"
            "__44318efd44f546b18db13edc8d10805b_2");
      }

      // Check timestamp range
      FragmentID fragment_uri{frag};
      range = fragment_uri.timestamp_range();
      CHECK(range.first == 2);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI(
          "file:///array_name/__fragments/"
          "__44318efd44f546b18db13edc8d10805b_2_1");

      // Check timestamp range
      FragmentID fragment_uri{frag};
      range = fragment_uri.timestamp_range();
      CHECK(range.first == 1);
      CHECK(range.second == 1);
    }
  }

  SECTION("name version 2") {
    SECTION("valid timestamps") {
      // Create fragment at timestamp 1-2
      auto frag = tiledb::sm::URI(
          "file:///array_name/__fragments/"
          "__1_2_44318efd44f546b18db13edc8d10805b");

      // Check timestamp range
      FragmentID fragment_uri{frag};
      range = fragment_uri.timestamp_range();
      CHECK(range.first == 1);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI(
          "file:///array_name/__fragments/"
          "__2_1_44318efd44f546b18db13edc8d10805b");

      // FragmentURI creation will fail on timestamp range fetching
      REQUIRE_THROWS_WITH(
          FragmentID(frag),
          Catch::Matchers::ContainsSubstring(
              "start timestamp cannot be after end timestamp"));
    }
  }

  SECTION("name version 3") {
    SECTION("valid timestamps") {
      // Create fragment at timestamp 1-2
      auto frag = tiledb::sm::URI(
          "file:///array_name/__fragments/"
          "__1_2_44318efd44f546b18db13edc8d10805b_5");

      // Check timestamp range
      FragmentID fragment_uri{frag};
      range = fragment_uri.timestamp_range();
      CHECK(range.first == 1);
      CHECK(range.second == 2);
    }

    SECTION("invalid timestamps") {
      // Create fragment at timestamp 2-1
      auto frag = tiledb::sm::URI(
          "file:///array_name/__fragments/"
          "__2_1_44318efd44f546b18db13edc8d10805b_5");

      // FragmentID creation will fail on timestamp range fetching
      REQUIRE_THROWS_WITH(
          FragmentID(frag),
          Catch::Matchers::ContainsSubstring(
              "start timestamp cannot be after end timestamp"));
    }
  }
}