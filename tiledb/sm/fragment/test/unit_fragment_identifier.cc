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

const std::string frag_dir{"file:///"};
struct failure_test_case {
  std::string path;
};

struct success_test_case {
  std::string path;  // input
  // outputs
  std::string name;
  timestamp_range_type timestamp_range;
  int name_version;
  format_version_t array_format_version;
  std::string uuid;
  std::optional<std::string> submillisecond;
};

TEST_CASE("FragmentID: Constructor: empty uri", "[fragment_id][empty_uri]") {
  failure_test_case failure_case{""};
  REQUIRE_THROWS(FragmentID{failure_case.path});
}

// Should fail but succeed (known defects)
TEST_CASE(
    "FragmentID: Constructor: Invalid uri",
    "[!shouldfail][fragment_id][invalid_uri]") {
  std::vector<failure_test_case> invalid_uris{{{""}, {"_"}, {"X"}}};

  std::vector<failure_test_case> empty_fields{{
      {"__"},    // 2 underscores only, no fields
      {"___"},   // 3 underscores only, all fields empty (version 1)
      {"____"},  // 4 underscores only, all fields empty (versions 2, 3)
      {"_____"}  // 5 underscores, all fields are empty (version 3)
  }};

  // (Version 1) Expects: __uuid_t
  std::vector<failure_test_case> two_fields{{
      {"__0123456789ABCDEF0123456789ABCDEF_1_"},  // __uuid_t_
      {"__1_0123456789ABCDEF0123456789ABCDEF"},   // __t_uuid
      {"___0123456789ABCDEF0123456789ABCDEF1"},   // ___uuidt
      {"_0123456789ABCDEF0123456789ABCDEF__1"},   // _uuid__t
      {"0123456789ABCDEF0123456789ABCDEF___1"}    // uuid___t
  }};

  // (Version 2, 3) Expects: __t1_t2_uuid
  std::vector<failure_test_case> three_fields{{
      {"__1_2_0123456789ABCDEF0123456789ABCDEF_"},  // __t1_t2_uuid_
      {"__1_0123456789ABCDEF0123456789ABCDEF_2"},   // __t1_uuid_t2
      {"__2_0123456789ABCDEF0123456789ABCDEF_1"},   // __t2_uuid_t1
      {"____120123456789ABCDEF0123456789ABCDEF"},   // ____t1t2uuid
      {"___1_20123456789ABCDEF0123456789ABCDEF"},   // ___t1_t2uuid
      {"__1__20123456789ABCDEF0123456789ABCDEF"},   // __t1__t2uuid
      {"_1___20123456789ABCDEF0123456789ABCDEF"},   // _t1___t2uuid
      {"1____20123456789ABCDEF0123456789ABCDEF"},   // t1____t2uuid
      {"1___2_0123456789ABCDEF0123456789ABCDEF"},   // t1___t2_uuid
      {"1__2__0123456789ABCDEF0123456789ABCDEF"},   // t1__t2__uuid
      {"1_2___0123456789ABCDEF0123456789ABCDEF"},   // t1_t2___uuid
      {"12____0123456789ABCDEF0123456789ABCDEF"},   // t1t2____uuid
  }};

  // (Version 3) Expects: __t1_t2_uuid_v
  std::vector<failure_test_case> four_fields{{
      {"__1_2_0123456789ABCDEF0123456789ABCDEF_5_"},  // __t1_t2_uuid_v_
      {"__1_2_5_0123456789ABCDEF0123456789ABCDEF"},   // __t1_t2_v_uuid
      {"__1_0123456789ABCDEF0123456789ABCDEF_2_5"},   // __t1_uuid_t2_v
      {"__0123456789ABCDEF0123456789ABCDEF_1_2_5"},   // __uuid_t1_t2_v
      {"_____120123456789ABCDEF0123456789ABCDEF5"},   // _____t1t2uuidv
      {"____1_20123456789ABCDEF0123456789ABCDEF5"},   // ____t1_t2uuidv
      {"___1__20123456789ABCDEF0123456789ABCDEF5"},   // ___t1__t2uuidv
      {"__1___20123456789ABCDEF0123456789ABCDEF5"},   // __t1___t2uuidv
      {"_1____20123456789ABCDEF0123456789ABCDEF5"},   // _t1____t2uuidv
      {"1____20123456789ABCDEF0123456789ABCDEF5"},    // t1_____t2uuidv
      {"1___2_0123456789ABCDEF0123456789ABCDEF5"},    // t1____t2_uuidv
      {"1__2__0123456789ABCDEF0123456789ABCDEF5"},    // t1__t2___uuidv
      {"1_2___0123456789ABCDEF0123456789ABCDEF5"},    // t1_t2___uuidv
      {"1_2__0123456789ABCDEF0123456789ABCDEF_5"},    // t1_t2__uuid_v
      {"1_2_0123456789ABCDEF0123456789ABCDEF__5"},    // t1_t2_uuid__v
      {"1_20123456789ABCDEF0123456789ABCDEF___5"},    // t1_t2uuid___v
  }};

  // Timestamps and uuid are identical
  std::vector<failure_test_case> uuid_timestamps{{
      {"__0123456789ABCDEF0123456789ABCDEF_"
       "0123456789ABCDEF0123456789ABCDEF"},  // version1: __uuid_t1
      {"__0123456789ABCDEF0123456789ABCDEF_"
       "0123456789ABCDEF0123456789ABCDEF_"
       "0123456789ABCDEF0123456789ABCDEF"},  // version1: __uuid_t1_t2,
                                             // versions 2,3:
                                             // __t1_t2_uuid
      {"__0123456789ABCDEF0123456789ABCDEF_"
       "0123456789ABCDEF0123456789ABCDEF_0123456789ABCDEF0123456789ABCDEF_"
       "5"},  // version3: __t1_t2_uuid_v
  }};

  std::vector<std::vector<failure_test_case>> failure_cases{
      invalid_uris,
      empty_fields,
      two_fields,
      three_fields,
      four_fields,
      uuid_timestamps};

  for (auto failure_case : failure_cases) {
    for (auto failure : failure_case) {
      auto uri = frag_dir + failure.path;
      DYNAMIC_SECTION(uri) {
        REQUIRE_THROWS(FragmentID{uri});
      }
    }
  }
}

// Should succeed and do
TEST_CASE("FragmentID: Valid uris", "[fragment_id][valid_uri]") {
  std::vector<success_test_case> success_cases{{
      {"file:///__0123456789ABCDEF0123456789ABCDEF_1",
       "__0123456789ABCDEF0123456789ABCDEF_1",
       std::pair{1, 1},
       1,
       2,
       "0123456789ABCDEF0123456789ABCDEF",
       std::nullopt},
      {"file:///__0123456789ABCDEF0123456789ABCDEF_1_2",
       "__0123456789ABCDEF0123456789ABCDEF_1_2",
       std::pair{2, 2},
       1,
       2,
       "0123456789ABCDEF0123456789ABCDEF",
       std::nullopt},
      {"file:///__0123456789ABCDEF0123456789ABCDEF_2_1",
       "__0123456789ABCDEF0123456789ABCDEF_2_1",
       std::pair{1, 1},
       1,
       2,
       "0123456789ABCDEF0123456789ABCDEF",
       std::nullopt},
      {"file:///__1_2_0123456789ABCDEF0123456789ABCDEF",
       "__1_2_0123456789ABCDEF0123456789ABCDEF",
       std::pair{1, 2},
       2,
       4,
       "0123456789ABCDEF0123456789ABCDEF",
       std::nullopt},
      {"file:///__1_2_0123456789ABCDEF0123456789ABCDEF_5",
       "__1_2_0123456789ABCDEF0123456789ABCDEF_5",
       std::pair{1, 2},
       3,
       5,
       "0123456789ABCDEF0123456789ABCDEF",
       std::nullopt},
      {"file:///__1_2_123456789ABCDEF0123456789ABCDEF0_21",
       "__1_2_123456789ABCDEF0123456789ABCDEF0_21",
       std::pair{1, 2},
       3,
       21,
       "123456789ABCDEF0123456789ABCDEF0",
       std::nullopt},
      {"file:///__1_2_23456789ABCDEF0123456789ABCDEF01_22",
       "__1_2_23456789ABCDEF0123456789ABCDEF01_22",
       std::pair{1, 2},
       3,
       22,
       "23456789ABCDEF0123456789ABCDEF01",
       "23456789"},
  }};

  for (auto success_case : success_cases) {
    auto uri = success_case.path;
    DYNAMIC_SECTION(uri) {
      FragmentID f{uri};
      CHECK(f.name() == success_case.name);
      CHECK(f.timestamp_range() == success_case.timestamp_range);
      CHECK(f.name_version() == success_case.name_version);
      CHECK(f.array_format_version() == success_case.array_format_version);
    }
  }
}
