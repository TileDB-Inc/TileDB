/**
 * @file unit_array_directory.cc
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
 * Tests the `ArrayDirectory` class.
 */

#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/storage_manager/context_resources.h"

#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::sm;

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

namespace tiledb::sm {
class WhiteboxArrayDirectory {
 public:
  bool timestamps_overlap(
      ArrayDirectory& array_directory,
      const std::pair<uint64_t, uint64_t>& fragment_timestamp_range,
      const bool consolidation_with_timestamps) const {
    return array_directory.timestamps_overlap(
        fragment_timestamp_range, consolidation_with_timestamps);
  }
};
}  // namespace tiledb::sm

TEST_CASE(
    "Array directory: Test timestamp overlap",
    "[array-directory][timestamp-overlap]") {
  WhiteboxArrayDirectory wb_array_dir;
  Config cfg;
  auto logger = make_shared<Logger>(HERE(), "foo");
  ContextResources resources{cfg, logger, 1, 1, ""};
  ArrayDirectory array_dir(
      resources,
      URI(arrays_dir + "/dense_array_v1_3_0"),
      2,
      4,
      ArrayDirectoryMode::READ);

  // Only full overlap should be included for regular fragments.

  // Fragment before open timestamps.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(0, 0), false));

  // Fragment after open timestamps.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(5, 5), false));

  // Only begin included.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(3, 5), false));

  // Only end included.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(1, 3), false));

  // Begin and end not included.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(0, 5), false));

  // Begin and end included.
  CHECK(wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(2, 4), false));

  // Partial overlap should be included for fragments consolidated with
  // timestamps.

  // Fragment before open timestamps.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(0, 0), true));

  // Fragment after open timestamps.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(5, 5), true));

  // Only begin included.
  CHECK(wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(3, 5), true));

  // Only end included.
  CHECK(wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(1, 3), true));

  // Begin and end not included.
  CHECK(!wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(0, 5), false));

  // Begin and end included.
  CHECK(wb_array_dir.timestamps_overlap(
      array_dir, std::pair<uint64_t, uint64_t>(2, 4), true));
}

TEST_CASE("Array directory: Vac file fix", "[array-directory][vac-file-fix]") {
  CHECK(
      ArrayDirectory::get_full_vac_uri(
          "base/", "file://not/related/__fragments/test.vac") ==
      "base/__fragments/test.vac");
  CHECK(
      ArrayDirectory::get_full_vac_uri(
          "base/", "file://not/related/__meta/test.vac") ==
      "base/__meta/test.vac");
  CHECK(
      ArrayDirectory::get_full_vac_uri(
          "base/", "file://not/related/test.vac") == "base/test.vac");
}
