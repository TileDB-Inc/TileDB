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

#include <test/support/tdb_catch.h>
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

TEST_CASE(
    "Array directory: Commits mode populates raw delete locations",
    "[array-directory][commits-mode-del]") {
  Config cfg;
  auto logger = make_shared<Logger>(HERE(), "foo");
  ContextResources resources{cfg, logger, 1, 1, ""};
  auto& vfs = resources.vfs();

  // Setup a test array with a commits directory
  URI array_uri(arrays_dir + "/test_array_dir_commits_mode");
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
  vfs.create_dir(array_uri);
  URI commits_dir = array_uri.join_path("__commits");
  vfs.create_dir(commits_dir);

  // Create a test raw .del file (write version 12 format)
  URI del_uri =
      commits_dir.join_path("__0_0_6e1138cbb833e380c227d402c1d2fc38_22.del");
  vfs.touch(del_uri);

  // Load ArrayDirectory explicitly in COMMITS mode
  ArrayDirectory array_dir(
      resources, array_uri, 0, 1, ArrayDirectoryMode::COMMITS);

  // Verify the location map was populated
  auto locs = array_dir.delete_and_update_tiles_location();

  REQUIRE(locs.size() == 1);
  CHECK(
      locs[0].condition_marker() ==
      "__commits/"
      "__0_0_6e1138cbb833e380c227d402c1d2fc38_22.del");
  CHECK(locs[0].offset() == 0);

  vfs.remove_dir(array_uri);
}

TEST_CASE(
    "Array directory: Vacuum verification skips binary delete payloads",
    "[array-directory][vacuum-binary-skip]") {
  Config cfg;
  auto logger = make_shared<Logger>(HERE(), "foo");
  ContextResources resources{cfg, logger, 1, 1, ""};
  auto& vfs = resources.vfs();

  // Setup a test array
  URI array_uri(arrays_dir + "/test_array_dir_vacuum_skip");
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
  vfs.create_dir(array_uri);
  URI commits_dir = array_uri.join_path("__commits");
  vfs.create_dir(commits_dir);

  // Mock physical files so they exist in the directory listing
  vfs.touch(
      commits_dir.join_path("__0_0_5c336fca5fe8057791d69cad6c15e981_22.wrt"));
  vfs.touch(
      commits_dir.join_path("__1_1_6e1138cbb833e380c227d402c1d2fc38_22.del"));

  // Create a .con file containing a binary payload
  URI con_uri =
      commits_dir.join_path("__0_1_51cfc7c8956f93998a1ba2a42e7ec0d5_22.con");
  std::string wrt_uri =
      "__commits/"
      "__0_0_5c336fca5fe8057791d69cad6c15e981_22.wrt\n";
  std::string del_uri =
      "__commits/"
      "__1_1_6e1138cbb833e380c227d402c1d2fc38_22.del\n";
  // 4 bytes of binary data
  storage_size_t payload_size = 4;
  std::string binary_payload = "BEEF";

  // Pack the bytes exactly like the consolidator does
  std::vector<uint8_t> data;
  data.insert(data.end(), wrt_uri.begin(), wrt_uri.end());
  data.insert(data.end(), del_uri.begin(), del_uri.end());

  auto* size_ptr = reinterpret_cast<uint8_t*>(&payload_size);
  data.insert(data.end(), size_ptr, size_ptr + sizeof(storage_size_t));
  data.insert(data.end(), binary_payload.begin(), binary_payload.end());

  vfs.write(con_uri, data.data(), data.size());
  auto status = vfs.close_file(con_uri);
  CHECK(status.ok());

  // Create a newer, encompassing .con file.
  URI con_uri2 =
      commits_dir.join_path("__0_2_017ebf26d62191760cc867d23ccd1458_22.con");
  std::vector<uint8_t> data2 = data;
  std::string wrt_uri2 =
      "__commits/"
      "__2_2_06e30b6f9ed34137f9ac342cf6211c84_22.wrt\n";
  data2.insert(data2.end(), wrt_uri2.begin(), wrt_uri2.end());

  vfs.write(con_uri2, data2.data(), data2.size());
  status = vfs.close_file(con_uri2);
  CHECK(status.ok());

  // Load the ArrayDirectory in COMMITS mode
  ArrayDirectory array_dir(
      resources, array_uri, 0, 2, ArrayDirectoryMode::COMMITS);

  // Verify the .con file passed verification and was marked for vacuuming
  auto vacuum_uris = array_dir.consolidated_commits_uris_to_vacuum();

  REQUIRE(vacuum_uris.size() == 1);
  std::cout << vacuum_uris[0].to_string() << std::endl;
  std::cout << con_uri.to_string() << std::endl;
  CHECK(vacuum_uris[0] == con_uri);

  vfs.remove_dir(array_uri);
}
