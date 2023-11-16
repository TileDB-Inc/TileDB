/**
 * @file unit_posix_directory_entries.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests for the posix directory entries class.
 */

#include <test/support/tdb_catch.h>
#include "../posix_directory_entries.h"

#include <iostream>

using namespace tiledb::sm;

#ifndef _WIN32

static const std::string arrays_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays";

TEST_CASE(
    "Posix directory entries: invalid directory",
    "[posix][directory-entries]") {
  CHECK_THROWS_WITH(
      PosixDirectoryEntries(arrays_dir + "1"),
      Catch::Matchers::ContainsSubstring("Cannot list files in directory"));
}

TEST_CASE(
    "Posix directory entries: directories", "[posix][directory-entries]") {
  PosixDirectoryEntries entries(arrays_dir);
  std::unordered_set<std::string> expected_dirs = {
      "dense_array_v1_3_0", "non_split_coords_v1_4_0"};
  size_t found = 0;
  for (size_t i = 0; i < entries.size(); i++) {
    if (expected_dirs.count(entries[i].d_name) != 0 &&
        entries[i].d_type == DT_DIR) {
      found++;
    }
  }

  CHECK(found == 2);
}

TEST_CASE("Posix directory entries: files", "[posix][directory-entries]") {
  PosixDirectoryEntries entries(arrays_dir + "/dense_array_v1_3_0");
  std::unordered_set<std::string> expected_files = {
      "__array_schema.tdb", "__lock.tdb"};
  size_t found = 0;
  for (size_t i = 0; i < entries.size(); i++) {
    if (expected_files.count(entries[i].d_name) != 0 &&
        entries[i].d_type == DT_REG) {
      found++;
    }
  }

  CHECK(found == 2);
}

#endif  // !_WIN32
