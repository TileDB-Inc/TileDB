/**
 * @file   hdfs-unit-filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests for HDFS API filesystem functions.
 */

#ifdef HAVE_HDFS

#include "catch.hpp"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/filesystem/uri.h"

#include <fstream>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::sm::hdfs;

TEST_CASE("Test HDFS filesystem", "[hdfs]") {
  Config config;
  HDFS hdfs;

  Status st = hdfs.init(config);
  REQUIRE(st.ok());

  bool is_dir;
  st = hdfs.is_dir(URI("hdfs:///tiledb_test"), &is_dir);
  CHECK(st.ok());
  if (is_dir) {
    st = hdfs.remove_dir(URI("hdfs:///tiledb_test"));
    CHECK(st.ok());
  }

  st = hdfs.create_dir(URI("hdfs:///tiledb_test"));
  CHECK(st.ok());

  CHECK(hdfs.is_dir(URI("hdfs:///tiledb_test"), &is_dir).ok());
  CHECK(is_dir);

  st = hdfs.create_dir(URI("hdfs:///tiledb_test"));
  CHECK(!st.ok());

  st = hdfs.touch(URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());

  bool is_file;
  CHECK(hdfs.is_file(URI("hdfs:///tiledb_test_file"), &is_file).ok());
  CHECK(is_file);

  st = hdfs.remove_file(URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs.touch(URI("hdfs:///tiledb_test/tiledb_test_file"));
  CHECK(st.ok());

  uint64_t buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (uint64_t i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = hdfs.write(
      URI("hdfs:///tiledb_test/tiledb_test_file"), write_buffer, buffer_size);
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = hdfs.read(
      URI("hdfs:///tiledb_test/tiledb_test_file"), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  st = hdfs.read(
      URI("hdfs:///tiledb_test/tiledb_test_file"), 11, read_buffer, 26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; ++i) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  std::vector<std::string> paths;
  st = hdfs.ls(URI("hdfs:///"), &paths);
  CHECK(st.ok());
  CHECK(paths.size() > 0);

  // ls_with_sizes
  // Dir structure:
  // ...../subdir
  // ...../subdir/file
  // ...../subdir/subsubdir

  std::string subdir = "hdfs://localhost:9000/tiledb_test/subdir";
  std::string file = subdir + "/file";
  std::string subsubdir = subdir + "/subsubdir";

  CHECK(hdfs.create_dir(URI(subdir)).ok());
  CHECK(hdfs.create_dir(URI(subsubdir)).ok());
  CHECK(hdfs.touch(URI(file)).ok());

  std::string s = "abcdef";
  CHECK(hdfs.write(URI(file), s.data(), s.size()).ok());

  auto&& [status, rv] = hdfs.ls_with_sizes(URI(subdir));
  auto children = *rv;
  REQUIRE(status.ok());

  REQUIRE(children.size() == 2);
  CHECK(children[0].path().native() == file);
  CHECK(children[1].path().native() == subsubdir.substr(0, subsubdir.size()));

  CHECK(children[0].file_size() == s.size());
  // Directories don't get a size
  CHECK(children[1].file_size() == 0);
  // Cleanup
  CHECK(hdfs.remove_dir(URI(subdir)).ok());

  uint64_t nbytes = 0;
  st = hdfs.file_size(URI("hdfs:///tiledb_test/tiledb_test_file"), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st = hdfs.remove_file(URI("hdfs:///tiledb_test/i_dont_exist"));
  CHECK(!st.ok());

  st = hdfs.remove_file(URI("hdfs:///tiledb_test/tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs.remove_dir(URI("hdfs:///tiledb_test"));
  CHECK(st.ok());

  st = hdfs.disconnect();
  CHECK(st.ok());
}

#endif
