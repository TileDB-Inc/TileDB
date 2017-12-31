/**
 * @file   hdfs-unit-filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
#include "hdfs_filesystem.h"
#include "status.h"

#include <cassert>
#include <fstream>
#include <iostream>

using namespace tiledb;

struct HDFSFx {
  HDFSFx() {
    bool success = false;
    if (path_exists("/tiledb_test_dir")) {
      success = remove_path("/tiledb_test_dir");
      assert(success);
    }
    if (path_exists("/tiledb_test_file")) {
      success = remove_path("/tiledb_test_file");
      assert(success);
    }
  }

  ~HDFSFx() {
    bool success = remove_path("/tiledb_test_dir");
    assert(success);
  }

  bool path_exists(std::string path) {
    std::string cmd = std::string("hadoop fs -test -e ") + path;
    return (system(cmd.c_str()) == 0);
  }

  bool remove_path(std::string path) {
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
    return (system(cmd.c_str()) == 0);
  }
};

TEST_CASE_METHOD(HDFSFx, "Test HDFS filesystem", "[hdfs]") {
  Status st;
  hdfsFS fs;

  st = hdfs::connect(fs);
  REQUIRE(st.ok());

  st = hdfs::create_dir(fs, URI("hdfs:///tiledb_test_dir"));
  CHECK(st.ok());

  CHECK(hdfs::is_dir(fs, URI("hdfs:///tiledb_test_dir")));

  st = hdfs::create_dir(fs, URI("hdfs:///tiledb_test_dir"));
  CHECK(!st.ok());

  st = hdfs::create_file(fs, URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());
  CHECK(hdfs::is_file(fs, URI("hdfs:///tiledb_test_file")));

  st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs::create_file(fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"));
  CHECK(st.ok());

  tSize buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = hdfs::write_to_file(
      fs,
      URI("hdfs:///tiledb_test_dir/tiledb_test_file"),
      write_buffer,
      buffer_size);
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = hdfs::read_from_file(
      fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = hdfs::read_from_file(
      fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), 11, read_buffer, 26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; ++i) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  std::vector<std::string> paths;
  st = hdfs::ls(fs, URI("hdfs:///"), &paths);
  CHECK(st.ok());
  CHECK(paths.size() > 0);

  uint64_t nbytes = 0;
  st = hdfs::file_size(
      fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st = hdfs::remove_path(fs, URI("hdfs:///tiledb_test_dir/i_dont_exist"));
  CHECK(!st.ok());

  st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs::remove_path(fs, URI("hdfs:///tiledb_test_dir"));
  CHECK(st.ok());

  st = hdfs::disconnect(fs);
  CHECK(st.ok());
}

#endif
