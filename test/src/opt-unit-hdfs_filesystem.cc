/**
 * @file   opt-unit_hdfs_fisystem.cc
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

#include "catch.hpp"

#include <fstream>
#include <iostream>

#include <hdfs_filesystem.h>
#include <status.h>

using namespace tiledb;

TEST_CASE("Test HDFS filesystem") {
  hdfsFS fs;

  Status st = vfs::hdfs::connect(fs);
  REQUIRE(st.ok());

  st = vfs::hdfs::create_dir("/tiledb_test_dir", fs);
  CHECK(st.ok());

  CHECK(vfs::hdfs::is_dir("/tiledb_test_dir", fs));

  st = vfs::hdfs::create_dir("/tiledb_test_dir", fs);
  CHECK(!st.ok());

  st = vfs::hdfs::create_file("/tiledb_test_file", fs);
  CHECK(st.ok());
  CHECK(vfs::hdfs::is_file("/tiledb_test_file", fs));

  st = vfs::hdfs::delete_file("/tiledb_test_file", fs);
  CHECK(st.ok());

  st = vfs::hdfs::create_file("/tiledb_test_dir/tiledb_test_file", fs);
  CHECK(st.ok());

  tSize buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = vfs::hdfs::write_to_file(
      "/tiledb_test_dir/tiledb_test_file", write_buffer, buffer_size, fs);
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = vfs::hdfs::read_from_file(
      "/tiledb_test_dir/tiledb_test_file", 0, read_buffer, 26, fs);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = vfs::hdfs::read_from_file(
      "/tiledb_test_dir/tiledb_test_file", 11, read_buffer, 26, fs);
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
  st = vfs::hdfs::ls("/", paths, fs);
  CHECK(st.ok());
  CHECK(paths.size() > 0);

  std::vector<std::string> files;
  st = vfs::hdfs::ls_files("/tiledb_test_dir", files, fs);
  CHECK(st.ok());
  CHECK(files.size() == 1);

  std::vector<std::string> dirs;
  st = vfs::hdfs::ls_dirs("/tiledb_test_dir", dirs, fs);
  CHECK(st.ok());
  CHECK(dirs.size() == 0);

  st = vfs::hdfs::create_dir("/tiledb_test_dir/tiledb_test_dir", fs);
  CHECK(st.ok());
  st = vfs::hdfs::ls_dirs("/tiledb_test_dir", dirs, fs);
  CHECK(st.ok());
  CHECK(dirs.size() == 1);

  size_t nbytes;
  st = vfs::hdfs::filesize("/tiledb_test_dir/tiledb_test_file", &nbytes, fs);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st = vfs::hdfs::delete_dir("/tiledb_test_dir/tiledb_test_dir", fs);
  CHECK(st.ok());

  st = vfs::hdfs::delete_file("/tiledb_test_dir/tiledb_test_file", fs);
  CHECK(st.ok());

  st = vfs::hdfs::delete_dir("/tiledb_test_dir", fs);
  CHECK(st.ok());

  st = vfs::hdfs::disconnect(fs);
  CHECK(st.ok());
}
