/**
 * @file   hdfs-unit-filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/filesystem/hdfs_filesystem.h"
#include "tiledb/sm/storage_manager/config.h"

#include <fstream>
#include <iostream>

using namespace tiledb::sm;

TEST_CASE("Test HDFS filesystem", "[hdfs]") {
  hdfsFS fs;
  const Config::HDFSParams config;

  Status st = hdfs::connect(fs, config);
  REQUIRE(st.ok());

  if (hdfs::is_dir(fs, URI("hdfs:///tiledb_test"))) {
    st = hdfs::remove_dir(fs, URI("hdfs:///tiledb_test"));
    CHECK(st.ok());
  }

  st = hdfs::create_dir(fs, URI("hdfs:///tiledb_test"));
  CHECK(st.ok());

  CHECK(hdfs::is_dir(fs, URI("hdfs:///tiledb_test")));

  st = hdfs::create_dir(fs, URI("hdfs:///tiledb_test"));
  CHECK(!st.ok());

  st = hdfs::touch(fs, URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());
  CHECK(hdfs::is_file(fs, URI("hdfs:///tiledb_test_file")));

  st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs::touch(fs, URI("hdfs:///tiledb_test/tiledb_test_file"));
  CHECK(st.ok());

  tSize buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = hdfs::write(
      fs,
      URI("hdfs:///tiledb_test/tiledb_test_file"),
      write_buffer,
      buffer_size);
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = hdfs::read(
      fs, URI("hdfs:///tiledb_test/tiledb_test_file"), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  st = hdfs::read(
      fs, URI("hdfs:///tiledb_test/tiledb_test_file"), 11, read_buffer, 26);
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
  st = hdfs::ls(fs, URI("hdfs:///"), &paths);
  CHECK(st.ok());
  CHECK(paths.size() > 0);

  uint64_t nbytes = 0;
  st =
      hdfs::file_size(fs, URI("hdfs:///tiledb_test/tiledb_test_file"), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test/i_dont_exist"));
  CHECK(!st.ok());

  st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test/tiledb_test_file"));
  CHECK(st.ok());

  st = hdfs::remove_dir(fs, URI("hdfs:///tiledb_test"));
  CHECK(st.ok());

  st = hdfs::disconnect(fs);
  CHECK(st.ok());
}

#endif
