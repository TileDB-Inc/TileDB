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
 * Test for HDFS API filesystem functions.
 */

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>

#include <status.h>
#include <fstream>
#include <iostream>

#include "../../core/include/vfs/hdfs_filesystem.h"
#include "catch.hpp"
#include "hdfs.h"
#include "tiledb.h"

using namespace tiledb;

struct LibHDFSFfilesystemx {};

TEST_CASE_METHOD(LibHDFSFfilesystemx, "Test hdfs filesystem") {
  hdfsFS fs;

  Status st = vfs::hdfs::connect(fs);
  CHECK(st.ok());

  st = vfs::hdfs::create_dir("/test_dir", fs);
  CHECK(st.ok());

  CHECK(vfs::hdfs::is_dir("/test_dir", fs));

  st = vfs::hdfs::create_dir("/test_dir", fs);
  CHECK(!st.ok());

  st = vfs::hdfs::create_file("/test_file", fs);
  CHECK(st.ok());
  CHECK(vfs::hdfs::is_file("/test_file", fs));

  st = vfs::hdfs::delete_file("/test_file", fs);
  CHECK(st.ok());

  st = vfs::hdfs::create_file("/test_file", fs);
  CHECK(st.ok());

  // data to be written to the file
  tSize bufferSize = 100000;
  char *buffer = (char *)malloc(sizeof(char) * bufferSize);
  if (buffer == NULL) {
    fprintf(stderr, "Could not allocate buffer of size %d\n", bufferSize);
    exit(-1);
  }
  for (int i = 0; i < bufferSize; ++i) {
    buffer[i] = 'a' + (i % 26);
  }

  st = vfs::hdfs::write_to_file("/test_file", buffer, bufferSize, fs);
  CHECK(st.ok());

  char *read_buffer = (char *)malloc(sizeof(char) * 26);
  if (read_buffer == NULL) {
    fprintf(stderr, "Could not allocate buffer of size %d\n", 26);
    exit(-1);
  }

  st = vfs::hdfs::read_from_file("/test_file", 0, read_buffer, 26, fs);
  CHECK(st.ok());

  for (int i = 0; i < 26; ++i) {
    CHECK(read_buffer[i] == (char)('a' + i));
  }

  st = vfs::hdfs::read_from_file("/test_file", 11, read_buffer, 26, fs);
  CHECK(st.ok());

  for (int i = 0; i < 26; ++i) {
    CHECK(read_buffer[i] == (char)('a' + (i + 11) % 26));
  }

  std::vector<std::string> paths;
  st = vfs::hdfs::ls("/", paths, fs);
  CHECK(st.ok());

  for (std::vector<std::string>::const_iterator i = paths.begin();
       i != paths.end();
       ++i) {
    fprintf(stderr, "%s\n", i->c_str());
  }

  std::vector<std::string> files;
  st = vfs::hdfs::ls_files("/", files, fs);
  CHECK(st.ok());

  for (std::vector<std::string>::const_iterator i = files.begin();
       i != files.end();
       ++i) {
    fprintf(stderr, "File %s\n", i->c_str());
  }

  std::vector<std::string> dirs;
  st = vfs::hdfs::ls_dirs("/", dirs, fs);
  CHECK(st.ok());

  for (std::vector<std::string>::const_iterator i = dirs.begin();
       i != dirs.end();
       ++i) {
    fprintf(stderr, "Dir %s\n", i->c_str());
  }

  size_t nbytes;
  st = vfs::hdfs::filesize("/test_file", &nbytes, fs);
  CHECK(st.ok());
  CHECK(nbytes == (size_t)bufferSize);
  fprintf(stderr, "Size %ld\n", nbytes);

  st = vfs::hdfs::delete_dir("/test_dir", fs);
  CHECK(st.ok());

  st = vfs::hdfs::delete_file("/test_file", fs);
  CHECK(st.ok());

  st = vfs::hdfs::disconnect(fs);
  CHECK(st.ok());
}
