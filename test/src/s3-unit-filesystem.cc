/**
 * @file   s3-unit-filesystem.cc
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
 * Tests for S3 API filesystem functions.
 */

#include "catch.hpp"
#include "s3.h"

#include <fstream>

using namespace tiledb;

struct S3Fx {
  tiledb::S3 s3_;
};

TEST_CASE_METHOD(S3Fx, "Test S3 filesystem", "[s3]") {
  S3::S3Config s3_config;
  Status st = s3_.connect(s3_config);
  REQUIRE(st.ok());

  std::string bucket = "tiledb";

  if (!s3_.bucket_exists(bucket.c_str())) {
    st = s3_.create_bucket(bucket.c_str());
    CHECK(st.ok());
  }

  st = s3_.create_dir(URI("s3://" + bucket + "/tiledb_test_dir"));
  CHECK(st.ok());
  st = s3_.create_dir(URI("s3://" + bucket + "/tiledb_test_dir/folder"));
  CHECK(st.ok());
  st = s3_.create_dir(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/subfolder"));
  CHECK(st.ok());

  int buffer_size = 5 * 1024 * 1024;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = s3_.write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"),
      write_buffer,
      buffer_size);
  CHECK(st.ok());

  int buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (int i = 0; i < buffer_size_small; i++) {
    write_buffer_small[i] = 'a' + (i % 26);
  }

  st = s3_.write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"),
      write_buffer_small,
      buffer_size_small);
  CHECK(st.ok());

  st = s3_.write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/smallfile"),
      write_buffer_small,
      buffer_size_small);
  CHECK(st.ok());

  st = s3_.flush_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"));
  CHECK(st.ok());

  st = s3_.flush_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/smallfile"));
  CHECK(st.ok());

  uint64_t nbytes = 0;
  st = s3_.file_size(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == (buffer_size + buffer_size_small));
  nbytes = 0;
  st = s3_.file_size(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/smallfile"), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == (buffer_size_small));

  st = s3_.create_dir(URI("s3://" + bucket + "/tiledb_test_dir/folder2"));
  CHECK(st.ok());
  bool is_dir = s3_.is_dir(URI("s3://" + bucket + "/tiledb_test_dir/folder2"));
  CHECK(is_dir);

  st = s3_.write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder2/file1"),
      write_buffer_small,
      buffer_size_small);
  CHECK(st.ok());
  st = s3_.write_to_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder2/file2"),
      write_buffer_small,
      buffer_size_small);
  CHECK(st.ok());

  st = s3_.flush_file(URI("s3://" + bucket + "/tiledb_test_dir/folder2/file1"));
  CHECK(st.ok());
  st = s3_.flush_file(URI("s3://" + bucket + "/tiledb_test_dir/folder2/file2"));
  CHECK(st.ok());

  std::vector<std::string> paths;
  st = s3_.ls(URI("s3://" + bucket + "/tiledb_test_dir"), &paths);
  CHECK(st.ok());
  CHECK(paths.size() == 2);
  std::vector<std::string> paths1;
  st = s3_.ls(URI("s3://" + bucket + "/tiledb_test_dir/folder"), &paths1);
  CHECK(st.ok());
  CHECK(paths1.size() == 3);
  std::vector<std::string> paths2;
  st = s3_.ls(URI("s3://" + bucket + "/tiledb_test_dir/folder2"), &paths2);
  CHECK(st.ok());
  CHECK(paths2.size() == 2);

  auto read_buffer = new char[26];
  st = s3_.read_from_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"),
      0,
      read_buffer,
      26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = s3_.read_from_file(
      URI("s3://" + bucket + "/tiledb_test_dir/folder/largefile"),
      11,
      read_buffer,
      26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = s3_.remove_path(URI("s3://" + bucket + "/tiledb_test_dir/folder"));
  CHECK(st.ok());
  is_dir = s3_.is_dir(URI("s3://" + bucket + "/tiledb_test_dir/folder"));
  CHECK(!is_dir);

  std::vector<std::string> paths3;
  st = s3_.ls(URI("s3://" + bucket + "/tiledb_test_dir"), &paths3);
  CHECK(st.ok());
  CHECK(paths3.size() == 1);

  //  st = s3_.disconnect();
  //  CHECK(st.ok());
}
