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

#ifdef HAVE_S3

#include "catch.hpp"
#include "s3.h"
#include "utils.h"

#include <fstream>
#include <thread>

using namespace tiledb;

struct S3Fx {
  tiledb::S3 s3_;
  const std::string S3_PREFIX = "s3://";
  const tiledb::URI S3_BUCKET =
      tiledb::URI(S3_PREFIX + random_bucket_name("tiledb") + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";

  static std::string random_bucket_name(const std::string& prefix);
};

std::string S3Fx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(S3Fx, "Test S3 filesystem", "[s3]") {
  S3::S3Config s3_config;
  s3_config.endpoint_override_ = "localhost:9999";
  Status st = s3_.connect(s3_config);
  REQUIRE(st.ok());

  if (!s3_.is_bucket(S3_BUCKET)) {
    st = s3_.create_bucket(S3_BUCKET);
    CHECK(st.ok());
  }

  st = s3_.create_dir(URI(TEST_DIR));
  CHECK(st.ok());
  auto folder = TEST_DIR + "folder/";
  st = s3_.create_dir(URI(folder));
  CHECK(st.ok());
  st = s3_.create_dir(URI(TEST_DIR + "subfolder"));
  CHECK(st.ok());

  int buffer_size = 5 * 1024 * 1024;
  auto write_buffer = new char[buffer_size];
  auto largefile = folder + "largefile";
  for (int i = 0; i < buffer_size; i++)
    write_buffer[i] = (char)('a' + (i % 26));
  st = s3_.write(URI(largefile), write_buffer, buffer_size);
  CHECK(st.ok());

  int buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (int i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  st = s3_.write(URI(largefile), write_buffer_small, buffer_size_small);
  CHECK(st.ok());

  auto smallfile = folder + "smallfile";
  st = s3_.write(URI(smallfile), write_buffer_small, buffer_size_small);
  CHECK(st.ok());

  st = s3_.flush_file(URI(largefile));
  CHECK(st.ok());

  st = s3_.flush_file(URI(smallfile));
  CHECK(st.ok());

  uint64_t nbytes = 0;
  st = s3_.file_size(URI(largefile), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == (buffer_size + buffer_size_small));
  nbytes = 0;
  st = s3_.file_size(URI(smallfile), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == (buffer_size_small));

  auto folder2 = TEST_DIR + "folder2/";
  st = s3_.create_dir(URI(folder2));
  CHECK(st.ok());
  CHECK(s3_.is_dir(URI(folder2)));

  auto file1 = folder2 + "file1";
  auto file2 = folder2 + "file2";
  st = s3_.write(URI(file1), write_buffer_small, buffer_size_small);
  CHECK(st.ok());
  st = s3_.write(URI(file2), write_buffer_small, buffer_size_small);
  CHECK(st.ok());

  st = s3_.flush_file(URI(file1));
  CHECK(st.ok());
  st = s3_.flush_file(URI(file2));
  CHECK(st.ok());

  std::vector<std::string> paths;
  st = s3_.ls(URI(TEST_DIR), &paths);
  CHECK(st.ok());
  CHECK(paths.size() == 3);
  std::vector<std::string> paths1;
  st = s3_.ls(URI(folder), &paths1);
  CHECK(st.ok());
  CHECK(paths1.size() == 2);
  std::vector<std::string> paths2;
  st = s3_.ls(URI(folder2), &paths2);
  CHECK(st.ok());
  CHECK(paths2.size() == 2);

  auto read_buffer = new char[26];
  st = s3_.read(URI(largefile), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  st = s3_.read(URI(largefile), 11, read_buffer, 26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  st = s3_.remove_path(URI(folder));
  CHECK(st.ok());
  CHECK(!s3_.is_dir(URI(folder)));

  std::vector<std::string> paths3;
  st = s3_.ls(URI(TEST_DIR), &paths3);
  CHECK(st.ok());
  CHECK(paths3.size() == 2);

  st = s3_.remove_path(URI(TEST_DIR));
  CHECK(st.ok());

  if (s3_.is_bucket(S3_BUCKET)) {
    st = s3_.delete_bucket(S3_BUCKET);
    CHECK(st.ok());
  }
}

#endif
