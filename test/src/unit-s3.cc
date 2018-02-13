/**
 * @file   unit-s3.cc
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
 * Tests for S3 API filesystem functions.
 */

#ifdef HAVE_S3

#include "catch.hpp"
#include "tiledb/sm/filesystem/s3.h"
#include "tiledb/sm/misc/utils.h"

#include <fstream>
#include <thread>

using namespace tiledb::sm;

struct S3Fx {
  const std::string S3_PREFIX = "s3://";
  const tiledb::sm::URI S3_BUCKET =
      tiledb::sm::URI(S3_PREFIX + random_bucket_name("tiledb") + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";
  tiledb::sm::S3 s3_;

  S3Fx();
  ~S3Fx();

  static std::string random_bucket_name(const std::string& prefix);
};

S3Fx::S3Fx() {
  // Connect
  S3::S3Config s3_config;
  s3_config.endpoint_override_ = "localhost:9999";
  REQUIRE(s3_.connect(s3_config).ok());

  // Create bucket
  if (s3_.is_bucket(S3_BUCKET))
    REQUIRE(s3_.delete_bucket(S3_BUCKET).ok());

  REQUIRE(!s3_.is_bucket(S3_BUCKET));
  REQUIRE(s3_.create_bucket(S3_BUCKET).ok());

  // Check if bucket is empty
  bool is_empty;
  REQUIRE(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  CHECK(is_empty);
}

S3Fx::~S3Fx() {
  // Empty bucket
  bool is_empty;
  CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  if (!is_empty) {
    CHECK(s3_.empty_bucket(S3_BUCKET).ok());
    CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
    CHECK(is_empty);
  }

  // Delete bucket
  CHECK(s3_.delete_bucket(S3_BUCKET).ok());
}

std::string S3Fx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(
    S3Fx, "Test S3 filesystem, directory/file management", "[s3]") {
  /* Create the following directory/file hierarchy:
   *
   * TEST_DIR
   *    |_ dir1
   *    |   |_ subdir1
   *    |   |     |_ file1
   *    |   |     |_ file2
   *    |   |_ subdir2
   *    |_ dir2
   *    |_ file3
   */
  CHECK(s3_.create_dir(URI(TEST_DIR)).ok());
  CHECK(s3_.is_dir(URI(TEST_DIR)));
  auto dir1 = TEST_DIR + "dir1/";

  // Check that bucket is not empty after the creation of directories
  bool is_empty;
  CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  CHECK(!is_empty);

  // Continue building the hierarchy
  CHECK(s3_.create_dir(URI(dir1)).ok());
  CHECK(s3_.is_dir(URI(dir1)));
  auto dir2 = TEST_DIR + "dir2/";
  CHECK(s3_.create_dir(URI(dir2)).ok());
  CHECK(s3_.is_dir(URI(dir2)));
  auto subdir1 = dir1 + "subdir1";
  CHECK(s3_.create_dir(URI(subdir1)).ok());
  CHECK(s3_.is_dir(URI(subdir1)));
  auto subdir2 = dir1 + "subdir2";
  CHECK(s3_.create_dir(URI(subdir2)).ok());
  CHECK(s3_.is_dir(URI(subdir2)));
  auto file1 = subdir1 + "/file1";
  CHECK(s3_.create_file(URI(file1)).ok());
  CHECK(s3_.is_file(URI(file1)));
  auto file2 = subdir1 + "/file2";
  CHECK(s3_.create_file(URI(file2)).ok());
  CHECK(s3_.is_file(URI(file2)));
  auto file3 = TEST_DIR + "file3";
  CHECK(s3_.create_file(URI(file3)).ok());
  CHECK(s3_.is_file(URI(file3)));

  // Check invalid directory and file
  CHECK(!s3_.is_dir(URI(TEST_DIR + "foo")));
  CHECK(!s3_.is_file(URI(TEST_DIR + "foo")));

  // List directories
  std::vector<std::string> paths;
  CHECK(s3_.ls(URI(TEST_DIR), &paths).ok());
  CHECK(paths.size() == 3);
  paths.clear();
  CHECK(s3_.ls(URI(dir1), &paths).ok());
  CHECK(paths.size() == 2);
  paths.clear();
  CHECK(s3_.ls(URI(dir2), &paths).ok());
  CHECK(paths.size() == 0);

  // Move files
  auto new_file3 = subdir1 + "/new_file3";
  CHECK(s3_.move_path(URI(file3), URI(new_file3)).ok());
  CHECK(s3_.is_file(URI(new_file3)));
  CHECK(!s3_.is_file(URI(file3)));

  // Move directories
  auto new_dir1 = TEST_DIR + "new_dir1";
  CHECK(s3_.move_path(URI(dir1), URI(new_dir1)).ok());

  /* The hierarchy should now be
   *
   * TEST_DIR
   *    |_ new_dir1
   *    |   |_ subdir1
   *    |   |     |_ file1
   *    |   |     |_ file2
   *    |   |     |_ new_file3
   *    |   |_ subdir2
   *    |_ dir2
   */
  CHECK(!s3_.is_dir(URI(dir1)));
  CHECK(!s3_.is_dir(URI(subdir1)));
  CHECK(!s3_.is_dir(URI(subdir2)));
  CHECK(!s3_.is_file(URI(file1)));
  CHECK(!s3_.is_file(URI(file2)));
  CHECK(!s3_.is_file(URI(new_file3)));
  CHECK(s3_.is_dir(URI(new_dir1)));
  CHECK(s3_.is_dir(URI(new_dir1 + "/subdir1")));
  CHECK(s3_.is_dir(URI(new_dir1 + "/subdir2")));
  CHECK(s3_.is_file(URI(new_dir1 + "/subdir1/file1")));
  CHECK(s3_.is_file(URI(new_dir1 + "/subdir1/file2")));
  CHECK(s3_.is_file(URI(new_dir1 + "/subdir1/new_file3")));

  // Remove files
  CHECK(s3_.remove_file(URI(new_dir1 + "/subdir1/new_file3")).ok());
  CHECK(!s3_.is_file(URI(new_dir1 + "/subdir1/new_file3")));

  // Remove directories
  CHECK(s3_.remove_path(URI(new_dir1 + "/")).ok());
  CHECK(!s3_.is_file(URI(new_dir1 + "/subdir1/file1")));
  CHECK(!s3_.is_file(URI(new_dir1 + "/subdir1/file2")));
  CHECK(!s3_.is_dir(URI(new_dir1 + "/subdir1/")));
  CHECK(!s3_.is_dir(URI(new_dir1 + "/subdir2/")));
  CHECK(!s3_.is_dir(URI(new_dir1)));
}

TEST_CASE_METHOD(S3Fx, "Test S3 filesystem, file I/O", "[s3]") {
  // Prepare buffers
  uint64_t buffer_size = 5 * 1024 * 1024;
  auto write_buffer = new char[buffer_size];
  for (uint64_t i = 0; i < buffer_size; i++)
    write_buffer[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  auto largefile = TEST_DIR + "largefile";
  CHECK(s3_.write(URI(largefile), write_buffer, buffer_size).ok());
  CHECK(s3_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  CHECK(s3_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the files do not exist
  CHECK(!s3_.is_file(URI(largefile)));
  CHECK(!s3_.is_file(URI(smallfile)));

  // Flush the files
  CHECK(s3_.flush_file(URI(largefile)).ok());
  CHECK(s3_.flush_file(URI(smallfile)).ok());

  // After flushing, the files exist
  CHECK(s3_.is_file(URI(largefile)));
  CHECK(s3_.is_file(URI(smallfile)));

  // Get file sizes
  uint64_t nbytes = 0;
  CHECK(s3_.file_size(URI(largefile), &nbytes).ok());
  CHECK(nbytes == (buffer_size + buffer_size_small));
  CHECK(s3_.file_size(URI(smallfile), &nbytes).ok());
  CHECK(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  CHECK(s3_.read(URI(largefile), 0, read_buffer, 26).ok());
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Read from a different offset
  CHECK(s3_.read(URI(largefile), 11, read_buffer, 26).ok());
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);
}

#endif
