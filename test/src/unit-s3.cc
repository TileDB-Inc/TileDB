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
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/storage_manager/config.h"

#include <fstream>
#include <thread>

using namespace tiledb::sm;

struct S3Fx {
  const std::string S3_PREFIX = "s3://";
  const tiledb::sm::URI S3_BUCKET =
      tiledb::sm::URI(S3_PREFIX + random_bucket_name("tiledb") + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";
  tiledb::sm::S3 s3_;
  ThreadPool thread_pool_;

  S3Fx();
  ~S3Fx();

  static std::string random_bucket_name(const std::string& prefix);
};

S3Fx::S3Fx() {
  // Connect
  S3::S3Config s3_config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  s3_config.endpoint_override_ = "localhost:9999";
  s3_config.scheme_ = "http";
  s3_config.use_virtual_addressing_ = false;
#endif
  REQUIRE(s3_.init(s3_config, &thread_pool_).ok());

  // Create bucket
  if (s3_.is_bucket(S3_BUCKET))
    REQUIRE(s3_.remove_bucket(S3_BUCKET).ok());

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
  CHECK(s3_.remove_bucket(S3_BUCKET).ok());
}

std::string S3Fx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(S3Fx, "Test S3 filesystem, file management", "[s3]") {
  /* Create the following file hierarchy:
   *
   * TEST_DIR/dir/subdir/file1
   * TEST_DIR/dir/subdir/file2
   * TEST_DIR/dir/file3
   * TEST_DIR/file4
   * TEST_DIR/file5
   */
  auto dir = TEST_DIR + "dir/";
  auto dir2 = TEST_DIR + "dir2/";
  auto subdir = dir + "subdir/";
  auto file1 = subdir + "file1";
  auto file2 = subdir + "file2";
  auto file3 = dir + "file3";
  auto file4 = TEST_DIR + "file4";
  auto file5 = TEST_DIR + "file5";
  auto file6 = TEST_DIR + "file6";

  // Check that bucket is empty
  bool is_empty;
  CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  CHECK(is_empty);

  // Continue building the hierarchy
  CHECK(s3_.touch(URI(file1)).ok());
  CHECK(s3_.is_object(URI(file1)));
  CHECK(s3_.touch(URI(file2)).ok());
  CHECK(s3_.is_object(URI(file2)));
  CHECK(s3_.touch(URI(file3)).ok());
  CHECK(s3_.is_object(URI(file3)));
  CHECK(s3_.touch(URI(file4)).ok());
  CHECK(s3_.is_object(URI(file4)));
  CHECK(s3_.touch(URI(file5)).ok());
  CHECK(s3_.is_object(URI(file5)));

  // Check that the bucket is not empty
  CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  CHECK(!is_empty);

  // Check invalid file
  CHECK(!s3_.is_object(URI(TEST_DIR + "foo")));

  // List with prefix
  std::vector<std::string> paths;
  CHECK(s3_.ls(URI(TEST_DIR), &paths).ok());
  CHECK(paths.size() == 3);
  paths.clear();
  CHECK(s3_.ls(URI(dir), &paths).ok());
  CHECK(paths.size() == 2);
  paths.clear();
  CHECK(s3_.ls(URI(subdir), &paths).ok());
  CHECK(paths.size() == 2);
  paths.clear();
  CHECK(s3_.ls(S3_BUCKET, &paths, "").ok());  // No delimiter
  CHECK(paths.size() == 5);

  // Check if a directory exists
  bool is_dir;
  CHECK(s3_.is_dir(URI(file1), &is_dir).ok());
  CHECK(!is_dir);  // Not a dir
  CHECK(s3_.is_dir(URI(file4), &is_dir).ok());
  CHECK(!is_dir);  // Not a dir
  CHECK(s3_.is_dir(URI(dir), &is_dir).ok());
  CHECK(is_dir);  // This is viewed as a dir
  CHECK(s3_.is_dir(URI(TEST_DIR + "dir"), &is_dir).ok());
  CHECK(is_dir);  // This is viewed as a dir

  // Move file
  CHECK(s3_.move_object(URI(file5), URI(file6)).ok());
  CHECK(!s3_.is_object(URI(file5)));
  CHECK(s3_.is_object(URI(file6)));
  paths.clear();
  CHECK(s3_.ls(S3_BUCKET, &paths, "").ok());  // No delimiter
  CHECK(paths.size() == 5);

  // Move directory
  CHECK(s3_.move_dir(URI(dir), URI(dir2)).ok());
  CHECK(s3_.is_dir(URI(dir), &is_dir).ok());
  CHECK(!is_dir);
  CHECK(s3_.is_dir(URI(dir2), &is_dir).ok());
  CHECK(is_dir);
  paths.clear();
  CHECK(s3_.ls(S3_BUCKET, &paths, "").ok());  // No delimiter
  CHECK(paths.size() == 5);

  // Remove files
  CHECK(s3_.remove_object(URI(file4)).ok());
  CHECK(!s3_.is_object(URI(file4)));

  // Remove directories
  CHECK(s3_.remove_dir(URI(dir2)).ok());
  CHECK(!s3_.is_object(URI(file1)));
  CHECK(!s3_.is_object(URI(file2)));
  CHECK(!s3_.is_object(URI(file3)));
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
  CHECK(!s3_.is_object(URI(largefile)));
  CHECK(!s3_.is_object(URI(smallfile)));

  // Flush the files
  CHECK(s3_.flush_object(URI(largefile)).ok());
  CHECK(s3_.flush_object(URI(smallfile)).ok());

  // After flushing, the files exist
  CHECK(s3_.is_object(URI(largefile)));
  CHECK(s3_.is_object(URI(smallfile)));

  // Get file sizes
  uint64_t nbytes = 0;
  CHECK(s3_.object_size(URI(largefile), &nbytes).ok());
  CHECK(nbytes == (buffer_size + buffer_size_small));
  CHECK(s3_.object_size(URI(smallfile), &nbytes).ok());
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
