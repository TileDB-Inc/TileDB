/**
 * @file   unit-s3.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/s3.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_time.h"

#include <fstream>
#include <thread>

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

struct S3Fx {
  S3Fx();
  ~S3Fx();
  static Config set_config_params();

  const std::string S3_PREFIX = "s3://";
  const tiledb::sm::URI S3_BUCKET =
      tiledb::sm::URI(S3_PREFIX + random_label("tiledb-") + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";
  ThreadPool thread_pool_{2};
  tiledb::sm::S3 s3_{&g_helper_stats, &thread_pool_, set_config_params()};
};

S3Fx::S3Fx() {
  // Create bucket
  bool exists;
  REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
  if (exists)
    REQUIRE(s3_.remove_bucket(S3_BUCKET).ok());

  REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
  REQUIRE(!exists);
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
  CHECK(s3_.disconnect().ok());
}

Config S3Fx::set_config_params() {
  // Connect
  Config config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE(config.set("vfs.s3.endpoint_override", "localhost:9999").ok());
  REQUIRE(config.set("vfs.s3.scheme", "https").ok());
  REQUIRE(config.set("vfs.s3.use_virtual_addressing", "false").ok());
  REQUIRE(config.set("vfs.s3.verify_ssl", "false").ok());
#endif
  return config;
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
  bool exists = false;
  CHECK(s3_.touch(URI(file1)).ok());
  CHECK(s3_.is_object(URI(file1), &exists).ok());
  CHECK(exists);
  CHECK(s3_.touch(URI(file2)).ok());
  CHECK(s3_.is_object(URI(file2), &exists).ok());
  CHECK(exists);
  CHECK(s3_.touch(URI(file3)).ok());
  CHECK(s3_.is_object(URI(file3), &exists).ok());
  CHECK(exists);
  CHECK(s3_.touch(URI(file4)).ok());
  CHECK(s3_.is_object(URI(file4), &exists).ok());
  CHECK(exists);
  CHECK(s3_.touch(URI(file5)).ok());
  CHECK(s3_.is_object(URI(file5), &exists).ok());
  CHECK(exists);

  // Check that the bucket is not empty
  CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
  CHECK(!is_empty);

  // Check invalid file
  CHECK(s3_.is_object(URI(TEST_DIR + "foo"), &exists).ok());
  CHECK(!exists);

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
  bool is_dir = false;
  CHECK(s3_.is_dir(URI(file1), &is_dir).ok());
  CHECK(!is_dir);  // Not a dir
  CHECK(s3_.is_dir(URI(file4), &is_dir).ok());
  CHECK(!is_dir);  // Not a dir
  CHECK(s3_.is_dir(URI(dir), &is_dir).ok());
  CHECK(is_dir);  // This is viewed as a dir
  CHECK(s3_.is_dir(URI(TEST_DIR + "dir"), &is_dir).ok());
  CHECK(is_dir);  // This is viewed as a dir

  // ls_with_sizes
  std::string s = "abcdef";
  CHECK(s3_.write(URI(file3), s.data(), s.size()).ok());
  CHECK(s3_.flush_object(URI(file3)).ok());

  auto&& [status, rv] = s3_.ls_with_sizes(URI(dir));
  auto children = *rv;
  REQUIRE(status.ok());

  REQUIRE(children.size() == 2);
  CHECK(children[0].path().native() == file3);
  CHECK(children[1].path().native() == subdir.substr(0, subdir.size() - 1));

  CHECK(children[0].file_size() == s.size());
  // Directories don't get a size
  CHECK(children[1].file_size() == 0);

  // Move file
  CHECK(s3_.move_object(URI(file5), URI(file6)).ok());
  CHECK(s3_.is_object(URI(file5), &exists).ok());
  CHECK(!exists);
  CHECK(s3_.is_object(URI(file6), &exists).ok());
  CHECK(exists);
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
  CHECK(s3_.is_object(URI(file4), &exists).ok());
  CHECK(!exists);

  // Remove directories
  CHECK(s3_.remove_dir(URI(dir2)).ok());
  CHECK(s3_.is_object(URI(file1), &exists).ok());
  CHECK(!exists);
  CHECK(s3_.is_object(URI(file2), &exists).ok());
  CHECK(!exists);
  CHECK(s3_.is_object(URI(file3), &exists).ok());
  CHECK(!exists);
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
  bool exists = false;
  CHECK(s3_.is_object(URI(largefile), &exists).ok());
  CHECK(!exists);
  CHECK(s3_.is_object(URI(smallfile), &exists).ok());
  CHECK(!exists);

  // Flush the files
  CHECK(s3_.flush_object(URI(largefile)).ok());
  CHECK(s3_.flush_object(URI(smallfile)).ok());

  // After flushing, the files exist
  CHECK(s3_.is_object(URI(largefile), &exists).ok());
  CHECK(exists);
  CHECK(s3_.is_object(URI(smallfile), &exists).ok());
  CHECK(exists);

  // Get file sizes
  uint64_t nbytes = 0;
  CHECK(s3_.object_size(URI(largefile), &nbytes).ok());
  CHECK(nbytes == (buffer_size + buffer_size_small));
  CHECK(s3_.object_size(URI(smallfile), &nbytes).ok());
  CHECK(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  CHECK(s3_.read(URI(largefile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Read from a different offset
  CHECK(s3_.read(URI(largefile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);
}

TEST_CASE_METHOD(S3Fx, "Test S3 multiupload abort path", "[s3]") {
  // Prepare a large buffer
  uint64_t buffer_size = 100 * 1024 * 1024;
  auto write_buffer = new char[buffer_size];
  for (uint64_t i = 0; i < buffer_size; i++)
    write_buffer[i] = (char)('a' + (i % 26));

  for (const int nth_failure : {2, 5, 10}) {
    UnitTestConfig::instance().s3_fail_every_nth_upload_request.set(
        nth_failure);

    // Write one large file, the write will fail
    auto largefile =
        TEST_DIR + "failed_largefile_" + std::to_string(nth_failure);
    CHECK(!s3_.write(URI(largefile), write_buffer, buffer_size).ok());

    // Before flushing, the file does not exist
    bool exists = false;
    CHECK(s3_.is_object(URI(largefile), &exists).ok());
    CHECK(!exists);

    // Flush the file
    CHECK(s3_.flush_object(URI(largefile)).ok());

    // After flushing, the file does not exist
    CHECK(s3_.is_object(URI(largefile), &exists).ok());
    CHECK(!exists);
  }
}

TEST_CASE_METHOD(
    S3Fx, "Test S3 setting bucket/object canned acls", "[s3][config]") {
  Config config;
  REQUIRE(config.set("vfs.s3.bucket_canned_acl", "private_").ok());
  REQUIRE(config.set("vfs.s3.bucket_canned_acl", "public_read").ok());
  REQUIRE(config.set("vfs.s3.bucket_canned_acl", "public_read_write").ok());
  REQUIRE(config.set("vfs.s3.bucket_canned_acl", "authenticated_read").ok());
  REQUIRE(config.set("vfs.s3.bucket_canned_acl", "NOT_SET").ok());

  REQUIRE(config.set("vfs.s3.object_canned_acl", "private_").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "public_read").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "public_read_write").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "authenticated_read").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "aws_exec_read").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "bucket_owner_read").ok());
  REQUIRE(
      config.set("vfs.s3.object_canned_acl", "bucket_owner_full_control").ok());
  REQUIRE(config.set("vfs.s3.object_canned_acl", "NOT_SET").ok());
}

TEST_CASE_METHOD(S3Fx, "Test S3 use BucketCannedACL", "[s3]") {
  Config config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE(config.set("vfs.s3.endpoint_override", "localhost:9999").ok());
  REQUIRE(config.set("vfs.s3.scheme", "https").ok());
  REQUIRE(config.set("vfs.s3.use_virtual_addressing", "false").ok());
  REQUIRE(config.set("vfs.s3.verify_ssl", "false").ok());
#endif

  // function to try creating bucket with BucketCannedACL indicated
  // by string parameter.
  auto try_with_bucket_canned_acl = [&](const char* bucket_acl_to_try) {
    REQUIRE(config.set("vfs.s3.bucket_canned_acl", bucket_acl_to_try).ok());
    tiledb::sm::S3 s3_{&g_helper_stats, &thread_pool_, config};

    // Create bucket
    bool exists;
    REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
    if (exists)
      REQUIRE(s3_.remove_bucket(S3_BUCKET).ok());

    REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
    REQUIRE(!exists);
    REQUIRE(s3_.create_bucket(S3_BUCKET).ok());

    // Check if bucket is empty
    bool is_empty;
    REQUIRE(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
    CHECK(is_empty);

    CHECK(s3_.disconnect().ok());
  };

  try_with_bucket_canned_acl("NOT_SET");
  try_with_bucket_canned_acl("private_");
  try_with_bucket_canned_acl("public_read");
  try_with_bucket_canned_acl("public_read_write");
  try_with_bucket_canned_acl("authenticated_read");
}

TEST_CASE_METHOD(S3Fx, "Test S3 use Bucket/Object CannedACL", "[s3]") {
  Config config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE(config.set("vfs.s3.endpoint_override", "localhost:9999").ok());
  REQUIRE(config.set("vfs.s3.scheme", "https").ok());
  REQUIRE(config.set("vfs.s3.use_virtual_addressing", "false").ok());
  REQUIRE(config.set("vfs.s3.verify_ssl", "false").ok());
#endif

  // function exercising SetACL() for objects
  // using functionality cloned from file management test case.
  auto exercise_object_canned_acl = [&]() {
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
    bool exists = false;
    CHECK(s3_.touch(URI(file1)).ok());
    CHECK(s3_.is_object(URI(file1), &exists).ok());
    CHECK(exists);
    CHECK(s3_.touch(URI(file2)).ok());
    CHECK(s3_.is_object(URI(file2), &exists).ok());
    CHECK(exists);
    CHECK(s3_.touch(URI(file3)).ok());
    CHECK(s3_.is_object(URI(file3), &exists).ok());
    CHECK(exists);
    CHECK(s3_.touch(URI(file4)).ok());
    CHECK(s3_.is_object(URI(file4), &exists).ok());
    CHECK(exists);
    CHECK(s3_.touch(URI(file5)).ok());
    CHECK(s3_.is_object(URI(file5), &exists).ok());
    CHECK(exists);

    // Check that the bucket is not empty
    CHECK(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
    CHECK(!is_empty);

    // Check invalid file
    CHECK(s3_.is_object(URI(TEST_DIR + "foo"), &exists).ok());
    CHECK(!exists);

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
    bool is_dir = false;
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
    CHECK(s3_.is_object(URI(file5), &exists).ok());
    CHECK(!exists);
    CHECK(s3_.is_object(URI(file6), &exists).ok());
    CHECK(exists);
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
    CHECK(s3_.is_object(URI(file4), &exists).ok());
    CHECK(!exists);

    // Remove directories
    CHECK(s3_.remove_dir(URI(dir2)).ok());
    CHECK(s3_.is_object(URI(file1), &exists).ok());
    CHECK(!exists);
    CHECK(s3_.is_object(URI(file2), &exists).ok());
    CHECK(!exists);
    CHECK(s3_.is_object(URI(file3), &exists).ok());
    CHECK(!exists);
  };
  // function to try creating bucket with BucketCannedACL indicated
  // by string parameter.
  auto try_with_bucket_object_canned_acl = [&](const char* bucket_acl_to_try,
                                               const char* object_acl_to_try) {
    REQUIRE(config.set("vfs.s3.bucket_canned_acl", bucket_acl_to_try).ok());
    REQUIRE(config.set("vfs.s3.object_canned_acl", object_acl_to_try).ok());

    tiledb::sm::S3 s3_{&g_helper_stats, &thread_pool_, config};

    // Create bucket
    bool exists;
    REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
    if (exists)
      REQUIRE(s3_.remove_bucket(S3_BUCKET).ok());

    REQUIRE(s3_.is_bucket(S3_BUCKET, &exists).ok());
    REQUIRE(!exists);
    REQUIRE(s3_.create_bucket(S3_BUCKET).ok());

    // Check if bucket is empty
    bool is_empty;
    REQUIRE(s3_.is_empty_bucket(S3_BUCKET, &is_empty).ok());
    CHECK(is_empty);

    exercise_object_canned_acl();

    CHECK(s3_.disconnect().ok());
  };

  // basic test, not trying all combinations
  try_with_bucket_object_canned_acl("NOT_SET", "NOT_SET");
  try_with_bucket_object_canned_acl("private_", "private_");
  try_with_bucket_object_canned_acl("public_read", "public_read");
  try_with_bucket_object_canned_acl("public_read_write", "public_read_write");
  try_with_bucket_object_canned_acl("authenticated_read", "authenticated_read");
}
#endif
