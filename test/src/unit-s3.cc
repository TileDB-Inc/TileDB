/**
 * @file   unit-s3.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
#include "test/support/src/vfs_helpers.h"
#include "tiledb/common/thread_pool/thread_pool.h"
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
      tiledb::sm::URI(S3_PREFIX + "tiledb-" + random_label() + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";
  ThreadPool thread_pool_{2};
  tiledb::sm::S3 s3_{&g_helper_stats, &thread_pool_, set_config_params()};
};

S3Fx::S3Fx() {
  // Create bucket
  bool exists = s3_.is_bucket(S3_BUCKET);
  if (exists)
    REQUIRE_NOTHROW(s3_.remove_bucket(S3_BUCKET));

  exists = s3_.is_bucket(S3_BUCKET);
  REQUIRE(!exists);
  REQUIRE_NOTHROW(s3_.create_bucket(S3_BUCKET));

  // Check if bucket is empty
  bool is_empty = s3_.is_empty_bucket(S3_BUCKET);
  CHECK(is_empty);
}

S3Fx::~S3Fx() {
  // Empty bucket
  bool is_empty = s3_.is_empty_bucket(S3_BUCKET);
  if (!is_empty) {
    CHECK_NOTHROW(s3_.empty_bucket(S3_BUCKET));
    is_empty = s3_.is_empty_bucket(S3_BUCKET);
    CHECK(is_empty);
  }

  // Delete bucket
  CHECK_NOTHROW(s3_.remove_bucket(S3_BUCKET));
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
    CHECK_THROWS(s3_.write(URI(largefile), write_buffer, buffer_size));

    // Before flushing, the file does not exist
    CHECK(!s3_.is_file(URI(largefile)));

    // Flush the file
    CHECK_THROWS(s3_.flush(URI(largefile)));

    // After flushing, the file does not exist
    CHECK(!s3_.is_file(URI(largefile)));
  }
}

TEST_CASE("Test S3 setting bucket/object canned acls", "[s3][config]") {
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
    bool exists = s3_.is_bucket(S3_BUCKET);
    if (exists)
      REQUIRE_NOTHROW(s3_.remove_bucket(S3_BUCKET));

    exists = s3_.is_bucket(S3_BUCKET);
    REQUIRE(!exists);
    REQUIRE_NOTHROW(s3_.create_bucket(S3_BUCKET));

    // Check if bucket is empty
    bool is_empty = s3_.is_empty_bucket(S3_BUCKET);
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
    bool is_empty = s3_.is_empty_bucket(S3_BUCKET);
    CHECK(is_empty);

    // Continue building the hierarchy
    CHECK_NOTHROW(s3_.touch(URI(file1)));
    CHECK(s3_.is_file(URI(file1)));
    CHECK_NOTHROW(s3_.touch(URI(file2)));
    CHECK(s3_.is_file(URI(file2)));
    CHECK_NOTHROW(s3_.touch(URI(file3)));
    CHECK(s3_.is_file(URI(file3)));
    CHECK_NOTHROW(s3_.touch(URI(file4)));
    CHECK(s3_.is_file(URI(file4)));
    CHECK_NOTHROW(s3_.touch(URI(file5)));
    CHECK(s3_.is_file(URI(file5)));

    // Check that the bucket is not empty
    is_empty = s3_.is_empty_bucket(S3_BUCKET);
    CHECK(!is_empty);

    // Check invalid file
    CHECK(!s3_.is_file(URI(TEST_DIR + "foo")));

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
    CHECK(!s3_.is_dir(URI(file1)));            // Not a dir
    CHECK(!s3_.is_dir(URI(file4)));            // Not a dir
    CHECK(s3_.is_dir(URI(dir)));               // Viewed as a dir
    CHECK(s3_.is_dir(URI(TEST_DIR + "dir")));  // Viewed as a dir

    // Move file
    CHECK(s3_.move_object(URI(file5), URI(file6)).ok());
    CHECK(!s3_.is_file(URI(file5)));
    CHECK(s3_.is_file(URI(file6)));
    paths.clear();
    CHECK(s3_.ls(S3_BUCKET, &paths, "").ok());  // No delimiter
    CHECK(paths.size() == 5);

    // Move directory
    CHECK_NOTHROW(s3_.move_dir(URI(dir), URI(dir2)));
    CHECK(!s3_.is_dir(URI(dir)));
    CHECK(s3_.is_dir(URI(dir2)));
    paths.clear();
    CHECK(s3_.ls(S3_BUCKET, &paths, "").ok());  // No delimiter
    CHECK(paths.size() == 5);

    // Remove files
    REQUIRE_NOTHROW(s3_.remove_file(URI(file4)));
    CHECK(!s3_.is_file(URI(file4)));

    // Remove directories
    CHECK_NOTHROW(s3_.remove_dir(URI(dir2)));
    CHECK(!s3_.is_file(URI(file1)));
    CHECK(!s3_.is_file(URI(file2)));
    CHECK(!s3_.is_file(URI(file3)));
  };
  // function to try creating bucket with BucketCannedACL indicated
  // by string parameter.
  auto try_with_bucket_object_canned_acl = [&](const char* bucket_acl_to_try,
                                               const char* object_acl_to_try) {
    REQUIRE(config.set("vfs.s3.bucket_canned_acl", bucket_acl_to_try).ok());
    REQUIRE(config.set("vfs.s3.object_canned_acl", object_acl_to_try).ok());

    tiledb::sm::S3 s3_{&g_helper_stats, &thread_pool_, config};

    // Create bucket
    bool exists = s3_.is_bucket(S3_BUCKET);
    if (exists)
      REQUIRE_NOTHROW(s3_.remove_bucket(S3_BUCKET));

    exists = s3_.is_bucket(S3_BUCKET);
    REQUIRE(!exists);
    REQUIRE_NOTHROW(s3_.create_bucket(S3_BUCKET));

    // Check if bucket is empty
    bool is_empty = s3_.is_empty_bucket(S3_BUCKET);
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

TEST_CASE(
    "S3: S3Scanner iterator to populate vector", "[s3][ls-scan-iterator]") {
  S3Test s3_test({10, 50});
  bool recursive = true;
  // 1000 is the default max_keys for S3. This is the same default used by
  // S3Scanner. Testing with small max_keys validates the iterator handles batch
  // collection and filtering appropriately.
  int max_keys = GENERATE(1000, 10, 7);

  DYNAMIC_SECTION("Testing with " << max_keys << " max keys from S3") {
    ResultFilter result_filter;
    auto expected = s3_test.expected_results();

    SECTION("Accept all objects") {
      result_filter = [](const std::string_view&, uint64_t) { return true; };
      std::sort(expected.begin(), expected.end());
    }

    SECTION("Reject all objects") {
      result_filter = [](const std::string_view&, uint64_t) { return false; };
    }

    SECTION("Filter objects including 'test_file_1' in key") {
      result_filter = [](const std::string_view& path, uint64_t) {
        if (path.find("test_file_1") != std::string::npos) {
          return true;
        }
        return false;
      };
    }

    SECTION("Scan for a single object") {
      result_filter = [](const std::string_view& path, uint64_t) {
        if (path.find("test_file_50") != std::string::npos) {
          return true;
        }
        return false;
      };
    }

    // Filter expected results to apply result_filter.
    std::erase_if(expected, [&result_filter](const auto& a) {
      return !result_filter(a.first, a.second);
    });

    auto scan = s3_test.get_s3().scanner(
        s3_test.temp_dir_, result_filter, recursive, max_keys);
    std::vector results_vector(scan.begin(), scan.end());

    CHECK(results_vector.size() == expected.size());
    for (const auto& s3_object : results_vector) {
      CHECK(result_filter(s3_object.GetKey(), s3_object.GetSize()));
      auto uri = s3_test.temp_dir_.to_string() + "/" + s3_object.GetKey();
      CHECK_THAT(
          expected,
          Catch::Matchers::Contains(
              std::make_pair(uri, static_cast<size_t>(s3_object.GetSize()))));
    }
  }
}

TEST_CASE("S3: S3Scanner iterator", "[s3][ls-scan-iterator]") {
  S3Test s3_test({10, 50, 7});
  bool recursive = true;
  int max_keys = GENERATE(1000, 11);

  std::vector<Aws::S3::Model::Object> results_vector;
  DYNAMIC_SECTION("Testing with " << max_keys << " max keys from S3") {
    auto scan = s3_test.get_s3().scanner(
        s3_test.temp_dir_,
        tiledb::sm::LsScanner::accept_all,
        recursive,
        max_keys);

    SECTION("for loop") {
      SECTION("range based for") {
        for (const auto& result : scan) {
          results_vector.push_back(result);
        }
      }
      SECTION("prefix operator") {
        for (auto it = scan.begin(); it != scan.end(); ++it) {
          results_vector.push_back(*it);
        }
      }
      SECTION("postfix operator") {
        for (auto it = scan.begin(); it != scan.end(); it++) {
          results_vector.push_back(*it);
        }
      }
    }

    SECTION("vector::assign") {
      results_vector.assign(scan.begin(), scan.end());
    }

    SECTION("std::move") {
      std::move(scan.begin(), scan.end(), std::back_inserter(results_vector));
    }
  }

  auto expected = s3_test.expected_results();
  CHECK(results_vector.size() == expected.size());
  for (size_t i = 0; i < expected.size(); i++) {
    const auto& s3_object = results_vector[i];
    auto full_uri =
        s3_test.temp_dir_.to_string() + "/" + std::string(s3_object.GetKey());
    CHECK_THAT(
        expected,
        Catch::Matchers::Contains(std::make_pair(
            full_uri, static_cast<size_t>(s3_object.GetSize()))));
  }
}

#endif
