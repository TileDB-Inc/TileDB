/**
 * @file   unit-gcs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * Tests for GCS API filesystem functions.
 */

#ifdef HAVE_GCS

#include "catch.hpp"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/gcs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/utils.h"

#include <sstream>

using namespace tiledb::sm;

struct GCSFx {
  const std::string GCS_PREFIX = "gcs://";
  const tiledb::sm::URI GCS_BUCKET =
      tiledb::sm::URI(GCS_PREFIX + random_bucket_name("tiledb") + "/");
  const std::string TEST_DIR = GCS_BUCKET.to_string() + "tiledb_test_dir/";
  tiledb::sm::GCS gcs_;
  ThreadPool thread_pool_;

  GCSFx();
  ~GCSFx();

  static std::string random_bucket_name(const std::string& prefix);
};

GCSFx::GCSFx() {
  Config config;
  REQUIRE(thread_pool_.init(2).ok());
  REQUIRE(gcs_.init(config, &thread_pool_).ok());

  // Create bucket
  bool is_bucket;
  REQUIRE(gcs_.is_bucket(GCS_BUCKET, &is_bucket).ok());
  if (is_bucket) {
    REQUIRE(gcs_.remove_bucket(GCS_BUCKET).ok());
  }

  REQUIRE(gcs_.is_bucket(GCS_BUCKET, &is_bucket).ok());
  REQUIRE(!is_bucket);
  REQUIRE(gcs_.create_bucket(GCS_BUCKET).ok());
}

GCSFx::~GCSFx() {
  // Empty bucket
  bool is_empty;
  REQUIRE(gcs_.is_empty_bucket(GCS_BUCKET, &is_empty).ok());
  if (!is_empty) {
    REQUIRE(gcs_.empty_bucket(GCS_BUCKET).ok());
    REQUIRE(gcs_.is_empty_bucket(GCS_BUCKET, &is_empty).ok());
    REQUIRE(is_empty);
  }

  // Delete bucket
  REQUIRE(gcs_.remove_bucket(GCS_BUCKET).ok());
}

std::string GCSFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}

TEST_CASE_METHOD(GCSFx, "Test GCS filesystem, file management", "[gcs]") {
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
  REQUIRE(gcs_.is_empty_bucket(GCS_BUCKET, &is_empty).ok());
  REQUIRE(is_empty);

  // Continue building the hierarchy
  bool is_object;
  REQUIRE(gcs_.touch(URI(file1)).ok());
  REQUIRE(gcs_.is_object(URI(file1), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.touch(URI(file2)).ok());
  REQUIRE(gcs_.is_object(URI(file2), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.touch(URI(file3)).ok());
  REQUIRE(gcs_.is_object(URI(file3), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.touch(URI(file4)).ok());
  REQUIRE(gcs_.is_object(URI(file4), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.touch(URI(file5)).ok());
  REQUIRE(gcs_.is_object(URI(file5), &is_object).ok());
  REQUIRE(is_object);

  // Check that bucket is not empty
  REQUIRE(gcs_.is_empty_bucket(GCS_BUCKET, &is_empty).ok());
  REQUIRE(!is_empty);

  // Check invalid file
  REQUIRE(gcs_.is_object(URI(TEST_DIR + "foo"), &is_object).ok());
  REQUIRE(!is_object);

  // List with prefix
  std::vector<std::string> paths;
  REQUIRE(gcs_.ls(URI(TEST_DIR), &paths).ok());
  REQUIRE(paths.size() == 3);
  paths.clear();
  REQUIRE(gcs_.ls(URI(dir), &paths).ok());
  REQUIRE(paths.size() == 2);
  paths.clear();
  REQUIRE(gcs_.ls(URI(subdir), &paths).ok());
  REQUIRE(paths.size() == 2);
  paths.clear();
  REQUIRE(gcs_.ls(GCS_BUCKET, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Check if a directory exists
  bool is_dir = false;
  REQUIRE(gcs_.is_dir(URI(file1), &is_dir).ok());
  REQUIRE(!is_dir);  // Not a dir
  REQUIRE(gcs_.is_dir(URI(file4), &is_dir).ok());
  REQUIRE(!is_dir);  // Not a dir
  REQUIRE(gcs_.is_dir(URI(dir), &is_dir).ok());
  REQUIRE(is_dir);  // This is viewed as a dir
  REQUIRE(gcs_.is_dir(URI(TEST_DIR + "dir"), &is_dir).ok());
  REQUIRE(is_dir);  // This is viewed as a dir

  // Move file
  REQUIRE(gcs_.move_object(URI(file5), URI(file6)).ok());
  REQUIRE(gcs_.is_object(URI(file5), &is_object).ok());
  REQUIRE(!is_object);
  REQUIRE(gcs_.is_object(URI(file6), &is_object).ok());
  REQUIRE(is_object);
  paths.clear();
  REQUIRE(gcs_.ls(GCS_BUCKET, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Move directory
  REQUIRE(gcs_.move_dir(URI(dir), URI(dir2)).ok());
  REQUIRE(gcs_.is_dir(URI(dir), &is_dir).ok());
  REQUIRE(!is_dir);
  REQUIRE(gcs_.is_dir(URI(dir2), &is_dir).ok());
  REQUIRE(is_dir);
  paths.clear();
  REQUIRE(gcs_.ls(GCS_BUCKET, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Remove files
  REQUIRE(gcs_.remove_object(URI(file4)).ok());
  REQUIRE(gcs_.is_object(URI(file4), &is_object).ok());
  REQUIRE(!is_object);

  // Remove directories
  REQUIRE(gcs_.remove_dir(URI(dir2)).ok());
  REQUIRE(gcs_.is_object(URI(file1), &is_object).ok());
  REQUIRE(!is_object);
  REQUIRE(gcs_.is_object(URI(file2), &is_object).ok());
  REQUIRE(!is_object);
  REQUIRE(gcs_.is_object(URI(file3), &is_object).ok());
  REQUIRE(!is_object);
}

#endif
