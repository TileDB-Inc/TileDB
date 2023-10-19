/**
 * @file   unit-gcs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <test/support/tdb_catch.h>
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/gcs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_math.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/utils.h"

#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm;

struct GCSFx {
  const std::string GCS_PREFIX = "gcs://";
  const tiledb::sm::URI GCS_BUCKET =
      tiledb::sm::URI(GCS_PREFIX + random_bucket_name("tiledb") + "/");
  const std::string TEST_DIR = GCS_BUCKET.to_string() + "tiledb_test_dir/";

  tiledb::sm::GCS gcs_;
  ThreadPool thread_pool_{2};

  GCSFx() = default;
  ~GCSFx();

  void init_gcs(Config&& config);

  static std::string random_bucket_name(const std::string& prefix);
};

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

void GCSFx::init_gcs(Config&& config) {
  REQUIRE(config.set("vfs.gcs.project_id", "TODO").ok());
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

  // Check if bucket is empty
  bool is_empty;
  REQUIRE(gcs_.is_empty_bucket(GCS_BUCKET, &is_empty).ok());
  REQUIRE(is_empty);
}

std::string GCSFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}

TEST_CASE_METHOD(GCSFx, "Test GCS init", "[gcs]") {
  try {
    Config config;
    REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "true").ok());
    init_gcs(std::move(config));
  } catch (...) {
    INFO(
        "GCS initialization failed. In order to run GCS tests, be sure to "
        "source scripts/run-gcs.sh in this shell session before starting test "
        "runner.");
    REQUIRE(false);
  }
}

TEST_CASE_METHOD(GCSFx, "Test GCS filesystem, file management", "[gcs]") {
  Config config;
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "true").ok());
  init_gcs(std::move(config));

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
  bool is_object = false;
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

  // ls_with_sizes
  std::string s = "abcdef";
  CHECK(gcs_.write(URI(file3), s.data(), s.size()).ok());
  REQUIRE(gcs_.flush_object(URI(file3)).ok());

  auto&& [status, rv] = gcs_.ls_with_sizes(URI(dir));
  auto children = *rv;
  REQUIRE(status.ok());

  REQUIRE(children.size() == 2);
  CHECK(children[0].path().native() == file3);
  CHECK(children[1].path().native() == subdir.substr(0, subdir.size() - 1));

  CHECK(children[0].file_size() == s.size());
  // Directories don't get a size
  CHECK(children[1].file_size() == 0);

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

TEST_CASE_METHOD(
    GCSFx,
    "Test GCS filesystem I/O, multipart, serial",
    "[gcs][multipart][serial]") {
  Config config;
  const uint64_t max_parallel_ops = 1;
  const uint64_t multi_part_size = 4 * 1024 * 1024;
  REQUIRE(
      config.set("vfs.gcs.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "true").ok());
  REQUIRE(config.set("vfs.gcs.multi_part_size", std::to_string(multi_part_size))
              .ok());
  init_gcs(std::move(config));

  const uint64_t write_cache_max_size = max_parallel_ops * multi_part_size;

  // Prepare buffers
  uint64_t buffer_size_large = write_cache_max_size;
  auto write_buffer_large = new char[buffer_size_large];
  for (uint64_t i = 0; i < buffer_size_large; i++)
    write_buffer_large[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  auto largefile = TEST_DIR + "largefile";
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_large, buffer_size_large).ok());
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  REQUIRE(
      gcs_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the files do not exist
  bool is_object = false;
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(!is_object);
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(!is_object);

  // Flush the files
  REQUIRE(gcs_.flush_object(URI(largefile)).ok());
  REQUIRE(gcs_.flush_object(URI(smallfile)).ok());

  // After flushing, the files exist
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(is_object);

  // Get file sizes
  uint64_t nbytes = 0;
  REQUIRE(gcs_.object_size(URI(largefile), &nbytes).ok());
  REQUIRE(nbytes == (buffer_size_large + buffer_size_small));
  REQUIRE(gcs_.object_size(URI(smallfile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(gcs_.read(URI(largefile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(gcs_.read(URI(largefile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);
}

TEST_CASE_METHOD(
    GCSFx,
    "Test GCS filesystem I/O, non-multipart, serial",
    "[gcs][non-multipart][serial]") {
  Config config;
  const uint64_t max_parallel_ops = 1;
  const uint64_t multi_part_size = 4 * 1024 * 1024;
  REQUIRE(
      config.set("vfs.gcs.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "false").ok());
  REQUIRE(config.set("vfs.gcs.multi_part_size", std::to_string(multi_part_size))
              .ok());
  init_gcs(std::move(config));

  const uint64_t write_cache_max_size = max_parallel_ops * multi_part_size;

  // Prepare buffers
  uint64_t buffer_size_large = write_cache_max_size;
  auto write_buffer_large = new char[buffer_size_large];
  for (uint64_t i = 0; i < buffer_size_large; i++)
    write_buffer_large[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  auto largefile = TEST_DIR + "largefile";
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_large, buffer_size_large).ok());
  REQUIRE(
      !gcs_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  REQUIRE(
      gcs_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the file does not exist
  bool is_object = false;
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(!is_object);

  // Flush the file
  REQUIRE(gcs_.flush_object(URI(smallfile)).ok());

  // After flushing, the file exists
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(is_object);

  // Get file size
  uint64_t nbytes = 0;
  REQUIRE(gcs_.object_size(URI(smallfile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(gcs_.read(URI(smallfile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(gcs_.read(URI(smallfile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);
}

TEST_CASE_METHOD(
    GCSFx,
    "Test GCS filesystem I/O, multipart, concurrent",
    "[gcs][multipart][concurrent]") {
  Config config;
  const uint64_t max_parallel_ops = 4;
  const uint64_t multi_part_size = 4 * 1024 * 1024;
  REQUIRE(
      config.set("vfs.gcs.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "true").ok());
  REQUIRE(config.set("vfs.gcs.multi_part_size", std::to_string(multi_part_size))
              .ok());
  init_gcs(std::move(config));

  const uint64_t write_cache_max_size = max_parallel_ops * multi_part_size;

  // Prepare buffers
  uint64_t buffer_size_large = write_cache_max_size;
  auto write_buffer_large = new char[buffer_size_large];
  for (uint64_t i = 0; i < buffer_size_large; i++)
    write_buffer_large[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  auto largefile = TEST_DIR + "largefile";
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_large, buffer_size_large).ok());
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  REQUIRE(
      gcs_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the files do not exist
  bool is_object = false;
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(!is_object);
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(!is_object);

  // Flush the files
  REQUIRE(gcs_.flush_object(URI(largefile)).ok());
  REQUIRE(gcs_.flush_object(URI(smallfile)).ok());

  // After flushing, the files exist
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(is_object);
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(is_object);

  // Get file sizes
  uint64_t nbytes = 0;
  REQUIRE(gcs_.object_size(URI(largefile), &nbytes).ok());
  REQUIRE(nbytes == (buffer_size_large + buffer_size_small));
  REQUIRE(gcs_.object_size(URI(smallfile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(gcs_.read(URI(largefile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(gcs_.read(URI(largefile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);
}

TEST_CASE_METHOD(
    GCSFx,
    "Test GCS filesystem I/O, non-multipart, concurrent",
    "[gcs][non-multipart][concurrent]") {
  Config config;
  const uint64_t max_parallel_ops = 4;
  const uint64_t multi_part_size = 4 * 1024 * 1024;
  REQUIRE(
      config.set("vfs.gcs.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "false").ok());
  REQUIRE(config.set("vfs.gcs.multi_part_size", std::to_string(multi_part_size))
              .ok());
  init_gcs(std::move(config));

  const uint64_t write_cache_max_size = max_parallel_ops * multi_part_size;

  // Prepare buffers
  uint64_t buffer_size_large = write_cache_max_size;
  auto write_buffer_large = new char[buffer_size_large];
  for (uint64_t i = 0; i < buffer_size_large; i++)
    write_buffer_large[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  auto largefile = TEST_DIR + "largefile";
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_large, buffer_size_large).ok());
  REQUIRE(
      !gcs_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  REQUIRE(
      gcs_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the file does not exist
  bool is_object = false;
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(!is_object);

  // Flush the file
  REQUIRE(gcs_.flush_object(URI(smallfile)).ok());

  // After flushing, the file exists
  REQUIRE(gcs_.is_object(URI(smallfile), &is_object).ok());
  REQUIRE(is_object);

  // Get file size
  uint64_t nbytes = 0;
  REQUIRE(gcs_.object_size(URI(smallfile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(gcs_.read(URI(smallfile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(gcs_.read(URI(smallfile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);
}

TEST_CASE_METHOD(
    GCSFx,
    "Test GCS filesystem I/O, multipart, composition",
    "[gcs][multipart][composition]") {
  Config config;
  const uint64_t max_parallel_ops = 4;
  const uint64_t multi_part_size = 4 * 1024;
  REQUIRE(
      config.set("vfs.gcs.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config.set("vfs.gcs.use_multi_part_upload", "true").ok());
  REQUIRE(config.set("vfs.gcs.multi_part_size", std::to_string(multi_part_size))
              .ok());
  init_gcs(std::move(config));

  const uint64_t write_cache_max_size = max_parallel_ops * multi_part_size;

  // Prepare a buffer that will write 200 (50 * 4 threads) objects.
  // The maximum number of objects per composition operation is 32.
  uint64_t buffer_size_large = 50 * write_cache_max_size;
  auto write_buffer_large = new char[buffer_size_large];
  for (uint64_t i = 0; i < buffer_size_large; i++)
    write_buffer_large[i] = (char)('a' + (i % 26));

  // Write to the file
  auto largefile = TEST_DIR + "largefile";
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_large, buffer_size_large).ok());

  // Before flushing, the file does not exist
  bool is_object = false;
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(!is_object);

  // Flush the file
  REQUIRE(gcs_.flush_object(URI(largefile)).ok());

  // After flushing, the file exists
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(is_object);

  // Get file size
  uint64_t nbytes = 0;
  REQUIRE(gcs_.object_size(URI(largefile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_large);

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(gcs_.read(URI(largefile), 0, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(gcs_.read(URI(largefile), 11, read_buffer, 26, 0, &bytes_read).ok());
  CHECK(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Prepare a buffer that will overwrite the original with a smaller
  // size.
  uint64_t buffer_size_overwrite = 10 * write_cache_max_size;
  auto write_buffer_overwrite = new char[buffer_size_overwrite];
  for (uint64_t i = 0; i < buffer_size_overwrite; i++)
    write_buffer_overwrite[i] = (char)('a' + (i % 26));

  // Write to the file
  REQUIRE(
      gcs_.write(URI(largefile), write_buffer_overwrite, buffer_size_overwrite)
          .ok());

  // Flush the file
  REQUIRE(gcs_.flush_object(URI(largefile)).ok());

  // After flushing, the file exists
  REQUIRE(gcs_.is_object(URI(largefile), &is_object).ok());
  REQUIRE(is_object);

  // Get file size
  nbytes = 0;
  REQUIRE(gcs_.object_size(URI(largefile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_overwrite);
}

#endif
