/**
 * @file   unit-s3-no-multipart.cc
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
 * Tests for S3 API filesystem functions.
 */

#ifdef HAVE_S3

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/s3.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/time.h"

#include <fstream>
#include <thread>

using namespace tiledb::test;
using namespace tiledb::common;
using namespace tiledb::sm;

struct S3DirectFx {
  const std::string S3_PREFIX = "s3://";
  const tiledb::sm::URI S3_BUCKET =
      tiledb::sm::URI(S3_PREFIX + random_name("tiledb") + "/");
  const std::string TEST_DIR = S3_BUCKET.to_string() + "tiledb_test_dir/";
  tiledb::sm::S3 s3_;
  ThreadPool thread_pool_;

  S3DirectFx();
  ~S3DirectFx();

  static std::string random_name(const std::string& prefix);
};

S3DirectFx::S3DirectFx() {
  // Connect
  Config config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE(config.set("vfs.s3.endpoint_override", "localhost:9999").ok());
  REQUIRE(config.set("vfs.s3.scheme", "https").ok());
  REQUIRE(config.set("vfs.s3.use_virtual_addressing", "false").ok());
  REQUIRE(config.set("vfs.s3.verify_ssl", "false").ok());
#endif
  REQUIRE(config.set("vfs.s3.max_parallel_ops", "1").ok());
  // set max buffer size to 10 MB
  REQUIRE(config.set("vfs.s3.multipart_part_size", "10000000").ok());
  REQUIRE(config.set("vfs.s3.use_multipart_upload", "false").ok());
  REQUIRE(thread_pool_.init(2).ok());
  REQUIRE(s3_.init(&g_helper_stats, config, &thread_pool_).ok());

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

S3DirectFx::~S3DirectFx() {
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
  s3_.disconnect();
}

TEST_CASE_METHOD(
    S3DirectFx,
    "Test S3 filesystem, file I/O with multipart API disabled",
    "[s3]") {
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
  assert(26 == bytes_read);
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
  assert(26 == bytes_read);
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Try to write 11 MB file, should fail with given buffer configuration
  auto badfile = TEST_DIR + "badfile";
  auto badbuffer = (char*)malloc(11000000);
  CHECK(!(s3_.write(URI(badfile), badbuffer, 11000000).ok()));
}

std::string S3DirectFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}
#endif
