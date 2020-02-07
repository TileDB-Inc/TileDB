/**
 * @file   unit-azure.cc
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
 * Tests for AZURE API filesystem functions.
 */

#ifdef HAVE_AZURE

#include "catch.hpp"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/utils.h"

#include <fstream>
#include <thread>

using namespace tiledb::sm;

struct AzureFx {
  const std::string AZURE_PREFIX = "azure://";
  const tiledb::sm::URI AZURE_CONTAINER =
      tiledb::sm::URI(AZURE_PREFIX + random_container_name("tiledb") + "/");
  const std::string TEST_DIR = AZURE_CONTAINER.to_string() + "tiledb_test_dir/";
  tiledb::sm::Azure azure_;
  ThreadPool thread_pool_;

  AzureFx();
  ~AzureFx();

  static std::string random_container_name(const std::string& prefix);
};

AzureFx::AzureFx() {
  // Connect
  Config config;
#ifndef TILEDB_TESTS_AWS_AZURE_CONFIG
  // REQUIRE(config.set("azure.s3.endpoint_override", "localhost:9999").ok());
  // REQUIRE(config.set("azure.s3.scheme", "https").ok());
  // REQUIRE(config.set("azure.s3.use_virtual_addressing", "false").ok());
  // REQUIRE(config.set("azure.s3.verify_ssl", "false").ok());
#endif
  REQUIRE(thread_pool_.init(2).ok());
  REQUIRE(azure_.init(config, &thread_pool_).ok());

  // Create container
  bool is_container;
  REQUIRE(azure_.is_container(AZURE_CONTAINER, &is_container).ok());
  if (is_container)
    REQUIRE(azure_.remove_container(AZURE_CONTAINER).ok());

  REQUIRE(azure_.is_container(AZURE_CONTAINER, &is_container).ok());
  REQUIRE(!is_container);
  REQUIRE(azure_.create_container(AZURE_CONTAINER).ok());

  // Check if container is empty
  bool is_empty;
  REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
  REQUIRE(is_empty);
}

AzureFx::~AzureFx() {
  // Empty container
  bool is_empty;
  REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
  if (!is_empty) {
    REQUIRE(azure_.empty_container(AZURE_CONTAINER).ok());
    REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
    REQUIRE(is_empty);
  }

  // Delete container
  REQUIRE(azure_.remove_container(AZURE_CONTAINER).ok());
}

std::string AzureFx::random_container_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}

TEST_CASE_METHOD(AzureFx, "Test Azure filesystem, file management", "[azure]") {
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

  // Check that container is empty
  bool is_empty;
  REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
  REQUIRE(is_empty);

  // Continue building the hierarchy
  bool is_blob;
  REQUIRE(azure_.touch(URI(file1)).ok());
  REQUIRE(azure_.is_blob(URI(file1), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.touch(URI(file2)).ok());
  REQUIRE(azure_.is_blob(URI(file2), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.touch(URI(file3)).ok());
  REQUIRE(azure_.is_blob(URI(file3), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.touch(URI(file4)).ok());
  REQUIRE(azure_.is_blob(URI(file4), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.touch(URI(file5)).ok());
  REQUIRE(azure_.is_blob(URI(file5), &is_blob).ok());
  REQUIRE(is_blob);

  // Check that container is not empty
  REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
  REQUIRE(!is_empty);

  // Check invalid file
  REQUIRE(azure_.is_blob(URI(TEST_DIR + "foo"), &is_blob).ok());
  REQUIRE(!is_blob);

  // List with prefix
  std::vector<std::string> paths;
  REQUIRE(azure_.ls(URI(TEST_DIR), &paths).ok());
  REQUIRE(paths.size() == 3);
  paths.clear();
  REQUIRE(azure_.ls(URI(dir), &paths).ok());
  REQUIRE(paths.size() == 2);
  paths.clear();
  REQUIRE(azure_.ls(URI(subdir), &paths).ok());
  REQUIRE(paths.size() == 2);
  paths.clear();
  REQUIRE(azure_.ls(AZURE_CONTAINER, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Check if a directory exists
  bool is_dir = false;
  REQUIRE(azure_.is_dir(URI(file1), &is_dir).ok());
  REQUIRE(!is_dir);  // Not a dir
  REQUIRE(azure_.is_dir(URI(file4), &is_dir).ok());
  REQUIRE(!is_dir);  // Not a dir
  REQUIRE(azure_.is_dir(URI(dir), &is_dir).ok());
  REQUIRE(is_dir);  // This is viewed as a dir
  REQUIRE(azure_.is_dir(URI(TEST_DIR + "dir"), &is_dir).ok());
  REQUIRE(is_dir);  // This is viewed as a dir

  // Move file
  REQUIRE(azure_.move_object(URI(file5), URI(file6)).ok());
  REQUIRE(azure_.is_blob(URI(file5), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(file6), &is_blob).ok());
  REQUIRE(is_blob);
  paths.clear();
  REQUIRE(azure_.ls(AZURE_CONTAINER, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Move directory
  REQUIRE(azure_.move_dir(URI(dir), URI(dir2)).ok());
  REQUIRE(azure_.is_dir(URI(dir), &is_dir).ok());
  REQUIRE(!is_dir);
  REQUIRE(azure_.is_dir(URI(dir2), &is_dir).ok());
  REQUIRE(is_dir);
  paths.clear();
  REQUIRE(azure_.ls(AZURE_CONTAINER, &paths, "").ok());  // No delimiter
  REQUIRE(paths.size() == 5);

  // Remove files
  REQUIRE(azure_.remove_blob(URI(file4)).ok());
  REQUIRE(azure_.is_blob(URI(file4), &is_blob).ok());
  REQUIRE(!is_blob);

  // Remove directories
  REQUIRE(azure_.remove_dir(URI(dir2)).ok());
  REQUIRE(azure_.is_blob(URI(file1), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(file2), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(file3), &is_blob).ok());
  REQUIRE(!is_blob);
}

TEST_CASE_METHOD(AzureFx, "Test Azure filesystem, file I/O", "[azure]") {
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
  REQUIRE(azure_.write(URI(largefile), write_buffer, buffer_size).ok());
  REQUIRE(
      azure_.write(URI(largefile), write_buffer_small, buffer_size_small).ok());
  auto smallfile = TEST_DIR + "smallfile";
  REQUIRE(
      azure_.write(URI(smallfile), write_buffer_small, buffer_size_small).ok());

  // Before flushing, the files do not exist
  bool is_blob;
  REQUIRE(azure_.is_blob(URI(largefile), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(smallfile), &is_blob).ok());
  REQUIRE(!is_blob);

  // Flush the files
  REQUIRE(azure_.flush_blob(URI(largefile)).ok());
  REQUIRE(azure_.flush_blob(URI(smallfile)).ok());

  // After flushing, the files exist
  REQUIRE(azure_.is_blob(URI(largefile), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.is_blob(URI(smallfile), &is_blob).ok());
  REQUIRE(is_blob);

  // Get file sizes
  uint64_t nbytes = 0;
  REQUIRE(azure_.blob_size(URI(largefile), &nbytes).ok());
  REQUIRE(nbytes == (buffer_size + buffer_size_small));
  REQUIRE(azure_.blob_size(URI(smallfile), &nbytes).ok());
  REQUIRE(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  REQUIRE(azure_.read(URI(largefile), 0, read_buffer, 26).ok());
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Read from a different offset
  REQUIRE(azure_.read(URI(largefile), 11, read_buffer, 26).ok());
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);
}

#endif
