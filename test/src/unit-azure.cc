/**
 * @file   unit-azure.cc
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
 * Tests for AZURE API filesystem functions.
 */

#ifdef HAVE_AZURE

#include <test/support/tdb_catch.h>
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/azure.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_time.h"

#include <fstream>
#include <thread>

using namespace tiledb::common;
using namespace tiledb::sm;

using ConfMap = std::map<std::string, std::string>;
using ConfList = std::vector<ConfMap>;

static ConfList test_settings = {
    {{"vfs.azure.storage_account_name", "devstoreaccount1"},
     {"vfs.azure.storage_account_key",
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
      "K1SZFPTOtr/KBHBeksoGMGw=="},
     {"vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount1"}},
    // Currently disabled because it does not work with the Azurite emulator
    // The SAS path was manually tested against the Azure Blob Service.
    //{{"vfs.azure.storage_account_name", "devstoreaccount2"},
    // {"vfs.azure.storage_sas_token", ""},
    // {"vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount2"}}
};

struct AzureFx {
  const std::string AZURE_PREFIX = "azure://";
  const tiledb::sm::URI AZURE_CONTAINER =
      tiledb::sm::URI(AZURE_PREFIX + random_container_name("tiledb") + "/");
  const std::string TEST_DIR = AZURE_CONTAINER.to_string() + "tiledb_test_dir/";

  tiledb::sm::Azure azure_;
  ThreadPool thread_pool_{2};

  AzureFx() = default;
  ~AzureFx();

  void init_azure(Config&& config, ConfMap);

  static std::string random_container_name(const std::string& prefix);
};

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

void AzureFx::init_azure(Config&& config, ConfMap settings) {
  auto set_conf = [&](auto iter) {
    std::string key = iter.first;
    std::string val = iter.second;
    REQUIRE(config.set(key, val).ok());
  };

  // Set provided config settings for connection
  std::for_each(settings.begin(), settings.end(), set_conf);

  // Initialize
  REQUIRE(azure_.init(config, &thread_pool_).ok());

  // Create container
  bool is_container;
  REQUIRE(azure_.is_container(AZURE_CONTAINER, &is_container).ok());
  if (is_container) {
    REQUIRE(azure_.remove_container(AZURE_CONTAINER).ok());
  }

  REQUIRE(azure_.is_container(AZURE_CONTAINER, &is_container).ok());
  REQUIRE(!is_container);
  REQUIRE(azure_.create_container(AZURE_CONTAINER).ok());

  // Check if container is empty
  bool is_empty;
  REQUIRE(azure_.is_empty_container(AZURE_CONTAINER, &is_empty).ok());
  REQUIRE(is_empty);
}

std::string AzureFx::random_container_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}

TEST_CASE_METHOD(AzureFx, "Test Azure filesystem, file management", "[azure]") {
  Config config;
  REQUIRE(config.set("vfs.azure.use_block_list_upload", "true").ok());

  auto settings =
      GENERATE(from_range(test_settings.begin(), test_settings.end()));
  init_azure(std::move(config), settings);

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
  bool is_blob = false;
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

  // ls_with_sizes
  std::string s = "abcdef";
  CHECK(azure_.write(URI(file3), s.data(), s.size()).ok());
  REQUIRE(azure_.flush_blob(URI(file3)).ok());

  auto&& [status, rv] = azure_.ls_with_sizes(URI(dir));
  auto children = *rv;
  REQUIRE(status.ok());

  REQUIRE(children.size() == 2);
  CHECK(children[0].path().native() == file3);
  CHECK(children[1].path().native() == subdir.substr(0, subdir.size() - 1));

  CHECK(children[0].file_size() == s.size());
  // Directories don't get a size
  CHECK(children[1].file_size() == 0);

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

TEST_CASE_METHOD(
    AzureFx, "Test Azure filesystem, file I/O", "[azure][multipart]") {
  Config config;
  const uint64_t max_parallel_ops = 2;
  const uint64_t block_list_block_size = 4 * 1024 * 1024;
  REQUIRE(config.set("vfs.azure.use_block_list_upload", "true").ok());
  REQUIRE(
      config.set("vfs.azure.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config
              .set(
                  "vfs.azure.block_list_block_size",
                  std::to_string(block_list_block_size))
              .ok());

  auto settings =
      GENERATE(from_range(test_settings.begin(), test_settings.end()));
  init_azure(std::move(config), settings);

  const uint64_t write_cache_max_size =
      max_parallel_ops * block_list_block_size;

  // Prepare buffers
  uint64_t buffer_size = write_cache_max_size * 5;
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
  bool is_blob = false;
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
  uint64_t bytes_read = 0;
  REQUIRE(azure_.read(URI(largefile), 0, read_buffer, 26, 0, &bytes_read).ok());
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
  REQUIRE(
      azure_.read(URI(largefile), 11, read_buffer, 26, 0, &bytes_read).ok());
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
    AzureFx,
    "Test Azure filesystem, file I/O, no multipart",
    "[azure][no_multipart]") {
  Config config;
  const uint64_t max_parallel_ops = 2;
  const uint64_t block_list_block_size = 4 * 1024 * 1024;
  REQUIRE(config.set("vfs.azure.use_block_list_upload", "false").ok());
  REQUIRE(
      config.set("vfs.azure.max_parallel_ops", std::to_string(max_parallel_ops))
          .ok());
  REQUIRE(config
              .set(
                  "vfs.azure.block_list_block_size",
                  std::to_string(block_list_block_size))
              .ok());

  auto settings =
      GENERATE(from_range(test_settings.begin(), test_settings.end()));
  init_azure(std::move(config), settings);

  const uint64_t write_cache_max_size =
      max_parallel_ops * block_list_block_size;

  // Prepare a large buffer that can fit in the write cache.
  uint64_t large_buffer_size = write_cache_max_size;
  auto large_write_buffer = new char[large_buffer_size];
  for (uint64_t i = 0; i < large_buffer_size; i++)
    large_write_buffer[i] = (char)('a' + (i % 26));

  // Prepare a small buffer that can fit in the write cache.
  uint64_t small_buffer_size = write_cache_max_size / 1024;
  auto small_write_buffer = new char[small_buffer_size];
  for (uint64_t i = 0; i < small_buffer_size; i++)
    small_write_buffer[i] = (char)('a' + (i % 26));

  // Prepare a buffer too large to fit in the write cache.
  uint64_t oob_buffer_size = write_cache_max_size + 1;
  auto oob_write_buffer = new char[oob_buffer_size];
  for (uint64_t i = 0; i < oob_buffer_size; i++)
    oob_write_buffer[i] = (char)('a' + (i % 26));

  auto large_file = TEST_DIR + "largefile";
  REQUIRE(azure_.write(URI(large_file), large_write_buffer, large_buffer_size)
              .ok());

  auto small_file_1 = TEST_DIR + "smallfile1";
  REQUIRE(azure_.write(URI(small_file_1), small_write_buffer, small_buffer_size)
              .ok());

  auto small_file_2 = TEST_DIR + "smallfile2";
  REQUIRE(azure_.write(URI(small_file_2), small_write_buffer, small_buffer_size)
              .ok());
  REQUIRE(azure_.write(URI(small_file_2), small_write_buffer, small_buffer_size)
              .ok());

  auto oob_file = TEST_DIR + "oobfile";
  REQUIRE(!azure_.write(URI(oob_file), oob_write_buffer, oob_buffer_size).ok());

  // Before flushing, the files do not exist
  bool is_blob = false;
  REQUIRE(azure_.is_blob(URI(large_file), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(small_file_1), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(small_file_2), &is_blob).ok());
  REQUIRE(!is_blob);
  REQUIRE(azure_.is_blob(URI(oob_file), &is_blob).ok());
  REQUIRE(!is_blob);

  // Flush the files
  REQUIRE(azure_.flush_blob(URI(small_file_1)).ok());
  REQUIRE(azure_.flush_blob(URI(small_file_2)).ok());
  REQUIRE(azure_.flush_blob(URI(large_file)).ok());

  // After flushing, the files exist
  REQUIRE(azure_.is_blob(URI(large_file), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.is_blob(URI(small_file_1), &is_blob).ok());
  REQUIRE(is_blob);
  REQUIRE(azure_.is_blob(URI(small_file_2), &is_blob).ok());
  REQUIRE(is_blob);

  // Get file sizes
  uint64_t nbytes = 0;
  REQUIRE(azure_.blob_size(URI(large_file), &nbytes).ok());
  CHECK(nbytes == large_buffer_size);
  REQUIRE(azure_.blob_size(URI(small_file_1), &nbytes).ok());
  CHECK(nbytes == small_buffer_size);
  REQUIRE(azure_.blob_size(URI(small_file_2), &nbytes).ok());
  CHECK(nbytes == (small_buffer_size + small_buffer_size));

  // Read from the beginning
  auto read_buffer = new char[26];
  uint64_t bytes_read = 0;
  REQUIRE(
      azure_.read(URI(large_file), 0, read_buffer, 26, 0, &bytes_read).ok());
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
  REQUIRE(
      azure_.read(URI(large_file), 11, read_buffer, 26, 0, &bytes_read).ok());
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

#endif
