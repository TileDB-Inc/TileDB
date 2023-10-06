/**
 * @file unit_vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Tests the `VFS` class.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/filesystem/vfs.h"

struct VfsFixture {
  VfsFixture()
      : stats_("unit_vfs")
      , compute_(4)
      , io_(4)
      , vfs_(&stats_, &compute_, &io_, tiledb::sm::Config()) {
  }

  void create_objects(
      const tiledb::sm::URI& dir, size_t count, const std::string& prefix) {
    for (size_t i = 0; i < count; i++) {
      auto uri = dir.join_path(prefix + std::to_string(i));
      vfs_.touch(uri).ok();
      std::string data(i * 10, 'a');
      vfs_.write(uri, data.data(), data.size()).ok();
      expected_results_.emplace_back(uri.to_path(), data.size(), false);
    }
  }

 protected:
  // A dummy `Stats` instance for testing internal VFS functions.
  tiledb::sm::stats::Stats stats_;

  ThreadPool compute_, io_;
  tiledb::sm::VFS vfs_;

  std::vector<filesystem::directory_entry> expected_results_;
};

TEST_CASE_METHOD(
    VfsFixture, "VFS: Default arguments ls_recursive", "[vfs][ls_recursive]]") {
  tiledb::sm::URI temp_dir("vfs_default_args");
  vfs_.create_dir(temp_dir).ok();
  SECTION("Empty directory") {
    create_objects(temp_dir, 0, "file");
  }

  SECTION("Single file") {
    create_objects(temp_dir, 1, "file");
  }

  SECTION("Multiple files in single directory") {
    create_objects(temp_dir, 10, "file");
  }

  SECTION("Multiple files in nested directories") {
    create_objects(temp_dir, 10, "file");

    auto subdir = temp_dir.join_path("subdir");
    vfs_.create_dir(subdir).ok();
    create_objects(subdir, 10, "file");
  }

  auto results = vfs_.ls_recursive(temp_dir);
  REQUIRE(results.size() == expected_results_.size());
  for (size_t i = 0; i < expected_results_.size(); i++) {
    CHECK(results[i].path().native() == expected_results_[i].path().native());
    CHECK(results[i].file_size() == expected_results_[i].file_size());
  }
  vfs_.remove_dir(temp_dir).ok();
}

TEST_CASE_METHOD(
    VfsFixture, "VFS: Throwing callback ls_recursive", "[vfs][ls_recursive]") {
  auto cb = [](const char*, size_t, uint64_t, void*) -> int32_t {
    throw std::logic_error("Throwing callback");
  };
  tiledb::sm::URI temp_dir("vfs_throwing_callback");
  int data;
  SECTION("Throwing callback with 0 objects should not throw") {
    CHECK_NOTHROW(vfs_.ls_recursive(temp_dir, cb, &data));
  }
  SECTION("Throwing callback with N objects should throw") {
    vfs_.create_dir(temp_dir).ok();
    vfs_.touch(temp_dir.join_path("file")).ok();
    CHECK_THROWS_AS(vfs_.ls_recursive(temp_dir, cb, &data), std::logic_error);
    vfs_.remove_dir(temp_dir).ok();
  }
}
