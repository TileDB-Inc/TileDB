/**
 * @file unit_ls_filtered.cc
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
 * Tests internal ls recursive filter.
 */

#include <test/support/tdb_catch.h>
#include <filesystem>
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/vfs.h"

class VFSTest {
 public:
  /**
   * Requires derived class to create a temporary directory.
   *
   * @param test_tree Vector used to build test directory and objects.
   *    For each element we create a nested directory with N objects.
   * @param prefix The URI prefix to use for the test directory.
   */
  explicit VFSTest(
      const std::vector<size_t>& test_tree, const std::string& prefix)
      : stats_("unit_ls_filtered")
      , io_(4)
      , compute_(4)
      , vfs_(&stats_, &io_, &compute_, tiledb::sm::Config())
      , test_tree_(test_tree)
      , prefix_(prefix)
      , temp_dir_(prefix_)
      , is_supported_(vfs_.supports_uri_scheme(temp_dir_)) {
  }

  virtual ~VFSTest() {
    bool is_dir = false;
    vfs_.is_dir(temp_dir_, &is_dir).ok();
    if (is_dir) {
      vfs_.remove_dir(temp_dir_).ok();
    }
  }

  inline bool is_supported() const {
    return is_supported_;
  }

  /** FilePredicate for passing to ls_filtered that accepts all files. */
  static bool accept_all_files(const std::string_view&, uint64_t) {
    return true;
  }

  /** Resources needed to construct VFS */
  tiledb::sm::stats::Stats stats_;
  ThreadPool io_, compute_;
  tiledb::sm::VFS vfs_;

  std::vector<size_t> test_tree_;
  std::string prefix_;
  tiledb::sm::URI temp_dir_;
  tiledb::sm::LsObjects expected_results_;

 protected:
  bool is_supported_;
};

// TODO: Disable shouldfail when file:// or mem:// support is added.
TEST_CASE(
    "VFS: Throwing FileFilter ls_recursive",
    "[vfs][ls_recursive][!shouldfail]") {
  std::string prefix = GENERATE("file://", "mem://");
  prefix += std::filesystem::current_path().string() + "/ls_filtered_test";

  VFSTest vfs_test({0}, prefix);
  auto file_filter = [](const std::string_view&, uint64_t) -> bool {
    throw std::logic_error("Throwing FileFilter");
  };
  SECTION("Throwing FileFilter with 0 objects should not throw") {
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_, file_filter, tiledb::sm::accept_all_dirs));
  }
  SECTION("Throwing FileFilter with N objects should throw") {
    vfs_test.vfs_.touch(vfs_test.temp_dir_.join_path("file")).ok();
    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, file_filter),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, file_filter),
        Catch::Matchers::ContainsSubstring("Throwing FileFilter"));
  }
}

TEST_CASE(
    "VFS: ls_recursive throws for unsupported filesystems",
    "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("file://", "mem://");
  prefix += std::filesystem::current_path().string() + "/ls_filtered_test";

  VFSTest vfs_test({1}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }
  std::string backend = vfs_test.temp_dir_.backend_name();

  // Currently only S3 is supported for VFS::ls_recursive.
  DYNAMIC_SECTION(backend << " unsupported backend should throw") {
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(
            vfs_test.temp_dir_, VFSTest::accept_all_files),
        Catch::Matchers::ContainsSubstring("storage backend is not supported"));
  }
}
