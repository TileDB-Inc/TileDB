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
#include "tiledb/common/random/prng.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/filesystem/vfs.h"

/**
 * Generates a random temp directory URI for use in VFS tests.
 *
 * @param prefix A prefix to use for the temp directory name. Should include
 *    `s3://`, `mem://` or other URI prefix for the backend.
 * @return URI which the caller can use to create a temp directory.
 */
tiledb::sm::URI test_dir(const std::string& prefix) {
  return tiledb::sm::URI(prefix + "tiledb-" + std::to_string(PRNG::get()()));
}

/**
 * Base class use for VFS and file system test objects. Deriving classes are
 * responsible for creating a temporary directory and populating it with test
 * objects for the related file system.
 */
class VFSTestBase {
 protected:
  /**
   * Requires derived class to create a temporary directory.
   *
   * @param test_tree Vector used to build test directory and objects.
   *    For each element we create a nested directory with N objects.
   * @param prefix The URI prefix to use for the test directory.
   */
  VFSTestBase(const std::vector<size_t>& test_tree, const std::string& prefix)
      : stats_("unit_ls_filtered")
      , compute_(4)
      , io_(4)
      , vfs_(&stats_, &io_, &compute_, create_test_config())
      , test_tree_(test_tree)
      , prefix_(prefix)
      , temp_dir_(test_dir(prefix_))
      , is_supported_(vfs_.supports_uri_scheme(temp_dir_)) {
    // TODO: Throw when we can provide list of supported filesystems to Catch2.
  }

 public:
  /** Type definition for objects returned from ls_recursive */
  using LsObjects = std::vector<std::pair<std::string, uint64_t>>;

  virtual ~VFSTestBase() {
    if (vfs_.supports_uri_scheme(temp_dir_)) {
      bool is_dir = false;
      vfs_.is_dir(temp_dir_, &is_dir).ok();
      if (is_dir) {
        vfs_.remove_dir(temp_dir_).ok();
      }
    }
  }

  /**
   * @return True if the URI prefix is supported by the build, else false.
   */
  inline bool is_supported() const {
    return is_supported_;
  }

  inline LsObjects& expected_results() {
    return expected_results_;
  }

  /**
   * Creates a config for testing VFS storage backends over local emulators.
   *
   * @return Fully initialized configuration for testing VFS storage backends.
   */
  static tiledb::sm::Config create_test_config() {
    tiledb::sm::Config cfg;
    // Set up connection to minio backend emulator.
    cfg.set("vfs.s3.endpoint_override", "localhost:9999").ok();
    cfg.set("vfs.s3.scheme", "https").ok();
    cfg.set("vfs.s3.use_virtual_addressing", "false").ok();
    cfg.set("vfs.s3.verify_ssl", "false").ok();
    cfg.set("vfs.azure.storage_account_name", "devstoreaccount1").ok();
    cfg.set(
           "vfs.azure.storage_account_key",
           "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
           "K1SZFPTOtr/KBHBeksoGMGw==")
        .ok();
    cfg.set(
           "vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount1")
        .ok();
    return cfg;
  }

  /** FilePredicate for passing to ls_filtered that accepts all files. */
  static bool accept_all_files(const std::string_view&, uint64_t) {
    return true;
  }

  /** Resources needed to construct VFS */
  tiledb::sm::stats::Stats stats_;
  ThreadPool compute_, io_;
  tiledb::sm::VFS vfs_;

  std::vector<size_t> test_tree_;
  std::string prefix_;
  tiledb::sm::URI temp_dir_;

 private:
  LsObjects expected_results_;
  bool is_supported_;
};

/**
 * Test object for tiledb::sm::VFS functionality. When constructed, this test
 * object creates a temporary directory and populates it using the test_tree
 * vector passed to the constructor. For each element in the vector, we create a
 * nested directory with N objects. The constructor also writes `10 * N` bytes
 * of data to each object created for testing returned object sizes are correct.
 *
 * This test object can be used for any valid VFS URI prefix, and is not
 * specific to any one backend.
 */
class VFSTest : public VFSTestBase {
 public:
  VFSTest(const std::vector<size_t>& test_tree, const std::string& prefix)
      : VFSTestBase(test_tree, prefix) {
    if (!is_supported()) {
      return;
    }

    if (temp_dir_.is_file() || temp_dir_.is_memfs() || temp_dir_.is_hdfs()) {
      vfs_.create_dir(temp_dir_).ok();
    } else {
      vfs_.create_bucket(temp_dir_).ok();
    }
    for (size_t i = 1; i <= test_tree_.size(); i++) {
      tiledb::sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
      // VFS::create_dir is a no-op for S3.
      vfs_.create_dir(path).ok();
      for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
        auto object_uri = path.join_path("test_file_" + std::to_string(j));
        vfs_.touch(object_uri).ok();
        std::string data(j * 10, 'a');
        vfs_.open_file(object_uri, tiledb::sm::VFSMode::VFS_WRITE).ok();
        vfs_.write(object_uri, data.data(), data.size()).ok();
        vfs_.close_file(object_uri).ok();
        expected_results().emplace_back(object_uri.to_string(), data.size());
      }
    }
    std::sort(expected_results().begin(), expected_results().end());
  }
};

/** Stub test object for tiledb::sm::Win and Posix functionality. */
class LocalFsTest : public VFSTestBase {
 public:
  explicit LocalFsTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "file://") {
    temp_dir_ =
        test_dir(prefix_ + std::filesystem::current_path().string() + "/");
  }
};

/** Stub test object for tiledb::sm::MemFilesystem functionality. */
class MemFsTest : public VFSTestBase {
 public:
  explicit MemFsTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "mem://") {
  }
};

#ifdef HAVE_S3

/** Test object for tiledb::sm::S3 functionality. */
class S3Test : public VFSTestBase, protected tiledb::sm::S3_within_VFS {
 public:
  explicit S3Test(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "s3://")
      , S3_within_VFS(&stats_, &io_, vfs_.config()) {
    s3().create_bucket(temp_dir_).ok();
    for (size_t i = 1; i <= test_tree_.size(); i++) {
      tiledb::sm::URI path = temp_dir_.join_path("subdir_" + std::to_string(i));
      // VFS::create_dir is a no-op for S3; Just create objects.
      for (size_t j = 1; j <= test_tree_[i - 1]; j++) {
        auto object_uri = path.join_path("test_file_" + std::to_string(j));
        s3().touch(object_uri).ok();
        std::string data(j * 10, 'a');
        s3().write(object_uri, data.data(), data.size()).ok();
        s3().flush_object(object_uri).ok();
        expected_results().emplace_back(object_uri.to_string(), data.size());
      }
    }
    std::sort(expected_results().begin(), expected_results().end());
  }

  /** Expose protected accessor from S3_within_VFS. */
  tiledb::sm::S3& get_s3() {
    return s3();
  }

  /** Expose protected const accessor from S3_within_VFS. */
  const tiledb::sm::S3& get_s3() const {
    return s3();
  }
};

TEST_CASE(
    "S3: S3Scanner iterator to populate vector", "[s3][ls-scan-iterator]") {
  S3Test s3_test({10, 50});
  bool recursive = true;
  // 1000 is the default max_keys for S3. This is the same default used by
  // S3Scanner. Testing with small max_keys validates the iterator handles batch
  // collection and filtering appropriately.
  int max_keys = GENERATE(1000, 10, 7);

  DYNAMIC_SECTION("Testing with " << max_keys << " max keys from S3") {
    tiledb::sm::FileFilter file_filter;
    auto expected = s3_test.expected_results();

    SECTION("Accept all objects") {
      file_filter = [](const std::string_view&, uint64_t) { return true; };
      std::sort(expected.begin(), expected.end());
    }

    SECTION("Reject all objects") {
      file_filter = [](const std::string_view&, uint64_t) { return false; };
    }

    SECTION("Filter objects including 'test_file_1' in key") {
      file_filter = [](const std::string_view& path, uint64_t) {
        if (path.find("test_file_1") != std::string::npos) {
          return true;
        }
        return false;
      };
    }

    SECTION("Scan for a single object") {
      file_filter = [](const std::string_view& path, uint64_t) {
        if (path.find("test_file_50") != std::string::npos) {
          return true;
        }
        return false;
      };
    }

    // Filter expected results to apply file_filter.
    std::erase_if(expected, [&file_filter](const auto& a) {
      return !file_filter(a.first, a.second);
    });

    auto scan = s3_test.get_s3().scanner(
        s3_test.temp_dir_,
        file_filter,
        tiledb::sm::accept_all_dirs,
        recursive,
        max_keys);
    std::vector results_vector(scan.begin(), scan.end());

    CHECK(results_vector.size() == expected.size());
    for (size_t i = 0; i < expected.size(); i++) {
      auto s3_object = results_vector[i];
      CHECK(file_filter(s3_object.GetKey(), s3_object.GetSize()));
      auto full_uri = s3_test.temp_dir_.to_string() + "/" + s3_object.GetKey();
      CHECK(full_uri == expected[i].first);
      CHECK(static_cast<uint64_t>(s3_object.GetSize()) == expected[i].second);
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
        VFSTest::accept_all_files,
        tiledb::sm::accept_all_dirs,
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
    auto s3_object = results_vector[i];
    auto full_uri = s3_test.temp_dir_.to_string() + "/" + s3_object.GetKey();
    CHECK(full_uri == expected[i].first);
    CHECK(static_cast<uint64_t>(s3_object.GetSize()) == expected[i].second);
  }
}

#else  // HAVE_S3

/**
 * Stub object for S3 functionality. Ensures S3Test is constructable for builds
 * with S3 disabled, allowing use of S3Test in TEMPLATE_LIST_TEST_CASE for any
 * build configuration.
 */
class S3Test : public VFSTestBase, protected tiledb::sm::S3_within_VFS {
 public:
  explicit S3Test(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "s3://")
      , S3_within_VFS(&stats_, &io_, vfs_.config()) {
  }
};

#endif  // HAVE_S3

/** Stub test object for tiledb::sm::Azure functionality. */
class AzureTest : public VFSTestBase {
 public:
  explicit AzureTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "azure://") {
  }
};

/** Stub test object for tiledb::sm::GCS functionality. */
class GCSTest : public VFSTestBase {
 public:
  explicit GCSTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "gcs://") {
  }
};

/** Stub test object for tiledb::sm::HDFS functionality. */
class HDFSTest : public VFSTestBase {
 public:
  explicit HDFSTest(const std::vector<size_t>& test_tree)
      : VFSTestBase(test_tree, "hdfs://") {
  }
};

// Currently only S3 is supported for VFS::ls_recursive.
using TestBackends = std::tuple<S3Test>;
TEMPLATE_LIST_TEST_CASE(
    "VFS: Test internal ls_filtered recursion argument",
    "[vfs][ls_filtered][recursion]",
    TestBackends) {
  TestType fs({10, 50});
  if (!fs.is_supported()) {
    return;
  }

  bool recursive = GENERATE(true, false);
  DYNAMIC_SECTION(
      fs.temp_dir_.backend_name()
      << " ls_filtered with recursion: " << (recursive ? "true" : "false")) {
#ifdef HAVE_S3
    // If testing with recursion use the root directory, otherwise use a subdir.
    auto path = recursive ? fs.temp_dir_ : fs.temp_dir_.join_path("subdir_1");
    auto ls_objects = fs.get_s3().ls_filtered(
        path,
        VFSTestBase::accept_all_files,
        tiledb::sm::accept_all_dirs,
        recursive);

    auto expected = fs.expected_results();
    if (!recursive) {
      // If non-recursive, all objects in the first directory should be
      // returned.
      std::erase_if(expected, [](const auto& p) {
        return p.first.find("subdir_1") == std::string::npos;
      });
    }

    CHECK(ls_objects.size() == expected.size());
    CHECK(ls_objects == expected);
#endif
  }
}

// TODO: Disable shouldfail when file:// or mem:// support is added.
TEST_CASE(
    "VFS: Throwing FileFilter ls_recursive",
    "[vfs][ls_recursive][!shouldfail]") {
  std::string prefix = GENERATE("file://", "mem://");
  if (prefix == "file://" || prefix == "mem://") {
    prefix += std::filesystem::current_path().string() + "/ls_filtered_test";
  }

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

// TODO: Combine with above test when file:// and mem:// support is added.
TEST_CASE(
    "VFS: Throwing FileFilter for ls_recursive",
    "[vfs][ls_recursive][file-filter]") {
  std::string prefix = "s3://";
  VFSTest vfs_test({0}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }

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
    "VFS: ls_recursive throws for unsupported backends",
    "[vfs][ls_recursive]") {
  std::string prefix =
      GENERATE("file://", "mem://", "s3://", "hdfs://", "azure://", "gcs://");
  if (prefix == "file://" || prefix == "mem://") {
    prefix += std::filesystem::current_path().string() + "/ls_filtered_test";
  }

  VFSTest vfs_test({1}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }
  std::string backend = vfs_test.temp_dir_.backend_name();

  if (vfs_test.temp_dir_.is_s3()) {
    DYNAMIC_SECTION(backend << " supported backend should not throw") {
      CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
          vfs_test.temp_dir_, VFSTestBase::accept_all_files));
    }
  } else {
    DYNAMIC_SECTION(backend << " unsupported backend should throw") {
      CHECK_THROWS_WITH(
          vfs_test.vfs_.ls_recursive(
              vfs_test.temp_dir_, VFSTestBase::accept_all_files),
          Catch::Matchers::ContainsSubstring(
              "storage backend is not supported"));
    }
  }
}
