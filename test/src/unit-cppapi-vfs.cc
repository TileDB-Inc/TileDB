/**
 * @file   unit-cppapi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB Inc.
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
 * Tests the C++ API for the VFS functionality.
 */

#include <test/support/src/helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/vfs_experimental.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::test;

TEST_CASE("C++ API: Test VFS ls", "[cppapi][cppapi-vfs][cppapi-vfs-ls]") {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);
#ifdef _WIN32
  std::string path = sm::Win::current_dir() + "\\vfs_test\\";
#else
  std::string path =
      std::string("file://") + sm::Posix::current_dir() + "/vfs_test/";
#endif

  // Clean up
  if (vfs.is_dir(path))
    vfs.remove_dir(path);

  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string file2 = dir + "/file2";
  std::string subdir = dir + "/subdir";
  std::string subdir2 = dir + "/subdir2";
  std::string subdir_empty = dir + "/subdir_empty";
  std::string subdir_file = subdir + "/file";
  std::string subdir_file2 = subdir2 + "/file2";

  // Create directories and files
  vfs.create_dir(path);
  vfs.create_dir(dir);
  vfs.create_dir(subdir);
  vfs.create_dir(subdir2);
  vfs.create_dir(subdir_empty);
  vfs.touch(file);
  vfs.touch(file2);
  vfs.touch(subdir_file);
  vfs.touch(subdir_file2);

  // List
  std::vector<std::string> children = vfs.ls(dir);

#ifdef _WIN32
  // Normalization only for Windows
  file = tiledb::sm::path_win::uri_from_path(file);
  file2 = tiledb::sm::path_win::uri_from_path(file2);
  subdir = tiledb::sm::path_win::uri_from_path(subdir);
  subdir2 = tiledb::sm::path_win::uri_from_path(subdir2);
  subdir_empty = tiledb::sm::path_win::uri_from_path(subdir_empty);
#endif

  // Check results
  std::sort(children.begin(), children.end());
  REQUIRE(children.size() == 5);
  CHECK(children[0] == file);
  CHECK(children[1] == file2);
  CHECK(children[2] == subdir);
  CHECK(children[3] == subdir2);
  CHECK(children[4] == subdir_empty);

  // Clean up
  vfs.remove_dir(path);
}

TEST_CASE(
    "C++ API: Test VFS dir size", "[cppapi][cppapi-vfs][cppapi-vfs-dir-size]") {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);

#ifdef _WIN32
  std::string path = sm::Win::current_dir() + "\\vfs_test\\";
#else
  std::string path =
      std::string("file://") + sm::Posix::current_dir() + "/vfs_test/";
#endif

  // Clean up
  if (vfs.is_dir(path))
    vfs.remove_dir(path);

  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string file2 = dir + "/file2";
  std::string subdir = dir + "/subdir";
  std::string subdir2 = dir + "/subdir2";
  std::string subdir_file = subdir + "/file";
  std::string subdir_file2 = subdir + "/file2";

  // Create directories and files
  vfs.create_dir(path);
  vfs.create_dir(dir);
  vfs.create_dir(subdir);
  vfs.create_dir(subdir2);
  vfs.touch(file);
  vfs.touch(file2);
  vfs.touch(subdir_file);
  vfs.touch(subdir_file2);

  // Write to file
  tiledb::VFS::filebuf fbuf(vfs);
  fbuf.open(file, std::ios::out);
  std::ostream os(&fbuf);
  std::string s1 = "abcd";
  os.write(s1.data(), s1.size());
  fbuf.close();

  // Write to file2
  fbuf.open(file2, std::ios::out);
  std::ostream os2(&fbuf);
  std::string s2 = "abcdefgh";
  os2.write(s2.data(), s2.size());
  fbuf.close();

  // Write to subdir_file
  fbuf.open(subdir_file, std::ios::out);
  std::ostream os3(&fbuf);
  std::string s3 = "a";
  os3.write(s3.data(), s3.size());
  fbuf.close();

  // Write to subdir_file2
  fbuf.open(subdir_file2, std::ios::out);
  std::ostream os4(&fbuf);
  std::string s4 = "ab";
  os4.write(s4.data(), s4.size());
  fbuf.close();

  // Get directory size
  auto dir_size = vfs.dir_size(path);
  CHECK(dir_size == 15);

  // Clean up
  vfs.remove_dir(path);
}

TEST_CASE(
    "C++ API: Test VFS copy file",
    "[cppapi][cppapi-vfs][cppapi-vfs-copy-file]") {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);

#ifndef _WIN32
  std::string path =
      std::string("file://") + sm::Posix::current_dir() + "/vfs_test/";

  // Clean up
  if (vfs.is_dir(path))
    vfs.remove_dir(path);

  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string file2 = dir + "/file2";

  // Create directories and files
  vfs.create_dir(path);
  vfs.create_dir(dir);
  vfs.touch(file);

  // Write to file
  tiledb::VFS::filebuf fbuf(vfs);
  fbuf.open(file, std::ios::out);
  std::ostream os(&fbuf);
  std::string s1 = "abcd";
  os.write(s1.data(), s1.size());

  // Copy file when running on POSIX
  vfs.copy_file(file, file2);
  REQUIRE(vfs.is_file(file2));

  fbuf.open(file, std::ios::in);
  std::istream is1(&fbuf);
  if (!is1.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s2;
  s2.resize(vfs.file_size(file));
  is1.read((char*)s2.data(), vfs.file_size(file));
  fbuf.close();

  fbuf.open(file2, std::ios::in);
  std::istream is2(&fbuf);
  if (!is2.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s3;
  s3.resize(vfs.file_size(file2));
  is2.read((char*)s3.data(), vfs.file_size(file2));
  fbuf.close();

  REQUIRE(s2 == s3);

  // Clean up
  vfs.remove_dir(path);

#endif
}

TEST_CASE(
    "C++ API: Test VFS copy directory",
    "[cppapi][cppapi-vfs][cppapi-vfs-copy-dir]") {
  using namespace tiledb;
  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);

#ifndef _WIN32
  std::string path =
      std::string("file://") + sm::Posix::current_dir() + "/vfs_test/";

  // Clean up
  if (vfs.is_dir(path))
    vfs.remove_dir(path);

  std::string dir = path + "ls_dir";
  std::string dir2 = path + "ls_dir2";
  std::string file = dir + "/file";
  std::string file2 = dir + "/file2";
  std::string subdir = dir + "/subdir";
  std::string subdir2 = dir + "/subdir2";
  std::string sub_subdir = subdir + "/subdir";
  std::string subdir_file = subdir + "/file";
  std::string subdir_file2 = subdir + "/file2";
  std::string sub_subdir_file = sub_subdir + "/file";

  std::string file3 = dir2 + "/file";
  std::string file4 = dir2 + "/file2";
  std::string subdir3 = dir2 + "/subdir";
  std::string subdir4 = dir2 + "/subdir2";
  std::string sub_subdir2 = subdir3 + "/subdir";
  std::string subdir_file3 = subdir3 + "/file";
  std::string subdir_file4 = subdir3 + "/file2";
  std::string sub_subdir_file2 = sub_subdir2 + "/file";

  // Create directories and files
  vfs.create_dir(path);
  vfs.create_dir(dir);
  vfs.create_dir(subdir);
  vfs.create_dir(sub_subdir);
  vfs.create_dir(subdir2);
  vfs.touch(file);
  vfs.touch(file2);
  vfs.touch(subdir_file);
  vfs.touch(subdir_file2);
  vfs.touch(sub_subdir_file);

  // Write to files
  tiledb::VFS::filebuf fbuf(vfs);
  fbuf.open(file, std::ios::out);
  std::ostream os(&fbuf);
  std::string s1 = "abcd";
  os.write(s1.data(), s1.size());

  fbuf.open(file2, std::ios::out);
  std::ostream os1(&fbuf);
  std::string s2 = "efgh";
  os1.write(s2.data(), s2.size());

  fbuf.open(subdir_file, std::ios::out);
  std::ostream os2(&fbuf);
  std::string s3 = "ijkl";
  os2.write(s3.data(), s3.size());

  fbuf.open(subdir_file2, std::ios::out);
  std::ostream os3(&fbuf);
  std::string s4 = "mnop";
  os3.write(s4.data(), s4.size());
  fbuf.close();

  fbuf.open(sub_subdir_file, std::ios::out);
  std::ostream os4(&fbuf);
  std::string s5 = "qrst";
  os3.write(s5.data(), s5.size());
  fbuf.close();

  // Copy directory when running on POSIX
  vfs.copy_dir(dir, dir2);
  REQUIRE(vfs.is_dir(dir2));

  // Check that ls trees are correct
  // and that all files / dirs exist as expected
  std::vector<std::string> dir_vector = vfs.ls(dir);
  std::vector<std::string> dir2_vector = vfs.ls(dir2);
  while (!dir_vector.empty() || !dir2_vector.empty()) {
    std::string dir_file_name_abs = dir_vector.front();
    std::string dir_file_name = dir_file_name_abs.substr(dir.length() + 1);
    std::string dir2_file_name_abs = dir2_vector.front();
    std::string dir2_file_name = dir2_file_name_abs.substr(dir2.length() + 1);

    if (vfs.is_dir(dir_file_name_abs)) {
      REQUIRE(vfs.is_dir(dir_file_name_abs));
      std::vector<std::string> dir_vector2 = vfs.ls(dir_file_name_abs);
      dir_vector.insert(
          dir_vector.end(), dir_vector2.begin(), dir_vector2.end());
    } else {
      REQUIRE(vfs.is_file(dir_file_name_abs));
    }
    if (vfs.is_dir(dir2_file_name_abs)) {
      REQUIRE(vfs.is_dir(dir2_file_name_abs));
      std::vector<std::string> dir2_vector2 = vfs.ls(dir2_file_name_abs);
      dir2_vector.insert(
          dir2_vector.end(), dir2_vector2.begin(), dir2_vector2.end());
    } else {
      REQUIRE(vfs.is_file(dir2_file_name_abs));
    }
    REQUIRE(dir_file_name == dir2_file_name);
    dir_vector.erase(dir_vector.begin());
    dir2_vector.erase(dir2_vector.begin());
  }

  // Check that files in dir2 are equal to their
  // corresponding file in dir
  fbuf.open(file, std::ios::in);
  std::istream is1(&fbuf);
  if (!is1.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s6;
  s6.resize(vfs.file_size(file));
  is1.read((char*)s6.data(), vfs.file_size(file));
  fbuf.close();
  fbuf.open(file3, std::ios::in);
  std::istream is2(&fbuf);
  if (!is2.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s7;
  s7.resize(vfs.file_size(file3));
  is2.read((char*)s7.data(), vfs.file_size(file3));
  fbuf.close();
  REQUIRE(s6 == s7);

  fbuf.open(file2, std::ios::in);
  std::istream is3(&fbuf);
  if (!is3.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s8;
  s8.resize(vfs.file_size(file2));
  is3.read((char*)s8.data(), vfs.file_size(file2));
  fbuf.close();
  fbuf.open(file4, std::ios::in);
  std::istream is4(&fbuf);
  if (!is4.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s9;
  s9.resize(vfs.file_size(file4));
  is4.read((char*)s9.data(), vfs.file_size(file4));
  fbuf.close();
  REQUIRE(s8 == s9);

  fbuf.open(subdir_file, std::ios::in);
  std::istream is5(&fbuf);
  if (!is5.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s10;
  s10.resize(vfs.file_size(subdir_file));
  is5.read((char*)s10.data(), vfs.file_size(subdir_file));
  fbuf.close();
  fbuf.open(subdir_file3, std::ios::in);
  std::istream is6(&fbuf);
  if (!is6.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s11;
  s11.resize(vfs.file_size(subdir_file3));
  is6.read((char*)s11.data(), vfs.file_size(subdir_file3));
  fbuf.close();
  REQUIRE(s10 == s11);

  fbuf.open(subdir_file2, std::ios::in);
  std::istream is7(&fbuf);
  if (!is7.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s12;
  s12.resize(vfs.file_size(subdir_file2));
  is7.read((char*)s12.data(), vfs.file_size(subdir_file2));
  fbuf.close();
  fbuf.open(subdir_file4, std::ios::in);
  std::istream is8(&fbuf);
  if (!is8.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s13;
  s13.resize(vfs.file_size(subdir_file4));
  is8.read((char*)s13.data(), vfs.file_size(subdir_file4));
  fbuf.close();
  REQUIRE(s12 == s13);

  fbuf.open(sub_subdir_file, std::ios::in);
  std::istream is9(&fbuf);
  if (!is9.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s14;
  s14.resize(vfs.file_size(sub_subdir_file));
  is9.read((char*)s14.data(), vfs.file_size(sub_subdir_file));
  fbuf.close();
  fbuf.open(sub_subdir_file2, std::ios::in);
  std::istream is10(&fbuf);
  if (!is10.good()) {
    std::cerr << "Error opening file.\n";
    return;
  }
  std::string s15;
  s15.resize(vfs.file_size(sub_subdir_file2));
  is10.read((char*)s15.data(), vfs.file_size(sub_subdir_file2));
  fbuf.close();
  REQUIRE(s14 == s15);

  // Clean up
  vfs.remove_dir(path);

#endif
}

TEST_CASE(
    "C++ API: Test VFS is_empty_bucket",
    "[cppapi][cppapi-vfs][vfs-is-empty-bucket]") {
  tiledb::Config config;
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
  REQUIRE_NOTHROW(config.set("vfs.s3.endpoint_override", "localhost:9999"));
  REQUIRE_NOTHROW(config.set("vfs.s3.scheme", "https"));
  REQUIRE_NOTHROW(config.set("vfs.s3.use_virtual_addressing", "false"));
  REQUIRE_NOTHROW(config.set("vfs.s3.verify_ssl", "false"));
#endif
  tiledb::Context ctx(config);
  int s3 = 0;
  REQUIRE(
      tiledb_ctx_is_supported_fs(ctx.ptr().get(), TILEDB_S3, &s3) == TILEDB_OK);
  if (s3) {
    tiledb::VFS vfs(ctx);
    std::string bucket_name = "s3://tiledb-" + random_label() + "/";
    if (vfs.is_bucket(bucket_name)) {
      REQUIRE_NOTHROW(vfs.remove_bucket(bucket_name));
    }
    REQUIRE(vfs.is_bucket(bucket_name) == false);

    REQUIRE_NOTHROW(vfs.create_bucket(bucket_name));
    CHECK(vfs.is_bucket(bucket_name) == true);
    CHECK(vfs.is_empty_bucket(bucket_name) == true);

    REQUIRE_NOTHROW(vfs.touch(bucket_name + "/test.txt"));
    CHECK(vfs.is_empty_bucket(bucket_name) == false);

    REQUIRE_NOTHROW(vfs.remove_file(bucket_name + "/test.txt"));
    CHECK(vfs.is_empty_bucket(bucket_name) == true);

    if (vfs.is_bucket(bucket_name)) {
      REQUIRE_NOTHROW(vfs.remove_bucket(bucket_name));
    }
  }
}

using ls_recursive_test_types = std::tuple<
    tiledb::test::LocalFsTest,
    tiledb::test::S3Test,
    tiledb::test::AzureTest,
    tiledb::test::GCSTest>;
TEMPLATE_LIST_TEST_CASE(
    "CPP API: VFS ls_recursive filter",
    "[cppapi][vfs][ls-recursive]",
    ls_recursive_test_types) {
  using namespace tiledb::test;
  TestType test({10, 100, 0});
  if (!test.is_supported()) {
    return;
  }
  auto expected_results = test.expected_results();

  vfs_config cfg;
  tiledb::Context ctx(tiledb::Config(&cfg.config));
  tiledb::VFS vfs(ctx);

  tiledb::VFSExperimental::LsObjects ls_objects;
  // Predicate filter to apply to ls_recursive.
  tiledb::VFSExperimental::LsInclude include;
  // Callback to populate ls_objects vector using a filter.
  tiledb::VFSExperimental::LsCallback cb = [&](std::string_view path,
                                               uint64_t size) {
    if (include(path, size)) {
      ls_objects.emplace_back(path, size);
    }
    return true;
  };

  SECTION("Default filter (include all)") {
    include = [](std::string_view, uint64_t) { return true; };
  }
  SECTION("Custom filter (include none)") {
    include = [](std::string_view, uint64_t) { return false; };
  }

  SECTION("Custom filter (search for test_file_50)") {
    include = [](std::string_view object_name, uint64_t) {
      return object_name.find("test_file_50") != std::string::npos;
    };
  }
  SECTION("Custom filter (search for test_file_1*)") {
    include = [](std::string_view object_name, uint64_t) {
      return object_name.find("test_file_1") != std::string::npos;
    };
  }
  SECTION("Custom filter (reject files over 50 bytes)") {
    include = []([[maybe_unused]] std::string_view entry, uint64_t size) {
      return size <= 50;
    };
  }

  // Test collecting results with LsInclude predicate.
  auto results = tiledb::VFSExperimental::ls_recursive_filter(
      ctx, vfs, test.temp_dir_.to_string(), include);
  std::erase_if(expected_results, [&include](const auto& object) {
    return !include(object.first, object.second);
  });
  std::sort(results.begin(), results.end());
  CHECK(results.size() == expected_results.size());
  CHECK(expected_results == results);

  // Test collecting results with LsCallback, writing data into ls_objects.
  tiledb::VFSExperimental::ls_recursive(
      ctx, vfs, test.temp_dir_.to_string(), cb);
  std::sort(ls_objects.begin(), ls_objects.end());
  CHECK(ls_objects.size() == expected_results.size());
  CHECK(expected_results == ls_objects);
}

TEST_CASE("CPP API: Callback stops traversal", "[cppapi][vfs][ls-recursive]") {
  using namespace tiledb::test;
  S3Test s3_test({10, 50, 15});
  if (!s3_test.is_supported()) {
    return;
  }
  auto expected_results = s3_test.expected_results();

  vfs_config cfg;
  tiledb::Context ctx(tiledb::Config(&cfg.config));
  tiledb::VFS vfs(ctx);

  tiledb::VFSExperimental::LsObjects ls_objects;
  size_t cb_count = GENERATE(1, 10, 11, 50);
  auto cb = [&](std::string_view path, uint64_t size) {
    // Always emplace to check the callback is not invoked more than `cb_count`.
    ls_objects.emplace_back(path, size);
    // Signal to stop traversal when we have seen `cb_count` objects.
    if (ls_objects.size() == cb_count) {
      return false;
    }
    return true;
  };
  tiledb::VFSExperimental::ls_recursive(
      ctx, vfs, s3_test.temp_dir_.to_string(), cb);
  expected_results.resize(cb_count);
  CHECK(ls_objects.size() == cb_count);
  CHECK(ls_objects == expected_results);
}

TEST_CASE("CPP API: Throwing filter", "[cppapi][vfs][ls-recursive]") {
  using namespace tiledb::test;
  S3Test s3_test({0});
  if (!s3_test.is_supported()) {
    return;
  }

  vfs_config cfg;
  tiledb::Context ctx(tiledb::Config(&cfg.config));
  tiledb::VFS vfs(ctx);

  tiledb::VFSExperimental::LsInclude filter = [](std::string_view,
                                                 uint64_t) -> bool {
    throw std::runtime_error("Throwing filter");
  };
  auto path = s3_test.temp_dir_.to_string();

  // If the test directory is empty the filter should not throw.
  SECTION("Throwing filter with 0 objects should not throw") {
    CHECK_NOTHROW(
        tiledb::VFSExperimental::ls_recursive_filter(ctx, vfs, path, filter));
    CHECK_NOTHROW(
        tiledb::VFSExperimental::ls_recursive(ctx, vfs, path, filter));
  }
  SECTION("Throwing filter with N objects should throw") {
    vfs.touch(s3_test.temp_dir_.join_path("test_file").to_string());
    CHECK_THROWS_AS(
        tiledb::VFSExperimental::ls_recursive_filter(ctx, vfs, path, filter),
        std::runtime_error);
    CHECK_THROWS_WITH(
        tiledb::VFSExperimental::ls_recursive_filter(ctx, vfs, path, filter),
        Catch::Matchers::ContainsSubstring("Throwing filter"));
    CHECK_THROWS_AS(
        tiledb::VFSExperimental::ls_recursive(ctx, vfs, path, filter),
        std::runtime_error);
    CHECK_THROWS_WITH(
        tiledb::VFSExperimental::ls_recursive(ctx, vfs, path, filter),
        Catch::Matchers::ContainsSubstring("Throwing filter"));
  }
}

TEST_CASE(
    "CPP API: CallbackWrapperCPP construction validation",
    "[ls-recursive][callback][wrapper]") {
  tiledb::VFSExperimental::LsObjects data;
  auto cb = [&](std::string_view, uint64_t) -> bool { return true; };
  SECTION("Null callback") {
    CHECK_THROWS(tiledb::VFSExperimental::CallbackWrapperCPP(nullptr));
  }
  SECTION("Valid callback") {
    CHECK_NOTHROW(tiledb::VFSExperimental::CallbackWrapperCPP(cb));
  }
}

TEST_CASE(
    "CPP API: CallbackWrapperCPP operator() validation",
    "[ls-recursive][callback][wrapper]") {
  tiledb::VFSExperimental::LsObjects data;
  auto cb = [&](std::string_view path, uint64_t object_size) -> bool {
    if (object_size > 100) {
      // Throw if object size is greater than 100 bytes.
      throw std::runtime_error("Throwing callback");
    } else if (!path.ends_with(".txt")) {
      // Reject non-txt files.
      return false;
    }
    data.emplace_back(path, object_size);
    return true;
  };
  tiledb::VFSExperimental::CallbackWrapperCPP wrapper(cb);

  SECTION("Callback return true accepts object") {
    CHECK(wrapper("file.txt", 10) == true);
    CHECK(data.size() == 1);
  }
  SECTION("Callback return false rejects object") {
    CHECK(wrapper("some/dir/", 0) == false);
    CHECK(data.empty());
  }
  SECTION("Callback exception is propagated") {
    CHECK_THROWS_WITH(wrapper("path", 101) == 0, "Throwing callback");
  }
}
