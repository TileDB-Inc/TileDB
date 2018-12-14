/**
 * @file   unit-win-filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Tests the Windows filesystem functionality.
 */

#ifdef _WIN32

#include "catch.hpp"

#include <cassert>
#include "tiledb/sm/filesystem/win.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/storage_manager/config.h"

using namespace tiledb::sm;

static bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

static bool ends_with(const std::string& value, const std::string& suffix) {
  if (suffix.size() > value.size())
    return false;
  return std::equal(suffix.rbegin(), suffix.rend(), value.rbegin());
}

struct WinFx {
  const std::string TEMP_DIR = Win::current_dir() + "/";
  Win win_;
  ThreadPool thread_pool_;
  Config::VFSParams vfs_params_;

  WinFx() {
    // Make sure parallel reads/writes are tested.
    vfs_params_.min_parallel_size_ = 100;
    REQUIRE(thread_pool_.init(4).ok());
    REQUIRE(win_.init(vfs_params_, &thread_pool_).ok());

    if (path_exists(TEMP_DIR + "tiledb_test_dir"))
      REQUIRE(win_.remove_dir(TEMP_DIR + "tiledb_test_dir").ok());
    if (path_exists(TEMP_DIR + "tiledb_test_file"))
      REQUIRE(win_.remove_file(TEMP_DIR + "tiledb_test_file").ok());
  }

  ~WinFx() {
    REQUIRE(win_.remove_dir(TEMP_DIR + "tiledb_test_dir").ok());
  }

  bool path_exists(std::string path) {
    return win_.is_file(path) || win_.is_dir(path);
  }
};

TEST_CASE_METHOD(WinFx, "Test Windows filesystem", "[windows]") {
  const std::string test_dir_path = win_.current_dir() + "/tiledb_test_dir";
  const std::string test_file_path =
      win_.current_dir() + "/tiledb_test_dir/tiledb_test_file";
  URI test_dir(test_dir_path);
  URI test_file(test_file_path);
  Status st;

  CHECK(Win::is_win_path("C:\\path"));
  CHECK(Win::is_win_path("C:path"));
  CHECK(Win::is_win_path("..\\path"));
  CHECK(Win::is_win_path("\\path"));
  CHECK(Win::is_win_path("path\\"));
  CHECK(Win::is_win_path("\\\\path1\\path2"));
  CHECK(Win::is_win_path("path1\\path2"));
  CHECK(Win::is_win_path("path"));
  CHECK(!Win::is_win_path("path1/path2"));
  CHECK(!Win::is_win_path("file:///path1/path2"));
  CHECK(!Win::is_win_path("hdfs:///path1/path2"));

  CHECK(Win::abs_path(test_dir_path) == test_dir_path);
  CHECK(Win::abs_path(test_file_path) == test_file_path);
  CHECK(Win::abs_path("") == Win::current_dir());
  CHECK(Win::abs_path("C:\\") == "C:\\");
  CHECK(Win::abs_path("C:\\path1\\path2\\") == "C:\\path1\\path2\\");
  CHECK(Win::abs_path("C:\\..") == "C:\\");
  CHECK(Win::abs_path("C:\\..\\path1") == "C:\\path1");
  CHECK(Win::abs_path("C:\\path1\\.\\..\\path2\\") == "C:\\path2\\");
  CHECK(Win::abs_path("C:\\path1\\.\\path2\\..\\path3") == "C:\\path1\\path3");
  CHECK(
      Win::abs_path("path1\\path2\\..\\path3") ==
      Win::current_dir() + "\\path1\\path3");
  CHECK(Win::abs_path("path1") == Win::current_dir() + "\\path1");
  CHECK(Win::abs_path("path1\\path2") == Win::current_dir() + "\\path1\\path2");
  CHECK(
      Win::abs_path("path1\\path2\\..\\path3") ==
      Win::current_dir() + "\\path1\\path3");

  CHECK(!win_.is_dir(test_dir.to_path()));
  st = win_.create_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_dir.to_path()));
  CHECK(win_.is_dir(test_dir.to_path()));

  CHECK(!win_.is_file(test_file.to_path()));
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  CHECK(win_.is_file(test_file.to_path()));
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  CHECK(win_.is_file(test_file.to_path()));

  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  st = win_.remove_file(test_file.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_file.to_path()));

  st = win_.remove_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_dir(test_dir.to_path()));

  st = win_.create_dir(test_dir.to_path());
  CHECK(st.ok());
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  st = win_.remove_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_dir(test_dir.to_path()));

  st = win_.create_dir(test_dir.to_path());
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());

  const unsigned buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = win_.write(test_file.to_path(), write_buffer, buffer_size);
  CHECK(st.ok());
  st = win_.sync(test_file.to_path());
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = win_.read(test_file.to_path(), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = win_.read(test_file.to_path(), 11, read_buffer, 26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; ++i) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  CHECK(
      tiledb::sm::stats::all_stats.counter_vfs_win32_write_num_parallelized >
      0);

  std::vector<std::string> paths;
  st = win_.ls(test_dir.to_path(), &paths);
  CHECK(st.ok());
  CHECK(paths.size() == 1);
  CHECK(!starts_with(paths[0], "file:///"));
  CHECK(ends_with(paths[0], "tiledb_test_dir\\tiledb_test_file"));
  CHECK(win_.is_file(paths[0]));

  uint64_t nbytes = 0;
  st = win_.file_size(test_file.to_path(), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st =
      win_.remove_file(URI("file:///tiledb_test_dir/i_dont_exist").to_string());
  CHECK(!st.ok());

  st = win_.move_path(test_file.to_path(), URI(test_file_path + "2").to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_file.to_path()));
  CHECK(win_.is_file(URI(test_file_path + "2").to_path()));
}

#endif  // _WIN32
