/**
 * @file   unit-cppapi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

TEST_CASE("C++ API: Test VFS ls", "[cppapi], [cppapi-vfs], [cppapi-vfs-ls]") {
  using namespace tiledb;
  Context ctx;
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
  std::string subdir_file2 = subdir2 + "/file2";

  // Create directories and files
  vfs.create_dir(path);
  vfs.create_dir(dir);
  vfs.create_dir(subdir);
  vfs.create_dir(subdir2);
  vfs.touch(file);
  vfs.touch(file2);
  vfs.touch(subdir_file);
  vfs.touch(subdir_file2);

  // List
  std::vector<std::string> children = vfs.ls(dir);

#ifdef _WIN32
  // Normalization only for Windows
  file = tiledb::sm::Win::uri_from_path(file);
  file2 = tiledb::sm::Win::uri_from_path(file2);
  subdir = tiledb::sm::Win::uri_from_path(subdir);
  subdir2 = tiledb::sm::Win::uri_from_path(subdir2);
#endif

  // Check results
  std::sort(children.begin(), children.end());
  REQUIRE(children.size() == 4);
  CHECK(children[0] == file);
  CHECK(children[1] == file2);
  CHECK(children[2] == subdir);
  CHECK(children[3] == subdir2);

  // Clean up
  vfs.remove_dir(path);
}

TEST_CASE(
    "C++ API: Test VFS dir size",
    "[cppapi], [cppapi-vfs], [cppapi-vfs-dir-size]") {
  using namespace tiledb;
  Context ctx;
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
