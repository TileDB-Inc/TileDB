/**
 * @file   unit-cppapi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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

TEST_CASE(
    "C++ API: Test VFS copy file",
    "[cppapi], [cppapi-vfs], [cppapi-vfs-copy-file]") {
  using namespace tiledb;
  Context ctx;
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
    "[cppapi], [cppapi-vfs], [cppapi-vfs-copy-dir]") {
  using namespace tiledb;
  Context ctx;
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