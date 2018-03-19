/**
 * @file   tiledb_vfs.cc
 *
 * @author Ravi Gaddipati
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
 * Exploring VFS tools. Simply run:
 *
 * $ ./tiledb_vfs_cpp
 *
 */

#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB VFS
  tiledb::VFS vfs(ctx);

  // Create directory
  if (!vfs.is_dir("dir_A")) {
    vfs.create_dir("dir_A");
    std::cout << "Created dir_A\n";
  } else {
    std::cout << "dir_A already exists\n";
  }

  // Creating an (empty) file
  if (!vfs.is_file("dir_A/file_A")) {
    vfs.touch("dir_A/file_A");
    std::cout << "Created empty file dir_A/file_A\n";
  } else {
    std::cout << "dir_A/file_A already exists\n";
  }

  // Getting the file size
  std::cout << "File size: " << vfs.file_size("dir_A/file_A") << "\n";

  // Moving files (moving directories is similar)
  std::cout << "Moving file dir_A/file_A to dir_A/file_B\n";
  vfs.move_file("dir_A/file_A", "dir_A/file_B");

  // Deleting files and directories
  std::cout << "Deleting dir_A/file_B and dir_A\n";
  vfs.remove_file("dir_A/file_B");
  vfs.remove_dir("dir_A");

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}