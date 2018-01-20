/**
 * @file   tiledb_vfs.cc
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * VFS tools.
 */

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  if (!vfs.is_dir("dirA")) {
    vfs.create_dir("dirA");
    std::cout << "Made dirA.\n";
  } else {
    std::cout << "dirA already exists.\n";
  }

  if (!vfs.is_file("dirA/fileA")) {
    vfs.touch("dirA/fileA");
    std::cout << "Made file dirA/fileA.\n";
  } else {
    std::cout << "dirA/fileA already exists.\n";
  }

  std::cout << "Appending data to dirA/fileA.\n";
  const char *data = "abcdef";
  vfs.write("dirA/fileA", data, 6);
  std::cout << "File size: " << vfs.file_size("dirA/fileA") << "\n";

  std::cout << "Moving file dirA/fileA to dirA/fileB.\n";
  vfs.move("dirA/fileA", "dirA/fileB");

  std::cout << "deleting fileB and dirA.\n";
  vfs.remove_file("dirA/fileB");
  vfs.remove_dir("dirA");

  return 0;
}