/**
 * @file   tiledb_vfs_write.cc
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
 * Write to a file with VFS. Simply run:
 *
 * $ ./tiledb_vfs_write_cpp
 *
 */

#include <fstream>
#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB VFS
  tiledb::VFS vfs(ctx);

  // Create VFS file buffer
  tiledb::VFS::filebuf fbuf(vfs);

  // Write binary data
  fbuf.open("tiledb_vfs.bin", std::ios::out);
  std::ostream os(&fbuf);
  if (!os.good()) {
    std::cerr << "Error opening file.\n";
    return 1;
  }
  float f1 = 153.0;
  std::string s1 = "abcd";
  os.write((char*)&f1, sizeof(f1));
  os.write(s1.data(), s1.size());

  // Write binary data again - this will overwrite the previous file
  fbuf.open("tiledb_vfs.bin", std::ios::out);
  if (!os.good()) {
    std::cerr << "Error opening file tiledb_vfs.bin for write.\n";
    return 1;
  }
  f1 = 153.1;
  s1 = "abcdef";
  os.write((char*)&f1, sizeof(f1));
  os.write(s1.data(), s1.size());

  // Append binary data to existing file
  fbuf.open("tiledb_vfs.bin", std::ios::app);
  if (!os.good()) {
    std::cerr << "Error opening file tiledb_vfs.bin for append.\n";
    return 1;
  }
  s1 = "ghijkl";
  os.write(s1.data(), s1.size());

  // Write formatted output
  fbuf.open("tiledb_vfs.txt", std::ios::out);
  if (!os.good()) {
    std::cerr << "Error opening file tiledb_vfs.txt for write.\n";
    return 1;
  }
  os << "tiledb " << 543 << '\n' << 123.4 << '\n';

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}