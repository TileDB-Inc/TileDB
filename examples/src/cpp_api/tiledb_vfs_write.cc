/**
 * @file   tiledb_vfs_write.cc
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
 * Write a file with the VFS.
 */

#include <fstream>
#include <tiledb>

int main() {
  tdb::Context ctx;
  tdb::VFS vfs(ctx);

  tdb::VFSFilebuf sbuf(vfs);
  sbuf.open("tiledb_vfs.txt", std::ios::out);
  std::ostream os(&sbuf);

  // Write formatted output
  os << "tiledb " << 543 << " " << 123.4 << ' ';

  // Write binary data
  sbuf.open("tiledb_vfs.bin", std::ios::out);
  float f1 = 153.234;
  std::string s1 = "abcdefghijkl";
  os.write((char*)&f1, sizeof(f1));
  os.write(s1.data(), s1.size());

  return 0;
}