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

#include <tiledb>

/** Any structs must be POD **/
struct Data {
  int a;
  double b;
  const char* c;
};

int main() {
  tdb::Context ctx;
  tdb::VFS vfs(ctx);
  tdb::VFSostream os(vfs, "tiledb_vfs.txt", std::ios::app);

  // Data to write
  std::vector<int> ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  Data d;
  d.a = 1;
  d.b = 2.0;
  d.c = "abcd";
  std::vector<Data> dvec(5, d);  // 5 copies of d.

  os << "tiledb " << 543 << " " << 123.4 << ' ' << ints;

  try {
    os << d << dvec;
  } catch (std::runtime_error& e) {
    std::cout << "Cannot write POD data in ASCII mode.\n";
  }

  // close file and open a new one in binary format.
  os.open("tiledb_vfs.bin", std::ios::app | std::ios::binary);

  os << "tiledb" << 543 << 123.4 << ints << d << dvec;

  // Syncs on stream destruction.
  return 0;
}