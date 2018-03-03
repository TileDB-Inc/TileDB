/**
 * @file   tiledb_vfs_read.cc
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
 * Read from a file with VFS.
 *
 * You need to run the following to make it work:
 *
 * $ ./tiledb_vfs_write_cpp
 * $ ./tiledb_vfs_read_cpp
 */

#include <fstream>
#include <tiledb/tiledb>

int main() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB VFS
  tiledb::VFS vfs(ctx);

  // Read binary data
  {
    tiledb::VFS::filebuf sbuf(vfs);
    sbuf.open("tiledb_vfs.bin", std::ios::in);
    std::istream is(&sbuf);
    if (!is.good()) {
      std::cerr << "Error opening file.\n";
      return 1;
    }

    float f1;
    std::string s1;
    s1.resize(12);

    is.read((char*)&f1, sizeof(f1));
    is.read((char*)s1.data(), 12);
    std::cout << "Binary read:\n" << f1 << '\n' << s1 << '\n';
  }

  // Read string data
  {
    tiledb::VFS::filebuf sbuf(vfs);
    sbuf.open("tiledb_vfs.txt", std::ios::in);
    std::istream is(&sbuf);
    if (!is.good()) {
      std::cerr << "Error opening file.\n";
      return 1;
    }

    std::cout << "\nString read:\n";
    std::string line;
    while (std::getline(is, line)) {
      std::cout << line << '\n';
    }
  }

  // Nothing to clean up - all C++ objects are deleted when exiting scope

  return 0;
}