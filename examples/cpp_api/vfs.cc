/**
 * @file   vfs.cc
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
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/vfs.html
 *
 * This program explores the various TileDB VFS tools.
 */

#include <iostream>
#include <tiledb/tiledb>

void dirs_files() {
  // Create TileDB context
  tiledb::Context ctx;

  // Create TileDB VFS
  tiledb::VFS vfs(ctx);

  // Create directory
  if (!vfs.is_dir("dir_A")) {
    vfs.create_dir("dir_A");
    std::cout << "Created 'dir_A'\n";
  } else {
    std::cout << "'dir_A' already exists\n";
  }

  // Creating an (empty) file
  if (!vfs.is_file("dir_A/file_A")) {
    vfs.touch("dir_A/file_A");
    std::cout << "Created empty file 'dir_A/file_A'\n";
  } else {
    std::cout << "'dir_A/file_A' already exists\n";
  }

  // Getting the file size
  std::cout << "Size of file 'dir_A/file_A': " << vfs.file_size("dir_A/file_A")
            << "\n";

  // Moving files (moving directories is similar)
  std::cout << "Moving file 'dir_A/file_A' to 'dir_A/file_B'\n";
  vfs.move_file("dir_A/file_A", "dir_A/file_B");

  // Deleting files and directories
  std::cout << "Deleting 'dir_A/file_B' and 'dir_A'\n";
  vfs.remove_file("dir_A/file_B");
  vfs.remove_dir("dir_A");
}

void write() {
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
    std::cerr << "Error opening file  'tiledb_vfs_bin'.\n";
    return;
  }
  float f1 = 153.0;
  std::string s1 = "abcd";
  os.write((char*)&f1, sizeof(f1));
  os.write(s1.data(), s1.size());

  // Write binary data again - this will overwrite the previous file
  fbuf.open("tiledb_vfs.bin", std::ios::out);
  if (!os.good()) {
    std::cerr << "Error opening file 'tiledb_vfs.bin' for write.\n";
    return;
  }
  f1 = 153.1;
  s1 = "abcdef";
  os.write((char*)&f1, sizeof(f1));
  os.write(s1.data(), s1.size());

  // Append binary data to existing file (this will NOT work on S3)
  fbuf.open("tiledb_vfs.bin", std::ios::app);
  if (!os.good()) {
    std::cerr << "Error opening file 'tiledb_vfs.bin' for append.\n";
    return;
  }
  s1 = "ghijkl";
  os.write(s1.data(), s1.size());
}

void read() {
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
      std::cerr << "Error opening file 'tiledb_vfs.bin'.\n";
      return;
    }

    float f1;
    std::string s1;
    auto s1_size = vfs.file_size("tiledb_vfs.bin") - sizeof(float);
    s1.resize(s1_size);

    is.read((char*)&f1, sizeof(f1));
    is.read((char*)s1.data(), 12);
    std::cout << "Binary read:\n" << f1 << '\n' << s1 << '\n';
  }
}

int main() {
  dirs_files();
  write();
  read();
}