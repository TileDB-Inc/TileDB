/**
 * @file   hdfs_filesystem.h
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
 * This file includes declarations of HDFS filesystem functions.
 */
#ifdef HAVE_HDFS
#ifndef TILEDB_FILESYSTEM_HDFS_H
#define TILEDB_FILESYSTEM_HDFS_H

#include <sys/types.h>
#include <string>
#include <vector>

#include "status.h"

#include "hdfs.h"

namespace tiledb {

namespace vfs {

namespace hdfs {

// create a connection to hdfs
Status connect(hdfsFS& fs);

// disconnect hdfs connection
Status disconnect(hdfsFS& fs);

// create a directory with the given path
Status create_dir(const std::string& path, hdfsFS fs);

// delete the directory with the given path
Status delete_dir(const std::string& path, hdfsFS fs);

// Is the given path a valid directory
bool is_dir(const std::string& path, hdfsFS fs);

// Is the given path a valid file
bool is_file(const std::string& path, hdfsFS fs);

// create a file with the given path
Status create_file(const std::string& path, hdfsFS fs);

// delete a file with the given path
Status delete_file(const std::string& path, hdfsFS fs);

// Read length bytes from file give by path from byte offset offset into pre
// allocated buffer buffer.
Status read_from_file(
    const std::string& path,
    off_t offset,
    void* buffer,
    size_t length,
    hdfsFS fs);

// Write length bytes of buffer to a given path
Status write_to_file(
    const std::string& path,
    const void* buffer,
    const size_t length,
    hdfsFS fs);

// List all subdirectories and files for a given path, appending them to paths.
// Ordering does not matter.
Status ls(const std::string& path, std::vector<std::string>& paths, hdfsFS fs);

// List all subfiles (1 level deep) for a given path, appending them to fpaths.
// Ordering does not matter.
Status ls_files(
    const std::string& path, std::vector<std::string>& fpaths, hdfsFS fs);

// List all subdirectories (1 level deep) for a given path, appending them to
// dpaths.  Ordering does not matter.
Status ls_dirs(
    const std::string& path, std::vector<std::string>& fpaths, hdfsFS fs);

// File size in bytes for a given path
Status filesize(const std::string& path, size_t* nbytes, hdfsFS fs);

}  // namespace hdfs

}  // namespace vfs

}  // namespace tiledb

#endif
#endif  // TILEDB_FILESYSTEM_HDFS_H
