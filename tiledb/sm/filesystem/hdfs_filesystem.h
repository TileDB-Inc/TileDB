/**
 * @file   hdfs_filesystem.h
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
 * This file includes declarations of HDFS filesystem functions.
 */

#ifndef TILEDB_HDFS_FILESYSTEM_H
#define TILEDB_HDFS_FILESYSTEM_H

#ifdef HAVE_HDFS

#include <sys/types.h>
#include <string>
#include <vector>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/config.h"

#include "hadoop/hdfs.h"

namespace tiledb {
namespace sm {

namespace hdfs {

class LibHDFS;

class HDFS {
 public:
  HDFS();
  ~HDFS() = default;

  /**
   * Connects to an HDFS filesystem
   *
   * @param fs Reference to a hdfsFS filesystem handle.
   * @return Status
   */
  Status connect(const Config::HDFSParams& config);

  /**
   * Disconnects an HDFS filesystem
   *
   * @return Status
   */
  Status disconnect();

  /**
   * Creates a new directory.
   *
   * @param uri The URI of the directory resource to be created.
   * @return Status
   */
  Status create_dir(const URI& uri);

  /**
   * Checks if the given URI is an existing HDFS directory
   * @param uri The URI of the directory to be checked
   * @param *True* if the *file* is an existing file, and *false* otherwise.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* is_dir);

  /**
   * Checks if the given URI is an existing HDFS file
   *
   * @param fs Reference to a connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be checked.
   * @return *True* if the *file* is an existing file, and *false* otherwise.
   */
  Status is_file(const URI& uri, bool* is_file);

  /**
   * Move a given filesystem path.
   *
   * @param old_uri The URI of the old directory.
   * @param new_uri The URI of the new directory.
   * @return Status
   */
  Status move_path(const URI& old_uri, const URI& new_uri);

  /**
   * Creates an empty file.
   *
   * @param fs Reference to a connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be created.
   * @return Status
   */
  Status touch(const URI& uri);

  /**
   * Delete a file with a given URI.
   *
   * @param fs Connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be deleted.
   * @return Status
   */
  Status remove_file(const URI& uri);

  /**
   * Remove a directory with a given URI (recursively)
   *
   * @param fs Connected hdfsFS filesystem handle.
   * @param uri The URI of the directory to be removed.
   * @return Status
   */
  Status remove_dir(const URI& uri);

  /**
   *  Reads data from a file into a buffer.
   *
   * @param uri The URI of the file to be read.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param buffer_size The size of the data to be read from the file.
   * @return Status
   */
  Status read(const URI& uri, off_t offset, void* buffer, uint64_t length);

  /**
   * Writes the input buffer to a file.
   *
   * If the file exists than it is created.
   * If the file does not exist than it is appended to.
   *
   * @param fs Connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be written to.
   * @param buffer The input buffer.
   * @param buffer_size The size of the input buffer.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, const uint64_t length);

  /**
   * Commits all changes to the persistent storage.
   *
   * @param fs The HDFS filesystem object.
   * @param uri The file to be synced.
   * @return Status
   */
  Status sync(const URI& uri);

  /**
   * Lists the files one level deep under a given path.
   *
   * @param uri The URI of the parent directory path.
   * @param paths Pointer ot a vector of URIs to store the retrieved paths.
   * @return Status
   */
  Status ls(const URI& uri, std::vector<std::string>* paths);

  /**
   * Returns the size of the input file with a given URI in bytes.
   *
   * @param uri The URI of the file.
   * @param nbytes Pointer to unint64_t bytes to return.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* nbytes);

 private:
  hdfsFS hdfs_;
  LibHDFS* libhdfs_;

  /** Connect to hdfsFS and return handle, stub for future cached dynamic
   * connections **/
  Status connect(hdfsFS* fs);

  HDFS(HDFS const& l);             // disable copy ctor
  HDFS& operator=(HDFS const& l);  // disable assignment
};

}  // namespace hdfs

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_HDFS
#endif  // TILEDB_HDFS_FILESYSTEM_H