/**
 * @file   hdfs_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file defines the HDFS class.
 */

#ifndef TILEDB_HDFS_FILESYSTEM_H
#define TILEDB_HDFS_FILESYSTEM_H

#ifdef HAVE_HDFS

#include <sys/types.h>
#include <string>
#include <vector>

#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/config/config.h"

using namespace tiledb::common;

// Declarations copied from hadoop/hdfs.h
// We do not include it here to avoid leaking it to consuming code.
struct hdfs_internal;
typedef struct hdfs_internal* hdfsFS;

namespace tiledb::common::filesystem {
class directory_entry;
}  // namespace tiledb::common::filesystem

namespace tiledb::sm {

class LibHDFS;
class URI;

/** Class for HDFS StatusExceptions. */
class HDFSException : public StatusException {
 public:
  explicit HDFSException(const std::string& msg)
      : StatusException("HDFS", msg) {
  }
};

/**
 * The HDFS-specific configuration parameters.
 *
 * @note The member variables' default declarations have not yet been moved
 * from the Config declaration into this struct.
 */
struct HDFSParameters {
  HDFSParameters() = delete;

  HDFSParameters(const Config& config)
      : name_node_uri_(config.get<std::string>(
            "vfs.hdfs.name_node_uri", Config::must_find))
      , username_(
            config.get<std::string>("vfs.hdfs.username", Config::must_find))
      , kerb_ticket_cache_path_(config.get<std::string>(
            "vfs.hdfs.kerb_ticket_cache_path", Config::must_find)){};

  ~HDFSParameters() = default;

  /** Name node for HDFS.  */
  std::string name_node_uri_;

  /** HDFS username.  */
  std::string username_;

  /** HDFS kerb ticket cache path.  */
  std::string kerb_ticket_cache_path_;
};

class HDFS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * If libhdfs is not found, returns immediately.
   * If libhdfs is found, attempts to connect to the
   * default name_node_uri_ defined in the HDFSParameters struct.
   *
   * @param config Configuration parameters.
   */
  HDFS(const Config& config);

  /** Destructor. */
  ~HDFS() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

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
  Status create_dir(const URI& uri) const;

  /**
   * Checks if the given URI is an existing HDFS directory
   * @param uri The URI of the directory to be checked
   * @param *True* if the *file* is an existing file, and *false* otherwise.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* is_dir) const;

  /**
   * Checks if the given URI is an existing HDFS file
   *
   * @param fs Reference to a connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be checked.
   * @return *True* if the *file* is an existing file, and *false* otherwise.
   */
  Status is_file(const URI& uri, bool* is_file) const;

  /**
   * Move a given filesystem path.
   *
   * @param old_uri The URI of the old directory.
   * @param new_uri The URI of the new directory.
   * @return Status
   */
  Status move_path(const URI& old_uri, const URI& new_uri) const;

  /**
   * Creates an empty file.
   *
   * @param fs Reference to a connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Delete a file with a given URI.
   *
   * @param fs Connected hdfsFS filesystem handle.
   * @param uri The URI of the file to be deleted.
   * @return Status
   */
  Status remove_file(const URI& uri) const;

  /**
   * Remove a directory with a given URI (recursively)
   *
   * @param fs Connected hdfsFS filesystem handle.
   * @param uri The URI of the directory to be removed.
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   *  Reads data from a file into a buffer.
   *
   * @param uri The URI of the file to be read.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param buffer_size The size of the data to be read from the file.
   * @return Status
   */
  Status read(
      const URI& uri, off_t offset, void* buffer, uint64_t length) const;

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
  Status write(const URI& uri, const void* buffer, const uint64_t length) const;

  /**
   * Commits all changes to the persistent storage.
   *
   * @param fs The HDFS filesystem object.
   * @param uri The file to be synced.
   * @return Status
   */
  Status sync(const URI& uri) const;

  /**
   * Lists the files one level deep under a given path.
   *
   * @param uri The URI of the parent directory path.
   * @param paths Pointer ot a vector of URIs to store the retrieved paths.
   * @return Status
   */
  Status ls(const URI& uri, std::vector<std::string>* paths) const;

  /**
   * Lists objects and object information that start with `uri`.
   *
   * @param uri The parent path to list sub-paths.
   * @return A list of directory_entry objects
   */
  std::vector<filesystem::directory_entry> ls_with_sizes(const URI& uri) const;

  /**
   * Returns the size of the input file with a given URI in bytes.
   *
   * @param uri The URI of the file.
   * @param nbytes Pointer to unint64_t bytes to return.
   * @return Status
   */
  Status file_size(const URI& uri, uint64_t* nbytes) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  hdfsFS hdfs_;
  LibHDFS* libhdfs_;

  /** The HDFS configuration parameters. */
  HDFSParameters hdfs_params_;

  /** Connect to hdfsFS and return handle, stub for future cached dynamic
   * connections **/
  Status connect(hdfsFS* fs) const;

  HDFS(HDFS const& l);             // disable copy ctor
  HDFS& operator=(HDFS const& l);  // disable assignment
};

}  // namespace tiledb::sm

#endif  // HAVE_HDFS
#endif  // TILEDB_HDFS_FILESYSTEM_H
