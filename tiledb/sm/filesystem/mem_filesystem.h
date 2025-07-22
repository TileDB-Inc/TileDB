/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2025 TileDB, Inc.
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
 * This file defines an in-memory filesystem
 */

#ifndef TILEDB_MEMORY_FILESYSTEM_H
#define TILEDB_MEMORY_FILESYSTEM_H

#include <string>
#include <unordered_map>
#include <vector>

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/filesystem_base.h"

using namespace tiledb::common;

namespace tiledb {

namespace common::filesystem {
class directory_entry;
}

namespace sm {

class URI;

/** Class for MemFS status exceptions. */
class MemFSException : public StatusException {
 public:
  explicit MemFSException(const std::string& msg)
      : StatusException("MemFS", msg) {
  }
};

/**
 * The in-memory filesystem.
 *
 * @invariant The MemFilesystem is associated with a single VFS instance.
 * @invariant The MemFilesystem exists on a single, global Context.
 */
class MemFilesystem : public FilesystemBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  MemFilesystem();

  /** Copy constructor. */
  DISABLE_COPY(MemFilesystem);

  /** Move constructor. */
  DISABLE_MOVE(MemFilesystem);

  /** Destructor. */
  ~MemFilesystem();

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  /** Copy-assignment. */
  DISABLE_COPY_ASSIGN(MemFilesystem);

  /** Move-assignment. */
  DISABLE_MOVE_ASSIGN(MemFilesystem);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Checks if this filesystem supports the given URI.
   *
   * @param uri The URI to check.
   * @return `true` if `uri` is supported on this filesystem, `false` otherwise.
   */
  bool supports_uri(const URI& uri) const override;

  /**
   * Creates a new directory.
   *
   * @param uri The URI of the directory to be created.
   */
  void create_dir(const URI& uri) const override;

  /**
   * Returns the size of an existing file.
   *
   * @param uri The URI of the file to retrieve the size of.
   * @return The size of the file.
   */
  uint64_t file_size(const URI& uri) const override;

  /**
   * Checks if a URI corresponds to an existing directory.
   *
   * @param uri The URI to the directory to be checked
   * @return *True* if *uri* is an existing directory,  *False* otherwise
   */
  bool is_dir(const URI& uri) const override;

  /**
   * Checks if a URI corresponds to an existing file.
   *
   * @param uri The URI to the file to be checked
   * @return *True* if *uri* is an existing file, *false* otherwise
   */
  bool is_file(const URI& uri) const override;

  /**
   *
   * Lists directory contents in alphabetical order
   *
   * @param path  The parent path to list sub-paths
   * @param paths A vector where all the full paths to the contents are stored
   *     If the input path is a file then the vector only contains this file
   * @return Status
   */
  Status ls(const std::string& path, std::vector<std::string>* paths) const;

  /**
   *
   * Lists files and files information under path
   *
   * @param path  The parent path to list sub-paths
   * @return A list of directory_entry objects
   */
  std::vector<tiledb::common::filesystem::directory_entry> ls_with_sizes(
      const URI& path) const override;

  /**
   * Move a given filesystem path.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   */
  void move(const std::string& old_path, const std::string& new_path) const;

  /**
   * Move a given file.
   *
   * @param old_uri The old uri.
   * @param new_uri The new uri.
   */
  void move_dir(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Move a given directory.
   *
   * @param old_uri The old uri.
   * @param new_uri The new uri.
   */
  void move_file(const URI& old_uri, const URI& new_uri) const override;

  /** Whether or not to use the read-ahead cache. */
  bool use_read_ahead_cache() const override {
    return false;
  }

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param read_ahead_nbytes The number of bytes to read ahead. Unused.
   */
  uint64_t read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      uint64_t read_ahead_nbytes = 0) override;

  /**
   * Removes a given path and its contents.
   *
   * @param path The full path of the directory/file to be deleted.
   * @param is_dir *True* if *path* is a directory, *false* if it's a file
   */
  void remove(const std::string& path, bool is_dir) const;

  /**
   * Removes a directory and its contents.
   *
   * @param uri The URI of the directory to be deleted.
   */
  void remove_dir(const URI& uri) const override;

  /**
   * Removes a file.
   *
   * @param uri The URI of the file to be deleted.
   */
  void remove_file(const URI& uri) const override;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file to be created.
   */
  void touch(const URI& path) const override;

  /**
   * Writes the input buffer to a file.
   *
   * If the file does not exist than it is created with data as content.
   * If the file exits than the data is appended to the end of the file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The size of the input buffer in bytes.
   * @param remote_global_order_write Unused flag. Reserved for S3 objects only.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write) override;

  /**
   * Copies a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_dir(const URI&, const URI&) const override {
    // No-op for MemFS; stub function for other filesystems.
  }

  /**
   * Copies a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_file(const URI&, const URI&) const override {
    // No-op for MemFS; stub function for other filesystems.
  }

  /**
   * Flushes an object store file.
   *
   * @invariant Performs a sync for local filesystems.
   *
   * @param uri The URI of the file.
   * @param finalize Unused flag. Reserved for finalizing S3 object upload only.
   */
  void flush(const URI&, bool) override {
    // No-op for MemFS; stub function for local filesystems.
  }

  /**
   * Syncs a local file.
   *
   * @invariant Valid only for local filesystems.
   *
   * @param uri The URI of the file.
   */
  void sync(const URI&) const override {
    // No-op for MemFS; stub function for local filesystems.
  }

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

  /* Class that models an entity in the in-memory filesystem tree */
  class FSNode;

  /* Subclass of FSNode that represents a file in the filesystem tree */
  class File;

  /* Subclass of FSNode that represents a directory in the filesystem tree */
  class Directory;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /* The node that represents the root of the directory tree. */
  tdb_unique_ptr<FSNode> root_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Finds the node in the filesystem tree that corresponds to a path
   *
   * @param path The full name of the file/directory to be looked up
   * @param node The output node, nullptr if not found.
   * @param node_lock Mutates to a lock on `node->mutex_`, if found.
   * @return Status
   */
  Status lookup_node(
      const std::string& path,
      FSNode** node,
      std::unique_lock<std::mutex>* node_lock) const;

  /**
   * Finds the node in the filesystem tree that corresponds to a vector
   * of tokens.
   *
   * @param tokens The path tokens.
   * @param node The output node, nullptr if not found.
   * @param node_lock Mutates to a lock on `node->mutex_`, if found.
   * @return Status
   */
  Status lookup_node(
      const std::vector<std::string>& tokens,
      FSNode** node,
      std::unique_lock<std::mutex>* node_lock) const;

  /**
   * Splits a path into file/directory names
   *
   * @param path The full name of the file/directory
   * @param delimiter The delimiter to use split the path in names
   * @return The vector of node names in the path
   */
  static std::vector<std::string> tokenize(
      const std::string& path, const char delim = '/');

  /**
   * Creates a new directory without acquiring the lock.
   *
   * @param path The full name of the directory to be created.
   * @return The node of the created directory.
   */
  FSNode* create_dir_internal(const std::string& path) const;

  /**
   * Creates an empty file without acquiring the lock.
   *
   * @param path The full name of the file to be created.
   * @return The node of the created file.
   */
  FSNode* touch_internal(const std::string& path) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MEMORY_FILESYSTEM_H
