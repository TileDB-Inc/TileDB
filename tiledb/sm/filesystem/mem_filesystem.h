/**
 * @file   mem_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB, Inc.
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
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

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
class MemFilesystem {
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
   * Creates a new directory.
   *
   * @param path The full name of the directory to be created
   * @return Status
   */
  Status create_dir(const std::string& path) const;

  /**
   * Returns the size of an existing file.
   *
   * @param path The full name of the file to retrieve the size of
   * @param nbytes A pointer to retrun the size of the file
   * @return Status
   */
  Status file_size(const std::string& path, uint64_t* size) const;

  /**
   * Checks if a path corresponds to an existing directory.
   *
   * @param path The path to the directory to be checked
   * @return *True* if *path* is an existing directory,  *False* otherwise
   */
  bool is_dir(const std::string& path) const;

  /**
   * if a path corresponds to an existing file.
   *
   * @param path The path to the file to be checked
   * @return *True* if *path* is an existing file, *false* otherwise
   */
  bool is_file(const std::string& path) const;

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
  std::vector<filesystem::directory_entry> ls_with_sizes(const URI& path) const;

  /**
   * Move a given filesystem path.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   * @return Status
   */
  Status move(const std::string& old_path, const std::string& new_path) const;

  /**
   * Reads data from a file into a buffer.
   *
   * @param path The full name of the file
   * @param offset The offset in the file from which the read will start
   * @param buffer The buffer into which the data will be written
   * @param nbytes The number of bytes to be read from the file
   * @return Status.
   */
  Status read(
      const std::string& path,
      const uint64_t offset,
      void* buffer,
      const uint64_t nbytes) const;

  /**
   * Removes a given path and its contents.
   *
   * @param path The full name of the directory/file to be deleted
   * @param is_dir *True* if *path* is a directory, *false* if it's a file
   * @return Status
   */
  Status remove(const std::string& path, bool is_dir) const;

  /**
   * Creates an empty file.
   *
   * @param path The full name of the file to be created
   * @return Status
   */
  Status touch(const std::string& path) const;

  /**
   * Writes the input buffer to a file.
   *
   * If the file does not exist than it is created with data as content.
   * If the file exits than the data is appended to the end of the file.
   *
   * @param path The full name of the file
   * @param data The input data
   * @param nbytes The size of the input buffer in bytes
   * @return Status
   */
  Status write(
      const std::string& path, const void* data, const uint64_t nbytes);

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
   * Creates a new directory without acquiring the lock
   *
   * @param path The full name of the directory to be created.
   * @param node Optional output argument to the node of the
   *    created directory.
   * @return Status
   */
  Status create_dir_internal(
      const std::string& path, FSNode** node = nullptr) const;

  /**
   * Creates an empty file without acquiring the lock
   *
   * @param path The full name of the file to be created.
   * @param node Optional output argument to the node of the
   *    created file.
   * @return Status
   */
  Status touch_internal(const std::string& path, FSNode** node = nullptr) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_MEMORY_FILESYSTEM_H
