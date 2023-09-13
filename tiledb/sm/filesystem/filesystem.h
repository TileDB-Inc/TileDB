/**
 * @file filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file declares the Filesystem base class.
 */

#ifndef TILEDB_FILESYSTEMBASE_H
#define TILEDB_FILESYSTEMBASE_H

#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/filesystem.h"
#include "uri.h"

#include <sys/stat.h>

namespace tiledb::sm::filesystem {

enum FilesystemType {
  AZURE,
  GCS,
  HDFS,
  MEMFS,
  POSIX,
  S3,
  WIN
};

class FilesystemException : public StatusException {
 public:
  explicit FilesystemException(const std::string fs_name, const std::string& message)
      : StatusException(fs_name, message) {
  }
};

class FilesystemEntry {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No Default Constructor. */
  FilesystemEntry() = delete;

  /**
   * Constructor
   *
   * @param uri The URI of the entry
   * @param size The size of the FilesystemEntry in bytes.
   * @param is_directory A boolean flag indicating whether the FilesystemEntry
   *        is a directory.
   */
  FilesystemEntry(const URI& uri, uint64_t size, bool is_directory)
      : uri_(p)
      , size_(size)
      , is_directory_(is_directory) {
  }

  /**
   * Default copy constructor.
   *
   * @param entry The FilesystemEntry to copy.
   */
  FilesystemEntry(const FilesystemEntry& entry) = default;

  /**
   * Default move constructor.
   *
   * @param entry The FilesystemEntry to move.
   */
  FilesystemEntry(FilesystemEntry&& entry) = default;

  /** Default Destructor. */
  ~FilesystemEntry() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Default Copy Assignment
   *
   * @param entry The FilesystemEntry to copy.
   */
  FilesystemEntry& operator=(const FilesystemEntry& entry) = default;

  /**
   * Default Move Assignment
   *
   * @param dentry directory_entry to move from
   */
  FilesystemEntry& operator=(FilesystemEntry&& entry) = default;

  /**
   * @return The URI of the FilesystemEntry.
   */
  const URI& uri() const {
    return uri_;
  }

  /**
   * @return The size of the FilesystemEntry in bytes.
   */
  uint64_t size() const {
    return size_;
  }

  /**
   * @return Whether the FilesystemEntry represents a directory or not.
   */
  bool is_directory() const {
    return is_directory_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The URI of the filesystem entry */
  URI uri_;

  /** The size of a filesystem entry */
  uint64_t size_;

  /** Stores whether the filesystem entry points to a directory */
  bool is_directory_;
};

class Filesystem {
 public:
  Filesystem(const Config& config)
      : config_(config) {
  }

  virtual ~Filesystem() = 0;

  /**
   * Check if a URI refers to a directory.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a directory or not.
   */
  virtual bool is_dir(const URI& uri) const = 0;

  /**
   * Check if a URI refers to a file.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a file or not.
   */
  virtual bool is_file(const URI& uri) const = 0;

  /**
   * Create a directory.
   *
   * @param uri The URI of the directory.
   */
  virtual void create_dir(const URI& uri) = 0;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<FilesystemEntry>
  ls(const URI& parent) const = 0;

  /**
   * Copy a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   * @return Status
   */
  virtual void copy_dir(const URI& old_uri, const URI& new_uri) = 0;

  /**
   * Recursively remove a directory.
   *
   * @param uri The uri of the directory to be removed.
   */
  virtual void remove_dir(const URI& uri) = 0;

  /**
   * Create an empty file.
   *
   * @param uri The URI of the file.
   * @return Status
   */
  virtual void touch(const URI& uri) = 0;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return uint64_t The size of the file in bytes.
   */
  virtual uint64_t file_size(const URI& uri) const = 0;

  /**
   * Write the contents of a buffer to a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @return Status
   */
  virtual Status write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size) = 0;

  /**
   * Read from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  virtual void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes) = 0;

  /**
   * Syncs (flushes) a file.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) = 0;

  /**
   * Copy a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   * @return Status
   */
  virtual void copy_file(const URI& old_uri, const URI& new_uri) = 0;

  /**
   * Rename a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   * @return Status
   */
  virtual void move_file(const URI& old_uri, const URI& new_uri) = 0;

  /**
   * Delete a file.
   *
   * @param uri The URI of the file to remove.
   * @return Status
   */
  virtual void remove_file(const URI& uri) = 0;

 protected:
  Config config_;

  FilesystemType fs_type_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILESYSTEMBASE_H
