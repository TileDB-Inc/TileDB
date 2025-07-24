/**
 * @file filesystem_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2025 TileDB, Inc.
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

#include "tiledb/common/exception/exception.h"
#include "tiledb/common/filesystem/directory_entry.h"
#include "tiledb/sm/filesystem/ls_scanner.h"
#include "uri.h"

#include <vector>

namespace tiledb::sm {
class IOError : public StatusException {
 public:
  explicit IOError(const std::string& message)
      : StatusException("IO Error", message) {
  }
};

class FilesystemException : public StatusException {
 public:
  explicit FilesystemException(const std::string& message)
      : StatusException("Filesystem", message) {
  }
};

class UnsupportedOperation : public FilesystemException {
 public:
  explicit UnsupportedOperation(const std::string& operation)
      : FilesystemException(std::string(
            operation + " is not supported on the given filesystem.")) {
  }
};

class UnsupportedURI : public FilesystemException {
 public:
  explicit UnsupportedURI(const std::string& uri)
      : FilesystemException("Unsupported URI scheme: " + uri) {
  }
};

class FilesystemBase {
 public:
  FilesystemBase() = default;

  virtual ~FilesystemBase() = default;

  /**
   * Checks if the filesystem supports the given URI.
   *
   * - s3.supports_uri(s3://test) will return true.
   * - posix.supports_uri(s3://test) will return false.
   *
   * @param uri The URI to check.
   * @return `true` if `uri` is supported on the filesystem, `false` otherwise.
   */
  virtual bool supports_uri(const URI& uri) const = 0;

  /**
   * Creates a directory.
   *
   * - On S3, this is a noop.
   * - On all other backends, if the directory exists, the function
   *   just succeeds without doing anything.
   *
   * @param uri The URI of the directory.
   */
  virtual void create_dir(const URI& uri) const = 0;

  /**
   * Creates an empty file.
   *
   * @param uri The URI of the file.
   */
  virtual void touch(const URI& uri) const = 0;

  /**
   * Checks if a directory exists.
   *
   * @param uri The URI to check for existence.
   * @return True if the directory exists, else False.
   */
  virtual bool is_dir(const URI& uri) const = 0;

  /**
   * Checks if a file exists.
   *
   * @param uri The URI to check for existence.
   * @return True if the file exists, else False.
   */
  virtual bool is_file(const URI& uri) const = 0;

  /**
   * Removes a given directory (recursive)
   *
   * @param uri The uri of the directory to be removed
   */
  virtual void remove_dir(const URI& uri) const = 0;

  /**
   * Deletes a file.
   *
   * @param uri The URI of the file.
   */
  virtual void remove_file(const URI& uri) const = 0;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return The file size.
   */
  virtual uint64_t file_size(const URI& uri) const = 0;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<tiledb::common::filesystem::directory_entry>
  ls_with_sizes(const URI& parent) const = 0;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the FileFilter on each entry collected and the DirectoryFilter on
   * common prefixes for pruning.
   *
   * Currently this API is only supported for local files, S3 and Azure.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FileFilter to invoke on each object for filtering.
   * @param d The DirectoryFilter to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @param recursive Whether to list the objects recursively.
   * @return Vector of results with each entry being a pair of the string URI
   *    and object size.
   */
  virtual LsObjects ls_filtered(
      const URI& parent, FileFilter f, DirectoryFilter d, bool recursive) const;

  /**
   * Renames a file.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Renames a directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_dir(const URI& old_uri, const URI& new_uri) const;

  /**
   * Copies a file.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Copies directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_dir(const URI& old_uri, const URI& new_uri) const;

  /**
   * Whether or not to use the read-ahead cache.
   *
   * Defaults to `true` for object-store, `false` for local filesystems.
   */
  virtual bool use_read_ahead_cache() const;

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   */
  virtual uint64_t read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) = 0;

  /**
   * Flushes an object store file.
   *
   * @invariant Performs a sync on local filesystem files.
   *
   * @param uri The URI of the file.
   * @param finalize For S3 objects only. If `true`, flushes as a result of a
   * remote global order write `finalize()` call.
   */
  virtual void flush(
      const URI& uri, [[maybe_unused]] bool finalize = false) = 0;

  /**
   * Syncs a local file.
   *
   * @invariant Valid only for local filesystems.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) const;

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @param remote_global_order_write For S3 objects only. If `true`, performs
   * a remote global order write.
   */
  virtual void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      [[maybe_unused]] bool remote_global_order_write = false) = 0;

  /**
   * Checks if an object store bucket exists.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket exists, false otherwise.
   */
  virtual bool is_bucket(const URI& uri) const;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket is empty, false otherwise.
   */
  virtual bool is_empty_bucket(const URI& uri) const;

  /**
   * Creates an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be created.
   */
  virtual void create_bucket(const URI& uri) const;

  /**
   * Deletes an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be deleted.
   */
  virtual void remove_bucket(const URI& uri) const;

  /**
   * Deletes the contents of an object store bucket.
   *
   * @invariant Valid only for object store filesystems.
   *
   * @param uri The name of the bucket to be emptied.
   */
  virtual void empty_bucket(const URI& uri) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILESYSTEMBASE_H
