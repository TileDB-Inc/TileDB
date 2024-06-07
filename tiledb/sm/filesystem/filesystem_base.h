/**
 * @file filesystem_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
#include "uri.h"

#include <vector>

namespace tiledb::sm {
class IOError : public StatusException {
 public:
  explicit IOError(const std::string& message)
      : StatusException("IO Error", message) {
  }
};

class FilesystemBase {
 public:
  FilesystemBase() = default;

  virtual ~FilesystemBase() = default;

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
   * @param size The file size to be retrieved.
   */
  virtual void file_size(const URI& uri, uint64_t* size) const = 0;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<filesystem::directory_entry> ls_with_sizes(
      const URI& parent) const = 0;

  /**
   * Renames a file.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_file(const URI& old_uri, const URI& new_uri) const = 0;

  /**
   * Renames a directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void move_dir(const URI& old_uri, const URI& new_uri) const = 0;

  /**
   * Copies a file.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_file(const URI& old_uri, const URI& new_uri) const = 0;

  /**
   * Copies directory.
   * Both URI must be of the same backend type. (e.g. both s3://, file://, etc)
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  virtual void copy_dir(const URI& old_uri, const URI& new_uri) const = 0;

  /**
   * Reads from a file.
   *
   * @param uri The URI of the file.
   * @param offset The offset where the read begins.
   * @param buffer The buffer to read into.
   * @param nbytes Number of bytes to read.
   * @param use_read_ahead Whether to use the read-ahead cache.
   */
  virtual void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead = true) const = 0;

  /**
   * Syncs (flushes) a file. Note that for S3 this is a noop.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) const = 0;

  /**
   * Writes the contents of a buffer into a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   * @param remote_global_order_write Remote global order write
   */
  virtual void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write) = 0;

  /**
   * Checks if an object store bucket exists.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket exists, false otherwise.
   */
  virtual bool is_bucket(const URI& uri) const = 0;

  /**
   * Checks if an object-store bucket is empty.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket is empty, false otherwise.
   */
  virtual bool is_empty_bucket(const URI& uri) const = 0;

  /**
   * Creates an object store bucket.
   *
   * @param uri The name of the bucket to be created.
   */
  virtual void create_bucket(const URI& uri) const = 0;

  /**
   * Deletes an object store bucket.
   *
   * @param uri The name of the bucket to be deleted.
   */
  virtual void remove_bucket(const URI& uri) const = 0;

  /**
   * Deletes the contents of an object store bucket.
   *
   * @param uri The name of the bucket to be emptied.
   */
  virtual void empty_bucket(const URI& uri) const = 0;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FILESYSTEMBASE_H
