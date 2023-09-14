/**
 * @file   posix.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file includes declarations of posix filesystem functions.
 */

#ifndef TILEDB_FILESYSTEM_POSIX_H
#define TILEDB_FILESYSTEM_POSIX_H
#ifndef _WIN32

#include <sys/types.h>

#include <functional>
#include <string>
#include <vector>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem.h"


namespace tiledb::sm::filesystem {

class URI;

/**
 * This class implements the POSIX filesystem functions.
 */
class Posix : public Filesystem {
 public:
  /** Constructor. */
  explicit Posix(const Config& config);

  /** Destructor. */
  ~Posix() override = default;

  /**
   * Check if a URI refers to a directory.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a directory or not.
   */
  virtual bool is_dir(const URI& uri) const override;

  /**
   * Check if a URI refers to a file.
   *
   * @param uri The URI to check.
   * @return bool Whether uri refers to a file or not.
   */
  virtual bool is_file(const URI& uri) const override;

  /**
   * Create a directory.
   *
   * @param uri The URI of the directory.
   */
  virtual void create_dir(const URI& uri) override;

  /**
   * Retrieves all the entries contained in the parent.
   *
   * @param parent The target directory to list.
   * @return All entries that are contained in the parent
   */
  virtual std::vector<FilesystemEntry>
  ls(const URI& parent) const override;

  /**
   * Copy a directory.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void copy_dir(const URI& old_uri, const URI& new_uri) override;

  /**
   * Recursively remove a directory.
   *
   * @param uri The uri of the directory to be removed.
   */
  virtual void remove_dir(const URI& uri) override;

  /**
   * Create an empty file.
   *
   * @param uri The URI of the file.
   */
  virtual void touch(const URI& uri) override;

  /**
   * Retrieves the size of a file.
   *
   * @param uri The URI of the file.
   * @return uint64_t The size of the file in bytes.
   */
  virtual uint64_t file_size(const URI& uri) const override;

  /**
   * Write the contents of a buffer to a file.
   *
   * @param uri The URI of the file.
   * @param buffer The buffer to write from.
   * @param buffer_size The buffer size.
   */
  virtual Status write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size) override;

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
      uint64_t nbytes) override;

  /**
   * Syncs (flushes) a file.
   *
   * @param uri The URI of the file.
   */
  virtual void sync(const URI& uri) override;

  /**
   * Copy a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void copy_file(const URI& src_uri, const URI& tgt_uri) override;

  /**
   * Rename a file.
   *
   * @param src_uri The old URI.
   * @param tgt_uri The new URI.
   */
  virtual void move_file(const URI& src_uri, const URI& tgt_uri) override;

  /**
   * Delete a file.
   *
   * @param uri The URI of the file to remove.
   */
  virtual void remove_file(const URI& uri) const override;

 private:
  /**
   * Depth first filesystem entry traversal.
   *
   * @param uri The directory to traverse recursively.
   * @param callback A function invoked for every FilesystemEntry
   */
  void traverse(const URI& base, std::function<void(FilesystemEntry)> callback, bool top_down = true);

  /**
   * Write data from the given buffer to the file descriptor, beginning at the
   * given offset. Multiple threads can safely write to the same open file
   * descriptor.
   *
   * @param fd Open file descriptor to write to
   * @param file_offset Offset in the file at which to start writing
   * @param buffer Buffer of data to write
   * @param buffer_size Number of bytes to write
   * @return Status
   */
  static void write_at(
      int fd, uint64_t offset, const void* buffer, uint64_t nbytes);

  /**
   * Reads all nbytes from the given file descriptor, retrying as necessary.
   *
   * @param fd Open file descriptor to read from
   * @param buffer Buffer to hold read data
   * @param nbytes Number of bytes to read
   * @param offset Offset in file to start reading from.
   * @return Status
   */
  static void read_all(
      int fd, uint64_t offset, void* buffer, uint64_t nbytes);

  /**
   * @return Permissions to use when creating directories.
   */
  uint32_t get_directory_permissions() const;

  /**
   * @return Permissions to use when creating files.
   */
  uint32_t get_file_permissions() const;

};

}  // namespace tiledb::sm::filesystem

#endif  // !_WIN32
#endif  // TILEDB_FILESYSTEM_POSIX_H
