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

#ifndef TILEDB_POSIX_FILESYSTEM_H
#define TILEDB_POSIX_FILESYSTEM_H

#ifndef _WIN32

#include <ftw.h>
#include <sys/types.h>

#include <functional>
#include <string>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem_base.h"

using namespace tiledb::common;

namespace tiledb {

namespace common::filesystem {
class directory_entry;
}

namespace sm {

class URI;

/**
 * This class implements the POSIX filesystem functions.
 */
class Posix : public FilesystemBase {
 public:
  /** Default constructor. */
  Posix()
      : Posix(Config()) {
  }

  /** Constructor. */
  explicit Posix(const Config& config);

  /** Destructor. */
  ~Posix() override = default;

  /**
   * Returns the absolute posix (string) path of the input in the
   * form "file://<absolute path>"
   */
  static std::string abs_path(const std::string& path);

  /**
   * Creates a new directory.
   *
   * @param dir The name of the directory to be created.
   * @return Status
   */
  Status create_dir(const URI& uri) const override;

  /**
   * Creates an empty file.
   *
   * @param filename The name of the file to be created.
   * @return Status
   */
  Status touch(const URI& uri) const override;

  /**
   * Returns the directory where the program is executed.
   *
   * @return The directory path where the program is executed. If the program
   * cannot retrieve the current working directory, the empty string is
   * returned.
   */
  static std::string current_dir();

  /**
   * Removes a given directory recursively.
   *
   * @param path The path of the directory to be deleted.
   * @return Status
   */
  Status remove_dir(const URI& path) const override;

  /** Deletes the file in the input path. */

  /**
   * Removes a given path.
   *
   * @param path The path of the file / empty directory to be deleted.
   * @return Status
   */
  Status remove_file(const URI& path) const override;

  /**
   * Returns the size of the input file.
   *
   * @param path The name of the file whose size is to be retrieved.
   * @param nbytes Pointer to a value
   * @return Status
   */
  Status file_size(const URI& path, uint64_t* size) const override;

  /**
   * Checks if the input is an existing directory.
   *
   * @param dir The directory to be checked.
   * @return *True* if *dir* is an existing directory, and *False* otherwise.
   */
  bool is_dir(const URI& uri) const override;

  /**
   * Checks if the input is an existing file.
   *
   * @param file The file to be checked.
   * @return *True* if *file* is an existing file, and *false* otherwise.
   */
  bool is_file(const URI& uri) const override;

  /**
   *
   * Lists files one level deep under a given path.
   *
   * @param path  The parent path to list sub-paths.
   * @param paths Pointer to a vector of strings to store the retrieved paths.
   * @return Status
   */
  Status ls(const std::string& path, std::vector<std::string>* paths) const;

  /**
   *
   * Lists files and file information one level deep under a given path.
   *
   * @param uri The parent path to list sub-paths.
   * @return A list of directory_entry objects
   */
  tuple<Status, optional<std::vector<filesystem::directory_entry>>>
  ls_with_sizes(const URI& uri) const override;

  /**
   * Move a given filesystem path.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   * @return Status
   */
  Status move_file(const URI& old_path, const URI& new_path) const override;

  /**
   * Copy a given filesystem file.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   * @return Status
   */
  Status copy_file(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Copy a given filesystem directory.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   * @return Status
   */
  Status copy_dir(const URI& old_path, const URI& new_path) const override;

  /**
   * Reads data from a file into a buffer.
   *
   * @param path The name of the file.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param nbytes The size of the data to be read from the file.
   * @return Status.
   */
  Status read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead = true) override;

  /**
   * Syncs a file or directory.
   *
   * @param path The name of the file.
   * @return Status
   */
  Status sync(const URI& uri) override;

  /**
   * Writes the input buffer to a file.
   *
   * If the file exists than it is created.
   * If the file does not exist than it is appended to.
   *
   * @param path The name of the file.
   * @param buffer The input buffer.
   * @param buffer_size The size of the input buffer.
   * @return Status
   */
  Status write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write = false) override;

 private:
  static void adjacent_slashes_dedup(std::string* path);

  static bool both_slashes(char a, char b);

  // Internal logic for 'abs_path()'.
  static std::string abs_path_internal(const std::string& path);

  /**
   * It takes as input an **absolute** path, and returns it in its canonicalized
   * form, after appropriately replacing "./" and "../" in the path.
   *
   * @param path The input path passed by reference, which will be modified
   *     by the function to hold the canonicalized absolute path. Note that the
   *     path must be absolute, otherwise the function fails. In case of error
   *     (e.g., if "../" are not properly used in *path*, or if *path* is not
   *     absolute), the function sets the empty string (i.e., "") to *path*.
   * @return void
   */
  static void purge_dots_from_path(std::string* path);

  /**
   * Reads all nbytes from the given file descriptor, retrying as necessary.
   *
   * @param fd Open file descriptor to read from
   * @param buffer Buffer to hold read data
   * @param nbytes Number of bytes to read
   * @param offset Offset in file to start reading from.
   * @return Status
   */
  static Status read_all(
      int fd, void* buffer, uint64_t nbytes, uint64_t offset);

  static int unlink_cb(
      const char* fpath,
      const struct stat* sb,
      int typeflag,
      struct FTW* ftwbuf);

  /**
   * Write data from the given buffer to the file descriptor, beginning at the
   * given offset.
   *
   * @param fd Open file descriptor to write to
   * @param file_offset Offset in the file at which to start writing
   * @param buffer Buffer of data to write
   * @param buffer_size Number of bytes to write
   * @return Status
   */
  static Status write_at(
      int fd, uint64_t file_offset, const void* buffer, uint64_t buffer_size);

 private:
  uint32_t file_permissions_, directory_permissions_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // !_WIN32

#endif  // TILEDB_POSIX_FILESYSTEM_H
