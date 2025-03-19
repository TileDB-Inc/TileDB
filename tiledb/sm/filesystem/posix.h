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

#include <dirent.h>
#include <ftw.h>
#include <sys/types.h>
#include <unistd.h>

#include <filesystem>
#include <functional>
#include <string>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem_base.h"
#include "tiledb/sm/filesystem/local.h"
#include "tiledb/sm/filesystem/ls_scanner.h"

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
class Posix : public FilesystemBase, public LocalFilesystem {
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
   * Creates a new directory.
   *
   * @param dir The name of the directory to be created.
   */
  void create_dir(const URI& uri) const override;

  /**
   * Creates an empty file.
   *
   * @param filename The name of the file to be created.
   */
  void touch(const URI& uri) const override;

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
   * Removes a given directory recursively.
   *
   * @param path The path of the directory to be deleted.
   */
  void remove_dir(const URI& path) const override;

  /**
   * Removes a given empty directory.
   *
   * @param path The path of the directory.
   * @return true if the directory was removed, false otherwise.
   */
  bool remove_dir_if_empty(const std::string& path) const;

  /**
   * Removes a given path.
   *
   * @param path The path of the file / empty directory to be deleted.
   */
  void remove_file(const URI& path) const override;

  /**
   * Returns the size of the input file.
   *
   * @param path The name of the file whose size is to be retrieved.
   * @param nbytes Pointer to a value
   */
  void file_size(const URI& path, uint64_t* size) const override;

  /**
   * Move a given filesystem path.
   * Both URI must be of the same file:// backend type.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void move_file(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Renames a directory.
   * Both URI must be of the same file:// backend type.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void move_dir(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Copy a given filesystem file.
   * Both URI must be of the same file:// backend type.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_file(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Copy a given filesystem directory.
   * Both URI must be of the same file:// backend type.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_dir(const URI& old_uri, const URI& new_uri) const override;

  /**
   * Reads data from a file into a buffer.
   *
   * @param path The name of the file.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param nbytes The size of the data to be read from the file.
   */
  void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead = true) const override;

  /**
   * Syncs a file or directory.
   *
   * @param path The name of the file.
   */
  void sync(const URI& uri) const override;

  /**
   * Writes the input buffer to a file.
   *
   * If the file exists than it is created.
   * If the file does not exist than it is appended to.
   *
   * @param path The name of the file.
   * @param buffer The input buffer.
   * @param buffer_size The size of the input buffer.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write = false) override;

  /**
   * Checks if an object store bucket exists.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket exists, false otherwise.
   */
  bool is_bucket(const URI&) const override {
    // No concept of buckets for Posix.
    return false;
  }

  /**
   * Checks if an object-store bucket is empty.
   *
   * @param uri The name of the object store bucket.
   * @return True if the bucket is empty, false otherwise.
   */
  bool is_empty_bucket(const URI&) const override {
    // No concept of buckets for Posix.
    return true;
  }

  /**
   * Creates an object store bucket.
   *
   * @param uri The name of the bucket to be created.
   */
  void create_bucket(const URI&) const override {
    // No-op for Posix, stub function for cloud filesystems.
  }

  /**
   * Deletes an object store bucket.
   *
   * @param uri The name of the bucket to be deleted.
   */
  void remove_bucket(const URI&) const override {
    // No-op for Posix, stub function for cloud filesystems.
  }

  /**
   * Deletes the contents of an object store bucket.
   *
   * @param uri The name of the bucket to be emptied.
   */
  void empty_bucket(const URI&) const override {
    // No-op for Posix, stub function for cloud filesystems.
  }

  /**
   *
   * Lists files and file information one level deep under a given path.
   *
   * @param uri The parent path to list sub-paths.
   * @return A list of directory_entry objects
   */
  std::vector<filesystem::directory_entry> ls_with_sizes(
      const URI& uri) const override;

  /**
   * Lists objects and object information that start with `prefix`, invoking
   * the FilePredicate on each entry collected and the DirectoryPredicate on
   * common prefixes for pruning.
   *
   * @param parent The parent prefix to list sub-paths.
   * @param f The FilePredicate to invoke on each object for filtering.
   * @param d The DirectoryPredicate to invoke on each common prefix for
   *    pruning. This is currently unused, but is kept here for future support.
   * @param recursive Whether to recursively list subdirectories.
   *
   * Note: the return type LsObjects does not match the other "ls" methods so as
   * to match the S3 equivalent API.
   */
  template <FilePredicate F, DirectoryPredicate D>
  LsObjects ls_filtered(
      const URI& parent,
      F f,
      D d = accept_all_dirs,
      bool recursive = false) const {
    return std_filesystem_ls_filtered<F, D>(parent, f, d, recursive);
  }

  /**
   * Lists files one level deep under a given path.
   *
   * @param path  The parent path to list sub-paths.
   * @param paths Pointer to a vector of strings to store the retrieved paths.
   * @return Status
   */
  Status ls(const std::string& path, std::vector<std::string>* paths) const;

  /**
   * Returns the absolute posix (string) path of the input in the
   * form "file://<absolute path>"
   */
  static std::string abs_path(std::string_view path);

  /**
   * Returns the directory where the program is executed.
   *
   * @return The directory path where the program is executed. If the program
   * cannot retrieve the current working directory, the empty string is
   * returned.
   */
  static std::string current_dir();

 private:
  static void adjacent_slashes_dedup(std::string* path);

  static bool both_slashes(char a, char b);

  // Internal logic for 'abs_path()'.
  static std::string abs_path_internal(std::string_view path);

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
