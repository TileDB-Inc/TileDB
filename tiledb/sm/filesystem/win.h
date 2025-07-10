/**
 * @file   win.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file includes declarations of Windows filesystem functions.
 */

#ifndef TILEDB_WIN_FILESYSTEM_H
#define TILEDB_WIN_FILESYSTEM_H

#ifdef _WIN32

#include <sys/types.h>
#include <string>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/filesystem/filesystem_base.h"
#include "tiledb/sm/filesystem/local.h"
#include "tiledb/sm/filesystem/ls_scanner.h"

using namespace tiledb::common;

namespace tiledb::common::filesystem {
class directory_entry;
}

namespace tiledb::sm {

class URI;

/** Class for Windows status exceptions. */
class WindowsException : public StatusException {
 public:
  explicit WindowsException(const std::string& msg)
      : StatusException("Windows", msg) {
  }
};

/** Typedef this here so we don't have to include Windows.h */
typedef void* HANDLE;

/**
 * This class implements Windows filesystem functions.
 */
class Win : FilesystemBase, LocalFilesystem {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  Win()
      : Win(Config()) {
  }

  /**
   * Constructor.
   *
   * @param config Configuration parameters.
   */
  explicit Win(const Config& config);

  /** Destructor. */
  ~Win() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Returns the absolute (string) path of the input in the
   * form of a Windows path.
   */
  static std::string abs_path(const std::string& path);

  /**
   * Creates a new directory.
   *
   * @param uri The URI of the directory to be created.
   */
  void create_dir(const URI& uri) const;

  /**
   * Creates an empty file.
   *
   * @param uri The uri of the file to be created.
   */
  void touch(const URI& uri) const;

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
   * @param uri The uri of the directory to be deleted.
   */
  void remove_dir(const URI& uri) const;

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
   * @param uri The uri of the file to be deleted.
   */
  void remove_file(const URI& uri) const;

  /**
   * Returns the size of the input file.
   *
   * @param uri The uri of the file whose size is to be retrieved.
   * @return The size of the input file.
   */
  uint64_t file_size(const URI& uri) const;

  /**
   * Checks if the input is an existing directory.
   *
   * @param uri The uri of the directory to be checked.
   * @return `true` if `dir` is an existing directory, `false` otherwise.
   */
  bool is_dir(const URI& uri) const;

  /**
   * Checks if the input is an existing file.
   *
   * @param uri The uri of the file to be checked.
   * @return `true` if `file` is an existing file, `false` otherwise.
   */
  bool is_file(const URI& uri) const;

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
   * Lists files and file information under a given path.
   *
   * @param path The parent path to list sub-paths.
   * @return A list of directory_entry objects
   */
  std::vector<filesystem::directory_entry> ls_with_sizes(const URI& path) const;

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
   * Move a given filesystem path.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   */
  void move_path(const URI& old_path, const URI& new_path) const;

  /**
   * Renames a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void move_dir(const URI& old_uri, const URI& new_uri) const;

  /**
   * Renames a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void move_file(const URI& old_uri, const URI& new_uri) const;

  /**
   * Copies a directory.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_dir(const URI&, const URI&) const {
    // Currently a no-op for Windows, stub function for other filesystems.
  }

  /**
   * Copies a file.
   *
   * @param old_uri The old URI.
   * @param new_uri The new URI.
   */
  void copy_file(const URI&, const URI&) const {
    // Currently a no-op for Windows, stub function for other filesystems.
  }

  /**
   * Reads data from a file into a buffer.
   *
   * @param uri The uri of the file.
   * @param offset The offset in the file from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param nbytes The size of the data to be read from the file.
   * @param use_read_ahead Whether to use a read-ahead cache. Unused internally.
   */
  void read(
      const URI& uri,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes,
      bool use_read_ahead) const;

  /**
   * Flushes a file or directory.
   *
   * @param uri The URI of the file.
   * @param finalize Unused flag. Reserved for finalizing S3 object upload only.
   */
  void flush(const URI& uri, bool finalize);

  /**
   * Syncs a file or directory.
   *
   * @param uri The uri of the file.
   */
  void sync(const URI& uri) const;

  /**
   * Writes the input buffer to a file.
   *
   * If the file exists than it is created.
   * If the file does not exist than it is appended to.
   *
   * @param uri The uri of the file.
   * @param buffer The input buffer.
   * @param buffer_size The size of the input buffer.
   * @param remote_global_order_write Unused flag. Reserved for S3 objects only.
   */
  void write(
      const URI& uri,
      const void* buffer,
      uint64_t buffer_size,
      bool remote_global_order_write);

 private:
  /** Config parameters from parent VFS instance. */
  Config config_;

  /**
   * Recursively removes the directory at the given path.
   *
   * @param path Directory to remove.
   * @return Status
   */
  Status recursively_remove_directory(const std::string& path) const;

  /**
   * Write data from the given buffer to the file handle, beginning at the
   * given offset.
   *
   * @param file_h Open file handle to write to
   * @param file_offset Offset in the file at which to start writing
   * @param buffer Buffer of data to write
   * @param buffer_size Number of bytes to write
   * @return Status
   */
  static Status write_at(
      HANDLE file_h,
      uint64_t file_offset,
      const void* buffer,
      uint64_t buffer_size);
};

}  // namespace tiledb::sm

#endif  // _WIN32

#endif  // TILEDB_WIN_FILESYSTEM_H
