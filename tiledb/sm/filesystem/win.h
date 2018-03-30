/**
 * @file   win.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/config.h"

namespace tiledb {
namespace sm {

/** Typedef this here so we don't have to include Windows.h */
typedef void* HANDLE;

/**
 * This class implements Windows filesystem functions.
 */
class Win {
 public:
  /**
   * Returns the absolute (string) path of the input in the
   * form of a Windows path.
   */
  static std::string abs_path(const std::string& path);

  /**
   * Creates a new directory.
   *
   * @param dir The name of the directory to be created.
   * @return Status
   */
  Status create_dir(const std::string& path) const;

  /**
   * Creates an empty file.
   *
   * @param filename The name of the file to be created.
   * @return Status
   */
  Status touch(const std::string& filename) const;

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
  Status remove_dir(const std::string& path) const;

  /**
   * Removes a given path.
   *
   * @param path The path of the file / empty directory to be deleted.
   * @return Status
   */
  Status remove_file(const std::string& path) const;

  /**
   * Returns the size of the input file.
   *
   * @param path The name of the file whose size is to be retrieved.
   * @param nbytes Pointer to a value
   * @return Status
   */
  Status file_size(const std::string& path, uint64_t* size) const;

  /**
   * Lock a given filename and retrieve an open file descriptor handle.
   *
   * @param filename The filelock to lock
   * @param fd A pointer to a file descriptor
   * @param shared *True* if this is a shared lock, *false* if it is an
   * exclusive lock.
   * @return Status
   */
  Status filelock_lock(
      const std::string& filename, filelock_t* fd, bool shared) const;

  /**
   * Unlock an opened file descriptor
   *
   * @param fd the open file descriptor to unlock
   * @return Status
   */
  Status filelock_unlock(filelock_t fd) const;

  /**
   * Initialize this instance with the given parameters.
   *
   * @param vfs_params Params from the parent VFS instance.
   * @param vfs_thread_pool ThreadPool from the parent VFS instance.
   * @return Status
   */
  Status init(const Config::VFSParams& vfs_params, ThreadPool* vfs_thread_pool);

  /**
   * Checks if the input is an existing directory.
   *
   * @param dir The directory to be checked.
   * @return *True* if *dir* is an existing directory, and *False* otherwise.
   */
  bool is_dir(const std::string& path) const;

  /**
   * Checks if the input is an existing file.
   *
   * @param file The file to be checked.
   * @return *True* if *file* is an existing file, and *false* otherwise.
   */
  bool is_file(const std::string& path) const;

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
   * Move a given filesystem path.
   *
   * @param old_path The old path.
   * @param new_path The new path.
   * @return Status
   */
  Status move_path(
      const std::string& old_path, const std::string& new_path) const;

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
      const std::string& path,
      uint64_t offset,
      void* buffer,
      uint64_t nbytes) const;

  /**
   * Syncs a file or directory.
   *
   * @param path The name of the file.
   * @return Status
   */
  Status sync(const std::string& path) const;

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
      const std::string& path, const void* buffer, uint64_t buffer_size) const;

  /**
   * Converts a Windows path to a "file:///" URI.
   *
   * @param path The Windows path to convert.
   * @status A path file URI.
   */
  static std::string uri_from_path(const std::string& path);

  /**
   * Converts a "file:///" URI to a Windows path.
   *
   * @param path The URI to convert.
   * @status A Windows path.
   */
  static std::string path_from_uri(const std::string& uri);

  /**
   * Returns true if the given string is a Windows path.
   *
   * @param path The path to check.
   * @return True if the path is a Windows path.
   */
  static bool is_win_path(const std::string& path);

 private:
  /** Config parameters from parent VFS instance. */
  Config::VFSParams vfs_params_;

  /** Thread pool from parent VFS instance. */
  ThreadPool* vfs_thread_pool_;

  /**
   * Recursively removes the directory at the given path.
   *
   * @param path Directory to remove.
   * @return Status
   */
  Status recursively_remove_directory(const std::string& path) const;

  /**
   * Write data from the given buffer to the file handle, beginning at the
   * given offset. Multiple threads can safely write to the same open file
   * descriptor.
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

}  // namespace sm
}  // namespace tiledb

#endif  // _WIN32

#endif  // TILEDB_WIN_FILESYSTEM_H
