/**
 * @file   posix_filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include <sys/types.h>
#include <string>
#include <vector>

#include "buffer.h"
#include "status.h"
#include "uri.h"

class Buffer;

namespace tiledb {

namespace posix {

Status create_file(const std::string& filename);

/**
 * Share-lock a given filename and return open file descriptor handle.
 *
 * @param filename the lockfile to lock
 * @param fd a pointer to a file descriptor
 * @param shared True if this is a shared lock, false if it is an exclusive lock
 * @return Status
 */
Status filelock_lock(const std::string& filename, int* fd, bool shared);

/**
 * Unlock an opened file descriptor
 *
 * @param fd the open file descriptor to unlock
 * @return Status
 */
Status filelock_unlock(int fd);

/**
 * Move a given filesystem path
 *
 * @param old_path the current path
 * @param new_path the new path
 * @return Status
 */
Status move_dir(const std::string& old_path, const std::string& new_path);

/**
 *
 * List files one level deep under a given path
 *
 * @param path  The parent path to list sub-paths
 * @param paths Pointer to a vector of strings to store absolute paths
 * @return
 */
Status ls(const std::string& path, std::vector<std::string>* paths);

/**
 * Creates a new directory.
 *
 * @param dir The name of the directory to be created.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
Status create_dir(const std::string& path);

/**
 * Returns the directory where the program is executed.
 *
 * @return The directory where the program is executed. If the program cannot
 *     retrieve the current working directory, the empty string is returned.
 */
std::string current_dir();

/**
 * Deletes a directory. Note that the directory must not contain other
 * directories, but it should only contain files.
 *
 * @param dirname The name of the directory to be deleted.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
// TODO Status delete_dir(const std::string& path);

// TODO: (jcb) uri
// TODO: Status delete_dir(const URI& path);

/**
 * Returns the size of the input file.
 *
 * @param path The name of the file whose size is to be retrieved.
 * @return The file size on success, and TILEDB_UT_ERR for error.
 */
Status file_size(const std::string& path, uint64_t* size);

Status delete_file(const std::string& path);

/**
 * Checks if the input is an existing directory.
 *
 * @param dir The directory to be checked.
 * @return *true* if *dir* is an existing directory, and *false* otherwise.
 */
bool is_dir(const std::string& path);

/**
 * Checks if the input is an existing file.
 *
 * @param file The file to be checked.
 * @return *True* if *file* is an existing file, and *false* otherwise.
 */
bool is_file(const std::string& path);

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
void purge_dots_from_path(std::string& path);

/**
 * Reads data from a file into a buffer.
 *
 * @param path The name of the file.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return TILEDB_UT_OK on success and TILEDB_UT_ERR on error.
 */
Status read_from_file(
    const std::string& path, uint64_t offset, void* buffer, uint64_t length);

/**
 * Read contents of a file into a (growable) byte buffer.
 *
 * @param path  The name of a file.
 * @param buff A pointer to the allocated buffe
 * @param buffer_size The number of bytes allocated to hold the buffer
 * @return Status
 */
Status read_from_file(const std::string& path, Buffer** buff);

std::string abs_path(const std::string& path);

/**
 * Syncs a file or directory. If the file/directory does not exist,
 * the function gracefully exits (i.e., it ignores the syncing).
 *
 * @param path The name of the file.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
Status sync(const std::string& path);

/**
 * Writes the input buffer to a file.
 *
 * @param path The name of the file.
 * @param buffer The input buffer.
 * @param buffer_size The size of the input buffer.
 * @return TILEDB_UT_OK on success, and TILEDB_UT_ERR on error.
 */
Status write_to_file(
    const std::string& path, const void* buffer, uint64_t buffer_size);

}  // namespace posix

}  // namespace tiledb

#endif  // TILEDB_POSIX_FILESYSTEM_H
