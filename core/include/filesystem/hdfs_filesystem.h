/**
 * @file   hdfs_filesystem.h
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
 * This file includes declarations of HDFS filesystem functions.
 */

#ifndef TILEDB_FILESYSTEM_HDFS_H
#define TILEDB_FILESYSTEM_HDFS_H

#ifdef HAVE_HDFS

#include <sys/types.h>
#include <string>
#include <vector>

#include "buffer.h"
#include "status.h"
#include "uri.h"

#include "hdfs.h"

namespace tiledb {

namespace hdfs {

/**
 * Connects to an HDFS filesystem
 *
 * @param fs Reference to a hdfsFS filesystem handle.
 * @return Status
 */
Status connect(hdfsFS& fs);

/**
 * Disconnects an HDFS filesystem
 *
 * @param fs Reference to an hdfsFS filesystem handle.
 * @return Status
 */
Status disconnect(hdfsFS& fs);

/**
 * Creates a new directory.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the directory resource to be created.
 * @return Status
 */
Status create_dir(hdfsFS fs, const URI& uri);

/**
 * Deletes a given directory.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the directory resource to be deleted.
 * @return Status
 */
Status delete_dir(hdfsFS fs, const URI& uri);

/**
 * Checks if the URI is an existing HDFS directory.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the directory to be checked
 * @return *True* if *uri* is an existing directory, *False* otherwise.
 */
bool is_dir(hdfsFS fs, const URI& uri);

/**
 * Move a given filesystem directory path.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param old_uri The URI of the old directory.
 * @param new_uri The URI of the new directory.
 * @return Status
 */
Status move_dir(hdfsFS fs, const URI& old_uri, const URI& new_uri);

/**
 * Checks if the given URI is an existing HDFS directory.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the file to be checked.
 * @return *True* if the *file* is an existing file, and *false* otherwise.
 */
bool is_file(hdfsFS fs, const URI& uri);

/**
 * Creates an empty file.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the file to be created.
 * @return Status
 */
Status create_file(hdfsFS fs, const URI& uri);

/**
 * Delete a file with a given URI.
 *
 * @param fs Connected hdfsFS filesystem handle.
 * @param uri The URI of the file to be deleted.
 * @return Status
 */
Status delete_file(hdfsFS fs, const URI& uri);

/**
 *  Reads data from a file into a buffer.
 *
 * @param fs Connected hdfsFS filesystem handle.
 * @param uri The URI of the file to be read.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return Status
 */
Status read_from_file(
    hdfsFS fs, const URI& uri, off_t offset, void* buffer, uint64_t length);

/**
 * Writes the input buffer to a file.
 *
 * If the file exists than it is created.
 * If the file does not exist than it is appended to.
 *
 * @param fs Connected hdfsFS filesystem handle.
 * @param uri The URI of the file to be written to.
 * @param buffer The input buffer.
 * @param length The size of the input buffer.
 * @return Status
 */
Status write_to_file(
    hdfsFS fs, const URI& uri, const void* buffer, const uint64_t length);

/**
 * Lists the files one level deep under a given path.
 *
 * @param fs Connected hdfsFS filesystem handle.
 * @param uri The URI of the parent directory path.
 * @param paths Pointer ot a vector of URIs to store the retrieved paths.
 * @return Status
 */
Status ls(hdfsFS fs, const URI& uri, std::vector<std::string>* paths);

/**
 * Returns the size of the input file with a given URI in bytes.
 *
 * @param fs Reference to a connected hdfsFS filesystem handle.
 * @param uri The URI of the file.
 * @param nbytes Pointer to unint64_t bytes to return.
 * @return Status
 */
// File size in bytes for a given path
Status file_size(hdfsFS fs, const URI& uri, uint64_t* nbytes);

}  // namespace hdfs

}  // namespace tiledb

#endif

#endif  // TILEDB_FILESYSTEM_HDFS_H
