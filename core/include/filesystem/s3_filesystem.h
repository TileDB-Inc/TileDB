/**
 * @file   s3_filesystem.h
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
 * This file includes declarations of S3 filesystem functions.
 */

#ifndef TILEDB_S3_FILESYSTEM_H
#define TILEDB_S3_FILESYSTEM_H

#include <sys/types.h>
#include <string>
#include <vector>

#include "status.h"
#include "uri.h"

namespace tiledb {

namespace s3 {

#ifdef HAVE_S3
/**
 * Connects an S3 client
 *
 * @return Status
 */
Status connect();

/**
 * Disconnects a S3 client
 *
 * @return Status
 */
Status disconnect();

/**
 * Check if a bucket exists.
 *
 * @param bucket The name of the bucket.
 * @return bool
 */
bool bucket_exists(const char* bucket);

/**
 * Creates a bucket.
 *
 * @param bucket The name of the bucket to be created.
 * @return Status
 */
Status create_bucket(const char* bucket);

/**
 * Deletes a bucket.
 *
 * @param bucket The name of the bucket to be deleted.
 * @return Status
 */
Status delete_bucket(const char* bucket);

/**
 * Creates a new directory. Directories are not really supported in S3. Instead
 * we just create an empty file having a ".dir" suffix
 *
 * @param uri The URI of the directory resource to be created.
 * @return Status
 */
Status create_dir(const URI& uri);

/**
 * Checks if the URI is an existing S3 directory. Checks if the ".dir" object
 * exists
 *
 * @param uri The URI of the directory to be checked
 * @return *True* if *uri* is an existing directory, *False* otherwise.
 */
bool is_dir(const URI& uri);

/**
 * Move a given filesystem path. This is a difficult task for S3 because we need
 * to recursively rename all objects inside the directory
 *
 * @param old_uri The URI of the old directory.
 * @param new_uri The URI of the new directory.
 * @return Status
 */
Status move_path(const URI& old_uri, const URI& new_uri);

Status copy_path(const URI& old_uri, const URI& new_uri);

/**
 * Checks if the given URI is an existing S3 object.
 *
 * @param uri The URI of the file to be checked.
 * @return *True* if the *file* is an existing file, and *false* otherwise.
 */
bool is_file(const URI& uri);

/**
 * Creates an empty object.
 *
 * @param uri The URI of the object to be created.
 * @return Status
 */
Status create_file(const URI& uri);

/**
 * Flushes a file to s3.
 *
 * @param uri The URI of the object to be flushed.
 * @return Status
 */
Status flush_file(const URI& uri);

/**
 * Delete a file with a given URI.
 *
 * @param uri The URI of the file to be deleted.
 * @return Status
 */
Status remove_file(const URI& uri);

/**
 * Remove a path with a given URI (recursively)
 *
 * @param uri The URI of the path to be removed.
 * @return Status
 */
Status remove_path(const URI& uri);

/**
 *  Reads data from a file into a buffer.
 *
 * @param uri The URI of the file to be read.
 * @param offset The offset in the file from which the read will start.
 * @param buffer The buffer into which the data will be written.
 * @param length The size of the data to be read from the file.
 * @return Status
 */
Status read_from_file(
    const URI& uri, off_t offset, void* buffer, uint64_t length);

/**
 * Writes the input buffer to a file.
 *
 * If the file does not  exists than it is created.
 * If the file exist then it is appended to.
 *
 * @param uri The URI of the file to be written to.
 * @param buffer The input buffer.
 * @param length The size of the input buffer.
 * @return Status
 */
Status write_to_file_no_cache(
    const URI& uri, const void* buffer, const uint64_t length);

/**
 * Writes the input buffer using write cache
 *
 * If the file does not  exists than it is created.
 * If the file exist then it is appended to.
 *
 * @param uri The URI of the file to be written to.
 * @param buffer The input buffer.
 * @param length The size of the input buffer.
 * @return Status
 */
Status write_to_file(const URI& uri, const void* buffer, const uint64_t length);

/**
 * Lists the files one level deep under a given path.
 *
 * @param uri The URI of the parent directory path.
 * @param paths Pointer ot a vector of URIs to store the retrieved paths.
 * @return Status
 */
Status ls(const URI& uri, std::vector<std::string>* paths);

/**
 * Returns the size of the input file with a given URI in bytes.
 *
 * @param uri The URI of the file.
 * @param nbytes Pointer to unint64_t bytes to return.
 * @return Status
 */
Status file_size(const URI& uri, uint64_t* nbytes);

#endif

}  // namespace s3

}  // namespace tiledb

#endif  // TILEDB_S3_FILESYSTEM_H
