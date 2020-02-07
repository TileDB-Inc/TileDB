/**
 * @file   azure.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines the Azure class.
 */

#ifndef TILEDB_AZURE_H
#define TILEDB_AZURE_H

#ifdef HAVE_AZURE
#include <blob/blob_client.h>
#include <storage_account.h>
#include <storage_credential.h>
#include <unordered_map>

#include "tiledb/sm/config/config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

class Azure {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Azure();

  /** Destructor. */
  ~Azure();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Initializes and connects an Azure client.
   *
   * @param config Configuration parameters.
   * @param thread_pool The parent VFS thread pool.
   * @return Status
   */
  Status init(const Config& config, ThreadPool* thread_pool);

  /**
   * Creates a container.
   *
   * @param container The name of the container to be created.
   * @return Status
   */
  Status create_container(const URI& container) const;

  /** Removes the contents of an Azure container. */
  Status empty_container(const URI& container) const;

  /**
   * Flushes an blob to Azure, finalizing the upload.
   *
   * @param uri The URI of the blob to be flushed.
   * @return Status
   */
  Status flush_blob(const URI& uri);

  /**
   * Check if a container is empty.
   *
   * @param container The name of the container.
   * @param is_empty Mutates to `true` if the container is empty.
   * @return Status
   */
  Status is_empty_container(const URI& uri, bool* is_empty) const;

  /**
   * Check if a container exists.
   *
   * @param container The name of the container.
   * @param is_container Mutates to `true` if `uri` is a container.
   * @return Status
   */
  Status is_container(const URI& uri, bool* is_container) const;

  /**
   * Checks if there is an object with prefix `uri/`. For instance, suppose
   * the following objects exist:
   *
   * `azure://some_container/foo/bar1`
   * `azure://some_container/foo2`
   *
   * `is_dir(`azure://some_container/foo`) and
   * `is_dir(`azure://some_container/foo`) will both return `true`, whereas
   * `is_dir(`azure://some_container/foo2`) will return `false`. This is because
   * the function will first convert the input to `azure://some_container/foo2/`
   * (appending `/` in the end) and then check if there exists any object with
   * prefix `azure://some_container/foo2/` (in this case there is not).
   *
   * @param uri The URI to check.
   * @param exists Sets it to `true` if the above mentioned condition holds.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* exists) const;

  /**
   * Checks if the given URI is an existing Azure blob.
   *
   * @param uri The URI of the object to be checked.
   * @param is_blob Mutates to `true` if `uri` is an existing blob, and `false`
   * otherwise.
   */
  Status is_blob(const URI& uri, bool* is_blob) const;

  /**
   * Lists the objects that start with `uri`. Full URI paths are
   * retrieved for the matched objects. If a delimiter is specified,
   * the URI paths will be truncated to the first delimiter character.
   * For instance, if there is a hierarchy:
   *
   * - `foo/bar/baz`
   * - `foo/bar/bash`
   * - `foo/bar/bang`
   * - `foo/boo`
   *
   * and the delimiter is `/`, the returned URIs will be
   *
   * - `foo/boo`
   * - `foo/bar/`
   *
   * @param uri The prefix URI.
   * @param paths Pointer of a vector of URIs to store the retrieved paths.
   * @param delimiter The delimiter that will
   * @param max_paths The maximum number of paths to be retrieved. The default
   *     `-1` indicates that no upper bound is specified.
   * @return Status
   */
  Status ls(
      const URI& uri,
      std::vector<std::string>* paths,
      const std::string& delimiter = "/",
      int max_paths = -1) const;

  /**
   * Renames an object.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status move_object(const URI& old_uri, const URI& new_uri);

  /**
   * Renames a directory. Note that this is an expensive operation.
   * The function will essentially copy all objects with directory
   * prefix `old_uri` to new objects with prefix `new_uri` and then
   * delete the old ones.
   *
   * @param old_uri The URI of the old path.
   * @param new_uri The URI of the new path.
   * @return Status
   */
  Status move_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Returns the size of the input blob with a given URI in bytes.
   *
   * @param uri The URI of the blob.
   * @param nbytes Pointer to `uint64_t` bytes to return.
   * @return Status
   */
  Status blob_size(const URI& uri, uint64_t* nbytes) const;

  /**
   * Reads data from an object into a buffer.
   *
   * @param uri The URI of the object to be read.
   * @param offset The offset in the object from which the read will start.
   * @param buffer The buffer into which the data will be written.
   * @param length The size of the data to be read from the object.
   * @return Status
   */
  Status read(
      const URI& uri, off_t offset, void* buffer, uint64_t length) const;

  /**
   * Deletes a container.
   *
   * @param uri The URI of the container to be deleted.
   * @return Status
   */
  Status remove_container(const URI& uri) const;

  /**
   * Deletes an blob with a given URI.
   *
   * @param uri The URI of the blob to be deleted.
   * @return Status
   */
  Status remove_blob(const URI& uri) const;

  /**
   * Deletes all objects with prefix `uri/` (if the ending `/` does not
   * exist in `uri`, it is added by the function.
   *
   * For instance, suppose there exist the following objects:
   * - `azure://some_container/foo/bar1`
   * - `azure://some_container/foo/bar2/bar3
   * - `azure://some_container/foo/bar4
   * - `azure://some_container/foo2`
   *
   * `remove("azure://some_container/foo")` and
   * `remove("azure://some_container/foo/")` will delete objects:
   *
   * - `azure://some_container/foo/bar1`
   * - `azure://some_container/foo/bar2/bar3
   * - `azure://some_container/foo/bar4
   *
   * In contrast, `remove("azure://some_container/foo2")` will not delete
   * anything; the function internally appends `/` to the end of the URI, and
   * therefore there is not object with prefix "azure://some_container/foo2/" in
   * this example.
   *
   * @param uri The prefix uri of the objects to be deleted.
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

  /**
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

  /**
   * Writes the input buffer to an Azure object. Note that this is essentially
   * an append operation implemented via multipart uploads.
   *
   * @param uri The URI of the object to be written to.
   * @param buffer The input buffer.
   * @param length The size of the input buffer.
   * @return Status
   */
  Status write(const URI& uri, const void* buffer, uint64_t length);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  // The Azure blob storage client.
  std::shared_ptr<azure::storage_lite::blob_client> client_;

  // Maps a URI to unflushed writes.
  std::unordered_map<std::string, std::stringstream> write_cache_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Parses a URI into a container name and blob path. For example,
   * URI "azure://my-container/dir1/file1" will parse into
   * `*container_name == "my-container"` and `*blob_path == "dir1/file1"`.
   *
   * @param uri The URI to parse.
   * @param container_name Mutates to the container name.
   * @param blob_path Mutates to the blob path.
   * @return Status
   */
  Status parse_azure_uri(
      const URI& uri,
      std::string* container_name,
      std::string* blob_path) const;

  /**
   * Copies the blob at 'old_uri' to `new_uri`.
   *
   * @param old_uri The blob's current URI.
   * @param new_uri The blob's URI to move to.
   * @return Status
   */
  Status copy_blob(const URI& old_uri, const URI& new_uri);

  /**
   * Waits for a blob with `container_name` and `blob_path`
   * to exist on Azure.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's path
   * @return Status
   */
  Status wait_for_blob_to_propagate(
      const std::string& container_name, const std::string& blob_path) const;

  /**
   * Waits for a blob with `container_name` and `blob_path`
   * to not exist on Azure.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's path
   * @return Status
   */
  Status wait_for_blob_to_be_deleted(
      const std::string& container_name, const std::string& blob_path) const;

  /**
   * Waits for a container with `container_name`
   * to exist on Azure.
   *
   * @param container_name The container's name.
   * @return Status
   */
  Status wait_for_container_to_propagate(
      const std::string& container_name) const;

  /**
   * Waits for a container with `container_name`
   * to not exist on Azure.
   *
   * @param container_name The container's name.
   * @return Status
   */
  Status wait_for_container_to_be_deleted(
      const std::string& container_name) const;

  /**
   * Check if 'container_name' is a container on Azure.
   *
   * @param container_name The container's name.
   * @param is_container Mutates to the output.
   * @return Status
   */
  Status is_container(
      const std::string& container_name, bool* const is_container) const;

  /**
   * Check if 'is_blob' is a blob on Azure.
   *
   * @param container_name The blob's container name.
   * @param blob_path The blob's path.
   * @param is_blob Mutates to the output.
   * @return Status
   */
  Status is_blob(
      const std::string& container_name,
      const std::string& blob_path,
      bool* const is_blob) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_AZURE
#endif  // TILEDB_AZURE_H
