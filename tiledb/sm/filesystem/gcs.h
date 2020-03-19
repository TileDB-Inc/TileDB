/**
 * @file   gcs.h
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
 * This file defines the GCS class.
 */

#ifndef TILEDB_GCS_H
#define TILEDB_GCS_H

#ifdef HAVE_GCS

#include "google/cloud/storage/client.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

class GCS {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  GCS();

  /** Destructor. */
  ~GCS();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Initializes and connects a GCS client.
   *
   * @param config Configuration parameters.
   * @param thread_pool The parent VFS thread pool.
   * @return Status
   */
  Status init(const Config& config, ThreadPool* thread_pool);

  /**
   * Creates a bucket.
   *
   * @param uri The uri of the bucket to be created.
   * @return Status
   */
  Status create_bucket(const URI& uri) const;

  /** Removes the contents of a GCS bucket. */
  Status empty_bucket(const URI& uri) const;

  /**
   * Check if a bucket is empty.
   *
   * @param bucket The name of the bucket.
   * @param is_empty Mutates to `true` if the bucket is empty.
   * @return Status
   */
  Status is_empty_bucket(const URI& uri, bool* is_empty) const;

  /**
   * Check if a bucket exists.
   *
   * @param bucket The name of the bucket.
   * @param is_bucket Mutates to `true` if `uri` is a bucket.
   * @return Status
   */
  Status is_bucket(const URI& uri, bool* is_bucket) const;

  /**
   * Check if 'is_object' is a object on GCS.
   *
   * @param uri Uri of the object.
   * @param is_object Mutates to the output.
   * @return Status
   */
  Status is_object(const URI& uri, bool* const is_object) const;

  /**
   * Checks if there is an object with prefix `uri/`. For instance, suppose
   * the following objects exist:
   *
   * `gcs://some_gcs/foo/bar1`
   * `gcs://some_gcs/foo2`
   *
   * `is_dir(`gcs://some_gcs/foo`) and
   * `is_dir(`gcs://some_gcs/foo`) will both return `true`, whereas
   * `is_dir(`gcs://some_gcs/foo2`) will return `false`. This is because
   * the function will first convert the input to `gcs://some_gcs/foo2/`
   * (appending `/` in the end) and then check if there exists any object with
   * prefix `gcs://some_gcs/foo2/` (in this case there is not).
   *
   * @param uri The URI to check.
   * @param exists Sets it to `true` if the above mentioned condition holds.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* exists) const;

  /**
   * Deletes a bucket.
   *
   * @param uri The URI of the bucket to be deleted.
   * @return Status
   */
  Status remove_bucket(const URI& uri) const;

  /**
   * Deletes an object with a given URI.
   *
   * @param uri The URI of the object to be deleted.
   * @return Status
   */
  Status remove_object(const URI& uri) const;

  /**
   * Deletes all objects with prefix `uri/` (if the ending `/` does not
   * exist in `uri`, it is added by the function.
   *
   * For instance, suppose there exist the following objects:
   * - `gcs://some_gcs/foo/bar1`
   * - `gcs://some_gcs/foo/bar2/bar3
   * - `gcs://some_gcs/foo/bar4
   * - `gcs://some_gcs/foo2`
   *
   * `remove("gcs://some_gcs/foo")` and
   * `remove("gcs://some_gcs/foo/")` will delete objects:
   *
   * - `gcs://some_gcs/foo/bar1`
   * - `gcs://some_gcs/foo/bar2/bar3
   * - `gcs://some_gcs/foo/bar4
   *
   * In contrast, `remove("gcs://some_gcs/foo2")` will not delete
   * anything; the function internally appends `/` to the end of the URI, and
   * therefore there is not object with prefix "gcs://some_gcs/foo2/" in
   * this example.
   *
   * @param uri The prefix uri of the objects to be deleted.
   * @return Status
   */
  Status remove_dir(const URI& uri) const;

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
   * - `foo/bar`
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
   * Creates an empty object.
   *
   * @param uri The URI of the object to be created.
   * @return Status
   */
  Status touch(const URI& uri) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  // The GCS project id.
  std::string project_id_;

  // The GCS REST client.
  mutable google::cloud::StatusOr<google::cloud::storage::Client> client_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Parses a URI into a bucket name and object path. For example,
   * URI "gcs://my-bucket/dir1/file1" will parse into
   * `*bucket_name == "my-bucket"` and `*object_path == "dir1/file1"`.
   *
   * @param uri The URI to parse.
   * @param bucket_name Mutates to the bucket name.
   * @param object_path Mutates to the object path.
   * @return Status
   */
  Status parse_gcs_uri(
      const URI& uri, std::string* bucket_name, std::string* object_path) const;

  /**
   * Removes a leading slash from 'path' if it exists.
   *
   * @param path the string to remove the leading slash from.
   */
  std::string remove_front_slash(const std::string& path) const;

  /**
   * Adds a trailing slash from 'path' if it doesn't already have one.
   *
   * @param path the string to add the trailing slash to.
   */
  std::string add_trailing_slash(const std::string& path) const;

  /**
   * Removes a trailing slash from 'path' if it exists.
   *
   * @param path the string to remove the trailing slash from.
   */
  std::string remove_trailing_slash(const std::string& path) const;

  /**
   * Copies the object at 'old_uri' to `new_uri`.
   *
   * @param old_uri The object's current URI.
   * @param new_uri The object's URI to move to.
   * @return Status
   */
  Status copy_object(const URI& old_uri, const URI& new_uri);

  /**
   * Waits for a object with `bucket_name` and `object_path`
   * to exist on Azure.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path
   * @return Status
   */
  Status wait_for_object_to_propagate(
      const std::string& bucket_name, const std::string& object_path) const;

  /**
   * Waits for a object with `bucket_name` and `object_path`
   * to not exist on Azure.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path
   * @return Status
   */
  Status wait_for_object_to_be_deleted(
      const std::string& bucket_name, const std::string& object_path) const;

  /**
   * Waits for a bucket with `bucket_name`
   * to exist on Azure.
   *
   * @param bucket_name The bucket's name.
   * @return Status
   */
  Status wait_for_bucket_to_propagate(const std::string& bucket_name) const;

  /**
   * Waits for a bucket with `bucket_name`
   * to not exist on Azure.
   *
   * @param bucket_name The bucket's name.
   * @return Status
   */
  Status wait_for_bucket_to_be_deleted(const std::string& bucket_name) const;

  /**
   * Check if 'is_object' is a object on Azure.
   *
   * @param bucket_name The object's bucket name.
   * @param object_path The object's path.
   * @param is_object Mutates to the output.
   * @return Status
   */
  Status is_object(
      const std::string& bucket_name,
      const std::string& object_path,
      bool* const is_object) const;

  /**
   * Check if 'bucket_name' is a bucket on Azure.
   *
   * @param bucket_name The bucket's name.
   * @param is_bucket Mutates to the output.
   * @return Status
   */
  Status is_bucket(const std::string& bucket_name, bool* const is_bucket) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_GCS
#endif  // TILEDB_GCS_H
