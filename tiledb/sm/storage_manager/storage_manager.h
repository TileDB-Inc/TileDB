/**
 * @file   storage_manager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class StorageManager.
 */

#ifndef TILEDB_STORAGE_MANAGER_H
#define TILEDB_STORAGE_MANAGER_H

#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#ifdef HAVE_TBB
#include <tbb/task_scheduler_init.h>
#endif

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/cache/lru_cache.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/config.h"
#include "tiledb/sm/storage_manager/consolidator.h"
#include "tiledb/sm/storage_manager/open_array.h"

namespace tiledb {
namespace sm {

/** The storage manager that manages pretty much everything in TileDB. */
class StorageManager {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Enables iteration over TileDB objects in a path. */
  class ObjectIter {
   public:
    /**
     * There is a one-to-one correspondence between `expanded_` and `objs_`.
     * An `expanded_` value is `true` if the corresponding `objs_` path
     * has been expanded to the paths it contains in a post ored traversal.
     * This is not used in a preorder traversal.
     */
    std::list<bool> expanded_;
    /** The next URI in string format. */
    std::string next_;
    /** The next objects to be visited. */
    std::list<URI> objs_;
    /** The traversal order of the iterator. */
    WalkOrder order_;
    /** `True` if the iterator will recursively visit the directory tree. */
    bool recursive_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StorageManager();

  /** Destructor. */
  ~StorageManager();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Closes an array. */
  Status array_close(const URI& array_uri);

  /**
   * Opens an array, retrieving its schema and fragment metadata. If the
   * array is exclusively locked with `array_xlock`, this function will
   * wait until the array is unlocked with `array_xunlock`.
   *
   * @param array_uri The array URI.
   * @param open_array The open array object to be retrieved.
   * @return Status
   */
  Status array_open(const URI& array_uri, OpenArray** open_array);

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes.
   *
   * @param open_array The opened array.
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param attributes The attributes to focus on.
   * @param attribute_num The number of attributes.
   * @param buffer_sizes The buffer sizes to be retrieved. Note that one
   *     buffer size corresponds to a fixed-sized attributes, and two
   *     buffer sizes for a variable-sized attribute (the first is the
   *     size of the offsets, whereas the second is the size of the
   *     actual variable-sized cell values.
   * @return Status
   */
  Status array_compute_max_read_buffer_sizes(
      OpenArray* open_array,
      const void* subarray,
      const char** attributes,
      unsigned attribute_num,
      uint64_t* buffer_sizes);

  /**
   * Computes the partitions a given subarray must be decomposed into, given
   * buffer size budgets for a set of attributes.
   *
   * @param open_array The open array.
   * @param subarray The subarray.
   * @param layout The layout in which the results are retrieved in the
   * subarray.
   * @param attributes The attributes.
   * @param attribute_num The number of attributes.
   * @param buffer_sizes The buffer size budgets. There is one buffer size per
   *     fixed-sized attribute, and two for var-sized attributes (the first is
   *     for the offsets, the second for the actual values).
   * @param subarray_partitions The subarray partitions to be retrieved.
   * @param npartitions The number of the retrieved partitions.
   * @return Status
   *
   * @note The user is responsible for freeing `subarray_partitions`.
   */
  Status array_compute_subarray_partitions(
      OpenArray* open_array,
      const void* subarray,
      Layout layout,
      const char** attributes,
      unsigned attribute_num,
      const uint64_t* buffer_sizes,
      void*** subarray_partitions,
      uint64_t* npartitions);

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes.
   *
   * @param array_schema The array schema
   * @param fragment_metadata The fragment metadata of the array.
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param buffer_sizes The buffer sizes to be retrieved. This is a map
   *     from an attribute to a size pair. For fixed-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first size
   *     is the offsets size, and the second size is the values size.
   * @return Status
   */
  Status array_compute_max_read_buffer_sizes(
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata,
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Consolidates the fragments of an array into a single one.
   *
   * @param array_name The name of the array to be consolidated.
   * @return Status
   */
  Status array_consolidate(const char* array_name);

  /**
   * Creates a TileDB array storing its schema.
   *
   * @param array_uri The URI of the array to be created.
   * @param array_schema The array schema.
   * @return Status
   */
  Status array_create(const URI& array_uri, ArraySchema* array_schema);

  /**
   * Retrieves the non-empty domain from an array. This is the union of the
   * non-empty domains of the array fragments.
   *
   * @param open_array An open array object (must be already open).
   * @param domain The domain to be retrieved.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain(
      OpenArray* open_array, void* domain, bool* is_empty);

  /**
   * Exclusively locks an array. This function will wait on the array to
   * be closed if it is already open. After an array is locked in this mode,
   * any attempt to open an array will have to wait until the array is
   * unlocked with `xunlock_array`.
   *
   * An array is exclusively locked only for a short time upon consolidation,
   * during removing the old fragments that got consolidated.
   */
  Status array_xlock(const URI& array_uri);

  /** Releases an exclusive lock for the input array. */
  Status array_xunlock(const URI& array_uri);

  /**
   * Pushes an async query to the queue.
   *
   * @param query The async query.
   * @return Status
   */
  Status async_push_query(Query* query);

  /** Cancels all background tasks. */
  Status cancel_all_tasks();

  /** Returns true while all tasks are being cancelled. */
  bool cancellation_in_progress();

  /** Returns the configuration parameters. */
  Config config() const;

  /** Creates a directory with the input URI. */
  Status create_dir(const URI& uri);

  /** Creates an empty file with the input URI. */
  Status touch(const URI& uri);

  /** Deletes a fragment directory. */
  Status delete_fragment(const URI& uri) const;

  /**
   * Creates a TileDB group.
   *
   * @param group The URI of the group to be created.
   * @return Status
   */
  Status group_create(const std::string& group);

  /**
   * Initializes the storage manager.
   *
   * @param config The configuration parameters.
   * @return Status
   */
  Status init(Config* config);

  /**
   * Checks if the input URI represents an array.
   *
   * @param The URI to be checked.
   * @param is_array Set to `true` if the URI is an array and `false` otherwise.
   * @return Status
   */
  Status is_array(const URI& uri, bool* is_array) const;

  /**
   * Checks if the input URI represents a directory.
   *
   * @param The URI to be checked.
   * @param is_dir Set to `true` if the URI is a directory and `false`
   *     otherwise.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* is_dir) const;

  /**
   * Checks if the input URI represents a fragment.
   *
   * @param The URI to be checked.
   * @param is_fragment Set to `true` if the URI is a fragment and `false`
   *     otherwise.
   * @return Status
   */
  Status is_fragment(const URI& uri, bool* is_fragment) const;

  /**
   * Checks if the input URI represents a group.
   *
   * @param The URI to be checked.
   * @param is_group Set to `true` if the URI is a group and `false`
   *     otherwise.
   * @return Status
   */
  Status is_group(const URI& uri, bool* is_group) const;

  /**
   * Checks if the input URI represents a file.
   *
   * @param The URI to be checked.
   * @param is_file Set to `true` if the URI is a file and `false`
   *     otherwise.
   * @return Status
   */
  Status is_file(const URI& uri, bool* is_file) const;

  /**
   * Checks if the input URI represents a key-value store.
   *
   * @param The URI to be checked.
   * @param is_kv Set to `true` if the URI is a key-value store and `false`
   *     otherwise.
   * @return Status
   */
  Status is_kv(const URI& uri, bool* is_kv) const;

  /**
   * Loads the schema of an array from persistent storage into memory.
   *
   * @param array_uri The URI path of the array.
   * @param object_type This is either ARRAY or KEY_VALUE.
   * @param array_schema The array schema to be retrieved.
   * @return Status
   */
  Status load_array_schema(
      const URI& array_uri, ObjectType object_type, ArraySchema** array_schema);

  /**
   * Loads the fragment metadata of an array from persistent storage into
   * memory.
   *
   * @param metadata The fragment metadata to be loaded.
   * @return Status
   */
  Status load_fragment_metadata(FragmentMetadata* metadata);

  /** Removes a TileDB object (group, array, kv). */
  Status object_remove(const char* path) const;

  /**
   * Renames a TileDB object (group, array, kv). If
   * `new_path` exists, `new_path` will be overwritten.
   */
  Status object_move(const char* old_path, const char* new_path) const;

  /**
   * Creates a new object iterator for the input path. The iteration
   * in this case will be recursive in the entire directory tree rooted
   * at `path`.
   *
   * @param obj_iter The object iterator to be created (memory is allocated for
   *     it by the function).
   * @param path The path the iterator will target at.
   * @param order The traversal order of the iterator.
   * @return Status
   */
  Status object_iter_begin(
      ObjectIter** obj_iter, const char* path, WalkOrder order);

  /**
   * Creates a new object iterator for the input path. The iteration will
   * not be recursive, and only the children of `path` will be visited.
   *
   * @param obj_iter The object iterator to be created (memory is allocated for
   *     it by the function).
   * @param path The path the iterator will target at.
   * @return Status
   */
  Status object_iter_begin(ObjectIter** obj_iter, const char* path);

  /** Frees the object iterator. */
  void object_iter_free(ObjectIter* obj_iter);

  /**
   * Retrieves the next object path and type.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Retrieves the next object in the post-order traversal.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next_postorder(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Retrieves the next object in the post-order traversal.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next_preorder(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Returns the tiledb object type
   *
   * @param uri Path to TileDB object resource
   * @param type The ObjectType to be retrieved.
   * @return Status
   */
  Status object_type(const URI& uri, ObjectType* type) const;

  /**
   * Creates a query.
   *
   * @param query The query to initialize.
   * @param open_array An opened array.
   * @param type The query type.
   * @param fragment_uri This is applicable only to write queries. This is
   *     to indicate that the new fragment created by a write will have
   *     a specific URI. This is useful mainly in consolidation, where
   *     the consolidated fragment URI must be explicitly created by
   *     the consolidator.
   * @return Status
   */
  Status query_create(
      Query** query,
      OpenArray* open_array,
      QueryType type,
      URI fragment_uri = URI(""));

  /** Submits a query for (sync) execution. */
  Status query_submit(Query* query);

  /**
   * Submits a query for async execution.
   *
   * @param query The query to submit.
   * @param callback The fuction that will be called upon query completion.
   * @param callback_data The data to be provided to the callback function.
   * @return Status
   */
  Status query_submit_async(
      Query* query, std::function<void(void*)> callback, void* callback_data);

  /**
   * Reads from the cache into the input buffer. `uri` and `offset` collectively
   * form the key of the cached object to be read. Essentially, this is used
   * to read potentially cached tiles. `uri` is the URI of the attribute the
   * tile belongs to, and `offset` is the offset in the attribute file where
   * the tile is located. Observe that the `uri`, `offset` pair is unique.
   *
   * @param uri The URI of the cached object.
   * @param offset The offset of the cached object.
   * @param buffer The buffer to write into. The function reallocates memory
   *     for the buffer, sets its size to *nbytes* and resets its offset.
   * @param nbytes Number of bytes to be read.
   * @param in_cache This is set to `true` if the object is in the cache,
   *     and `false` otherwise.
   * @return Status.
   */
  Status read_from_cache(
      const URI& uri,
      uint64_t offset,
      Buffer* buffer,
      uint64_t nbytes,
      bool* in_cache) const;

  /**
   * Reads from a file into the input buffer.
   *
   * @param uri The URI file to read from.
   * @param offset The offset in the file the read will start from.
   * @param buffer The buffer to write into. The function reallocates memory
   *     for the buffer, sets its size to *nbytes* and resets its offset.
   * @param nbytes The number of bytes to read.
   * @return Status.
   */
  Status read(
      const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const;

  /**
   * Stores an array schema into persistent storage.
   *
   * @param array_schema The array metadata to be stored.
   * @return Status
   */
  Status store_array_schema(ArraySchema* array_schema);

  /**
   * Stores the fragment metadata into persistent storage.
   *
   * @param metadata The fragment metadata to be stored.
   * @return Status
   */
  Status store_fragment_metadata(FragmentMetadata* metadata);

  /** Closes a file, flushing its contents to persistent storage. */
  Status close_file(const URI& uri);

  /** Syncs a file or directory, flushing its contents to persistent storage. */
  Status sync(const URI& uri);

  /** Returns the virtual filesystem object. */
  VFS* vfs() const;

  /**
   * Writes the contents of a buffer into the cache. `uri` and `offset`
   * collectively form the key of the object to be cached. Essentially, this is
   * used to cach tiles. `uri` is the URI of the attribute the
   * tile belongs to, and `offset` is the offset in the attribute file where
   * the tile is located. Observe that the `uri`, `offset` pair is unique.
   *
   * @param uri The URI of the cached object.
   * @param offset The offset of the cached object.
   * @param buffer The buffer whose contents will be cached.
   * @return Status.
   */
  Status write_to_cache(const URI& uri, uint64_t offset, Buffer* buffer) const;

  /**
   * Writes the contents of a buffer into a URI file.
   *
   * @param uri The file to write into.
   * @param buffer The buffer to write.
   * @return Status.
   */
  Status write(const URI& uri, Buffer* buffer) const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** An array schema cache. */
  LRUCache* array_schema_cache_;

  /** Set to true when tasks are being cancelled. */
  bool cancellation_in_progress_;

  /** Mutex protecting cancellation_in_progress_. */
  std::mutex cancellation_in_progress_mtx_;

  /**
   * The condition variable for exlcusively locking arrays. This is used
   * to wait for an array to be closed, before being exclusively locked
   * by `array_xlock`.
   */
  std::condition_variable xlock_cv_;

  /** Mutex for providing thread-safety upon creating TileDB objects. */
  std::mutex object_create_mtx_;

  /** Stores the TileDB configuration parameters. */
  Config config_;

  /** Object that handles array consolidation. */
  Consolidator* consolidator_;

  /** Stores exclusive filelocks for arrays. */
  std::unordered_map<std::string, filelock_t> xfilelocks_;

  /** A fragment metadata cache. */
  LRUCache* fragment_metadata_cache_;

  /**
   * Mutex for managing OpenArray objects. Its purpose is threefold:
   *
   * - Protect `open_arrays_` during `array_{open, close} and
   *   `array_{xlock, xunlock}`.
   * - Protect `xfilelocks_` during `array_{xlock, xunlock}
   * - Used by conditional variable `xlock_cv_`.
   */
  std::mutex open_array_mtx_;

  /** Stores the currently open arrays. */
  std::map<std::string, OpenArray*> open_arrays_;

  /** Count of the number of queries currently in progress. */
  uint64_t queries_in_progress_;

  /** Guards queries_in_progress_ counter. */
  std::mutex queries_in_progress_mtx_;

  /** Guards queries_in_progress_ counter. */
  std::condition_variable queries_in_progress_cv_;

  /** The storage manager's thread pool for async queries. */
  std::unique_ptr<ThreadPool> async_thread_pool_;

#ifdef HAVE_TBB
  /** The TBB scheduler, used for controlling the number of TBB threads. */
  std::unique_ptr<tbb::task_scheduler_init> tbb_scheduler_;
#endif

  /** A tile cache. */
  LRUCache* tile_cache_;

  /**
   * Virtual filesystem handler. It directs queries to the appropriate
   * filesystem backend. Note that this is stateful.
   */
  VFS* vfs_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Retrieves the non-empty domain from the input fragment metadata. This is
   * the union of the non-empty domains of the fragments.
   *
   * @param metadata The metadata of all fragments in the array.
   * @param dim_num The number of dimensions in the domain.
   * @param domain The domain to be retrieved.
   * @return void
   */
  template <class T>
  void array_get_non_empty_domain(
      const std::vector<FragmentMetadata*>& metadata,
      unsigned dim_num,
      T* domain);

  /** Decrement the count of in-progress queries. */
  void decrement_in_progress();

  /** Retrieves all the fragment URI's of an array. */
  Status get_fragment_uris(
      const URI& array_uri, std::vector<URI>* fragment_uris) const;

  /** Increment the count of in-progress queries. */
  void increment_in_progress();

  /**
   * Configures the TBB runtime. If TBB is not enabled, does nothing.
   *
   * @param config The configuration parameters
   * @return Status
   */
  Status init_tbb(Config::SMParams& config);

  /**
   * Loads the array schema into an open array.
   *
   * @param array_uri The array URI.
   * @param object_type This is either ARRAY or KEY_VALUE.
   * @param open_array The open array object.
   * @return Status
   */
  Status load_array_schema(
      const URI& array_uri, ObjectType object_type, OpenArray* open_array);

  /** Retrieves the fragment metadata of an open array for a given subarray. */
  Status load_fragment_metadata(OpenArray* open_array);

  /**
   * Sorts the input fragment URIs in ascending timestamp order, breaking
   * ties using the process id.
   */
  void sort_fragment_uris(std::vector<URI>* fragment_uris) const;

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes.
   *
   * @tparam T The domain type
   * @param array_schema The array schema
   * @param fragment_metadata The fragment metadata of the array.
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param buffer_sizes The buffer sizes to be retrieved. This is a map
   *     from an attribute to a size pair. For fixed-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first size
   *     is the offsets size, and the second size is the values size.
   * @return Status
   */
  template <class T>
  Status array_compute_max_read_buffer_sizes(
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata,
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /** Block until there are zero in-progress queries. */
  void wait_for_zero_in_progress();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_H
