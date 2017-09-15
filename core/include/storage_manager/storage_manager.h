/**
 * @file   storage_manager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "array_schema.h"
#include "open_array.h"
#include "query.h"
#include "status.h"
#include "uri.h"
#include "vfs.h"

namespace tiledb {

/**
 * The storage manager that manages pretty much everything in TileDB.
 */
class StorageManager {
 public:
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

  /**
   * Creates a TileDB array storing its schema.
   *
   * @param array_schema The array schema.
   * @return Status
   */
  Status array_create(ArraySchema* array_schema);

  /**
   * Creates a TileDB group.
   *
   * @param group The URI of the group to be created.
   * @return Status
   */
  Status group_create(const std::string& group) const;

  /**
   * Initializes the storage manager. It spawns two threads. The first is for
   * handling user asynchronous queries (submitted via the *query_submit_async*
   * function. The second handles internal asynchronous queries as part of some
   * either sync or async query.
   *
   * @return Status
   */
  Status init();

  /**
   * Loads the schema of an array from persistent storage into memory.
   *
   * @param array_name The name (path) of the array.
   * @param array_schema The array schema to be retrieved.
   * @return Status
   */
  Status load(const std::string& array_name, ArraySchema* array_schema);

  /**
   * Stores an array schema into persistent storage.
   *
   * @param array_schema The array schema to be stored.
   * @return Status
   */
  Status store(ArraySchema* array_schema);

  Status sync(const URI& uri);

  Status load(FragmentMetadata* metadata);

  Status store(FragmentMetadata* metadata);

  bool is_dir(const URI& uri);

  bool is_file(const URI& uri);

  Status create_file(const URI& uri);

  Status create_dir(const URI& uri);

  Status delete_file(const URI& uri) const;

  Status move_dir(const URI& old_uri, const URI& new_uri);

  /**
   * Pushes an async query to the queue.
   *
   * @param query The async query.
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user query, whereas if it is 1 it means an
   * internal query.
   * @return Status
   */
  Status async_push_query(Query* query, int i);

  Status query_init(
      Query* query,
      const char* array_name,
      QueryType query_type,
      const void* subarray,
      const char** attributes,
      int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  Status query_submit(Query* query);

  Status query_submit_async(
      Query* query, void* (*callback)(void*), void* callback_data);

  Status file_size(const URI& uri, uint64_t* size) const;

  Status read_from_file(
      const URI& uri, uint64_t offset, void* buffer, uint64_t length) const;

  Status write_to_file(
      const URI& uri, const void* buffer, uint64_t buffer_size) const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /**
   * Async condition variable. The first is for user async queries, the second
   * for internal async queries.
   * */
  std::condition_variable async_cv_[2];

  /** If true, the async thread will be eventually terminated. */
  bool async_done_;

  /**
   * Async query queue. The first is for user queries, the second for
   * internal queries. The queries are processed in a FIFO manner.
   */
  std::queue<Query*> async_queue_[2];

  /**
   * Async mutex. The first is for the user queries thread, the second for
   * the internal queries thread.
   */
  std::mutex async_mtx_[2];

  /**
   * Threads that handle all async queries. The first is for user queries,
   * the second for internal queries.
   */
  std::thread* async_thread_[2];

  /** Mutex for managing OpenArray objects. */
  std::mutex open_array_mtx_;

  /**
   * Stores the currently open arrays. An array is *opened* when a new query is
   * initialized via *query_init* for a particular array.
   */
  std::map<std::string, OpenArray*> open_arrays_;

  /**
   * Virtual filesystem handler. It directs queries to the appropriate
   * filesystem backend. Note that this is stateful.
   */
  VFS* vfs_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Starts listening to async queries.
   *
   * @param storage_manager The storage manager object that handles the
   *     async query threads.
   * @param i The index of the thread to execute the function. If it is
   *     equal to 0, it means a user query, whereas if it is 1 it means an
   *     internal query.
   */
  static void async_start(StorageManager* storage_manager, int i = 0);

  /** Stops the async query. */
  void async_stop();

  Status array_close(
      const URI& array,
      const std::vector<FragmentMetadata*>& fragment_metadata);

  Status array_open(
      const URI& array_uri,
      QueryType query_type,
      const void* subarray,
      const ArraySchema** array_schema,
      std::vector<FragmentMetadata*>* fragment_metadata);

  Status array_open_error(
      OpenArray* open_array,
      const std::vector<FragmentMetadata*>& fragment_metadata);

  Status open_array_get_entry(const URI& array_uri, OpenArray** open_array);

  Status open_array_load_schema(const URI& array_uri, OpenArray* open_array);

  Status open_array_load_fragment_metadata(
      OpenArray* open_array,
      const void* subarray,
      std::vector<FragmentMetadata*>* fragment_metadata);

  Status get_fragment_uris(
      const URI& array_uri,
      const void* subarray,
      std::vector<URI>* fragment_uris) const;

  void sort_fragment_uris(std::vector<URI>* fragment_uris) const;

  /** Handles a single async query. */
  void async_process_query(Query* query);

  /**
   * Starts handling async queries.
   *
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user query, whereas if it is 1 it means an
   * internal query.
   */
  void async_process_queries(int i);
};

}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_H

/* TODO
Status consolidation_finalize(
    Fragment* new_fragment, const std::vector<URI>& old_fragment_names);
Status group_clear(const URI& group) const;
Status group_delete(const URI& group) const;
Status group_move(const URI& old_group, const URI& new_group) const;
Status array_clear(const URI& array_schema) const;
Status array_delete(const URI& array_schema) const;
Status array_move(const URI& old_array, const URI& new_array) const;
*/

/**
 * Consolidates the fragments of an array into a single fragment.
 *
 * @param array_uri The identifier of the array to be consolidated.
 * @return Status
 */
//  TODO Status array_consolidate(const URI& array_schema);

/* ********************************* */
/*               MISC                */
/* ********************************* */

/**
 * Returns the type of the input direc
 *
 * @param dir The input directory.
 * @return It can be one of the following:
 *    - TILEDB_GROUP
 *    - TILEDB_ARRAY
 *    - TILEDB_METADATA
 *    - -1 (not a TileDB directory)
 */
// TODO int dir_type(const char* dir);

/**
 * Lists all the TileDB objects in a path, copying them into the input
 * buffers.
 *
 * @param parent_path The parent path of the TileDB objects to be listed.
 * @param object_paths An array of strings that will store the listed TileDB
 * objects. Note that the user is responsible for allocating the appropriate
 * memory space for this array of strings. A good idea for each string
 * length is to set is to TILEDB_NAME_MAX_LEN.
 * @param object_types The types of the corresponding TileDB objects, which
 * can be the following (they are self-explanatory): - TILEDB_GROUP -
 * TILEDB_ARRAY - TILEDB_METADATA
 * @param object_num The number of elements allocated by the user for
 * *object_paths*. After the function terminates, this will hold the actual
 * number of TileDB objects that were stored in *object_paths*.
 * @return Status
 */
/* TODO
  Status ls(
      const URI& parent_uri,
      char** object_paths,
      tiledb_object_t* object_types,
      int* num_objects) const;
      */

/**
 * Counts the TileDB objects in path
 *
 * @param parent_path The parent path of the TileDB objects to be listed.
 * @param object_num The number of TileDB objects to be returned.
 * @return Status
 */
// TODO Status ls_c(const URI& parent_uri, int* object_num) const;

/**
 * Clears a TileDB directory. The corresponding TileDB object
 * (group, array) will still exist after the execution of the
 * function, but it will be empty (i.e., as if it was just created).
 *
 * @param dir The directory to be cleared.
 * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
 */
// TODO Status clear(const URI& uri) const;

/**
 * Deletes a TileDB directory (group, array) entirely.
 *
 * @param dir The directory to be deleted.
 * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
 */
// TODO Status delete_entire(const URI& uri);

/**
 * Moves a TileDB directory (group, array).
 *
 * @param old_dir The old directory.
 * @param new_dir The new directory.
 * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
 */
// TODO Status move(const URI& old_uri, const URI& new_uri);
