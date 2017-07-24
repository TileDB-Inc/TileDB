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

#include "array.h"
#include "array_iterator.h"
#include "array_schema.h"
#include "configurator.h"
#include "lock_type.h"
#include "metadata.h"
#include "metadata_iterator.h"
#include "query.h"
#include "status.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif

namespace tiledb {

/**
 * The storage manager, which is repsonsible for creating, deleting, etc. of
 * TileDB objects (i.e., groups, arrays and metadata).
 */
class StorageManager {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** Implements an open array entry. */
  class OpenArray {
   public:
    OpenArray() {
      array_schema_ = nullptr;
      array_filelock_ = -1;
    }
    ~OpenArray() {
      if (array_schema_ != nullptr)
        delete array_schema_;
      for (auto& bk : bookkeeping_)
        if (bk != nullptr)
          delete bk;
    }

    /** Descriptor for the array filelock. */
    int array_filelock_;
    /** The array schema. */
    ArraySchema* array_schema_;
    /** The bookkeeping structures for all the fragments of the array. */
    std::vector<Bookkeeping*> bookkeeping_;
    /**
     * A counter for the number of times the array has been initialized after
     * it was opened.
     */
    int cnt_;
    /** The names of the fragments of the open array. */
    std::vector<std::string> fragment_names_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StorageManager();

  /** Destructor. */
  ~StorageManager();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Creates a new TileDB array.
   *
   * @param array_schema The array schema.
   * @return Status
   */
  Status array_create(ArraySchema* array_schema) const;

  Status array_close(Array* array);

  Status array_consolidate(const char* array);

  Status array_open(const std::string& name, Array** array);

  /** Sets a new configurator. */
  void config_set(Configurator* config);

  /**
   * Creates a new TileDB group.
   *
   * @param group The directory of the group to be created in the file system.
   * @return Status
   */
  Status group_create(const std::string& group) const;

  Status query_process(Query* query);

  Status query_sync(Query* query);

  Status init();

  /* ********************************* */
  /*              GROUP                */
  /* ********************************* */

  /* ********************************* */
  /*           BASIC ARRAY             */
  /* ********************************* */

  /**
   * Creates a new TileDB basic array.
   *
   * @param name The basic array name.
   * @return Status
   */
  //  Status basic_array_create(const char* name) const;

  /* ********************************* */
  /*              ARRAY                */
  /* ********************************* */

  /**
   * Consolidates the fragments of an array into a single fragment.
   *
   * @param array_dir The name of the array to be consolidated.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_consolidate(const char* array_dir);

  /**
   * Initializes a TileDB array.
   *
   * @param array The array object to be initialized. The function
   *     will allocate memory space for it.
   * @param array_dir The directory of the array to be initialized.
   * @param mode The mode of the array. It must be one of the following:
   *    - TILEDB_ARRAY_WRITE
   *    - TILEDB_ARRAY_WRITE_UNSORTED
   *    - TILEDB_ARRAY_READ
   *    - TILEDB_ARRAY_READ_SORTED_COL
   *    - TILEDB_ARRAY_READ_SORTED_ROW
   * @param subarray The subarray in which the array read/write will be
   *     constrained on. If it is NULL, then the subarray is set to the entire
   *     array domain. For the case of writes, this is meaningful only for
   *     dense arrays, and specifically dense writes.
   * @param attributes A subset of the array attributes the read/write will be
   *     constrained on. A NULL value indicates **all** attributes (including
   *     the coordinates in the case of sparse arrays).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_init(
  //      Array*& array,
  //      const char* array_dir,
  //      ArrayMode mode,
  //      const void* subarray,
  //      const char** attributes,
  //      int attribute_num);

  /**
   * Finalizes an array, properly freeing the memory space.
   *
   * @param array The array to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_finalize(Array* array);

  /**
   * Syncs all currently written files in the input array.
   *
   * @param array The array to be synced.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_sync(Array* array);

  /**
   * Syncs the currently written files associated with the input attribute
   * in the input array.
   *
   * @param array The array to be synced.
   * @param attribute The name of the attribute to be synced.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_sync_attribute(Array* array, const std::string& attribute);

  /**
   * Initializes an array iterator for reading cells, potentially constraining
   * it on a subset of attributes, as well as a subarray. The cells will be read
   * in the order they are stored on the disk, maximing performance.
   *
   * @param array_it The TileDB array iterator to be created. The
   *    function will allocate the appropriate memory space for the iterator.
   * @param array The directory of the array the iterator is initialized for.
   * @param mode The read mode, which can be one of the following:
   *    - TILEDB_ARRAY_READ\n
   *      Reads the cells in the native order they are stored on the disk.
   *    - TILEDB_ARRAY_READ_SORTED_COL\n
   *      Reads the cells in column-major order within the specified subarray.
   *    - TILEDB_ARRAY_READ_SORTED_ROW\n
   *      Reads the cells in column-major order within the specified subarray.
   * @param subarray The subarray in which the array iterator will be
   *     constrained on. If it is NULL, then the subarray is set to the entire
   *     array domain.
   * @param attributes A subset of the array attributes the iterator will be
   *     constrained on. A NULL value indicates **all** attributes (including
   *     the coordinates in the case of sparse arrays).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @param buffers This is an array of buffers similar to tiledb_array_read.
   *     It is the user that allocates and provides buffers that the iterator
   *     will use for internal buffering of the read cells. The iterator will
   *     read from the disk the relevant cells in batches, by fitting as many
   *     cell values as possible in the user buffers. This gives the user the
   *     flexibility to control the prefetching for optimizing performance
   *     depending on the application.
   * @param buffer_sizes The corresponding sizes (in bytes) of the allocated
   *     memory space for *buffers*. The function will prefetch from the
   *     disk as many cells as can fit in the buffers, whenever it finishes
   *     iterating over the previously prefetched data.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_iterator_init(
  //      ArrayIterator*& array_it,
  //      const char* array,
  //      ArrayMode mode,
  //      const void* subarray,
  //      const char** attributes,
  //      int attribute_num,
  //      void** buffers,
  //      size_t* buffer_sizes);

  /**
   * Finalizes an array iterator, properly freeing the allocating memory space.
   *
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status array_iterator_finalize(ArrayIterator* array_it);

  /* ********************************* */
  /*              METADATA             */
  /* ********************************* */

  /**
   * Consolidates the fragments of a metadata object into a single fragment.
   *
   * @param metadata_dir The name of the metadata to be consolidated.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status metadata_consolidate(const char* metadata_dir);

  /**
   * Creates a new TileDB metadata object.
   *
   * @param metadata_schema The metadata schema.
   * @return Status.
   */
  //  Status metadata_create(MetadataSchema* metadata_schema) const;

  /**
   * Loads the schema of a metadata object from the disk, allocating appropriate
   * memory space for it.
   *
   * @param metadata_dir The directory of the metadata.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status metadata_load_schema(
  //      const char* metadata_dir, ArraySchema*& array_schema) const;

  /**
   * Initializes a TileDB metadata object.
   *
   * @param metadata The metadata object to be initialized. The function
   *     will allocate memory space for it.
   * @param metadata_dir The directory of the metadata.
   * @param mode The mode of the metadata. It must be one of the following:
   *    - TILEDB_METADATA_WRITE
   *    - TILEDB_METADATA_READ
   * @param attributes A subset of the metadata attributes the read/write will
   *     be constrained on. A NULL value indicates **all** attributes (including
   *     the key as an extra attribute in the end).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status metadata_init(
  //      Metadata*& metadata,
  //      const char* metadata_dir,
  //      tiledb_metadata_mode_t mode,
  //      const char** attributes,
  //      int attribute_num);

  /**
   * Finalizes a TileDB metadata object, properly freeing the memory space.
   *
   * @param metadata The metadata to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status metadata_finalize(Metadata* metadata);

  /**
   * Initializes a metadata iterator, potentially constraining it
   * on a subset of attributes. The values will be read in the order they are
   * stored on the disk, maximing performance.
   *
   * @param metadata_it The TileDB metadata iterator to be created. The
   *     function will allocate the appropriate memory space for the iterator.
   * @param metadata_dir The directory of the metadata the iterator is
   *     initialized for.
   * @param attributes A subset of the metadata attributes the iterator will be
   *     constrained on. A NULL value indicates **all** attributes (including
   *     the key as an extra attribute in the end).
   * @param attribute_num The number of the input attributes. If *attributes* is
   *     NULL, then this should be set to 0.
   * @param buffers This is an array of buffers similar to tiledb_metadata_read.
   *     It is the user that allocates and provides buffers that the iterator
   *     will use for internal buffering of the read values. The iterator will
   *     read from the disk the values in batches, by fitting as many
   *     values as possible in the user buffers. This gives the user the
   *     flexibility to control the prefetching for optimizing performance
   *     depending on the application.
   * @param buffer_sizes The corresponding sizes (in bytes) of the allocated
   *     memory space for *buffers*. The function will prefetch from the
   *     disk as many cells as can fit in the buffers, whenever it finishes
   *     iterating over the previously prefetched data.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status metadata_iterator_init(
  //      MetadataIterator*& metadata_it,
  //      const char* metadata_dir,
  //      const char** attributes,
  //      int attribute_num,
  //      void** buffers,
  //      size_t* buffer_sizes);

  /**
   * Finalizes the iterator, properly freeing the allocating memory space.
   *
   * @param metadata_it The TileDB metadata iterator.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  //  Status metadata_iterator_finalize(MetadataIterator* metadata_it);

  /* ********************************* */
  /*               MISC                */
  /* ********************************* */

  /**
   * Returns the type of the input directory.
   *
   * @param dir The input directory.
   * @return It can be one of the following:
   *    - TILEDB_GROUP
   *    - TILEDB_ARRAY
   *    - TILEDB_METADATA
   *    - -1 (not a TileDB directory)
   */
  //  int dir_type(const char* dir);

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
  //  Status ls(
  //      const char* parent_dir,
  //      char** object_paths,
  //      tiledb_object_t* object_types,
  //      int* num_objects) const;

  /**
   * Counts the TileDB objects in path
   *
   * @param parent_path The parent path of the TileDB objects to be listed.
   * @param object_num The number of TileDB objects to be returned.
   * @return Status
   */
  //  Status ls_c(const char* parent_path, int* object_num) const;

  /**
   * Clears a TileDB directory. The corresponding TileDB object
   * (group, array, or metadata) will still exist after the execution of the
   * function, but it will be empty (i.e., as if it was just created).
   *
   * @param dir The directory to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status clear(const std::string& dir) const;

  /**
   * Deletes a TileDB directory (group, array, or metadata) entirely.
   *
   * @param dir The directory to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status delete_entire(const std::string& dir);

  /**
   * Moves a TileDB directory (group, array or metadata).
   *
   * @param old_dir The old directory.
   * @param new_dir The new directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status move(const std::string& old_dir, const std::string& new_dir);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /**
   * Async condition variable. The first is for user queries, the second for
   * internal queries.
   * */
  std::condition_variable async_cv_[2];

  /** If true, the async thread will be eventually terminated. */
  bool async_done_;

  /**
   * Mutex for async queries. The first is for user queries, the second for
   * internal queries.
   */
  std::mutex async_mtx_[2];

  /**
   * Async request queue. The first is for user queries, the second for
   * internal queries.
   */
  std::queue<Query*> async_queue_[2];

  /**
   * Thread that handles all user asynchronous queries. The first is for user
   * queries, the second for internal queries.
   */
  std::thread* async_thread_[2];

  /** The TileDB configuration parameters. */
  Configurator* config_;

  /** Mutex for creating/deleting an OpenArray object. */
  std::mutex open_array_mtx_;

  /** Stores the currently open arrays. */
  std::map<std::string, OpenArray*> open_arrays_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  Status array_filelock_create(const std::string& dir) const;

  Status array_lock(
      const std::string& array_name, int* fd, LockType lock_type) const;

  Status array_unlock(int fd) const;

  void async_query_process(Query* query);

  void async_process_queries(int i);

  Status async_enqueue_query(Query* query, int i = 0);

  /**
   * Starts listening to asynchronous queries.
   *
   * @param storage_manager The storage manager object that handles the
   * asynchronous queries.
   * @param i The index of the thread to execute the function. If it is
   * equal to 0, it means user queries, whereas if it is 1 it means
   * internal queries.
   */
  static void async_start(StorageManager* storage_manager, int i = 0);

  /** Stops listening to asynchronous queries. */
  void async_stop();

  Status load_bookkeeping(
      const ArraySchema* array_schema,
      const std::vector<std::string>& fragment_names,
      std::vector<Bookkeeping*>& bookkeeping) const;

  Status load_fragment_names(
      const std::string& array, std::vector<std::string>& fragment_names) const;

  Status open_array_get(const std::string& array, OpenArray** open_array);

  Status open_array_new(const std::string& array, OpenArray** open_array);

  /** Handles a single AIO request. */
  //  void aio_handle_request(AIORequest* aio_request);

  /**
   * Starts handling AIO requests.
   *
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user AIO, whereas if it is 1 it means an
   * internal AIO.
   */
  //  void aio_handle_requests(int i);

  /**
   * Pushes an AIO request to the queue.
   *
   * @param aio_request The AIO request.
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user AIO, whereas if it is 1 it means an
   * internal AIO.
   * @return Status
   */
  //  Status aio_push_request(AIORequest* aio_request, int i);

  /**
   * Clears a TileDB array. The array will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param array The array to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_clear(const std::string& array) const;

  /**
   * Decrements the number of times the input array is initialized. If this
   * number reaches 0, the it deletes the open array entry (and hence clears
   * the schema and fragment book-keeping of the array).
   *
   * @param array The array name.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_close(const std::string& array);

  /**
   * Deletes a TileDB array entirely.
   *
   * @param array The array to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_delete(const std::string& array) const;

  /**
   * Gets the names of the existing fragments of an array.
   *
   * @param array The input array.
   * @param fragment_names The fragment names to be returned.
   * @return void
   */
  //  void array_get_fragment_names(
  //      const std::string& array, std::vector<std::string>& fragment_names);

  /**
   * Gets an open array entry for the array being initialized. If this
   * is the first time the array is initialized, then the function creates
   * a new open array entry for this array.
   *
   * @param array The array name.
   * @param open_array The open array entry to be returned.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_get_open_array_entry(
  //      const std::string& array, OpenArray*& open_array);

  /**
   * Loads the book-keeping structures of all the fragments of an array from the
   * disk, allocating appropriate memory space for them.
   *
   * @param array_schema The array schema.
   * @param fragment_names The names of the fragments of the array.
   * @param book_keeping The book-keeping structures to be returned.
   * @param mode The array mode
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status array_load_book_keeping(
  //      const ArraySchema* array_schema,
  //      const std::vector<std::string>& fragment_names,
  //      std::vector<BookKeeping*>& book_keeping,
  //      ArrayMode mode);

  /**
   * Moves a TileDB array.
   *
   * @param old_array The old array directory.
   * @param new_array The new array directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_move(
  //      const std::string& old_array, const std::string& new_array) const;

  /**
   * Opens an array. This creates or updates an OpenArray entry for this array,
   * and loads the array schema and book-keeping if it is the first time this
   * array is being initialized. The book-keeping structures are loaded only
   * if the input mode is a read mode.
   *
   * @param array_name The array name (must be absolute path).
   * @param mode The mode in which the array is being initialized.
   * @param open_array The open array entry that is retrieved.
   * @param mode The array mode.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status array_open(
  //      const std::string& array_name, OpenArray*& open_array, ArrayMode
  //      mode);

  /**
   * Creates a special file that serves as lock needed for implementing
   * thread- and process-safety during consolidation. The file is
   * created inside an array or metadata directory.
   *
   * @param dir The array or metadata directory the filelock is created for.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status consolidation_lock_create(const std::string& dir) const;

  /**
   * Locks the consolidation file lock.
   *
   * @param array_name The name of the array the lock is applied on.
   * @param fd The file descriptor of the filelock.
   * @param shared The lock type true is shared or false if exclusive
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status consolidation_lock(
  //      const std::string& array_name, int* fd, bool shared) const;

  /**
   * Unlocks the consolidation file lock.
   *
   * @param fd The file descriptor of the filelock.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status consolidation_unlock(int fd) const;

  /**
   * Finalizes the consolidation process, applying carefully the locks so that
   * this can be done concurrently with other reads. The function finalizes the
   * new consolidated fragment, and deletes the old fragments that created it.
   *
   * @param new_fragment The new consolidated fragment that the function will
   *     finalize.
   * @param old_fragment_names The names of the old fragments that need to be
   *     deleted.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  //  Status consolidation_finalize(
  //      Fragment* new_fragment,
  //      const std::vector<std::string>& old_fragment_names);

  /**
   * Clears a TileDB group. The group will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param group The group to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status group_clear(const std::string& group) const;

  /**
   * Deletes a TileDB group entirely.
   *
   * @param group The group to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status group_delete(const std::string& group) const;

  /**
   * Moves a TileDB group.
   *
   * @param old_group The old group directory.
   * @param new_group The new group directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status group_move(
  //      const std::string& old_group, const std::string& new_group) const;

  /**
   * Clears a TileDB metadata object. The metadata will still exist after the
   * execution of the function, but it will be empty (i.e., as if it was just
   * created).
   *
   * @param metadata The metadata to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status metadata_clear(const std::string& metadata) const;

  /**
   * Deletes a TileDB metadata object entirely.
   *
   * @param metadata The metadata to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status metadata_delete(const std::string& metadata) const;

  /**
   * Moves a TileDB metadata object.
   *
   * @param old_metadata The old metadata directory.
   * @param new_metadata The new metadata directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  //  Status metadata_move(
  //      const std::string& old_metadata, const std::string& new_metadata)
  //      const;

  /**
   * Appropriately sorts the fragment names based on their name timestamps.
   * The result is stored in the input vector.
   *
   * @param fragment_names The fragment names to be sorted. This will also hold
   *     the result of the function after termination.
   * @return void
   */
  //  void sort_fragment_names(std::vector<std::string>& fragment_names) const;
};

}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_H
