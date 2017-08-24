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
#include "array_schema.h"
#include "config.h"
#include "status.h"
#include "uri.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif

namespace tiledb {

/**
 * The storage manager, which is repsonsible for creating, deleting, etc. of
 * TileDB objects (i.e., groups, arrays).
 */
class StorageManager {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** Implements an open array entry. */
  class OpenArray;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  StorageManager();

  /** Destructor. */
  ~StorageManager();

  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Initializes the storage manager. This function creates the TileDB home
   * directory, which by default is "~/.tiledb/". If the user home directory
   * cannot be retrieved, then the TileDB home directory is set to the current
   * working directory.
   *
   * @param config The configuration parameters. If it is NULL,
   *     then the default TileDB parameters are used.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status init(Config* config);

  /** Sets a new config. */
  void set_config(const Config* config);

  /* ********************************* */
  /*              GROUP                */
  /* ********************************* */

  /**
   * Creates a new TileDB group.
   *
   * @param group The directory of the group to be created in the file system.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status group_create(const uri::URI& group) const;

  /* ********************************* */
  /*           BASIC ARRAY             */
  /* ********************************* */

  /**
   * Creates a new TileDB basic array.
   *
   * @param name The basic array name.
   * @return Status
   */
  Status basic_array_create(const char* name) const;

  /* ********************************* */
  /*              ARRAY                */
  /* ********************************* */

  /**
   * Submits an asynchronous I/O. The second argument is 0 for user AIO, and 1
   * for internal AIO.
   */
  Status aio_submit(AIORequest* aio_request, int i = 0);

  /**
   * Consolidates the fragments of an array into a single fragment.
   *
   * @param array_uri The identifier of the array to be consolidated.
   * @return Status
   */
  Status array_consolidate(const uri::URI& array);

  /**
   * Creates a new TileDB array.
   *
   * @param array_schema The array schema.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_create(ArraySchema* array_schema) const;

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
  Status array_init(
      Array*& array,
      const uri::URI& array_uri,
      QueryMode mode,
      const void* subarray,
      const char** attributes,
      int attribute_num);

  /**
   * Finalizes an array, properly freeing the memory space.
   *
   * @param array The array to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status array_finalize(Array* array);

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
  int dir_type(const char* dir);

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
  Status ls(
      const uri::URI& parent_uri,
      char** object_paths,
      tiledb_object_t* object_types,
      int* num_objects) const;

  /**
   * Counts the TileDB objects in path
   *
   * @param parent_path The parent path of the TileDB objects to be listed.
   * @param object_num The number of TileDB objects to be returned.
   * @return Status
   */
  Status ls_c(const uri::URI& parent_uri, int* object_num) const;

  /**
   * Clears a TileDB directory. The corresponding TileDB object
   * (group, array) will still exist after the execution of the
   * function, but it will be empty (i.e., as if it was just created).
   *
   * @param dir The directory to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status clear(const uri::URI& uri) const;

  /**
   * Deletes a TileDB directory (group, array) entirely.
   *
   * @param dir The directory to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status delete_entire(const uri::URI& uri);

  /**
   * Moves a TileDB directory (group, array).
   *
   * @param old_dir The old directory.
   * @param new_dir The new directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status move(const uri::URI& old_uri, const uri::URI& new_uri);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /**
   * AIO condition variable. The first is for user AIO, the second for
   * internal IO.
   * */
  std::condition_variable aio_cv_[2];

  /** If true, the AIO thread will be eventually terminated. */
  bool aio_done_;

  /**
   * AIO request queue. The first is for user AIO, the second for
   * internal IO.
   */
  std::queue<AIORequest*> aio_queue_[2];

  /**
   * AIO mutex. The first is for user AIO, the second for
   * internal IO.
   */
  std::mutex aio_mutex_[2];

  /**
   * Thread tha handles all user asynchronous I/O. The first is for user AIO,
   * the second for internal IO.
   */
  std::thread* aio_thread_[2];

  /** The TileDB configuration parameters. */
  Config* config_;

  /** Mutex for creating/deleting an OpenArray object. */
  std::mutex open_array_mtx_;
  /** Stores the currently open arrays. */
  std::map<std::string, OpenArray*> open_arrays_;

  /* ********************************* */
  /*         PRIVATE CONSTANTS         */
  /* ********************************* */

  /**@{*/
  /** Lock types. */
  static const int SHARED_LOCK = 0;
  static const int EXCLUSIVE_LOCK = 1;
  /**@}*/

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /** Handles a single AIO request. */
  void aio_handle_request(AIORequest* aio_request);

  /**
   * Starts handling AIO requests.
   *
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user AIO, whereas if it is 1 it means an
   * internal AIO.
   */
  void aio_handle_requests(int i);

  /**
   * Pushes an AIO request to the queue.
   *
   * @param aio_request The AIO request.
   * @param i The index of the thread that executes the function. If it is
   * equal to 0, it means a user AIO, whereas if it is 1 it means an
   * internal AIO.
   * @return Status
   */
  Status aio_push_request(AIORequest* aio_request, int i);

  /**
   * Starts listening to asynchronous I/O.
   *
   * @param storage_manager The storage manager object that handles the
   * asynchronous I/O.
   * @param i The index of the thread to execute the function. If it is
   * equal to 0, it means a user AIO, whereas if it is 1 it means an
   * internal AIO.
   */
  static void aio_start(StorageManager* storage_manager, int i = 0);

  /**
   * Stops the asynchronous I/O.
   */
  void aio_stop();

  /**
   * Clears a TileDB array. The array will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param array The array to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_clear(const uri::URI& array) const;

  /**
   * Decrements the number of times the input array is initialized. If this
   * number reaches 0, the it deletes the open array entry (and hence clears
   * the schema and fragment metadata of the array).
   *
   * @param array The array name.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_close(const uri::URI& array);

  /**
   * Deletes a TileDB array entirely.
   *
   * @param array The array to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_delete(const uri::URI& array) const;

  /**
   * Gets the names of the existing fragments of an array.
   *
   * @param array The input array.
   * @param fragment_names The fragment names to be returned.
   * @return void
   */
  void array_get_fragment_names(
      const uri::URI& array_uri, std::vector<std::string>& fragment_names);

  /**
   * Gets an open array entry for the array being initialized. If this
   * is the first time the array is initialized, then the function creates
   * a new open array entry for this array.
   *
   * @param array The array name.
   * @param open_array The open array entry to be returned.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_get_open_array_entry(
      const uri::URI& array_uri, OpenArray*& open_array);

  /**
   * Loads the metadata structures of all the fragments of an array from the
   * disk, allocating appropriate memory space for them.
   *
   * @param array_schema The array schema.
   * @param fragment_names The names of the fragments of the array.
   * @param metadata The metadata structures to be returned.
   * @param mode The array mode
   * @return Status
   */
  Status array_load_metadata(
      const ArraySchema* array_schema,
      const std::vector<std::string>& fragment_names,
      std::vector<FragmentMetadata*>& metadata,
      QueryMode mode);

  /**
   * Moves a TileDB array.
   *
   * @param old_array The old array directory.
   * @param new_array The new array directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_move(const uri::URI& old_array, const uri::URI& new_array) const;

  /**
   * Opens an array. This creates or updates an OpenArray entry for this array,
   * and loads the array schema and metadata if it is the first time this
   * array is being initialized. The metadata structures are loaded only
   * if the input mode is a read mode.
   *
   * @param array_name The array name (must be absolute path).
   * @param mode The mode in which the array is being initialized.
   * @param open_array The open array entry that is retrieved.
   * @param mode The array mode.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_open(
      const uri::URI& array_name, OpenArray*& open_array, QueryMode mode);

  /**
   * Creates a special file that serves as lock needed for implementing
   * thread- and process-safety during consolidation. The file is
   * created inside an array directory.
   *
   * @param dir The array directory the filelock is created for.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_lock_create(const std::string& dir) const;

  /**
   * Locks the consolidation file lock.
   *
   * @param array_name The name of the array the lock is applied on.
   * @param fd The file descriptor of the filelock.
   * @param shared The lock type true is shared or false if exclusive
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_lock(
      const std::string& array_name, int* fd, bool shared) const;

  /**
   * Unlocks the consolidation file lock.
   *
   * @param fd The file descriptor of the filelock.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_unlock(int fd) const;

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
  Status consolidation_finalize(
      Fragment* new_fragment, const std::vector<uri::URI>& old_fragment_names);

  /**
   * Clears a TileDB group. The group will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param group The group to be cleared.
   * @return Status
   */
  Status group_clear(const uri::URI& group) const;

  /**
   * Deletes a TileDB group entirely.
   *
   * @param group The group to be deleted.
   * @return Status
   */
  Status group_delete(const uri::URI& group) const;

  /**
   * Moves a TileDB group.
   *
   * @param old_group The old group directory.
   * @param new_group The new group directory.
   * @return Status
   */
  Status group_move(const uri::URI& old_group, const uri::URI& new_group) const;

  /**
   * Appropriately sorts the fragment names based on their name timestamps.
   * The result is stored in the input vector.
   *
   * @param fragment_names The fragment names to be sorted. This will also hold
   *     the result of the function after termination.
   * @return void
   */
  void sort_fragment_names(std::vector<std::string>& fragment_names) const;
};

/**
 * Stores information about an open array. An array is open if it has been
 * initialized once (withour being finalized). The difference with array
 * initialization is that an array can be initialized multiple times,
 * but opened only once. This structure maintains the information that
 * can be used by multiple array objects that initialize the same array,
 * in order to avoid replication and speed-up performance (e.g., array
 * schema and metadata).
 */
class StorageManager::OpenArray {
 public:
  // ATTRIBUTES

  /** The array schema. */
  ArraySchema* array_schema_;
  /** The metadata structures for all the fragments of the array. */
  std::vector<FragmentMetadata*> fragment_metadata_;
  /**
   * A counter for the number of times the array has been initialized after
   * it was opened.
   */
  int cnt_;
  /** Descriptor for the consolidation filelock. */
  int consolidation_filelock_;
  /** The names of the fragments of the open array. */
  std::vector<std::string> fragment_names_;
  /**
   * A mutex used to lock the array when loading the array schema and
   * the fragment metadata structures from the disk.
   */
  std::mutex mtx_;

  // FUNCTIONS

  /** Locks the mutex. */
  void mtx_lock();

  /** Unlocks the mutex. */
  void mtx_unlock();
};

}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_H
