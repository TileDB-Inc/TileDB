/**
 * @file   storage_manager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 MIT, Intel Corporation and TileDB, Inc.
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

#ifndef __STORAGE_MANAGER_H__
#define __STORAGE_MANAGER_H__

#include <map>
#include "array.h"
#include "array_iterator.h"
#include "array_schema.h"
#include "array_schema_c.h"
#include "logger.h"
#include "metadata.h"
#include "metadata_iterator.h"
#include "metadata_schema_c.h"
#include "status.h"
#include "storage_manager_config.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif
#include <pthread.h>
#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Lock types. */
#define TILEDB_SM_SHARED_LOCK 0
#define TILEDB_SM_EXCLUSIVE_LOCK 1
/**@}*/

/** Name of the consolidation file lock. */
#define TILEDB_SM_CONSOLIDATION_FILELOCK_NAME ".__consolidation_lock"

/**@{*/  // TODO: these may have to be moved from here
/** Special TileDB file name. */
#define TILEDB_ARRAY_SCHEMA_FILENAME "__array_schema.tdb"
#define TILEDB_METADATA_SCHEMA_FILENAME "__metadata_schema.tdb"
#define TILEDB_BOOK_KEEPING_FILENAME "__book_keeping"
#define TILEDB_FRAGMENT_FILENAME "__tiledb_fragment.tdb"
#define TILEDB_GROUP_FILENAME "__tiledb_group.tdb"
/**@}*/

namespace tiledb {

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Stores potential error messages. */
// extern std::string tiledb_sm_errmsg;

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
   * Finalizes the storage manager, properly freeing memory.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status finalize();

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
  Status init(StorageManagerConfig* config);

  /* ********************************* */
  /*              GROUP                */
  /* ********************************* */

  /**
   * Creates a new TileDB group.
   *
   * @param group The directory of the group to be created in the file system.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status group_create(const std::string& group) const;

  /* ********************************* */
  /*              ARRAY                */
  /* ********************************* */

  /**
   * Consolidates the fragments of an array into a single fragment.
   *
   * @param array_dir The name of the array to be consolidated.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_consolidate(const char* array_dir);

  /**
   * Creates a new TileDB array.
   *
   * @param array_schema_c The array schema.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_create(const ArraySchemaC* array_schema_c) const;

  /**
   * Creates a new TileDB array.
   *
   * @param array_schema The array schema.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_create(const ArraySchema* array_schema) const;

  /**
   * Loads the schema of an array from the disk, allocating appropriate memory
   * space for it.
   *
   * @param array_dir The directory of the array.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status array_load_schema(
      const char* array_dir, ArraySchema*& array_schema) const;

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
      const char* array_dir,
      ArrayMode mode,
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

  /**
   * Syncs all currently written files in the input array.
   *
   * @param array The array to be synced.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status array_sync(Array* array);

  /**
   * Syncs the currently written files associated with the input attribute
   * in the input array.
   *
   * @param array The array to be synced.
   * @param attribute The name of the attribute to be synced.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status array_sync_attribute(Array* array, const std::string& attribute);

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
  Status array_iterator_init(
      ArrayIterator*& array_it,
      const char* array,
      ArrayMode mode,
      const void* subarray,
      const char** attributes,
      int attribute_num,
      void** buffers,
      size_t* buffer_sizes);

  /**
   * Finalizes an array iterator, properly freeing the allocating memory space.
   *
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status array_iterator_finalize(ArrayIterator* array_it);

  /* ********************************* */
  /*              METADATA             */
  /* ********************************* */

  /**
   * Consolidates the fragments of a metadata object into a single fragment.
   *
   * @param metadata_dir The name of the metadata to be consolidated.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_consolidate(const char* metadata_dir);

  /**
   * Creates a new TileDB metadata object.
   *
   * @param metadata_schema_c The metadata schema.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_create(const MetadataSchemaC* metadata_schema_c) const;

  /**
   * Creates a new TileDB metadata object.
   *
   * @param array_schema The metadata (array) schema.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_create(const ArraySchema* array_schema) const;

  /**
   * Loads the schema of a metadata object from the disk, allocating appropriate
   * memory space for it.
   *
   * @param metadata_dir The directory of the metadata.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status metadata_load_schema(
      const char* metadata_dir, ArraySchema*& array_schema) const;

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
  Status metadata_init(
      Metadata*& metadata,
      const char* metadata_dir,
      tiledb_metadata_mode_t mode,
      const char** attributes,
      int attribute_num);

  /**
   * Finalizes a TileDB metadata object, properly freeing the memory space.
   *
   * @param metadata The metadata to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status metadata_finalize(Metadata* metadata);

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
  Status metadata_iterator_init(
      MetadataIterator*& metadata_it,
      const char* metadata_dir,
      const char** attributes,
      int attribute_num,
      void** buffers,
      size_t* buffer_sizes);

  /**
   * Finalizes the iterator, properly freeing the allocating memory space.
   *
   * @param metadata_it The TileDB metadata iterator.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  Status metadata_iterator_finalize(MetadataIterator* metadata_it);

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
  int dir_type(const char* dir);

  /**
   * Lists all the TileDB objects in a directory, copying them into the input
   * buffers.
   *
   * @param parent_dir The parent directory of the TileDB objects to be listed.
   * @param dirs An array of strings that will store the listed TileDB objects.
   *     Note that the user is responsible for allocating the appropriate memory
   *     space for this array of strings. A good idea for each string length is
   *     to set is to TILEDB_NAME_MAX_LEN.
   * @param dir_types The types of the corresponding TileDB objects, which can
   *     be the following (they are self-explanatory):
   *    - TILEDB_GROUP
   *    - TILEDB_ARRAY
   *    - TILEDB_METADATA
   * @param dir_num The number of elements allocated by the user for *dirs*.
   *     After the function terminates, this will hold the actual number of
   *     TileDB objects that were stored in *dirs*.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status ls(
      const char* parent_dir,
      char** dirs,
      tiledb_object_t* dir_types,
      int& dir_num) const;

  /**
   * Counts the TileDB objects in a directory.
   *
   * @param parent_dir The parent directory of the TileDB objects to be listed.
   * @param dir_num The number of TileDB objects to be returned.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status ls_c(const char* parent_dir, int& dir_num) const;

  /**
   * Clears a TileDB directory. The corresponding TileDB object
   * (group, array, or metadata) will still exist after the execution of the
   * function, but it will be empty (i.e., as if it was just created).
   *
   * @param dir The directory to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status clear(const std::string& dir) const;

  /**
   * Deletes a TileDB directory (group, array, or metadata) entirely.
   *
   * @param dir The directory to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status delete_entire(const std::string& dir);

  /**
   * Moves a TileDB directory (group, array or metadata).
   *
   * @param old_dir The old directory.
   * @param new_dir The new directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status move(const std::string& old_dir, const std::string& new_dir);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The TileDB configuration parameters. */
  StorageManagerConfig* config_;

  /** The logger for this storage manager.*/
  Logger* logger_;

/** OpneMP mutex for creating/deleting an OpenArray object. */
#ifdef HAVE_OPENMP
  omp_lock_t open_array_omp_mtx_;
#endif
  /** Pthread mutex for creating/deleting an OpenArray object. */
  pthread_mutex_t open_array_pthread_mtx_;
  /** Stores the currently open arrays. */
  std::map<std::string, OpenArray*> open_arrays_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Clears a TileDB array. The array will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param array The array to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_clear(const std::string& array) const;

  /**
   * Decrements the number of times the input array is initialized. If this
   * number reaches 0, the it deletes the open array entry (and hence clears
   * the schema and fragment book-keeping of the array).
   *
   * @param array The array name.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_close(const std::string& array);

  /**
   * Deletes a TileDB array entirely.
   *
   * @param array The array to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_delete(const std::string& array) const;

  /**
   * Gets the names of the existing fragments of an array.
   *
   * @param array The input array.
   * @param fragment_names The fragment names to be returned.
   * @return void
   */
  void array_get_fragment_names(
      const std::string& array, std::vector<std::string>& fragment_names);

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
      const std::string& array, OpenArray*& open_array);

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
  Status array_load_book_keeping(
      const ArraySchema* array_schema,
      const std::vector<std::string>& fragment_names,
      std::vector<BookKeeping*>& book_keeping,
      ArrayMode mode);

  /**
   * Moves a TileDB array.
   *
   * @param old_array The old array directory.
   * @param new_array The new array directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_move(
      const std::string& old_array, const std::string& new_array) const;

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
  Status array_open(
      const std::string& array_name, OpenArray*& open_array, ArrayMode mode);

  /**
   * Stores the input array schema into the input array directory (serializing
   * it into a sequence of bytes and storing it in a binary file).
   *
   * @param dir The array directory where the array schema is stored.
   * @param array_schema The array schema to be stored.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status array_store_schema(
      const std::string& dir, const ArraySchema* array_schema) const;

  /**
   * It sets the TileDB configuration parameters.
   *
   * @param config The configuration parameters.
   * @return
   */
  void config_set(StorageManagerConfig* config);

  /**
   * Creates a special file that serves as lock needed for implementing
   * thread- and process-safety during consolidation. The file is
   * created inside an array or metadata directory.
   *
   * @param dir The array or metadata directory the filelock is created for.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_filelock_create(const std::string& dir) const;

  /**
   * Locks the consolidation file lock.
   *
   * @param array_name The name of the array the lock is applied on.
   * @param fd The file descriptor of the filelock.
   * @param lock_type The lock type, which can be either TILEDB_SM_SHARED_LOCK
   *     or TILEDB_SM_EXCLUSIVE_LOCK.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_filelock_lock(
      const std::string& array_name, int& fd, int lock_type) const;

  /**
   * Unlocks the consolidation file lock.
   *
   * @param fd The file descriptor of the filelock.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status consolidation_filelock_unlock(int fd) const;

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
      Fragment* new_fragment,
      const std::vector<std::string>& old_fragment_names);

  /**
   * Creates a special group file inside the group directory.
   *
   * @param dir The group directory.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status create_group_file(const std::string& dir) const;

  /**
   * Clears a TileDB group. The group will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param group The group to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status group_clear(const std::string& group) const;

  /**
   * Deletes a TileDB group entirely.
   *
   * @param group The group to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status group_delete(const std::string& group) const;

  /**
   * Moves a TileDB group.
   *
   * @param old_group The old group directory.
   * @param new_group The new group directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status group_move(
      const std::string& old_group, const std::string& new_group) const;

  /**
   * Clears a TileDB metadata object. The metadata will still exist after the
   * execution of the function, but it will be empty (i.e., as if it was just
   * created).
   *
   * @param metadata The metadata to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_clear(const std::string& metadata) const;

  /**
   * Deletes a TileDB metadata object entirely.
   *
   * @param metadata The metadata to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_delete(const std::string& metadata) const;

  /**
   * Moves a TileDB metadata object.
   *
   * @param old_metadata The old metadata directory.
   * @param new_metadata The new metadata directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status metadata_move(
      const std::string& old_metadata, const std::string& new_metadata) const;

  /**
   * Destroys open array the mutexes.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status open_array_mtx_destroy();

  /**
   * Initializes open array mutexes.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status open_array_mtx_init();

  /**
   * Locks open array mutexes.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status open_array_mtx_lock();

  /**
   * Unlocks open array mutexes.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  Status open_array_mtx_unlock();

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
 * schema and book-keeping).
 */
class StorageManager::OpenArray {
 public:
  // ATTRIBUTES

  /** The array schema. */
  ArraySchema* array_schema_;
  /** The book-keeping structures for all the fragments of the array. */
  std::vector<BookKeeping*> book_keeping_;
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
 * An OpenMP mutex used to lock the array when loading the array schema and
 * the book-keeping structures from the disk.
 */
#ifdef HAVE_OPENMP
  omp_lock_t omp_mtx_;
#endif
  /**
   * A pthread mutex used to lock the array when loading the array schema and
   * the book-keeping structures from the disk.
   */
  pthread_mutex_t pthread_mtx_;

  // FUNCTIONS

  /**
   * Destroys the mutexes.
   *
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status mutex_destroy();

  /**
   * Initializes the mutexes.
   *
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status mutex_init();

  /**
   * Locks the mutexes.
   *
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status mutex_lock();

  /**
   * Unlocks the mutexes.
   *
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  Status mutex_unlock();
};

};  // namespace tiledb

#endif
