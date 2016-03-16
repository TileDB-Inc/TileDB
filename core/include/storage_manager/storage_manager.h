/**
 * @file   storage_manager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corp.
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

#include "array.h"
#include "array_iterator.h"
#include "array_schema.h"
#include "array_schema_c.h"
#include "metadata.h"
#include "metadata_iterator.h"
#include "metadata_schema_c.h"
#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_SM_OK                                 0
#define TILEDB_SM_ERR                               -1
/**@}*/

/** Name of the master catalog. */
#define TILEDB_SM_MASTER_CATALOG      "master_catalog"

/** 
 * The storage manager, which is repsonsible for creating, deleting, etc. of
 * TileDB objects (i.e., workspaces, groups, arrays and metadata).
 */
class StorageManager {
 public: 
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** The operation type on the master catalog (insertion or deletion). */
  enum MasterCatalogOp {TILEDB_SM_MC_INS, TILEDB_SM_MC_DEL};

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
   * Initializes the storage manager. This function create the TileDB home
   * directory, which by default is "~/.tiledb/". If the user home directory
   * cannot be retrieved, then the TileDB home directory is set to the current
   * working directory.
   *
   * @param config_filename The input configuration file name. If it is NULL,
   *     then the default TileDB parameters are used. 
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int init(const char* config_filename);




  /* ********************************* */
  /*            WORKSPACE              */
  /* ********************************* */

  /**
   * Creates a TileDB workspace.
   *
   * @param workspace The directory of the workspace to be created. This
   *     directory should not be inside another TileDB workspace, group, array
   *     or metadata directory.
   * @return TILEDB_SM_OK for succes, and TILEDB_SM_ERR for error.
   */
  int workspace_create(const std::string& workspace) const; 

  /**
   * Lists all TileDB workspaces, copying their directory names in the input
   * string buffers.
   *
   * @param workspaces An array of strings that will store the listed
   *     workspaces. Note that this should be pre-allocated by the user. If the
   *     size of each string is smaller than the corresponding workspace path
   *     name, the function will probably segfault. It is a good idea to
   *     allocate to each workspace string TILEDB_NAME_MAX_LEN characters. 
   * @param workspace_num The number of allocated elements of the *workspaces*
   *     string array. After the function execution, this will hold the actual
   *     number of workspaces written in the *workspaces* string array. If the
   *     number of allocated elements is smaller than the number of existing
   *     TileDB workspaces, the function will return an error.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int ls_workspaces(
      char** workspaces,
      int& workspace_num) const;




  /* ********************************* */
  /*              GROUP                */
  /* ********************************* */

  /**
   * Creates a new TileDB group.
   *
   * @param group The directory of the group to be created in the file system. 
   *     This should be a directory whose parent is a TileDB workspace or
   *     another TileDB group.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
 */
  int group_create(const std::string& group) const; 




  /* ********************************* */
  /*              ARRAY                */
  /* ********************************* */

  /**
   * Creates a new TileDB array.
   *
   * @param tiledb_ctx The TileDB context.
   * @param array_schema_c The array schema. 
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int array_create(const ArraySchemaC* array_schema_c) const; 

  /**
   * Creates a new TileDB array.
   *
   * @param tiledb_ctx The TileDB context.
   * @param array_schema The array schema. 
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int array_create(const ArraySchema* array_schema) const; 

  /**
   * Loads the schema of an array from the disk, allocating appropriate memory
   * space for it.
   *
   * @param array_dir The directory of the array.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int array_load_schema(
      const char* array_dir, 
      ArraySchema*& array_schema) const;

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
  int array_init(
      Array*& array,
      const char* array_dir,
      int mode, 
      const void* subarray,
      const char** attributes,
      int attribute_num) const;

  /** 
   * Finalizes an array, properly freeing the memory space.
   *
   * @param array The array to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int array_finalize(Array* array) const;

  /**
   * Initializes an array iterator for reading cells, potentially constraining 
   * it on a subset of attributes, as well as a subarray. The cells will be read
   * in the order they are stored on the disk, maximing performance. 
   *
   * @param tiledb_array_it The TileDB array iterator to be created. The
   *    function will allocate the appropriate memory space for the iterator. 
   * @param array The directory of the array the iterator is initialized for.
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
  int array_iterator_init(
      ArrayIterator*& array_it,
      const char* array,
      const void* subarray,
      const char** attributes,
      int attribute_num,
      void** buffers,
      size_t* buffer_sizes) const;

  /**
   * Finalizes an array iterator, properly freeing the allocating memory space.
   * 
   * @param tiledb_array_it The TileDB array iterator to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int array_iterator_finalize(ArrayIterator* array_it) const;




  /* ********************************* */
  /*              METADATA             */
  /* ********************************* */

  /**
   * Creates a new TileDB metadata object.
   *
   * @param metadata_schema_c The metadata schema. 
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int metadata_create(const MetadataSchemaC* metadata_schema_c) const; 

  /**
   * Creates a new TileDB metadata object.
   *
   * @param metadata_schema The metadata schema. 
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int metadata_create(const ArraySchema* array_schema) const; 

  /**
   * Loads the schema of a metadata object from the disk, allocating appropriate
   * memory space for it.
   *
   * @param metadata_dir The directory of the metadata.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int metadata_load_schema(
      const char* metadata_dir, 
      ArraySchema*& array_schema) const;

  /**
   * Initializes a TileDB metadata object.
   *
   * @param metadata The metadata object to be initialized. The function
   *     will allocate memory space for it.
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
  int metadata_init(
      Metadata*& metadata,
      const char* metadata_dir,
      int mode, 
      const char** attributes,
      int attribute_num) const;

  /** 
   * Finalizes a TileDB metadata object, properly freeing the memory space. 
   *
   * @param tiledb_metadata The metadata to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int metadata_finalize(Metadata* metadata) const;

  /**
   * Initializes a metadata iterator, potentially constraining it 
   * on a subset of attributes. The values will be read in the order they are
   * stored on the disk, maximing performance. 
   *
   * @param tiledb_metadata_it The TileDB metadata iterator to be created. The
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
  int metadata_iterator_init(
      MetadataIterator*& metadata_it,
      const char* metadata_dir,
      const char** attributes,
      int attribute_num,
      void** buffers,
      size_t* buffer_sizes) const;

  /**
   * Finalizes the iterator, properly freeing the allocating memory space.
   * 
   * @param tiledb_metadata_it The TileDB metadata iterator.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int metadata_iterator_finalize(MetadataIterator* metadata_it) const;




  /* ********************************* */
  /*               MISC                */
  /* ********************************* */

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
   *    - TILEDB_WORKSPACE
   *    - TILEDB_GROUP
   *    - TILEDB_ARRAY
   *    - TILEDB_METADATA
   * @param dir_num The number of elements allocated by the user for *dirs*.
   *     After the function terminates, this will hold the actual number of
   *     TileDB objects that were stored in *dirs*.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int ls(
      const char* parent_dir,
      char** dirs,
      int* dir_types,
      int& dir_num) const;

  /**
   * Clears a TileDB directory. The corresponding TileDB object (workspace,
   * group, array, or metadata) will still exist after the execution of the
   * function, but it will be empty (i.e., as if it was just created).
   *
   * @param dir The directory to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int clear(const std::string& dir) const;

  /**
   * Deletes a TileDB directory (workspace, group, array, or metadata) entirely.
   *
   * @param dir The directory to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int delete_entire(const std::string& dir) const;

  /**
   * Moves a TileDB directory (workspace, group, array or metadata).
   *
   * @param old_dir The old directory.
   * @param new_dir The new directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int move(const std::string& old_dir, const std::string& new_dir) const;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The directory of the master catalog. */
  std::string master_catalog_dir_;
  /** The TileDB home directory. */
  std::string tiledb_home_;

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
  int array_clear(const std::string& array) const;

  /**
   * Deletes a TileDB array entirely.
   *
   * @param array The array to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int array_delete(const std::string& array) const;

  /**
   * Moves a TileDB array.
   *
   * @param old_array The old array directory.
   * @param new_array The new array directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int array_move(
       const std::string& old_array,
       const std::string& new_array) const;

  /** 
   * It sets the TileDB configuration parameters from a file.
   *
   * @param config_filename The name of the configuration file.
   *     Each line in the file correspond to a single parameter, and should
   *     be in the form <parameter> <value> (i.e., space-separated).
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int config_set(const char* config_filename);

  /** 
   * Sets the TileDB configuration parameters to default values. 
   *
   * @return void
   */
  void config_set_default();

  /**
   * Creates a special group file inside the group directory.
   *
   * @param group The group directory.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int create_group_file(const std::string& dir) const;

  /**
   * Creates a new master catalog entry for a new workspace, or a deleted
   * workspace, or a moved workspace.
   *
   * @param workspace The workspace the entry is created for.
   * @param op The operation performed on the master catalog, which can be
   *     - TILEDB_SM_MC_INS (insertion)
   *     - TILEDB_SM_MC_DEL (deletion)
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int create_master_catalog_entry(
      const std::string& workspace, 
      MasterCatalogOp op) const;

  /**
   * Creates a special workspace file inside the workpace directory.
   *
   * @param workspace The workspace directory.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int create_workspace_file(const std::string& workspace) const;

  /**
   * Clears a TileDB group. The group will still exist after the execution of
   * the function, but it will be empty (i.e., as if it was just created).
   *
   * @param group The group to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int group_clear(const std::string& group) const;

  /**
   * Deletes a TileDB group entirely.
   *
   * @param group The group to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int group_delete(const std::string& group) const;

  /**
   * Moves a TileDB group.
   *
   * @param old_group The old group directory.
   * @param new_group The new group directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int group_move(
       const std::string& old_group,
       const std::string& new_group) const;
  
  /** 
   * Consolidates the fragments of the master catalog.
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int master_catalog_consolidate() const;

  /**
   * Create a master catalog, which keeps information about the TileDB
   * workspaces. 
   *
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int master_catalog_create() const;

  /**
   * Clears a TileDB metadata object. The metadata will still exist after the
   * execution of the function, but it will be empty (i.e., as if it was just
   * created).
   *
   * @param metadata The metadata to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int metadata_clear(const std::string& metadata) const;

  /**
   * Deletes a TileDB metadata object entirely.
   *
   * @param metadata The metadata to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int metadata_delete(const std::string& metadata) const;

  /**
   * Moves a TileDB metadata object.
   *
   * @param old_metadata The old metadata directory.
   * @param new_metadata The new metadata directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int metadata_move(
       const std::string& old_metadata,
       const std::string& new_metadata) const;

  /**
   * Clears a TileDB workspace. The workspace will still exist after the
   * execution of the function, but it will be empty (i.e., as if it was just
   * created).
   *
   * @param workspace The workspace to be cleared.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int workspace_clear(const std::string& workspace) const;

  /**
   * Deletes a TileDB workspace entirely.
   *
   * @param workspace The workspace to be deleted.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int workspace_delete(const std::string& workspace) const;

  /**
   * Moves a TileDB workspace.
   *
   * @param old_workspace The old workspace directory.
   * @param new_workspace The new workspace directory.
   * @return TILEDB_SM_OK for success and TILEDB_SM_ERR for error.
   */
  int workspace_move(
       const std::string& old_workspace,
       const std::string& new_workspace) const;
}; 

#endif
