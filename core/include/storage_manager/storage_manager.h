/**
 * @file   storage_manager.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include "array_schema.h"
#include "array_schema_c.h"
#include <string>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

// Return codes
#define TILEDB_SM_OK        0
#define TILEDB_SM_ERR      -1

// Special file names
#define TILEDB_SM_ARRAY_SCHEMA_FILENAME   "__array_schema.tdb"
#define TILEDB_SM_GROUP_FILENAME          "__tiledb_group.tdb"
#define TILEDB_SM_WORKSPACE_FILENAME      "__tiledb_workspace.tdb"

/** 
 * The Storage Manager is the most important module of TileDB, which is
 * repsonsible pretty much for everything.
 */
class StorageManager {
 public: 
  // CONSTRUCTORS & DESTRUCTORS  

  /** Constructor. */
  StorageManager(const std::string& config_filename);

  /** Destructor. */
  ~StorageManager();

  // WORKSPACE

  /**
   * Checks if the input directory is a workspace.
   *
   * @param dir The directory to be checked.
   * @return True if the directory is a workspace, and false otherwise.
   */
  bool is_workspace(const std::string& dir) const;


  // TODO
  int workspace_create(const std::string& dir) const; 

  // GROUP

  /**
   * Checks if the input directory is a group.
   *
   * @param dir The directory to be checked.
   * @return True if the directory is a group, and false otherwise.
   */
  bool is_group(const std::string& dir) const;

  /**
   * Creates a new group.
   *
   * @param dir The name of the directory of the group. Note that the parent
   *      of this directory must be a TileDB workspace or group.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int group_create(const std::string& dir) const; 

  // ARRAY

  /**
   * Creates an array.
   *
   * @param array_schema_c A C-style array schema object.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int array_create(const ArraySchemaC* array_schema_c) const; 

  /**
   * Creates an array.
   *
   * @param array_schema A C++-style array schema object.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int array_create(const ArraySchema* array_schema) const; 

  /**
   * Initializes an array object.
   *
   * @param array The array object to be initialized.
   * @param dir The directory of the array to be initialized.
   * @param mode The mode of the array. It must be one of the following:
   *    - TILEDB_WRITE 
   *    - TILEDB_WRITE_UNSORTED 
   *    - TILEDB_READ 
   *    - TILEDB_READ_REVERSE 
   * @param range The range in which the array read/write will be constrained.
   * @param attributes A subset of the array attributes the read/write will be
   *     constrained.
   * @param attribute_num The number of the input attributes.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int array_init(
      Array*& array,
      const char* dir,
      int mode, 
      const void* range,
      const char** attributes,
      int attribute_num) const;

  /** 
   * Finalizes an array. 
   *
   * @param array The array to be finalized.
   * @return TILEDB_SM_OK on success, and TILEDB_SM_ERR on error.
   */
  int array_finalize(Array* array) const;

  /**
   * Loads the schema of an array from the disk.
   *
   * @param dir The directory of the array.
   * @param array_schema The schema to be loaded.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int array_load_schema(const char* dir, ArraySchema*& array_schema) const;

  // TODO
  int array_write(
      Array* array,
      const void** buffers, 
      const size_t* buffer_sizes) const;
 
  /**
   * Checks if the input directory is an array.
   *
   * @param dir The directory to be checked.
   * @return True if the directory is an array, and false otherwise.
   */
  bool is_array(const std::string& dir) const;


  // CLEAR, DELETE, MOVE
  // TODO
  int clear(const std::string& dir) const;

  // TODO
  int delete_entire(const std::string& dir) const;

  // TODO
  int move(const std::string& old_dir, const std::string& new_dir) const;

 private:
  // PRIVATE METHODS

  /** 
   * It sets the TileDB configuration parameters from a file.
   *
   * @param config_filename The name of the configuration file.
   *     Each line in the file correspond to a single parameter, and should
   *     be in the form <parameter> <value> (i.e., space-separated).
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int config_set(const std::string& config_filename);

  /** 
   * Sets the TileDB configuration parameters to default values. This is called
   * if config_set() fails. 
   *
   * @return void
   */
  void config_set_default();

  /**
   * Creates a special group file inside the group directory.
   *
   * @param dir The group directory.
   * @return TILEDB_SM_OK for success, and TILEDB_SM_ERR for error.
   */
  int create_group_file(const std::string& dir) const;
}; 

#endif
