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
#include <map>
#include <string>
#include <vector>


/* ********************************* */
/*             CONSTANTS            */
/* ********************************* */

#define SM_GROUP_FILENAME                              ".tiledb_group"
#define SM_OPEN_ARRAYS_MAX                                         100
#define SM_OPEN_METADATA_MAX                                       100
#define SM_WORKSPACE_FILENAME                      ".tiledb_workspace"
#define TILEDB_SM_OK                                                 0
#define TILEDB_SM_ERR                                               -1
#define TILEDB_SM_READ_BUFFER_OVERFLOW                              -2
#define TILEDB_SM_METADATA_SCHEMA_FILENAME           "metadata_schema"
#define TILEDB_SM_MASTER_CATALOG_FILENAME     ".tiledb_master_catalog"

/** 
 * The storage manager administrates the various TileDB objects, e.g., it 
 * defines/opens/closes/clears/deletes arrays, initializes cell iterators, etc.
 *
 * For better understanding of this class, some useful information is 
 * summarized below:
 *
 * - **Workspace** <br>
 *   This is the main place where the arrays persist on the disk. It is
 *   implemented as a directory in the underlying file system. 
 * - **Group** <br>
 *   Groups enable hierarchical organization of the arrays. They are 
 *   implemented as sub-directories inside the workspace directory. Even
 *   the workspace directory is regarded as a group (i.e., the root
 *   group of all groups in the workspace). Note that a group path inserted
 *   by the user is translated with respect to the workspace, i.e., all
 *   home ("~/"), current ("./") and root ("/") refer to the workspace.
 *   For instance, if the user gives "W1" as a workspace, and "~/G1" as
 *   a group, then the directory in which the array directory will be
 *   stored is "W1/G1".  
 * - **Canonicalized absolute workspace/group paths** <br>
 *   Most of the functions of this class take as arguments a workspace and
 *   a group path. These paths may be given in relative format (e.g., "W1") and 
 *   potentially including strings like "../". The canonicalized
 *   absolute format of a path is an absolute path that does not contain 
 *   "../" or mulitplicities of slashes. Moreover, the canonicalized absolute
 *   format of the group is the *full* path of the group in the disk. For
 *   instance, suppose the current working directory is "/stavros/TileDB",
 *   and the user provided "W1" as the workspace, and "~/G1/G2/../" as the 
 *   group. The canonicalized absolute path of the workspace is 
 *   "/stavros/TileDB/W1" and that of the group is 
 *   "/stavros/TileDB/W1/G2". Most functions take an extra argument called
 *   *real_path* or *real_paths*, which indicates whether the input workspace
 *   and group paths are already in canonicalized absolute (i.e., real) format,
 *   so that the function avoids redundant conversions. Finally, note that
 *   an empty ("") workspace refers to the current working directory, whereas
 *   an empty group refers to the default workspace group.
 * - <b>%Array</b> <br>
 *   A TileDB array. All the data of the array are stored in a directory 
 *   named after the array, which is placed in a certain group inside
 *   a workspace. See class Array for more information.
 * - <b>%Fragment</b> <br>
 *   A fragment is a snapshot of an array, which can be perceived as an
 *   independent array. An array may have multiple fragments, in which case
 *   any query 'merges' all fragments on-the-fly, in order to get the 
 *   global view of the array. Each fragment is implemented as a sub-directory 
 *   under the array directory, which contains the data corresponding only 
 *   to this fragment. See Fragment for more information.
 * - <b>%Array descriptor</b> <br> 
 *   When an array is opened, an array descriptor is returned. This 
 *   descriptor is used in all subsequent operations with this array.
 * - <b>%Array schema</b> <br>
 *   An array consists of *dimensions* and *attributes*. The dimensions have a
 *   specific domain that orients the *coordinates* of the array cells. The
 *   attributes and coordinates have potentially different data types. Each
 *   array specifies a *global cell order*. This determines the order in which
 *   the cells are stored on the disk. See ArraySchema for more information.
 * - <b>%Cell iterators</b> <br> 
 *   The storage manager can initialize a variaty of cell iterators for an 
 *   array, e.g., forward, reverse, in a range, projecting over a subset of
 *   attributes, etc. The iterators go through the cells (either focusing
 *   on the entire domain or a sub-domain) in the global cell order (or 
 *   reverse global cell order, in the case of reverse iterators). 
 * - <b>Binary cell format</b> <br>
 *   A binary cell in TileDB has the following general format: first appear
 *   the coordinates, followed by the attribute values (in binary, in their
 *   respective data types). The order of coordinates and attribute values
 *   respect the order of dimensions and attributes as defined in the array
 *   schema. If an attribute takes multiple, but a *fixed* number of values,
 *   then the attribute values simply appear next to each other. However,
 *   if an attribute takes a *variable* number of values, two things happen: 
 *   (i) Immediately after the coordinates, the size (**int32**) of the entire 
 *   cell (in bytes) appears, and (ii) immediately before the variable attribute
 *   values, the number of the variable attribute values appears. For example,
 *   if there are two **int32** coordinates (5,10), one **int32:3** attribute 
 *   with values 1,2,3, and a **char:var** attribute with value "stavros", then
 *   the cell has size 8(coordinates) + 4(cell size) + 12(first attribute) +
 *   4(number of characters in the secibd attribute) + 7*1(second
 *   attribute) = 35 bytes, and its format is as follows (where '|' stands for
 *   binary concatenation): <br>
 *   5 | 10 | 35 | 1 | 2 | 3 | 7 | s | t | a | v | r | o | s \n
 *   If an iterator focuses on a subset of attributes, then the binary cell 
 *   format is adjusted accordingly, as if only the selected attributes were
 *   present in the array schema.
 */
class StorageManager {
 public: 
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**  **Mnemonic:** [*array real directory*] --> array descriptor */
  typedef std::map<std::string, int> OpenArrays;
  /**  **Mnemonic:** [*metadata real directory*] --> metadata descriptor */
  typedef std::map<std::string, int> OpenMetadata;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor that initializes book-keeping structures for monitoring open 
   * arrays. 
   */
  StorageManager();

  /**
   * Checks if the constructor of the object was executed successfully.
   * Always check this function after creating a StorageManager object.
   *
   * @return *True* for successfull creation and *false* otherwise. 
   */
  bool created_successfully() const;

  /** 
   * Finalizes a StorageManager object. Always execute this function before 
   * deleting a StorageManager object (otherwise a warning will be printed in 
   * the destructor, if compiled in VERBOSE mode). 
   *
   * @return **0** for success and <b>-1</b> for error.
   */
  int finalize();

  /** Destructor. */
  ~StorageManager();


  /* ********************************* */
  /*          METADATA FUNCTIONS       */
  /* ********************************* */

  /* ********************************* */
  /*           ARRAY FUNCTIONS         */
  /* ********************************* */

  /** 
   * Clears all the data of an array, except of its schema. In other words,
   * the array remains defined after this function is executed. 
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_clear(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name,
      bool real_paths = false);



  /** 
   * Closes an array. 
   *
   * @param ad The array descriptor.
   * @return void
   */
  int array_close(int ad);


  /** 
   * Forces an array to close. This is typically done during abnormal execution.
   * If the array was opened in "write", the created fragment
   * must be deleted (since it was not properly loaded).
   *
   * @param ad The descriptor of the array to be closed.
   * @return **0** for success and <b>-1</b> for error.
   * @see array_open, array_close
   */
  int array_close_forced(int ad);

  /**
   * Consolidates the fragments of an array, using the technique described in
   * detail in tiledb_array_define().
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_consolidate(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name,
      bool real_paths = false);

  // TODO
  int metadata_consolidate(const std::string& metadata);

  // TODO
  int array_consolidate(const std::string& array);

  /** 
   * It deletes an array. If the array is open, it will be properly closed
   * before being deleted. 
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int array_delete(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name,
      bool real_paths = false);

  /** 
   * Checks if the array exists. 
   *
   * @param array_name The name of the array.
   * @param real_path *True* if both the array name is in
   * canonicalized absolute from, and *false* otherwise. 
   * @return *True* if the array exists, *false* otherwise.
   */
  bool array_exists(
      const std::string& array_name,
      bool real_path = false) const;


  /** 
   * Checks if the array has been defined (i.e., its schema has been stored). 
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return *True* if the array is defined, *false* if it does not exist.
   */
  bool array_is_defined(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name,
      bool real_paths = false) const;

  /** 
   * Checks if the input array name is valid. 
   *
   * @param array_name The array name to be checked.
   * @return *True* if the array name is valid, and *false* otherwise.
   * @note Currently, TileDB supports only POSIX names, i.e., the name can 
   * contain only alphanumerics, and characters '_', '-', and '.'.
   */
  bool array_name_is_valid(const char* array_name) const;

  /** 
   * Opens an array in the input mode. 
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param mode The following modes are supported:
   *  - **r:** Read mode
   *  - **w:** Write mode (if the array exists, it is cleared)
   *  - **a:** Append mode (used for udpates) 
   *  - **c:** Consolidate mode (used only for consolidation) 
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return An array descriptor (to be used in subsequent array operations),
   * which is >=0 in case of success, and <b>-1</b> in case of error. 
   */
  int array_open(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name, 
      const char* mode,
      bool real_paths = false);

  // TODO
  int array_open(const std::string& array_name, const char* mode);
 
  // TODO
  int metadata_open(const std::string& metadata_name, const char* mode);

  /** 
   * Returns the schema of an array. 
   *
   * @param workspace The workspace where the array is stored.
   * @param group The group inside the workspace where the array is stored.
   * @param array_name The name of the array.
   * @param array_schema Will store the array schema to be retrieved.
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   * @see ArraySchema
   */
  int array_schema_get(
      const std::string& workspace,
      const std::string& group,
      const std::string& array_name, 
      ArraySchema*& array_schema,
      bool real_paths = false) const;

  // TODO
  int array_schema_get(
      const std::string& array_name, 
      ArraySchema*& array_schema,
      bool real_paths = false) const;

  // TODO
  int metadata_schema_get(
      int md,
      const ArraySchema*& array_schema) const;

  // TODO
  int metadata_schema_get(
      const std::string& metadata_name, 
      ArraySchema*& array_schema,
      bool real_paths = false) const;

  /** 
   * Returns the schema of an array. 
   *
   * @param ad The array descriptor. 
   * @param array_schema Will store the array schema to be retrieved.
   * @return **0** for success and <b>-1</b> for error.
   * @see ArraySchema
   */
  int array_schema_get(int ad, const ArraySchema*& array_schema) const;

  /** 
   * Stores the input array schema on the disk, creating the appropriate 
   * workspace and group directories.
   *
   * @param workspace The array workspace directory. If it does not exist, all
   * the directories in the path will be created.  
   * @param group The array group directory inside the workspace. If it
   * does not exist, all the directories in the path will be created. 
   * @param array_schema The schema of the array being defined. 
   * @param real_paths *True* if both the workspace and group paths are in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   * @note The workspace, group and array names should be in POSIX format.
   * Currently, TileDB does not support Unicode (it will in the future). If
   * POSIX is not followed, the result is unpredictable.
   * @see ArraySchema
   */
  int array_schema_store(
      const std::string& workspace,
      const std::string& group,
      const ArraySchema* array_schema,
      bool real_paths = false);

  // TODO
  int array_schema_store(const ArraySchema* array_schema);

  /**
   * Creates a group directory (and all non-existent directories in the
   * group path) inside an *existing* workspace. It also creates a special 
   * "group file" in the folder, which will allow for sanity checks in the 
   * future.
   *
   * @param workspace The workspace path where the group will be placed. The
   * workspace must exist, otherwise the function fails.
   * @param group The group path to be created inside the workspace.
   * @param real_paths *True* if both the workspace and group paths are in 
   * canonicalized absolute form, and *false* otherwise.
   * @return **0** for success and <b>-1</b> for error.
   */
  int group_create(
      const std::string& workspace, 
      const std::string& group,
      bool real_paths = false) const;


  int workspace_clear(const std::string& workspace);

  int workspace_list(const std::string& workspace);

  int workspace_create(
      const std::string& workspace,
      const std::string& master_catalog);

  int workspace_delete(
      const std::string& workspace, 
      const std::string& master_catalog);

  int workspace_move(
      const std::string& old_workspace,
      const std::string& new_workspace,
      const std::string& master_catalog);

  bool is_fragment(const std::string& filename, bool real_path = false);

  int group_clear(const std::string& group);

  int group_list(const std::string& group);

  int array_list(const std::string& array);

  int fragment_list(const std::string& fragment);

  int metadata_list(const std::string& metadata);

  int group_create(const std::string& group) const;

  int group_delete(const std::string& group, bool real_path = false);

  int master_catalog_delete(const std::string& master_catalog, bool real_path = false);

  int master_catalog_clear(const std::string& master_catalog, bool real_path = false);

  bool is_array_schema(const std::string& file, bool real_path = false) const; 

  bool is_metadata_schema(const std::string& file, bool real_path = false) const; 

  int array_schema_print(const std::string& file, bool real_path = false) const;

  int metadata_schema_print(const std::string& file, bool real_path = false) const;

  // TODO
  int group_move(
      const std::string& old_group,
      const std::string& new_group,
      bool real_paths = false);

  // TODO
  int master_catalog_move(
      const std::string& old_master_catalog,
      const std::string& new_master_catalog,
      bool real_paths = false);

  /**
   * Checks if the input group exists.
   *
   * @param workspace The workspace path where the group is stored.
   * @param group The group path to be checked.
   * @param real_paths *True* if both the workspace and group paths are in 
   * canonicalized absolute form, and *false* otherwise.
   * @return *True* if the group exists and *false* otherwise.
   */  
  bool group_exists(
      const std::string& workspace,
      const std::string& group,
      bool real_paths = false) const; 

  /**
   * Checks if the input group exists.
   *
   * @param group The group path to be checked.
   * @param real_path *True* if the group path is in 
   * canonicalized absolute form, and *false* otherwise.
   * @return *True* if the group exists and *false* otherwise.
   */  
  bool group_exists(
      const std::string& group,
      bool real_path = false) const; 

  /** 
   * Clears the metadata, but leaves its folder and metadata schema. 
   *
   * @param metadata_name The name of the metadata.
   * @param real_path *True* if the metadata name is in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int metadata_clear(
      const std::string& metadata_name,
      bool real_path = false);

  // TODO
  int array_clear(
      const std::string& array_name,
      bool real_path = false);

  // TODO
  int metadata_move(
      const std::string& old_metadata,
      const std::string& new_metadata,
      bool real_paths = false);

  // TODO
  int array_move(
      const std::string& old_array,
      const std::string& new_array,
      bool real_paths = false);

  /** 
   * Closes the input metadata. 
   *
   * @param md The metadata descriptor.
   * @return **0** for success and <b>-1</b> for error.
   */
  int metadata_close(int md);

  /** 
   * Forces the input metadata to close. This is typically done 
   * during abnormal execution.
   * If the metadata was opened in "write", the created fragment
   * is deleted (since it was not properly loaded).
   *
   * @param md The descriptor of the metadata to be closed.
   * @return **0** for success and <b>-1</b> for error.
   * @see metadata_open, metadata_close
   */
  int metadata_close_forced(int md);

  /** 
   * Checks if the metadata exists. 
   *
   * @param metadata_name The name of the metadata.
   * @param real_path *True* if both the metadata name is in
   * canonicalized absolute from, and *false* otherwise. 
   * @return *True* if the metadata exists, *false* otherwise.
   */
  bool metadata_exists(
      const std::string& metadata_name,
      bool real_paths = false) const;

  /** 
   * It deletes the input metadata. 
   *
   * @param metadata_name The name of the metadata.
   * @param real_path *True* if both the metadata name is in
   * canonicalized absolute from, and *false* otherwise. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int metadata_delete(
      const std::string& metadata_name,
      bool real_path = false);

  // TODO
  int array_delete(
      const std::string& array_name,
      bool real_path = false);

  /** 
   * Stores the metadata schema, expressed essentially as an array schema.
   *
   * @param array_schema The array schema corresponding to the metadata schema
   * to be stored.
   */
  int metadata_schema_store(
      const ArraySchema* array_schema, 
      bool master_catalog = false);


  /**
   * Retrieves the real (i.e., absolute canonicalized) workspace and group 
   * paths. 
   *
   * @param workspace The input workspace path.
   * @param group The input group path.
   * @param workspace_real The output real workspace path.
   * @param group_real The output real group path. 
   * @param real_paths *True* if the input *workspace* and *group* are already
   * in absolute canonicalized form, in which case the outputs will be identical
   * to the inputs, and *false* otherwise.
   * @return **0** for success and <b>-1</b> for error.
   */
  int real_paths_get(
      const std::string& workspace,
      const std::string& group,
      std::string& workspace_real,
      std::string& group_real,
      bool real_paths = false) const;

  /**
   * Creates a workspace directory (and all non-existent directories in the
   * workspace path). It also creates a special "workspace file"
   * in the folder, which will allow for sanity checks in the future.
   *
   * @param workspace The workspace path to be created.
   * @param real_path *True* if the workspace path is in canonicalized absolute
   * form, and *false* otherwise.
   * @return **0** for success and <b>-1</b> for error.
   */
  int workspace_create_2(
      const std::string& workspace, 
      bool real_path = false) const;

  /**
   * Checks if the input workspace exists.
   *
   * @param workspace The workspace path to be checked.
   * @param real_path *True* if the workspace path is in canonicalized absolute
   * form, and *false* otherwise.
   * @return *True* if the workspace exists and *false* otherwise.
   */  
  bool workspace_exists(
      const std::string& workspace, 
      bool real_path = false) const;

  bool master_catalog_exists(
      const std::string& master_catalog,
      bool real_path = false) const;

  int master_catalog_create(
      const std::string& master_catalog,
      bool real_path = false);

  int list(const std::string& item);

  int master_catalog_list(const std::string& master_catalog);

  /* ********************************* */
  /*           CELL FUNCTIONS          */
  /* ********************************* */

  /**  
   * Writes a cell to an array. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor. 
   * @param cell The cell to be written. See in the description of the 
   * StorageManager class to get information about the binary cell format.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int cell_write(int ad, const void* cell) const; 

  template<class T>
  int metadata_write(int md, const void* cell) const; 

  template<class T>
  int metadata_write_sorted(int md, const void* cell) const; 

  /**  
   * Writes a cell to an array. This function is used only when it is guaranteed
   * that the cells are written respecting the global cell order (it has a large
   * impact on performance).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor. 
   * @param cell The cell to be written. See in the description of the 
   * StorageManager class to get information about the binary cell format.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int cell_write_sorted(int ad, const void* cell, bool without_coords = false) const; 

  /**  
   * Writes a set of cells to an array. 
   *
   * @param ad The array descriptor. 
   * @param cells The buffer of the cells to be written. See in the description
   * of the  StorageManager class to get information about the format of each 
   * binary cell in the buffer.
   * @param cells_size The size of the cells buffer.
   * @return **0** for success and <b>-1</b> for error.
   */
  int cells_write(int ad, const void* cells, size_t cells_size) const; 

  /**  
   * Writes a set of cells to an array. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor. 
   * @param cells The buffer of the cells to be written. See in the description
   * of the  StorageManager class to get information about the format of each 
   * binary cell in the buffer.
   * @param cells_size The size of the cells buffer.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int cells_write(int ad, const void* cells, size_t cells_size) const; 

  /**  
   * Writes a set of cells to an array. This function is used only when it is
   * guaranteed that the cells are written respecting the global cell order (it
   * has a large impact on performance).
   *
   * @param ad The array descriptor. 
   * @param cells The buffer of cells to be written. See in the description of 
   * the StorageManager class to get information about the format of each binary
   * cell in the buffer.
   * @param cells_size The size of the cells buffer.
   * @return **0** for success and <b>-1</b> for error.
   */
  int cells_write_sorted(int ad, const void* cells, size_t cells_size) const; 

  /**  
   * Writes a set of cells to an array. This function is used only when it is
   * guaranteed that the cells are written respecting the global cell order (it
   * has a large impact on performance).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor. 
   * @param cells The buffer of cells to be written. See in the description of 
   * the StorageManager class to get information about the format of each binary
   * cell in the buffer.
   * @param cells_size The size of the cells buffer.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int cells_write_sorted(int ad, const void* cells, size_t cells_size) const; 

  // TODO
  template<class T>
  int array_read_dense(
      int ad,
      const T* range,
      const std::vector<int>& attribute_ids,
      void* buffer,
      int* buffer_size);



  /* ********************************* */
  /*           CELL ITERATORS          */
  /* ********************************* */

  /** 
   * Returns a (forward) constant cell iterator for an array associated with the
   * input descriptor.  
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @retrun A (forward) constant cell iterator, or *NULL* in case of error.
   */
  template<class T>
  ArrayConstCellIterator<T>* begin(int ad) const;

  /** 
   * Returns a (forward) constant cell iterator for an array associated with the
   * input descriptor. The iterator focuses only on the input attribute ids,
   * i.e., when it is dereferenced, it produces a cell having only attribute
   * values for the input attribute ids, deduplicated and sorted in increasing
   * order of attribute ids. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param attribute_ids a vector with the ids of the selected attributes.
   * @retrun A (forward) constant cell iterator focusing on the selected 
   * attributes, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */  
  template<class T>
  ArrayConstCellIterator<T>* begin(
      int ad, 
      const std::vector<int>& attribute_ids) const;

  /** 
   * Returns a (forward) constant cell iterator for an array associated with the
   * input descriptor. The iterator focuses only on the subarray determined by
   * the input range. 
   *
   * @tparam T The array coordinates type.
   * @param range A hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order. 
   * @retrun A (forward) constant cell iterator focusing on the selected range,
   * or *NULL* in case of error.
   */  
  template<class T>
  ArrayConstCellIterator<T>* begin(int ad, const T* range) const;

  /** 
   * Returns a (forward) constant cell iterator for an array associated with the
   * input descriptor. The iterator focuses only on the subarray determined by
   * the input range. It also focuses only on the input attribute ids,
   * i.e., when it is dereferenced, it produces a cell having only attribute
   * values for the input attribute ids, deduplicated and sorted in increasing
   * order of attribute ids. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param range A hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order  
   * @param attribute_ids A vector with the ids of the selected attributes. 
   * @retrun A (forward) constant cell iterator focusing on the selected range
   * and attribute ids, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */
  template<class T>
  ArrayConstCellIterator<T>* begin(
      int ad, const T* range,
      const std::vector<int>& attribute_ids) const;

  // TODO
  template<class T>
  ArrayConstCellIterator<T>* metadata_begin(
      int md, const T* range,
      const std::vector<int>& attribute_ids) const;

  // TODO
  template<class T>
  ArrayConstCellIterator<T>* metadata_begin(int md) const;

  /** 
   * Returns a (forward) constant dense cell iterator for an array associated 
   * with the input descriptor. The difference of a dense cell iterator with the
   * 'normal' ones is that, even if the input array is sparse, the dense cell 
   * iterator will go through **all** the cells in the global cell order
   * (i.e., even the empty ones), filling the empty cells with special empty 
   * values (e.g., 0 for numerics and NULL_CHAR, '*', for strings).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @retrun A (forward) constant dense cell iterator, or NULL in case of error.
   */
  template<class T>
  ArrayConstDenseCellIterator<T>* begin_dense(int ad) const;

  /** 
   * Returns a (forward) constant dense cell iterator for an array associated 
   * with the input descriptor. The iterator focuses only on the input attribute
   * ids, i.e., when it is dereferenced, it produces a cell having only 
   * attribute values for the input attribute ids, deduplicated and sorted in 
   * increasing order of attribute ids. The difference of a dense 
   * cell iterator with the 'normal' ones is that, even if the input array is 
   * sparse, the dense cell iterator will go through **all** the cells in the 
   * global cell order (i.e., even the empty ones), filling the empty cells with
   * special empty values (e.g., 0 for numerics and NULL_CHAR, '*', for 
   * strings).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param attribute_ids A vector with the ids of the selected attributes. 
   * @return A (forward) constant dense cell iterator focusing on the selected 
   * attributes, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */
  template<class T>
  ArrayConstDenseCellIterator<T>* begin_dense(
      int ad, 
      const std::vector<int>& attribute_ids) const;

  /** 
   * Returns a (forward) constant dense cell iterator for an array associated
   * with the input descriptor. The iterator focuses only on the subarray
   * determined by the input range. The difference of a dense cell iterator with
   * the 'normal' ones is that, even if the input array is sparse, the dense 
   * cell iterator will go through **all** the cells in the global cell order 
   * (i.e., even the empty ones), filling the empty cells with special empty 
   * values (e.g., 0 for numerics and NULL_CHAR, '*', for strings).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param range A hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order. 
   * @retrun A (forward) constant dense cell iterator focusing on the selected
   * range, or *NULL* in case of error.
   */
  template<class T>
  ArrayConstDenseCellIterator<T>* begin_dense(
      int ad, 
      const T* range) const;

  /** 
   * Returns a (forward) constant dense cell iterator for an array associated
   * with the input descriptor. The iterator focuses only on the subarray
   * determined by the input range. It also focuses only on the input attribute
   * ids, i.e., when it is dereferenced, it produces a cell having only 
   * attribute values for the input attribute ids, deduplicated and sorted in 
   * increasing order of attribute ids. The difference of a dense 
   * cell iterator with the 'normal' ones is that, even if the input array is
   * sparse, the dense cell iterator will go through **all** the cells in the
   * global cell order (i.e., even the empty ones), filling the empty cells with
   * special empty values (e.g., 0 for numerics and NULL_CHAR, '*', for 
   * strings).
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param range A hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order.  
   * @param attribute_ids A vector with the ids of the selected attributes. 
   * @retrun A (forward) constant dense cell iterator focusing on the selected
   * range and attribute ids, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */
  template<class T>
  ArrayConstDenseCellIterator<T>* begin_dense(
      int ad, 
      const T* range,
      const std::vector<int>& attribute_ids) const;

  /** 
   * Returns a reverse constant cell iterator for an array associated with the
   * input descriptor. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @retrun A reverse constant cell iterator, or *NULL* in case of error.
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(int ad) const;

  /** 
   * Returns a reverse constant cell iterator for an array associated with the
   * input descriptor. The iterator focuses only on the input attribute
   * ids, i.e., when it is dereferenced, it produces a cell having only 
   * attribute values for the input attribute ids, deduplicated and sorted in 
   * increasing order of attribute ids. 
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param attribute_ids A vector with the ids of the selected attributes. 
   * @retrun A reverse constant cell iterator focusing on the selected 
   * attributes, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, 
      const std::vector<int>& attribute_ids) const;

  /** 
   * Returns a reverse constant cell iterator for an array associated with the
   * input descriptor. The function takes as input a multi-dimensional object,
   * which may be either a range or a single cell (specifically, its 
   * coordinates). If it is a range, the iterator focuses only on the subarray
   * determined by the range. If it is a cell, then the iterator starts from
   * that cell or, if it does not exists, the cell after it in the reverse 
   * global cell order. The last argument determines if the multi-dimensional 
   * object is a range or a cell.  
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param multi_D_obj A multi-dimensional object that is either a range or a 
   * single cell (i.e., its coordinates). If it is a range, it is practically
   * a hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order. If it is a cell, the iterator will
   * start iterating from that cell or the cell after it in the reverse global
   * cell order.
   * @param is_range *True* if the input multi-dimensional object is a range, 
   * and *false* if it is a single cell.
   * @retrun A reverse constant cell iterator focusing on the selected range, or
   * starting at the input cell or the cell after it (if it does not exist) in 
   * the reverse global cell order, or *NULL* in case of error.
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, 
      const T* multi_D_obj,
      bool is_range = true) const;

  /** 
   * Returns a reverse constant cell iterator for an array associated with the
   * input descriptor. The function takes as input a multi-dimensional object,
   * which may be either a range or a single cell (specifically, its 
   * coordinates). If it is a range, the iterator focuses only on the subarray
   * determined by the range. If it is a cell, then the iterator starts from
   * that cell or, if it does not exists, the cell after it in the reverse 
   * global cell order. The last argument determines if the multi-dimensional 
   * object is a range or a cell. It also focuses only on the input attribute
   * ids, i.e., when it is dereferenced, it produces a cell having only 
   * attribute values for the input attribute ids, deduplicated and sorted in 
   * increasing order of attribute ids.
   *
   * @tparam T The array coordinates type.
   * @param ad The array descriptor.
   * @param multi_D_obj A multi-dimensional object that is either a range or a 
   * single cell (i.e., its coordinates). If it is a range, it is practically
   * a hyper-rectangle (dim#1_low, dim#1_high, dim#2_low, ...) that
   * orients a subarray (i.e., a subspace inside the array domain), in which the
   * iterator will focus on. The iterator will iterate over the cells in this
   * subarray in the global cell order. If it is a cell, the iterator will
   * start iterating from that cell or the cell after it in the reverse global
   * cell order.
   * @param attribute_ids A vector with the ids of the selected attributes. 
   * @param is_range *True* if the input multi-dimensional object is a range, 
   * and *false* if it is a single cell.
   * @return A reverse constant cell iterator focusing on the selected range, or
   * starting at the input cell or the cell after it (if it does not exist) in 
   * the reverse global cell order, or *NULL* in case of error.
   * @note The iterator will eventually encompass only **unique** attribute ids,
   * sorted in increasing order. If the programmer needs to produce the actual
   * binary cell including arbitrary attribute id orders and multiplicities,
   * he/she should check the Cell class, and functions Cell::cell() and 
   * Cell::csv_line().
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, 
      const T* multi_D_obj,
      const std::vector<int>& attribute_ids,
      bool is_range = true) const;

 private: 
  // PRIVATE ATTRIBUTES
  /** Stores all the open arrays. */
  Array** arrays_; 
  /** *True* if the constructor succeeded, or *false* otherwise. */
  bool created_successfully_;
  /** *True* if the object was finalized, or *false* otherwise. */
  bool finalized_;
  /** Stores all the open metadata. */
  Array** metadata_; 
  /** Keeps track of the descriptors of the currently open arrays. */
  OpenArrays open_arrays_; 
  /** Keeps track of the descriptors of the currently open metadata. */
  OpenMetadata open_metadata_; 

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Creates a "group file" in every group folder from the input workspace
   * up to the full group path. Note that both the inputs must be in
   * canonicalized absolute form. For instance, if *workspace* is
   * "/stavros/workspace" and *group* is "/workspace/group_A/group_B", a group
   * file will be created in folders "/stavros/workspace", 
   * "/stavros/workspace/group_A", and "/stavros/workspace/group_A/group_B".
   */
  int group_files_create(
      const std::string& workspace, 
      const std::string& group) const;

  /**
   * Checks if the input path shares a sub-path with an array directory.
   *
   * @param path The path to be tested.
   * @param real_path *True* if the input path is in canonicalized absolute
   * fromat, and *false* otherwise.
   * @return *True* if the input path shares a sub-bath with an array directory,
   * and *false* otherwise.
   */
  bool path_inside_array_directory(
      const std::string& path, 
      bool real_path = false) const;
}; 

#endif
