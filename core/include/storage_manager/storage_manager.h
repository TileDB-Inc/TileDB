/**
 * @file   storage_manager.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H

#include "array.h"
#include "array_schema.h"
#include "fragment.h"
#include "tile.h"
#include "tiledb_error.h"
#include "utils.h"
#include <unistd.h>
#include <vector>
#include <string>
#include <map>
#include <limits>
#include <set>

/** 
 * A storage manager object is responsible for storing/fetching tiles to/from 
 * the disk. It maintains book-keeping structures in main memory to efficiently
 * locate the tile data on disk. 
 */
class StorageManager {
 public: 
  // TYPE DEFINITIONS
  /** Mnemonic: [array_name + "_" + array_name] --> array_descriptor */
  typedef std::map<std::string, int> OpenArrays;

  // CONSTRUCTORS & DESTRUCTORS
  /**
   * Upon its creation, a storage manager object needs a workspace path. The 
   * latter is a folder in the disk where the storage manager creates all the 
   * tile and book-keeping data. Note that the input path must 
   * exist. If the workspace folder exists, the function does nothing, 
   * otherwise it creates it. The segment size determines the amount of data 
   * exchanged in an I/O operation between the disk and the main memory. 
   * The MPI handler takes care of the MPI communication in the distributed
   * setting where there are multiple TileDB processes runnign simultaneously.
   */
  StorageManager(const std::string& path, 
                 size_t segment_size = SEGMENT_SIZE);
  /** When a storage manager object is deleted, it closes all open arrays. */
  ~StorageManager();

  // ACCESSORS
  /**
   * Returns the current error code. It is TILEDB_OK upon success.
   */
  int err() const;

  // MUTATORS
  /** Changes the default segment size. */
  void set_segment_size(size_t segment_size);
   
  // ARRAY FUNCTIONS
  /** Returns true if the array has been defined. */
  bool array_defined(const std::string& array_name) const;
  /** Deletes all the fragments of an array. The array remains defined. */
  int clear_array(const std::string& array_name);
  /** Closes an array. */
  void close_array(int ad);
  /** Defines an array (stores its array schema). */
  int define_array(const ArraySchema* array_schema);
  /** It deletes an array (regardless of whether it is open or not). */
  int delete_array(const std::string& array_name);
  /** 
   * Forces an array to close. This is done during abnormal execution. 
   * If the array was opened in write or append mode, the last fragment
   * must be deleted (since it was not properly loaded).
   */
  void forced_close_array(int ad);
  /** Returns the schema of an array. The input is an array descriptor. */
  int get_array_schema(
      int ad, const ArraySchema*& array_schema) const;
  /** Returns the schema of an array. */
  int get_array_schema(
      const std::string& array_name, 
      ArraySchema*& array_schema) const;
  /** Prints the version of the storage manager to the standard output. */
  static void get_version();
  /** 
   * Opens an array in the input mode. It returns an 'array descriptor',
   * which is used in subsequent array operations. Currently, the following
   * modes are supported:
   *
   * "r": Read mode
   *
   * "w": Write mode (if the array exists, it is deleted)
   *
   * "a": Append mode
   */
  int open_array(const std::string& array_name, const char* mode);

  // CELL FUNCTIONS
  /** Takes as input an array descriptor and returns a cell iterator. */
  template<class T>
  ArrayConstCellIterator<T>* begin(int ad) const;
  /** 
   * Takes as input an array descriptor and a list of attribute ids. It returns
   * a cell iterator that iterates over the specified attributes.
   */
  template<class T>
  ArrayConstCellIterator<T>* begin(
      int ad, const std::vector<int>& attribute_ids) const;
  /** 
   * Takes as input an array descriptor and a range. It returns a cell iterator
   * that iterates only over the cells whose coordinates lie within the input 
   * range (following the global cell order).    
   */
  template<class T>
  ArrayConstCellIterator<T>* begin(
      int ad, const T* range) const;
  /** 
   * Takes as input an array descriptor, a range, and a list of attribute
   * ids. It returns a cell iterator that iterates only over the cells
   * whose coordinates lie within the input range (following the global cell
   * order), and only on the specified attributes.
   */
  template<class T>
  ArrayConstCellIterator<T>* begin(
      int ad, const T* range,
      const std::vector<int>& attribute_ids) const;
  /** Takes as input an array descriptor and returns a cell iterator. */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(int ad) const;
  /** 
   * Takes as input an array descriptor and a list of attribute ids. It returns
   * a cell iterator that iterates over the specified attributes.
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, const std::vector<int>& attribute_ids) const;
  /** 
   * Takes as input an array descriptor and a range. It returns a cell iterator
   * that iterates only over the cells whose coordinates lie within the input 
   * range (following the global cell order).    
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, const T* range) const;
  /** 
   * Takes as input an array descriptor, a range, and a list of attribute
   * ids. It returns a cell iterator that iterates only over the cells
   * whose coordinates lie within the input range (following the global cell
   * order), and only on the specified attributes.
   */
  template<class T>
  ArrayConstReverseCellIterator<T>* rbegin(
      int ad, const T* range,
      const std::vector<int>& attribute_ids) const;
  /**
   * Takes as input an array descriptor and a multi-dimensional range, and 
   * returns the cells whose coordinates fall inside the range, as well as
   * their collective size in bytes (last two arguments).
   */
  void read_cells(int ad, const void* range, 
                  const std::vector<int>& attribute_ids,
                  void*& cells, size_t& cells_size) const;
  /**
   * Takes as input an array descriptor and a multi-dimensional range, and 
   * returns the cells whose coordinates fall inside the range, as well as
   * their collective size in bytes (last two arguments).
   */
  template<class T>
  void read_cells(int ad, const T* range, 
                  const std::vector<int>& attribute_ids,
                  void*& cells, size_t& cells_size) const;
  /**  
   * Writes a cell to an array. It takes as input an array descriptor, and
   * a cell pointer. The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  template<class T>
  void write_cell(int ad, const void* cell) const; 
  /**  
   * Writes a cell to an array. It takes as input an array descriptor, and
   * a cell pointer. The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema. This function
   * is used only when it is guaranteed that the cells are written
   * respecting the global cell order as specified in the array schema.
   */
  template<class T>
  void write_cell_sorted(int ad, const void* cell) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema.
   */
  void write_cells(int ad, const void* cells, size_t cells_size) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema.
   */
  template<class T>
  void write_cells(int ad, const void* cells, size_t cells_size) const; 

  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema. This function is used only when it is guaranteed that the
   * cells are written respecting the global cell order as specified in the
   * array schema.
   */
  void write_cells_sorted(int ad, const void* cells, size_t cells_size) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema. This function is used only when it is guaranteed that the
   * cells are written respecting the global cell order as specified in the
   * array schema.
   */
  template<class T>
  void write_cells_sorted(int ad, const void* cells, size_t cells_size) const; 

 private: 
  // PRIVATE ATTRIBUTES
  /** Stores all the open arrays. */
  Array** arrays_; 
  /** Error code (0 on success). */
  int err_;
  /** Keeps track of the descriptors of the currently open arrays. */
  OpenArrays open_arrays_;
  /** 
   * Determines the amount of data that can be exchanged between the 
   * hard disk and the main memory in a single I/O operation. 
   */
  size_t segment_size_;
  /** 
   * Is a folder in the disk where the storage manager creates 
   * all the array data (i.e., tile and index files). 
   */
  std::string workspace_;
  /** Max memory size of the write state when creating an array fragment. */
  size_t write_state_max_size_;
  
  // PRIVATE METHODS
  /** Checks when opening an array. */
  int check_on_open_array(
      const std::string& array_name, 
      const char* mode) const;
  /** Checks the validity of the array mode. */
  bool invalid_array_mode(const char* mode) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Stores an array object and returns an array descriptor. */
  int store_array(Array* array);
}; 

#endif
