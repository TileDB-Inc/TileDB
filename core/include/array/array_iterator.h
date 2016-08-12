/**
 * @file   array_iterator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * This file defines class ArrayIterator. 
 */

#ifndef __ARRAY_ITERATOR_H__
#define __ARRAY_ITERATOR_H__

#include "array.h"




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_AIT_OK        0
#define TILEDB_AIT_ERR      -1
/**@}*/


/** Default error message. */
#define TILEDB_AIT_ERRMSG std::string("[TileDB::ArrayIterator] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern std::string tiledb_ait_errmsg;




/** Enables iteration (read) over an array's cells. */
class ArrayIterator {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  
  /** Constructor. */
  ArrayIterator();

  /** Destructor. */
  ~ArrayIterator();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Return the array name. */
  const std::string& array_name() const;

  /**
   * Checks if the the iterator has reached its end.
   *
   * @return *true* if the iterator has reached its end and *false* otherwise.
   */
  bool end() const;

  /** 
   * Retrieves the current cell value for a particular attribute.
   *
   * @param attribute_id The id of the attribute for which the cell value
   *     is retrieved. This id corresponds to the position of the attribute name
   *     placed in the *attributes* input of Array::init() with which the array
   *     was initialized. If *attributes* was NULL in the above function, then
   *     the attribute id corresponds to the order in which the attributes were
   *     defined in the array schema upon the array creation. Note that, in that
   *     case, the extra coordinates attribute corresponds to the last extra
   *     attribute, i.e., its id is *attribute_num*. 
   * @param value The cell value to be retrieved. Note that its type is the
   *     same as that defined in the array schema.
   * @param value_size The size (in bytes) of the retrieved value.
   * @return TILEDB_AIT_OK on success, and TILEDB_AIT_ERR on error.
   */
  int get_value(int attribute_id, const void** value, size_t* value_size) const;




  /* ********************************* */
  /*             MUTATORS              */
  /* ********************************* */

  /**
   * Initializes an iterator for reading cells from an initialized array.  
   *
   * @param array The array the iterator is initialized for.
   * @param buffers This is an array of buffers similar to Array::read().
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
   * @return TILEDB_AIT_OK on success, and TILEDB_AIT_ERR on error.
   */
  int init(
      Array* array, 
      void** buffers, 
      size_t* buffer_sizes);

  /**
   * Finalizes the array iterator, properly freeing the allocating memory space.
   * 
   * @return TILEDB_AIT_OK on success, and TILEDB_AIT_ERR on error.
   */
  int finalize();

  /**
   * Advances the iterator by one cell.
   *
   * @return TILEDB_AIT_OK on success, and TILEDB_AIT_ERR on error.
   */
  int next();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the iterator works on. */
  Array* array_;

  /** The buffers used for prefetching. */
  void** buffers_;

  /** 
   * The corresponding sizes of *buffers*. These will be likely changed upon
   * each prefetching.
   */
  size_t* buffer_sizes_;

  /**
   *  The original corresponding sizes of *buffers*, as they were passed in
   * init(). 
   */
  std::vector<size_t> buffer_allocated_sizes_;

  /** A flag indicating whether the iterator has reached its end. */
  bool end_;

  /**
   * The position of the current value the iterator is on in the currently
   * buffered cells in *buffers*. A separate position is maintained for each
   * attribute buffer.
   */
  std::vector<int64_t> pos_;

  /** The number of cells currently in *buffers* (one per attribute). */
  std::vector<int64_t> cell_num_;

  /** Stores the cell size of each attribute. */
  std::vector<size_t> cell_sizes_;

  /** The index of the *buffers* element each attribute corresponds to. */
  std::vector<int> buffer_i_;
  
  /** Number of variable attributes. */
  int var_attribute_num_;
};

#endif
