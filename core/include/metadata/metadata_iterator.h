/**
 * @file   metadata_iterator.h
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
 * This file defines class MetadataIterator. 
 */

#ifndef __METADATA_ITERATOR_H__
#define __METADATA_ITERATOR_H__

#include "array_iterator.h"
#include "metadata.h"




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_MIT_OK          0
#define TILEDB_MIT_ERR        -1
/**@}*/

/** Default error message. */
#define TILEDB_MIT_ERRMSG std::string("[TileDB::MetadataIterator] Error: ")




/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

extern std::string tiledb_mit_errmsg;




/** Enables iteration (read) over metadata values. */
class MetadataIterator {
 public:
  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */
  
  /** Constructor. */
  MetadataIterator();

  /** Destructor. */
  ~MetadataIterator();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Return the metadata name. */
  const std::string& metadata_name() const;

  /**
   * Checks if the the iterator has reached its end.
   *
   * @return *true* if the iterator has reached its end and *false* otherwise.
   */
  bool end() const;

  /** 
   * Retrieves the current value for a particular attribute.
   *
   * @param attribute_id The id of the attribute for which the value
   *     is retrieved. This id corresponds to the position of the attribute name
   *     placed in the *attributes* input of init(). If *attributes* was NULL in
   *     the above function, then the attribute id corresponds to the order in
   *     which the attributes were defined in the array schema upon the array
   *     creation. Note that, in that case, the extra key attribute corresponds
   *     to the last extra attribute, i.e., its id is *attribute_num*. 
   * @param value The value to be retrieved. Note that its type is the
   *     same as that defined in the metadata schema.
   * @param value_size The size (in bytes) of the retrieved value.
   * @return TILEDB_MIT_OK on success, and TILEDB_MIT_ERR on error.
   */
  int get_value(int attribute_id, const void** value, size_t* value_size) const;

  // MUTATORS 

  /**
   * Finalizes the iterator, properly freeing the allocating memory space.
   * 
   * @return TILEDB_MIT_OK on success, and TILEDB_MIT_ERR on error.
   */
  int finalize();

  /**
   * Initializes a metadata iterator on an already initialized metadata object. 
   *
   * @param metadata The metadata the iterator is initialized for.
   * @param buffers This is an array of buffers similar to Metadata::read.
   *     It is the user that allocates and provides buffers that the iterator
   *     will use for internal buffering of the read values. The iterator will
   *     read from the disk the values in batches, by fitting as many
   *     values as possible in the user buffers. This gives the user the
   *     flexibility to control the prefetching for optimizing performance 
   *     depending on the application. 
   * @param buffer_sizes The corresponding sizes (in bytes) of the allocated 
   *     memory space for *buffers*. The function will prefetch from the
   *     disk as many values as can fit in the buffers, whenever it finishes
   *     iterating over the previously prefetched data.
   * @return TILEDB_MIT_OK on success, and TILEDB_MIT_ERR on error.
   */
  int init(
      Metadata* metadata, 
      void** buffers, 
      size_t* buffer_sizes);

  /**
   * Advances the iterator by one position.
   *
   * @return TILEDB_MIT_OK on success, and TILEDB_MIT_ERR on error.
   */
  int next();

 private:
  // PRIVATE ATTRIBUTES

  /** The array iterator that implements the metadata iterator. */
  ArrayIterator* array_it_;
  /** The metadata this iterator belongs to. */
  Metadata* metadata_;
};

#endif
