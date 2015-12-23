/**
 * @file   array.h
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
 * This file defines class Array. 
 */

#ifndef __ARRAY_H__
#define __ARRAY_H__

#include "array_schema.h"
#include "fragment.h"
#include "global.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_AR_OK     0
#define TILEDB_AR_ERR   -1

class Fragment;

/**
 * Manages an array object. This is typically used for writing to and reading
 * from a TileDB array.
 */
class Array {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  
  /** Constructor. */
  Array();

  /** Destructor. */
  ~Array();

  // ACCESSORS

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the attribute ids the array focuses on. */
  const std::vector<int>& attribute_ids() const;

  /** Returns the array mode. */
  int mode() const;

  /** Returns the array range. */
  const void* range() const;

  // MUTATORS
 
  /**
   * Initializes an array object.
   *
   * @param array_schema The schema of the array.
   * @param mode The mode of the array. It must be one of the following:
   *    - TILEDB_WRITE 
   *    - TILEDB_WRITE_UNSORTED 
   *    - TILEDB_READ 
   *    - TILEDB_READ_REVERSE 
   * @param range The range in which the array read/write will be constrained.
   * @param attributes A subset of the array attributes the read/write will be
   *     constrained.
   * @param attribute_num The number of the input attributes.
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int init(
      const ArraySchema* array_schema, 
      int mode,
      const char** attributes,
      int attribute_num,
      const void* range);

  /**
   * Finalizes the array.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  int finalize();

  // TODO
  int write(const void** buffers, const size_t* buffer_sizes); 
  
 private:
  // PRIVATE ATTRIBUTES

  /** The array schema. */
  const ArraySchema* array_schema_;
  /** 
   * The ids of the attributes the array is initialized with. Note that the
   * array may be initialized with a subset of attributes when writing or
   * reading.
   */
  std::vector<int> attribute_ids_;
  /** The array fragments. */
  std::vector<Fragment*> fragments_;
  /** 
   * The array mode. It must be one of the following:
   *    - TILEDB_WRITE 
   *    - TILEDB_WRITE_UNSORTED 
   *    - TILEDB_READ 
   *    - TILEDB_READ_REVERSE 
   */
  int mode_;
  /**
   * The range in which the array is constrained. Note that the type of the
   * range must be the same as the type of the array coordinates.
   */
  void* range_;

  // PRIVATE METHODS
  
  /** 
   * Returns a new fragment name, which is in the form: <br>
   * .__<process_id>_<current_timestamp>
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character. Moreover, the fragment name
   * may change later by a consolidation process.
   */
  std::string new_fragment_name() const;
};

#endif
