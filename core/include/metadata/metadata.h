/**
 * @file   metadata.h
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
 * This file defines class Metadata. 
 */

#ifndef __METADATA_H__
#define __METADATA_H__

#include "array.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_MT_OK     0
#define TILEDB_MT_ERR   -1

// TODO
class Metadata {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  
  /** Constructor. */
  Metadata();

  /** Destructor. */
  ~Metadata();

  // ACCESSORS

  // TODO
  bool overflow(int attribute_id) const;

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  // TODO
  Array* array() const;

  /** Returns the attribute ids the array focuses on. */
//  const std::vector<int>& attribute_ids() const;

  // TODO
//  std::vector<Fragment*> fragments() const;

  // TODO
//  int fragment_num() const;

  /** Returns the array mode. */
//  int mode() const;

  /** Returns the range in which the array is constrained. */
//  const void* range() const;

  // TODO
  int read(const char* key, void** buffers, size_t* buffer_sizes); 

  // MUTATORS

  // TODO
  int consolidate();

  // TODO
  int reset_attributes(const char** attributes, int attribute_num);
 
  int init(
      const ArraySchema* array_schema, 
      int mode,
      const char** attributes,
      int attribute_num);

  /**
   * Finalizes the array.
   *
   * @return TILEDB_AR_OK on success, and TILEDB_AR_ERR on error.
   */
  // TODO
  int finalize();

  // TODO
  int write(
      const char* keys,
      size_t keys_size,
      const void** buffers, 
      const size_t* buffer_sizes); 

 private:
  // PRIVATE ATTRIBUTES

  /** The array schema. */
//  const ArraySchema* array_schema_;
  // TODO
  Array* array_;
  /** 
   * The ids of the attributes the array is initialized with. Note that the
   * array may be initialized with a subset of attributes when writing or
   * reading.
   */
//  std::vector<int> attribute_ids_;
  /** The array fragments. */
//  std::vector<Fragment*> fragments_;
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
//  void* range_;

  // PRIVATE METHODS

  // TODO
  void compute_array_coords(
      const char* keys,
      size_t keys_size,
      size_t*& keys_offsets,
      size_t& keys_offsets_size,
      void*& coords,
      size_t& coords_size) const;

  // TODO
  void prepare_array_buffers(
      const char* keys,
      size_t keys_size,
      const size_t* keys_offsets,
      size_t keys_offsets_size,
      const void* coords,
      size_t coords_size,
      const void** buffers,
      const size_t* buffer_sizes,
      const void**& array_buffers,
      size_t*& array_buffer_sizes) const;
  
  /** 
   * Returns a new fragment name, which is in the form: <br>
   * .__<process_id>_<current_timestamp>
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character. Moreover, the fragment name
   * may change later by a consolidation process.
   */
//  std::string new_fragment_name() const;

  // TODO
//  int open_fragments();

  // TODO
//  void sort_fragment_names(std::vector<std::string>& fragment_names) const;
};

#endif
