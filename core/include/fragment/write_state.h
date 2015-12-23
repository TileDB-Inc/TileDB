/**
 * @file   write_state.h
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
 * This file defines class WriteState. 
 */

#ifndef __WRITE_STATE_H__
#define __WRITE_STATE_H__

#include "book_keeping.h"
#include "fragment.h"
#include <vector>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_WS_OK     0
#define TILEDB_WS_ERR   -1

class BookKeeping;

/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:
  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the write state belongs to.
   */
  WriteState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~WriteState();

  // WRITE FUNCTIONS
 
  // TODO 
  void flush();
 
  // TODO
  int write(
      const void** buffers, 
      const size_t* buffer_sizes);

 private:
  // PRIVATE ATTRIBUTES

  /** The book-keeping structure of the fragment the write state belongs to. */
  BookKeeping* book_keeping_;
  /** The number of cells written in the current tile. */
  std::vector<std::int64_t> current_tile_cell_num_;
  /** Internal buffers used in the case of compression. */
  std::vector<void*> current_tiles_;
  /** Internal buffers used in the case of compression. */
  std::vector<void*> current_tiles_compressed_;
  /** Offsets to the internal curent_tiles_ buffers used in compression. */
  std::vector<size_t> current_tile_offsets_;
  /** The fragment the write state belongs to. */
  const Fragment* fragment_;
  /** Internal buffers used in the case of compression. */
  std::vector<void*> segments_;
  /** The offsets inside the segments_ buffers. */
  std::vector<size_t> segment_offsets_;
  /** Total number of cells written. */
  std::vector<std::int64_t> total_cell_num_;

  // PRIVATE METHODS

  // TODO
  ssize_t compress_tile_into_segment(
      int attribute_id,
      void* tile,
      size_t tile_size);

  /** True if the coordinates are included in the fragment attributes. */
  bool has_coords() const;

  /** Iniatializes a tile buffer for the input attribute. */
  void init_current_tile(int attribute_id); 

  /** Iniatializes a tile buffer for the input attribute used in compression. */
  void init_current_tile_compressed(int attribute_id); 

  /** Iniatializes a segment for the input attribute. */
  void init_segment(int attribute_id); 

  // TODO
  int write_dense(
      const void** buffers, 
      const size_t* buffer_sizes);

  // TODO
  int write_dense_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_dense_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_dense_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_dense_attr_var(
      int attribute_id,
      const void* buffer_val, 
      size_t buffer_val_size,
      const void* buffer_val_num, 
      size_t buffer_val_num_size);

  // TODO
  int write_dense_attr_var_cmp_none(
      int attribute_id,
      const void* buffer_val, 
      size_t buffer_val_size,
      const void* buffer_val_num, 
      size_t buffer_val_num_size);

  // TODO
  int write_dense_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer_val, 
      size_t buffer_val_size,
      const void* buffer_val_num, 
      size_t buffer_val_num_size);

  // TODO
  int write_dense_in_range(
      const void** buffers, 
      const size_t* buffer_sizes);

  // TODO
  int write_sparse(
      const void** buffers, 
      const size_t* buffer_sizes);

  // TODO
  int write_sparse_unsorted(
      const void** buffers, 
      const size_t* buffer_sizes);


  // TODO
  int write_segment_to_file(int attribute_id);
};


#endif
