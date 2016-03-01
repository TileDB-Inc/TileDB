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
#include <iostream>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_WS_OK     0
#define TILEDB_WS_ERR   -1

class BookKeeping;

/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:
  // TYPE DEFINITIONS

  /** Custom comparator in cell sorting. */
  template<typename T> class SmallerIdCol;
  /** Custom comparator in cell sorting. */
  template<typename T> class SmallerIdRow;
  /** Custom comparator in cell sorting. */
  template<class T> class SmallerRow;
  /** Custom comparator in cell sorting. */
  template<class T> class SmallerCol;

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
  int write(
      const void** buffers, 
      const size_t* buffer_sizes);

  // MISC

  // TODO 
  int finalize();

 private:
  // PRIVATE ATTRIBUTES

  /** The book-keeping structure of the fragment the write state belongs to. */
  BookKeeping* book_keeping_;
  /** The first and last coordinates of the tile currently being populated. */
  void* bounding_coords_;
  /**  
   * The current buffer offsets of the variable-sized attributes in their 
   * respective files.
   */
  std::vector<size_t> buffer_var_offsets_;
  /** The fragment the write state belongs to. */
  const Fragment* fragment_;
  /** The MBR of the tile currently being populated. */
  void* mbr_;
  /** The number of cells written in the current tile for each attribute. */
  std::vector<int64_t> tile_cell_num_;
  /** Internal buffers used in the case of compression. */
  std::vector<void*> tiles_;
  /** Offsets of the current tiles inside the files. */
  std::vector<size_t> tiles_var_file_offsets_;
  /** Offsets to the internal variable tile buffers. */
  std::vector<size_t> tiles_var_offsets_;
  /** Internal buffers used in the case of compression for variable tiles. */
  std::vector<void*> tiles_var_;
  /** 
   * Sizes of internal buffers used in the case of compression for variable 
   * tiles. 
   */
  std::vector<size_t> tiles_var_sizes_;
  /** Internal buffer used in the case of compression. */
  void* tile_compressed_;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_;
  /** Offsets to the internal tile buffers used in compression. */
  std::vector<size_t> tile_offsets_;

  // PRIVATE METHODS

  // TODO
  int compress_and_write_tile(int attribute_id);

  // TODO
  int compress_and_write_tile_var(int attribute_id);

  // TODO
  template<class T>
  void expand_mbr(const T* coords);

  // TODO
  void shift_var_offsets(
      int attribute_id,
      size_t buffer_var_size,
      const void* buffer,
      size_t buffer_size,
      void* shifted_buffer);

  // TODO
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  // TODO
  template<class T>
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  // TODO
  void update_book_keeping(const void* buffer, size_t buffer_size);

  // TODO
  template<class T>
  void update_book_keeping(const void* buffer, size_t buffer_size);

  // TODO
  void update_tile_cell_num(int attribute_id, int64_t buffer_cell_num);

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
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_dense_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_dense_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_sparse(
      const void** buffers, 
      const size_t* buffer_sizes);

  // TODO
  int write_sparse_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_sparse_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_sparse_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  // TODO
  int write_sparse_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_sparse_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_sparse_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  // TODO
  int write_sparse_unsorted(
      const void** buffers, 
      const size_t* buffer_sizes);

  // TODO
  int write_sparse_unsorted_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_sparse_unsorted_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_sparse_unsorted_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_sparse_unsorted_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_sparse_unsorted_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_sparse_unsorted_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  // TODO
  int write_last_tile();
};

/** Wrapper of comparison function for sorting cells. */
template<class T>
class WriteState::SmallerIdRow {
 public:
  /** Constructor. */
  SmallerIdRow(const T* buffer, int dim_num, const std::vector<int64_t>& ids) 
      : buffer_(buffer),
        dim_num_(dim_num),
        ids_(ids) { }

  /** Comparison operator. */
  bool operator () (int64_t a, int64_t b) {
    if(ids_[a] < ids_[b])
      return true;

    if(ids_[a] > ids_[b])
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for(int i=0; i<dim_num_; ++i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  int dim_num_;
  /** The cell ids. */
  const std::vector<int64_t>& ids_;
};

/** Wrapper of comparison function for sorting cells. */
template<class T>
class WriteState::SmallerIdCol {
 public:
  /** Constructor. */
  SmallerIdCol(const T* buffer, int dim_num, const std::vector<int64_t>& ids) 
      : buffer_(buffer),
        dim_num_(dim_num),
        ids_(ids) { }

  /** Comparison operator. */
  bool operator () (int64_t a, int64_t b) {
    if(ids_[a] < ids_[b])
      return true;

    if(ids_[a] > ids_[b])
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for(int i=dim_num_-1; i>=0; --i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  int dim_num_;
  /** The cell ids. */
  const std::vector<int64_t>& ids_;
};

/** Wrapper of comparison function for sorting cells. */
template<class T>
class WriteState::SmallerRow {
 public:
  /** Constructor. */
  SmallerRow(const T* buffer, int dim_num) 
      : buffer_(buffer),
        dim_num_(dim_num) { }

  /** Comparison operator. */
  bool operator () (int64_t a, int64_t b) {
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for(int i=0; i<dim_num_; ++i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  int dim_num_;
};

/** Wrapper of comparison function for sorting cells. */
template<class T>
class WriteState::SmallerCol {
 public:
  /** Constructor. */
  SmallerCol(const T* buffer, int dim_num) 
      : buffer_(buffer),
        dim_num_(dim_num) { }

  /** Comparison operator. */
  bool operator () (int64_t a, int64_t b) {
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for(int i=dim_num_-1; i>=0; --i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  int dim_num_;
};

#endif
