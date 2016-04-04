/**
 * @file   write_state.h
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

/**@{*/
/** Return code. */
#define TILEDB_WS_OK        0
#define TILEDB_WS_ERR      -1
/**@}*/

class BookKeeping;




/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**@{*/
  /** Custom comparator in cell sorting. */
  template<typename T> class SmallerIdCol;
  template<typename T> class SmallerIdRow;
  template<class T> class SmallerCol;
  template<class T> class SmallerRow;
  /**@}*/




  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the write state belongs to.
   * @param book_keeping The book-keeping *fragment*.
   */
  WriteState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~WriteState();




  /* ********************************* */
  /*              MUTATORS             */
  /* ********************************* */

  /**
   * Finalizes the fragment.
   *
   * @return TILEDB_WS_OK for success and TILEDB_WS_ERR for error. 
   */
  int finalize();
  
  /**
   * Performs a write operation in the fragment. The cell values are provided
   * in a set of buffers (one per attribute specified upon the array 
   * initialization). Note that there must be a one-to-one correspondance
   * between the cell values across the attribute buffers.
   *
   * The array must have been initialized in one of the following write modes,
   * each of which having a different behaviour:
   *    - TILEDB_ARRAY_WRITE: \n
   *      In this mode, the cell values are provided in the buffers respecting
   *      the cell order on the disk. It is practically an **append** operation,
   *      where the provided cell values are simply written at the end of
   *      their corresponding attribute files.  
   *    - TILEDB_ARRAY_WRITE_UNSORTED: \n
   *      This mode is applicable to sparse arrays, or when writing sparse
   *      updates to a dense array. One of the buffers holds the coordinates.
   *      The cells in this mode are given in an arbitrary, unsorted order
   *      (i.e., without respecting how the cells must be stored on the disk
   *      according to the array schema definition). 
   * 
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     Array::init() or Array::reset_attributes(). The case of variable-sized
   *     attributes is special. Instead of providing a single buffer for such an
   *     attribute, **two** must be provided: the second holds the
   *     variable-sized cell values, whereas the first holds the start offsets
   *     of each cell in the second buffer.
   * @param buffer_sizes The sizes (in bytes) of the input buffers (there is
   *     a one-to-one correspondence).
   * @return TILEDB_WS_OK for success and TILEDB_WS_ERR for error.
   */
  int write(
      const void** buffers, 
      const size_t* buffer_sizes);




 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The book-keeping structure of the fragment the write state belongs to. */
  BookKeeping* book_keeping_;
  /** The first and last coordinates of the tile currently being populated. */
  void* bounding_coords_;
  /**  
   * The current offsets of the variable-sized attributes in their 
   * respective files, or alternatively, the current file size of each
   * variable-sized attribute.
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




  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Compresses the current tile for the input attribute, and writes (appends)
   * it to its corresponding file on the disk.
   *
   * @param attribute_id The id of the attribute whose tile is compressed and
   *     written.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_and_write_tile(int attribute_id);

  /**
   * Compresses the current variable-sized tile for the input attribute, and
   * writes (appends) it to its corresponding file on the disk.
   *
   * @param attribute_id The id of the attribute whose tile is compressed and
   *     written.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int compress_and_write_tile_var(int attribute_id);

  /**
   * Expands the current MBR with the input coordinates.
   *
   * @template T The type of the MBR and the input coordinates.
   * @param coords The input coordinates.
   * @return void
   */
  template<class T>
  void expand_mbr(const T* coords);

  /**
   * Shifts the offsets of the variable-sized cells recorded in the input
   * buffer, so that they correspond to the actual offsets in the corresponding
   * attribute file.
   *
   * @param attribute_id The id of the attribute whose variable cell offsets are
   *     shifted.
   * @param buffer_var_size The total size of the variable-sized cells written
   *     during this write operation.
   * @param buffer Holds the offsets of the variable-sized cells for this write
   *     operation.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param shifted_buffer Will hold the new shifted offsets.
   * @return void
   */
  void shift_var_offsets(
      int attribute_id,
      size_t buffer_var_size,
      const void* buffer,
      size_t buffer_size,
      void* shifted_buffer);

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   * 
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void 
   */
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  /**
   * Sorts the input cell coordinates according to the order specified in the
   * array schema. This is not done in place; the sorted positions are stored
   * in a separate vector.
   * 
   * @template T The type of coordinates stored in *buffer*.
   * @param buffer The buffer holding the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void 
   */
  template<class T>
  void sort_cell_pos(
      const void* buffer, 
      size_t buffer_size,
      std::vector<int64_t>& cell_pos) const;

  /**
   * Updates the book-keeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  void update_book_keeping(const void* buffer, size_t buffer_size);

  /**
   * Updates the book-keeping structures as tiles are written. Specifically, it
   * updates the MBR and bounding coordinates of each tile.
   *
   * @template T The coordinates type.
   * @param buffer The buffer storing the cell coordinates.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @return void
   */
  template<class T>
  void update_book_keeping(const void* buffer, size_t buffer_size);

  /**
   * Takes the appropriate actions for writing the very last tile of this write
   * operation, such as updating the book-keeping structures, and compressing
   * and writing the last tile on the disk. This is done for every attribute.
   *
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_last_tile();

  /**
   * Performs the write operation for the case of a dense fragment.
   *
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single fixed-sized attribute and the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a dense fragment, focusing
   * on a single variable-sized attribute and the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_dense_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single fixed-sized attribute and the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute and the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment, focusing
   * on a single variable-sized attribute and the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted.
   *
   * @param buffers See write().
   * @param buffer_sizes See write().
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted(
      const void** buffers, 
      const size_t* buffer_sizes);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute and
   * the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single fixed-sized attribute and
   * the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write().
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute and
   * the case of no compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var_cmp_none(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);

  /**
   * Performs the write operation for the case of a sparse fragment when the 
   * coordinates are unsorted, focusing on a single variable-sized attribute and
   * the case of GZIP compression.
   *
   * @param attribute_id The id of the attribute this operation focuses on.
   * @param buffer See write() - start offsets in *buffer_var*.
   * @param buffer_size See in write().
   * @param buffer_var See write() - actual variable-sized values.
   * @param buffer_size See write().
   * @param cell_pos The sorted positions of the cells.
   * @return TILEDB_WS_OK on success and TILEDB_WS_ERR on error.
   */
  int write_sparse_unsorted_attr_var_cmp_gzip(
      int attribute_id,
      const void* buffer, 
      size_t buffer_size,
      const void* buffer_var, 
      size_t buffer_var_size,
      const std::vector<int64_t>& cell_pos);
};

/** 
 * Wrapper of comparison function for sorting cells; first by the smallest id,
 * and then by column-major order of coordinates. 
 */
template<class T>
class WriteState::SmallerIdCol {
 public:
  /** 
   * Constructor. 
   * 
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   * @param ids The ids of the cells in the buffer.
   */
  SmallerIdCol(const T* buffer, int dim_num, const std::vector<int64_t>& ids) 
      : buffer_(buffer),
        dim_num_(dim_num),
        ids_(ids) { }

  /**
   * Comparison operator. 
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
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

/** 
 * Wrapper of comparison function for sorting cells; first by the smallest id,
 * and then by row-major order of coordinates. 
 */
template<class T>
class WriteState::SmallerIdRow {
 public:
  /** 
   * Constructor. 
   * 
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   * @param ids The ids of the cells in the buffer.
   */
  SmallerIdRow(const T* buffer, int dim_num, const std::vector<int64_t>& ids) 
      : buffer_(buffer),
        dim_num_(dim_num),
        ids_(ids) { }

  /**
   * Comparison operator. 
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
  bool operator () (int64_t a, int64_t b) {
    if(ids_[a] < ids_[b])
      return true;

    if(ids_[a] > ids_[b])
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for(int i=0; i<dim_num_; ++i) { 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue
    }

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

/** Wrapper of comparison function for sorting cells on column-major order. */
template<class T>
class WriteState::SmallerCol {
 public:
  /** 
   * Constructor. 
   * 
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   */
  SmallerCol(const T* buffer, int dim_num) 
      : buffer_(buffer),
        dim_num_(dim_num) { }

  /**
   * Comparison operator. 
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
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

/** Wrapper of comparison function for sorting cells on row-major order. */
template<class T>
class WriteState::SmallerRow {
 public:
  /** 
   * Constructor. 
   * 
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   */
  SmallerRow(const T* buffer, int dim_num) 
      : buffer_(buffer),
        dim_num_(dim_num) { }

  /**
   * Comparison operator. 
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
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

#endif
