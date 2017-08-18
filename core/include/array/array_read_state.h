/**
 * @file   array_read_state.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file defines class ArrayReadState.
 */

#ifndef TILEDB_ARRAY_READ_STATE_H
#define TILEDB_ARRAY_READ_STATE_H

#include <cinttypes>
#include <cstring>
#include <queue>
#include <vector>
#include "array_schema.h"
#include "query.h"

namespace tiledb {

class Query;
class ReadState;

/** Stores the state necessary when reading cells from the array fragments. */
class ArrayReadState {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /**
   * Class of fragment cell range objects used in the priority queue algorithm.
   */
  template <class T>
  class PQFragmentCellRange;

  /**
   * Wrapper of comparison function in the priority queue of the fragment cell
   * ranges.
   */
  template <class T>
  class SmallerPQFragmentCellRange;

  /** A cell position pair [first, second]. */
  typedef std::pair<int64_t, int64_t> CellPosRange;

  /** A pair [fragment_id, tile_pos]. */
  typedef std::pair<int, int64_t> FragmentInfo;

  /** A pair of fragment info and fragment cell position range. */
  typedef std::pair<FragmentInfo, CellPosRange> FragmentCellPosRange;

  /** A vector of fragment cell posiiton ranges. */
  typedef std::vector<FragmentCellPosRange> FragmentCellPosRanges;

  /** A vector of vectors of fragment cell position ranges. */
  typedef std::vector<FragmentCellPosRanges*> FragmentCellPosRangesVec;

  /**
   * A pair of fragment info and cell range, where the cell range is defined
   * by two bounding coordinates.
   */
  typedef std::pair<FragmentInfo, void*> FragmentCellRange;

  /** A vector of fragment cell ranges. */
  typedef std::vector<FragmentCellRange> FragmentCellRanges;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array The array this array read state belongs to.
   */
  ArrayReadState(Query* query);

  /** Destructor. */
  ~ArrayReadState();

  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

  /** Indicates whether the read on at least one attribute overflowed. */
  bool overflow() const;

  /** Indicates whether the read on a particular attribute overflowed. */
  bool overflow(int attribute_id) const;

  /**
   * Performs a read operation in an array, which must be initialized with mode
   * TILEDB_ARRAY_READ. The function retrieves the result cells that lie inside
   * the subarray specified in Array::init() or Array::reset_subarray(). The
   * results are written in input buffers provided by the user, which are also
   * allocated by the user. Note that the results are written in the buffers in
   * the same order they appear on the disk, which leads to maximum performance.
   *
   * @param buffers An array of buffers, one for each attribute. These must be
   *     provided in the same order as the attributes specified in
   *     Array::init() or Array::reset_attributes(). The case of variable-sized
   *     attributes is special. Instead of providing a single buffer for such an
   *     attribute, **two** must be provided: the second will hold the
   *     variable-sized cell values, whereas the first holds the start offsets
   *     of each cell in the second buffer.
   * @param buffer_sizes The sizes (in bytes) allocated by the user for the
   *     input buffers (there is a one-to-one correspondence). The function will
   *     attempt to write as many results as can fit in the buffers, and
   *     potentially alter the buffer size to indicate the size of the *useful*
   *     data written in the buffer. If a buffer cannot hold all results, the
   *     function will still succeed, writing as much data as it can and turning
   *     on an overflow flag which can be checked with function overflow(). The
   *     next invocation will resume for the point the previous one stopped,
   *     without inflicting a considerable performance penalty due to overflow.
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read(void** buffers, size_t* buffer_sizes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Query* query_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The number of array attributes. */
  int attribute_num_;

  /** The size of the array coordinates. */
  size_t coords_size_;

  /** Indicates whether the read operation for this query is done. */
  bool done_;

  /** State per attribute indicating the number of empty cells written. */
  std::vector<int64_t> empty_cells_written_;

  /**
   * The bounding coordinates of the current tiles for all fragments. Applicable
   * only to the **sparse** array case.
   */
  std::vector<void*> fragment_bounding_coords_;

  /** Holds the fragment cell positions ranges of all active read rounds. */
  FragmentCellPosRangesVec fragment_cell_pos_ranges_vec_;

  /** Practically records which read round each attribute is on. */
  std::vector<int64_t> fragment_cell_pos_ranges_vec_pos_;

  /** Number of array fragments. */
  int fragment_num_;

  /** Stores the read state of each fragment. */
  std::vector<ReadState*> fragment_read_states_;

  /**
   * The minimum bounding coordinates end point. Applicable only to the
   * **sparse** array case.
   */
  void* min_bounding_coords_end_;

  /** Indicates overflow for each attribute. */
  std::vector<bool> overflow_;

  /** Indicates whether the current read round is done for each attribute. */
  std::vector<bool> read_round_done_;

  /** The current tile coordinates of the query subarray. */
  void* subarray_tile_coords_;

  /** The tile domain of the query subarray. */
  void* subarray_tile_domain_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Cleans fragment cell positions that are processed by all attributes. */
  void clean_up_processed_fragment_cell_pos_ranges();

  /**
   * Computes the cell position ranges that must be copied from each fragment to
   * the user buffers for the current read round. The cell positions are
   * practically the relative positions of the cells in their tile on the
   * disk. The function properly cleans up the input fragment cell ranges.
   *
   * @tparam T The coordinates type.
   * @param fragment_cell_ranges The input fragment cell ranges.
   * @param fragment_cell_pos_ranges The output fragment cell position ranges.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status compute_fragment_cell_pos_ranges(
      FragmentCellRanges& fragment_cell_ranges,
      FragmentCellPosRanges& fragment_cell_pos_ranges) const;

  /**
   * Computes the smallest end bounding coordinates for the current read round.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void compute_min_bounding_coords_end();

  /**
   * Computes the relevant fragment cell ranges for the current read run,
   * focusing on the **dense* array case. These cell ranges will be properly
   * cut and sorted later on.
   *
   * @tparam T The coordinates type.
   * @param unsorted_fragment_cell_ranges It will hold the result of this
   *     function.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status compute_unsorted_fragment_cell_ranges_dense(
      std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges);

  /**
   * Computes the relevant fragment cell ranges for the current read run,
   * focusing on the **sparse* array case. These cell ranges will be properly
   * cut and sorted later on. This function also properly updates the start
   * bounding coordinates of the active tiles (to exceed the minimum bounding
   * coordinates end).
   *
   * @tparam T The coordinates type.
   * @param unsorted_fragment_cell_ranges It will hold the result of this
   *     function.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status compute_unsorted_fragment_cell_ranges_sparse(
      std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the read copy be performed into.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @return TILEDB_ARS on success and TILEDB_ARS_ERR on error.
   */
  Status copy_cells(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer.
   *
   * @tparam T The attribute type.
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the read copy be performed into.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param empty_type_value a reference to the empty type value
   * @param empty_type_size the size (in bytes) of the empty type value
   * @return Status
   */
  Status copy_cells_generic(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const void* empty_type_value,
      size_t empty_type_size);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer, focusing on a **variable-sized** attribute.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the read will be performed into - offsets of
   *     cells in *buffer_var*.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param buffer_var The buffer where the copy will be performed into - actual
   *     variable-sized cell values.
   * @param buffer_var_size The size (in bytes) of *buffer_var*.
   * @param buffer_var_offset The offset in *buffer_var* where the copy will
   *     start from.
   * @return TILEDB_ARS on success and TILEDB_ARS_ERR on error.
   */
  Status copy_cells_var(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer, focusing on a **variable-sized** attribute.
   *
   * @tparam T The attribute type.
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the read will be performed into - offsets of
   *     cells in *buffer_var*.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param buffer_var The buffer where the copy will be performed into - actual
   *     variable-sized cell values.
   * @param buffer_var_size The size (in bytes) of *buffer_var*.
   * @param buffer_var_offset The offset in *buffer_var* where the copy will
   *     start from.
   * @param empty_type_value a reference to the empty type value
   * @param empty_type_size the size (in bytes) of the empty type value
   * @return Status
   */
  Status copy_cells_var_generic(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const void* empty_type_value,
      size_t empty_type_size);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer, filling with special empty values.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the copy will be performed into.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param cell_pos_range The cell range to be copied.
   * @param empty_type_value a reference to the empty type value
   * @param empty_type_size the size (in bytes) of the empty type value
   */
  void copy_cells_with_empty_generic(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const CellPosRange& cell_pos_range,
      const void* empty_type_val,
      size_t emtpy_type_size);

  /**
   * Copies the cell ranges calculated in the current read round into the
   * targeted attribute buffer, feeling with special empty values, and focusing
   * on a **variable-sized** attribute.
   *
   * @tparam T The attribute type.
   * @param attribute_id The id of the targeted attribute.
   * @param buffer The buffer where the read will be performed into - offsets of
   *     cells in *buffer_var*.
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param buffer_var The buffer where the copy will be performed into - actual
   *     variable-sized cell values.
   * @param buffer_var_size The size (in bytes) of *buffer_var*.
   * @param buffer_var_offset The offset in *buffer_var* where the copy will
   *     start from.
   * @param cell_pos_range The cell range to be copied.
   * @param empty_type_val a reference to the empty type value
   * @param emtpy_type_size the size (in bytes) of the empty type value
   */
  void copy_cells_with_empty_var_generic(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const CellPosRange& cell_pos_range,
      const void* empty_type_val,
      size_t empty_type_size);

  /**
   * Returns a list of cell ranges accounting for the empty area in the overlap
   * between the subarray query and the current overlapping tile.
   *
   * @return A list of cell ranges representing empty cells.
   */
  template <class T>
  FragmentCellRanges empty_fragment_cell_ranges() const;

  /**
   * Gets the next fragment cell ranges that are relevant in the current read
   * round, focusing on the dense case.
   *
   * @tparam T The coordinates type.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status get_next_fragment_cell_ranges_dense();

  /**
   * Gets the next fragment cell ranges that are relevant in the current read
   * round, focusing on the sparse case.
   *
   * @tparam T The coordinates type.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status get_next_fragment_cell_ranges_sparse();

  /**
   * Gets the next overlapping tiles in the fragment read states, for the case
   * of **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void get_next_overlapping_tiles_dense();

  /**
   * Gets the next overlapping tiles in the fragment read states, for the case
   * of **sparse** arrays.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void get_next_overlapping_tiles_sparse();

  /**
   * Gets the next subarray tile coordinates inside the subarray tile domain.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void get_next_subarray_tile_coords();

  /**
   * Initializes the tile coordinates falling in the query subarray. Applicable
   * only to the **dense** array case.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void init_subarray_tile_coords();

  /**
   * Performs a read operation in a **dense** array.
   *
   * @param buffers See read().
   * @param buffer_sizes See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_dense(void** buffers, size_t* buffer_sizes);

  /**
   * Performs a read operation in a **dense** array, focusing on a single
   * attribute.
   *
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read().
   * @param buffer_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_dense_attr(int attribute_id, void* buffer, size_t& buffer_size);

  /**
   * Performs a read operation in a **dense** array, focusing on a single
   * attribute.
   *
   * @tparam T The coordinates type.
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read().
   * @param buffer_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  template <class T>
  Status read_dense_attr(int attribute_id, void* buffer, size_t& buffer_size);

  /**
   * Performs a read operation in a **dense** array, focusing on a single
   * **variable-sized** attribute.
   *
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read() - start offsets in *buffer_var*.
   * @param buffer_size See read().
   * @param buffer_var See read() - actual variable-sized cell values.
   * @param buffer_var_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_dense_attr_var(
      int attribute_id,
      void* buffer,
      size_t& buffer_size,
      void* buffer_var,
      size_t& buffer_var_size);

  /**
   * Performs a read operation in a **dense** array, focusing on a single
   * **variable-sized** attribute.
   *
   * @tparam T The coordinates type.
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read() - start offsets in *buffer_var*.
   * @param buffer_size See read().
   * @param buffer_var See read() - actual variable-sized cell values.
   * @param buffer_var_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  template <class T>
  Status read_dense_attr_var(
      int attribute_id,
      void* buffer,
      size_t& buffer_size,
      void* buffer_var,
      size_t& buffer_var_size);

  /**
   * Performs a read operation in a **sparse** array.
   *
   * @param buffers See read().
   * @param buffer_sizes See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_sparse(void** buffers, size_t* buffer_sizes);

  /**
   * Performs a read operation in a **sparse** array, focusing on a single
   * attribute.
   *
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read().
   * @param buffer_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_sparse_attr(int attribute_id, void* buffer, size_t& buffer_size);

  /**
   * Performs a read operation in a **sparse** array, focusing on a single
   * attribute.
   *
   * @tparam T The coordinates type.
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read().
   * @param buffer_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  template <class T>
  Status read_sparse_attr(int attribute_id, void* buffer, size_t& buffer_size);

  /**
   * Performs a read operation in a **sparse** array, focusing on a single
   * **variable-sized** attribute.
   *
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read() - start offsets in *buffer_var*.
   * @param buffer_size See read().
   * @param buffer_var See read() - actual variable-sized cell values.
   * @param buffer_var_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  Status read_sparse_attr_var(
      int attribute_id,
      void* buffer,
      size_t& buffer_size,
      void* buffer_var,
      size_t& buffer_var_size);

  /**
   * Performs a read operation in a **sparse** array, focusing on a single
   * **variable-sized** attribute.
   *
   * @tparam T Coordinates type.
   * @param attribute_id The attribute this read focuses on.
   * @param buffer See read() - start offsets in *buffer_var*.
   * @param buffer_size See read().
   * @param buffer_var See read() - actual variable-sized cell values.
   * @param buffer_var_size See read().
   * @return TILEDB_ARS_OK for success and TILEDB_ARS_ERR for error.
   */
  template <class T>
  Status read_sparse_attr_var(
      int attribute_id,
      void* buffer,
      size_t& buffer_size,
      void* buffer_var,
      size_t& buffer_var_size);

  /**
   * Uses the heap algorithm to cut and sort the relevant cell ranges for
   * the current read run. The function properly cleans up the input
   * unsorted fragment cell ranges.
   *
   * @tparam T The coordinates type.
   * @param unsorted_fragment_cell_ranges The unsorted fragment cell ranges.
   * @param fragment_cell_ranges The sorted fragment cell ranges output by
   *     the function as a result.
   * @return TILEDB_ARS_OK on success and TILEDB_ARS_ERR on error.
   */
  template <class T>
  Status sort_fragment_cell_ranges(
      std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges,
      FragmentCellRanges& fragment_cell_ranges) const;
};

/**
 * Class of fragment cell range objects used in the priority queue algorithm.
 */
template <class T>
class ArrayReadState::PQFragmentCellRange {
 public:
  /**
   * Constructor.
   *
   * @param array_schema The schema of the array.
   * @param fragment_read_states The read states of all fragments in the array.
   */
  PQFragmentCellRange(
      const ArraySchema* array_schema,
      const std::vector<ReadState*>* fragment_read_states);

  /** Returns true if the fragment the range belongs to is dense. */
  bool dense() const;

  /**
   * Returns true if the calling object begins after the end of the input
   * range.
   */
  bool begins_after(const PQFragmentCellRange* fcr) const;

  /** Returns true if the calling object ends after the input range. */
  bool ends_after(const PQFragmentCellRange* fcr) const;

  /** Exports information to a fragment cell range. */
  void export_to(FragmentCellRange& fragment_cell_range);

  /** Imports information from a fragment cell range. */
  void import_from(const FragmentCellRange& fragment_cell_range);

  /**
   * Returns true if the calling object range must be split by the input
   * range.
   */
  bool must_be_split(const PQFragmentCellRange* fcr) const;

  /**
   * Returns true if the input range must be trimmed by the callling object.
   */
  bool must_trim(const PQFragmentCellRange* fcr) const;

  /**
   * Splits the calling object into two ranges based on the first input. The
   * first range will replace the calling object. The second range will be
   * stored in the second input. The third input is necessary for the
   * splitting.
   */
  void split(
      const PQFragmentCellRange* fcr,
      PQFragmentCellRange* fcr_new,
      const T* tile_domain);

  /**
   * Splits the calling object into three ranges based on the input fcr.
   *    - First range: Non-overlapping part of calling object range, stored
   *      at fcr_left.
   *    - Second range: A unary range at the left end point of the
   *      first input, stored at fcr_unary. Note that this may not exist.
   *    - Third range: The updated calling object range, which is trimmed to
   *      start after the unary range.
   */
  void split_to_3(
      const PQFragmentCellRange* fcr,
      PQFragmentCellRange* fcr_left,
      PQFragmentCellRange* fcr_unary);

  /**
   * Trims the first input range to the non-overlapping range stored in
   * the second input range. If the cell range of fcr_trimmed is NULL,
   * then fcr_trimmed is empty. The third input is necessary for the
   * trimming.
   */
  void trim(
      const PQFragmentCellRange* fcr,
      PQFragmentCellRange* fcr_trimmed,
      const T* tile_domain) const;

  /** Returns true if the range is unary. */
  bool unary() const;

  /** The cell range as a pair of coordinates. */
  T* cell_range_;
  /** The fragment id. */
  int fragment_id_;
  /** The tile id of the left endpoint of the cell range. */
  int64_t tile_id_l_;
  /** The tile id of the right endpoint of the cell range. */
  int64_t tile_id_r_;
  /** The position on disk of the tile corresponding to the cell range. */
  int64_t tile_pos_;

 private:
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** Size of coordinates. */
  size_t coords_size_;
  /** Dimension number. */
  int dim_num_;
  /** Stores the read state of each fragment in the array. */
  const std::vector<ReadState*>* fragment_read_states_;
};

/**
 * Wrapper of comparison function in the priority queue of the fragment cell
 * ranges.
 */
template <class T>
class ArrayReadState::SmallerPQFragmentCellRange {
 public:
  /** Constructor. */
  SmallerPQFragmentCellRange();

  /** Constructor. */
  SmallerPQFragmentCellRange(const ArraySchema* array_schema);

  /**
   * Comparison operator. First the smallest tile id of the left range end point
   * wins, then the smallest start range endpoint, then the largest fragment id.
   */
  bool operator()(PQFragmentCellRange<T>* a, PQFragmentCellRange<T>* b) const;

 private:
  /** The array schema. */
  const ArraySchema* array_schema_;
};

}  // namespace tiledb

#endif  // TILEDB_ARRAY_READ_STATE_H
