/**
 * @file   array_read_state.h
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
 * This file defines class ArrayReadState. 
 */

#ifndef __ARRAY_READ_STATE_H__
#define __ARRAY_READ_STATE_H__

#include "array.h"
#include "array_schema.h"
#include <cstring>
#include <inttypes.h>
#include <queue>
#include <vector>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_ARS_OK     0
#define TILEDB_ARS_ERR   -1

/** Size of the starting offset of a variable cell value. */
#define TILEDB_CELL_VAR_OFFSET_SIZE     sizeof(size_t)

class Array;

/** Stores the state necessary when reading cells from multiple fragments. */
class ArrayReadState {
 public:
  // TYPE DEFINITIONS

  // TODO
  enum Overlap {NONE, FULL, PARTIAL_NON_CONTIG, PARTIAL_CONTIG};

  // TODO
  template<class T>
  class SmallerFragmentCellRange;

  // TODO
  typedef std::pair<int64_t, int64_t> CellPosRange;
  // TODO
  typedef std::pair<int, int64_t> FragmentInfo;
  // TODO
  typedef std::pair<FragmentInfo, CellPosRange> FragmentCellPosRange;
  // TODO
  typedef std::vector<FragmentCellPosRange> FragmentCellPosRanges;
  // TODO
  typedef std::vector<FragmentCellPosRanges> FragmentCellPosRangesVec;
  // TODO
  typedef std::pair<FragmentInfo, void*> FragmentCellRange;
  // TODO
  typedef std::vector<FragmentCellRange> FragmentCellRanges;
  
  // CONSTRUCTORS & DESTRUCTORS

  // TODO
  ArrayReadState(const Array* array);

  // TODO
  ~ArrayReadState();

  // ACCESSORS

  // TODO
  bool overflow(int attribute_id) const;

  // READ METHODS
  // TODO
  int read_multiple_fragments(void** buffers, size_t* buffer_sizes); 

 private:
  // PRIVATE ATTRIBUTES
  
  // TODO
  void* bounding_coords_end_;
  // TODO
  std::vector<int64_t> appended_tiles_;
  // TODO
  std::vector<int64_t> last_tile_i_;
  // TODO
  std::vector<int64_t> empty_cells_written_;
  // TODO
  std::vector<bool> tile_done_;
  // TODO
  const Array* array_;
  // TODO
  bool done_;
  // TODO
  std::vector<int64_t> fragment_cell_pos_ranges_pos_;
  // TODO
  FragmentCellPosRangesVec fragment_cell_pos_ranges_vec_;
  // TODO
  std::vector<int64_t> fragment_cell_pos_ranges_vec_pos_;
  // TODO
  std::vector<const void*> fragment_global_tile_coords_;
  // TODO
  std::vector<void*> fragment_bounding_coords_;
  // TODO
  int max_overlap_i_;
  // TODO
  Overlap max_overlap_type_;
  // TODO in relative to the tile coordinates
  void* max_overlap_range_;
  // TODO
  void* range_global_tile_coords_;
  // TODO
  void* range_global_tile_domain_;
  // TODO
  std::vector<bool> overflow_;

  // PRIVATE METHODS

  // TODO
  template<class T>
  void copy_cell_range_with_empty(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const CellPosRange& cell_pos_range);

  // TODO
  template<class T>
  void copy_cell_range_with_empty_var(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const CellPosRange& cell_pos_range);


  // TODO
  template<class T>
  void compute_max_overlap_range();

  template<class T>
  void compute_max_overlap_fragment_cell_ranges(
      FragmentCellRanges& fragment_cell_ranges) const; 

  // TODO
  void clean_up_processed_fragment_cell_pos_ranges();

  // TODO
  template<class T>
  int compute_fragment_cell_pos_ranges(
      const FragmentCellRanges& unsorted_fragment_cell_ranges,
      FragmentCellPosRanges& fragment_cell_pos_ranges) const;

  // TODO
  template<class T>
  int copy_cell_ranges(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset);

  // TODO
  template<class T>
  int copy_cell_ranges_var(
      int attribute_id,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset);

  // TODO
  template<class T>
  int get_next_cell_ranges_dense();

  // TODO
  template<class T>
  int get_next_cell_ranges_sparse();

  // TODO
  template<class T>
  void get_next_range_global_tile_coords();

  // TODO
  template<class T>
  void init_range_global_tile_coords();

  // TODO
  int read_multiple_fragments_dense(void** buffers, size_t* buffer_sizes); 

  // TODO
  int read_multiple_fragments_dense_attr(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size); 

  // TODO
  template<class T>
  int read_multiple_fragments_dense_attr(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);

  // TODO
  int read_multiple_fragments_dense_attr_var(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size,
      void* buffer_var, 
      size_t& buffer_var_size);

  // TODO
  template<class T>
  int read_multiple_fragments_dense_attr_var(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size,
      void* buffer_var, 
      size_t& buffer_var_size);

  // TODO
  int read_multiple_fragments_sparse(void** buffers, size_t* buffer_sizes); 

  // TODO
  int read_multiple_fragments_sparse_attr(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size); 

  // TODO
  template<class T>
  int read_multiple_fragments_sparse_attr(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);

  // TODO
  int read_multiple_fragments_sparse_attr_var(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size,
      void* buffer_var, 
      size_t& buffer_var_size);

  // TODO
  template<class T>
  int read_multiple_fragments_sparse_attr_var(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size,
      void* buffer_var, 
      size_t& buffer_var_size);
};

/** 
 * Wrapper of comparison function in the priority queue of the fragment cell 
 * position ranges. 
 */
template<class T>
class ArrayReadState::SmallerFragmentCellRange {
 public:
  /** Constructor. */
  SmallerFragmentCellRange();

  /** Constructor. */
  SmallerFragmentCellRange(const ArraySchema* array_schema);

  /** Comparison operator. */
  bool operator () (
      FragmentCellRange a, 
      FragmentCellRange b) const;

 private:
  /** The array schema. */
  const ArraySchema* array_schema_;
};

#endif
