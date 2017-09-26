/**
 * @file   pq_fragment_cell_range.h
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
 * This file defines class PQFragmentCellRange.
 */

#ifndef TILEDB_PQ_FRAGMENT_CELL_RANGE_H
#define TILEDB_PQ_FRAGMENT_CELL_RANGE_H

#include "array_metadata.h"
#include "read_state.h"

#include <cinttypes>

namespace tiledb {

/**
 * Class of fragment cell range objects used in the priority queue
 * read algorithm.
 */
template <class T>
class PQFragmentCellRange {
 public:
  /* ********************************* */
  /*          STATIC CONSTANTS         */
  /* ********************************* */

  /** Indicates an invalid uint64_t value. */
  static const uint64_t INVALID_UINT64;

  /** Indicates an invalid unsigned int value. */
  static const unsigned int INVALID_UINT;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array_metadata The metadata of the array.
   * @param fragment_read_states The read states of all fragments in the array.
   * @param tile_coords_aux Auxiliary variable used in computing tile ids to
   *     boost performance.
   */
  PQFragmentCellRange(
      const ArrayMetadata* array_metadata,
      const std::vector<ReadState*>* fragment_read_states,
      T* tile_coords_aux);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

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
  void export_to(ReadState::FragmentCellRange* fragment_cell_range);

  /** Imports information from a fragment cell range. */
  void import_from(const ReadState::FragmentCellRange& fragment_cell_range);

  /**
   * Returns true if the calling object range must be split by the input
   * range.
   */
  bool must_be_split(const PQFragmentCellRange* fcr) const;

  /**
   * Returns true if the input range must be trimmed by the calling object.
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

  /* ********************************* */
  /*          PUBLIC ATTRIBUTES        */
  /* ********************************* */

  /** The cell range as a pair of coordinates. */
  T* cell_range_;

  /** The fragment id. */
  unsigned int fragment_id_;

  /** The tile id of the left endpoint of the cell range. */
  uint64_t tile_id_l_;

  /** The tile id of the right endpoint of the cell range. */
  uint64_t tile_id_r_;

  /** The position on disk of the tile corresponding to the cell range. */
  uint64_t tile_pos_;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array metadata. */
  const ArrayMetadata* array_metadata_;

  /** Size of coordinates. */
  uint64_t coords_size_;

  /** Dimension number. */
  unsigned int dim_num_;

  /** Stores the read state of each fragment in the array. */
  const std::vector<ReadState*>* fragment_read_states_;

  /** Auxiliary variable used whenever a tile id needs to be computed. */
  T* tile_coords_aux_;
};

}  // namespace tiledb

#endif  // TILEDB_PQ_FRAGMENT_CELL_RANGE_H
