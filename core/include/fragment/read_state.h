/**
 * @file   read_state.h
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
 * This file defines class ReadState.
 */

#ifndef TILEDB_READ_STATE_H
#define TILEDB_READ_STATE_H

#include <vector>

#include "array.h"
#include "fragment.h"
#include "fragment_metadata.h"
#include "tile.h"
#include "tile_io.h"

namespace tiledb {

class Query;
class Fragment;

/** Stores the state necessary when reading cells from a fragment. */
class ReadState {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** A cell position pair [first, second]. */
  typedef std::pair<int64_t, int64_t> CellPosRange;

  /** A pair [fragment_id, tile_pos]. */
  typedef std::pair<int, int64_t> FragmentInfo;

  /** A pair of fragment info and fragment cell position range. */
  typedef std::pair<FragmentInfo, CellPosRange> FragmentCellPosRange;

  /**
   * A pair of fragment info and cell range, where the cell range is defined
   * by two bounding coordinates.
   */
  typedef std::pair<FragmentInfo, void*> FragmentCellRange;

  /** A vector of fragment cell ranges. */
  typedef std::vector<FragmentCellRange> FragmentCellRanges;

  /* ********************************* */
  /*    CONSTRUCTORS & DESTRUCTORS     */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param fragment The fragment the read state belongs to.
   * @param bookkeeping The bookkeeping of the fragment.
   */
  ReadState(
      const Fragment* fragment, Query* query, FragmentMetadata* bookkeeping);

  /** Destructor. */
  ~ReadState();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Copies the cells of the input attribute into the input buffers, as
   * determined by the input cell position range.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param tile_i The tile to copy from.
   * @param buffer The buffer to copy into - see Array::read().
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param cell_pos_range The cell position range to be copied.
   * @return Status
   */
  Status copy_cells(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const CellPosRange& cell_pos_range);

  /**
   * Copies the cells of the input **variable-sized** attribute into the input
   * buffers, as determined by the input cell position range.
   *
   * @param attribute_id The id of the targeted attribute.
   * @param tile_i The tile to copy from.
   * @param buffer The offsets buffer to copy into - see Array::read().
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param buffer_var The variable-sized cell buffer to copy into - see
   *     Array::read().
   * @param buffer_var_size The size (in bytes) of *buffer_var*.
   * @param buffer_var_offset The offset in *buffer_var* where the copy will
   *      start from.
   * @param cell_pos_range The cell position range to be copied.
   * @return Status
   */
  Status copy_cells_var(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const CellPosRange& cell_pos_range);

  /** Returns *true* if the read state corresponds to a dense fragment. */
  bool dense() const;

  /** Returns *true* if the read operation is finished for this fragment. */
  bool done() const;

  /**
   * Copies the bounding coordinates of the current search tile into the input
   * *bounding_coords*.
   */
  void get_bounding_coords(void* bounding_coords) const;

  /**
   * Retrieves the coordinates after the input coordinates in the search tile.
   *
   * @tparam T The coordinates type.
   * @param coords The target coordinates.
   * @param coords_after The coordinates to be retrieved.
   * @param coords_retrieved *true* if *coords_after* are indeed retrieved.
   * @return Status
   */
  template <class T>
  Status get_coords_after(
      const T* coords, T* coords_after, bool& coords_retrieved);

  /**
   * Given a target coordinates set, it returns the coordinates preceding and
   * succeeding it in a designated tile and inside an indicated coordinate
   * range.
   *
   * @tparam T The coordinates type.
   * @param tile_i The targeted tile position.
   * @param target_coords The target coordinates.
   * @param start_coords The starting coordinates of the target cell range.
   * @param end_coords The ending coordinates of the target cell range.
   * @param left_coords The returned preceding coordinates.
   * @param right_coords The returned succeeding coordinates.
   * @param left_retrieved *true* if the preceding coordinates are retrieved.
   * @param right_retrieved *true* if the succeeding coordinates are retrieved.
   * @param target_exists *true* if the target coordinates exist in the tile.
   * @return Status
   */
  template <class T>
  Status get_enclosing_coords(
      int tile_i,
      const T* target_coords,
      const T* start_coords,
      const T* end_coords,
      T* left_coords,
      T* right_coords,
      bool& left_retrieved,
      bool& right_retrieved,
      bool& target_exists);

  /**
   * Retrieves the cell position range corresponding to the input cell range,
   * for the case of **sparse** fragments.
   *
   * @tparam T The coordinates type.
   * @param fragment_info The (fragment id, tile position) pair corresponding
   *     to the cell position range to be retrieved.
   * @param cell_range The targeted cell range.
   * @param fragment_cell_pos_range The result cell position range.
   * @return Status
   */
  template <class T>
  Status get_fragment_cell_pos_range_sparse(
      const FragmentInfo& fragment_info,
      const T* cell_range,
      FragmentCellPosRange& fragment_cell_pos_range);

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile. Applicable only to **dense**.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id.
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return Status
   */
  template <class T>
  Status get_fragment_cell_ranges_dense(
      int fragment_i, FragmentCellRanges& fragment_cell_ranges);

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile. Applicable only to **sparse** fragments for **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id.
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return Status
   */
  template <class T>
  Status get_fragment_cell_ranges_sparse(
      int fragment_i, FragmentCellRanges& fragment_cell_ranges);

  /**
   * Computes the fragment cell ranges corresponding to the current search
   * tile, which are contained within the input start and end coordinates.
   * Applicable only to **sparse** fragments for **sparse** arrays.
   *
   * @tparam T The coordinates type.
   * @param fragment_i The fragment id.
   * @param start_coords The start coordinates of the specified range.
   * @param end_coords The end coordinates of the specified range.
   * @param fragment_cell_ranges The output fragment cell ranges.
   * @return Status
   */
  template <class T>
  Status get_fragment_cell_ranges_sparse(
      int fragment_i,
      const T* start_coords,
      const T* end_coords,
      FragmentCellRanges& fragment_cell_ranges);

  /**
   * Gets the next overlapping tile from the fragment, which may overlap or not
   * with the tile specified by the input tile coordinates. This is applicable
   * only to **dense** fragments.
   *
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @return void
   */
  template <class T>
  void get_next_overlapping_tile_dense(const T* tile_coords);

  /**
   * Gets the next overlapping tile from the fragment. This is applicable
   * only to **sparse** arrays.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void get_next_overlapping_tile_sparse();

  /**
   * Gets the next overlapping tile from the fragment, such that it overlaps or
   * succeeds the tile with the input tile coordinates. This is applicable
   * only to **sparse** fragments for **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @return void
   */
  template <class T>
  void get_next_overlapping_tile_sparse(const T* tile_coords);

  /**
   * Returns *true* if the MBR of the search tile overlaps with the current
   * tile under investigation. Applicable only to **sparse** fragments in
   * **dense** arrays. NOTE: if the MBR of the search tile has not not changed
   * and the function is invoked again, it will return *false*.
   */
  bool mbr_overlaps_tile() const;

  /** Returns *true* if the read buffers overflowed for the input attribute. */
  bool overflow(int attribute_id) const;

  /**
   * Resets the read state. Note that it does not flush any buffered tiles, so
   * that they can be reused later if a subsequent request happens to overlap
   * with them.
   *
   */
  void reset();

  /**
   * Resets the overflow flag of every attribute to *false*.
   *
   * @return void.
   */
  void reset_overflow();

  /**
   * True if the fragment non-empty domain fully covers the subarray area of
   * the current overlapping tile.
   */
  bool subarray_area_covered() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Query* query_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The number of array attributes. */
  int attribute_num_;

  /** The bookkeeping of the fragment the read state belongs to. */
  FragmentMetadata* bookkeeping_;

  /** The size of the array coordinates. */
  size_t coords_size_;

  /** Indicates if the read operation on this fragment finished. */
  bool done_;

  /** Keeps track of which tile is in main memory for each attribute. */
  std::vector<int64_t> fetched_tile_;

  /** The fragment the read state belongs to. */
  const Fragment* fragment_;

  /** Keeps track of whether each attribute is empty or not. */
  std::vector<bool> is_empty_attribute_;

  /**
   * Last investigated tile coordinates. Applicable only to **sparse** fragments
   * for **dense** arrays.
   */
  void* last_tile_coords_;

  /**
   * The overlap between an MBR and the current tile under investigation
   * in the case of **sparse** fragments in **dense** arrays. The overlap
   * can be one of the following:
   *    - 0: No overlap
   *    - 1: The query subarray fully covers the search tile
   *    - 2: Partial overlap
   *    - 3: Partial overlap contig
   */
  int mbr_tile_overlap_;

  /** Indicates buffer overflow for each attribute. */
  std::vector<bool> overflow_;

  /**
   * The type of overlap of the current search tile with the query subarray
   * is full or not. It can be one of the following:
   *    - 0: No overlap
   *    - 1: The query subarray fully covers the search tile
   *    - 2: Partial overlap
   *    - 3: Partial overlap contig
   */
  int search_tile_overlap_;

  /** The overlap between the current search tile and the query subarray. */
  void* search_tile_overlap_subarray_;

  /** The positions of the currently investigated tile. */
  int64_t search_tile_pos_;

  /**
   * True if the fragment non-empty domain fully covers the subarray area
   * in the current overlapping tile.
   */
  bool subarray_area_covered_;

  /**
   * Local tile buffers, one per attribute, plus two for coordinates
   * (the second one is for searching).
   */
  std::vector<Tile*> tiles_;

  /** Local tile buffers for the variable-sized attributes. */
  std::vector<Tile*> tiles_var_;

  /** Tile I/O objects for the tiles. */
  std::vector<TileIO*> tile_io_;

  /** Tile I/O objects for the variable-sized tiles. */
  std::vector<TileIO*> tile_io_var_;

  /**
   * The tile position range the search for overlapping tiles with the
   * subarray query will focus on.
   */
  int64_t tile_search_range_[2];

  /** Temporary coordinates. */
  void* tmp_coords_;

  /** Temporary offset. */
  uint64_t tmp_offset_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Computes the number of bytes to copy from the local tile buffers of a given
   * attribute in the case of variable-sized cells. It takes as input a range
   * of cells that should be copied ideally if the buffer sizes permit it.
   * Then, based on the available buffer sizes, this range is adjusted and the
   * final bytes to be copied for the two buffers are returned.
   *
   * @param attribute_id The id of the attribute.
   * @param start_cell_pos The starting position of the cell range.
   * @param end_cell_pos The ending position of the cell range. The function may
   *     alter this value upon termination.
   * @param buffer_free_space The free space in the buffer that will store the
   *     starting offsets of the variable-sized cells.
   * @param buffer_var_free_space The free space in the buffer that will store
   *     the actual variable-sized cells.
   * @param bytes_to_copy The returned bytes to copy into the the buffer that
   *     will store the starting offsets of the variable-sized cells.
   * @param bytes_var_to_copy The returned bytes to copy into the the buffer
   *     that will store the actual variable-sized cells.
   * @return Status
   */
  Status compute_bytes_to_copy(
      int attribute_id,
      uint64_t tile_var_size,
      int64_t start_cell_pos,
      int64_t& end_cell_pos,
      uint64_t buffer_free_space,
      uint64_t buffer_var_free_space,
      uint64_t& bytes_to_copy,
      uint64_t& bytes_var_to_copy);

  /**
   * Computes the size of a compressed tile with input parameters.
   *
   * @param tile_i The tile index.
   * @param attribute_id The attribute id.
   * @param tile_io The tile I/O object.
   * @param tile_compressed_size The size to be retrieved.
   * @return Status
   */
  Status compute_tile_compressed_size(
      int64_t tile_i,
      int attribute_id,
      TileIO* tile_io,
      uint64_t* tile_compressed_size) const;

  /**
   * Computes the size of a compressed variable-sized tile with input
   * parameters.
   *
   * @param tile_i The tile index.
   * @param attribute_id The attribute id.
   * @param tile_io The tile I/O object.
   * @param tile_compressed_size The size to be retrieved.
   * @return Status
   */
  Status compute_tile_compressed_var_size(
      int64_t tile_i,
      int attribute_id,
      TileIO* tile_io,
      uint64_t* tile_compressed_size) const;

  /**
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray.
   *
   * @return void
   */
  void compute_tile_search_range();

  /**
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void compute_tile_search_range();

  /**
   * Computes the ranges of tile positions that need to be searched for finding
   * overlapping tiles with the query subarray. This function focuses on the
   * case of column- or row-major cell orders.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void compute_tile_search_range_col_or_row();

  /**
   * Compares input coordinates to coordinates from the search tile.
   *
   * @param buffer The data buffer to be compared.
   * @param tile_offset The offset in the tile where the data comparison
   *     starts form.
   * @param isequal Indicates if the coordinates are equal.
   * @return Status
   */
  Status cmp_coords_to_search_tile(
      const void* buffer, uint64_t tile_offset, bool& isequal);

  /**
   * Returns the cell position in the search tile that is after the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @param The cell position in the search tile that is after the
   *     input coordinates.
   * @return Status
   */
  template <class T>
  Status get_cell_pos_after(const T* coords, int64_t* start_pos);

  /**
   * Returns the cell position in the search tile that is at or after the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @param end_pos The cell position in the search tile that is at or after the
   *     input coordinates.
   * @return Status
   */
  template <class T>
  Status get_cell_pos_at_or_after(const T* coords, int64_t* end_pos);

  /**
   * Returns the cell position in the search tile that is at or before the
   * input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordinates.
   * @param end_pos The cell position in the search tile that is at or before
   *     the input coordinates.
   * @return Status
   */
  template <class T>
  Status get_cell_pos_at_or_before(const T* coords, int64_t* end_pos);

  /**
   * Retrieves the pointer of the i-th coordinates in the search tile.
   *
   * @param i Indicates the i-th coordinates pointer to be retrieved.
   * @param coords The destination pointer.
   * @return Status
   */
  Status get_coords_from_search_tile(int64_t i, const void*& coords);

  /**
   * Retrieves the pointer of the i-th cell in the offset tile of a
   * variable-sized attribute.
   *
   * @param attribute_id The attribute id.
   * @param i Indicates the i-th offset pointer to be retrieved.
   * @param offset The destination pointer.
   * @return Status
   */
  Status get_offset(int attribute_id, int64_t i, const uint64_t*& offset);

  /** Returns *true* if the file of the input attribute is empty. */
  bool is_empty_attribute(int attribute_id) const;

  /**
   * Reads from a tile based on the input parameters.
   *
   * @param attribute_id The attribute id.
   * @param buffer The buffer to write to.
   * @param tile_offset The offset in the tile to start reading from.
   * @param bytes The number of bytes to be read.
   * @return Status
   */
  Status read_from_tile(
      int attribute_id, void* buffer, uint64_t tile_offset, uint64_t bytes);

  /**
   * Reads an entire tile.
   *
   * @param attribute_id The attribute id.
   * @param tile_i The tile index.
   * @return Status
   */
  Status read_tile(int attribute_id, int64_t tile_i);

  /**
   * Prepares a variable-sized tile from the disk for reading for an attribute.
   *
   * @param attribute_id The id of the attribute the tile is prepared for.
   * @param tile_i The tile position on the disk.
   * @return Status
   */
  Status read_tile_var(int attribute_id, int64_t tile_i);

  /**
   * Shifts the offsets stored in the tile buffer of the input attribute, such
   * that the first starts from 0 and the rest are relative to the first one.
   *
   * @param attribute_id The id of the attribute the tile corresponds to.
   * @return void
   */
  void shift_var_offsets(int attribute_id);

  /**
   * Shifts the offsets stored in the input buffer such that they are relative
   * to the input "new_start_offset".
   *
   * @param buffer The input buffer that stores the offsets.
   * @param offset_num The number of offsets in the buffer.
   * @param new_start_offset The new starting offset, i.e., the first element
   *     in the buffer will be equal to this value, and the rest of the offsets
   *     will be shifted relative to this offset.
   * @return void
   */
  void shift_var_offsets(
      void* buffer, int64_t offset_num, uint64_t new_start_offset);
};

}  // namespace tiledb

#endif  // TILEDB_READ_STATE_H
