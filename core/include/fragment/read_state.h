/**
 * @file   read_state.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corp.
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

#ifndef __READ_STATE_H__
#define __READ_STATE_H__

#include "book_keeping.h"
#include "fragment.h"
#include <vector>




/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

/**@{*/
/** Return code. */
#define TILEDB_RS_OK         0
#define TILEDB_RS_ERR       -1
/**@}*/




class BookKeeping;

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

  /** A vector of fragment cell posiiton ranges. */
  typedef std::vector<FragmentCellPosRange> FragmentCellPosRanges;

  /** A vector of vectors of fragment cell position ranges. */
  typedef std::vector<FragmentCellPosRanges> FragmentCellPosRangesVec;

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
   * @param fragment The fragment the write state belongs to.
   * @param book_keeping The book-keeping structures for this fragment.
   */
  // TODO
  ReadState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  // TODO
  ~ReadState();




  /* ********************************* */
  /*             ACCESSORS             */
  /* ********************************* */

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
   * Returns the current tile coordinates of the current search tile
   * qualifying the query subarray.
   */
  const void* get_tile_coords() const;

  /** Returns *true* if the read buffers overflowed for the input attribute. */
  bool overflow(int attribute_id) const;





  /* ********************************* */
  /*            MUTATORS               */
  /* ********************************* */

  /** 
   * Resets the overflow flag of every attribute to *false*. 
   *
   * @return void.
   */
  void reset_overflow();




  /* ********************************* */
  /*              MISC                 */
  /* ********************************* */

  /**
   * Copies the cells of the input attribute into the input buffers, as 
   * determined by the input cell position range.
   *
   * @param attribute_it The id of the targeted attribute.
   * @param tile_i The tile to copy from.
   * @param buffer The buffer to copy into - see Array::read().
   * @param buffer_size The size (in bytes) of *buffer*.
   * @param buffer_offset The offset in *buffer* where the copy will start from.
   * @param cell_pos_range The cell position range to be copied.
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  int copy_cells(
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
   * @param attribute_it The id of the targeted attribute.
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
   * @return TILEDB_RS_OK on success and TILEDB_RS_ERR on error.
   */
  int copy_cells_var(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const CellPosRange& cell_pos_range);

  // TODO
  template<class T>
  int get_enclosing_coords(
      int tile_i,
      const T* target_coords,
      const T* start_coords,
      const T* end_coords,
      T* left_coords,
      T* right_coords,
      bool& left_retrieved,
      bool& right_retrieved,
      bool& target_exists);

  // TODO
  template<class T>
  int get_first_coords_after(
      T* start_coords_after,
      T* first_coords,
      bool& coords_retrieved);

  // TODO
  template<class T>
  int get_fragment_cell_pos_range_sparse(
      const FragmentInfo& fragment_info,
      const T* cell_range,
      FragmentCellPosRange& fragment_cell_pos_range);

  // TODO
  template<class T>
  int get_fragment_cell_ranges_dense(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

  // TODO
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

  // TODO
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      const T* start_coords,
      const T* end_coords,
      FragmentCellRanges& fragment_cell_ranges); 

  // TODO
  template<class T>
  void get_next_overlapping_tile_dense(const T* subarray_tile_coords);

  // TODO
  template<class T>
  void get_next_overlapping_tile_sparse();

  // TODO
  template<class T>
  void get_next_overlapping_tile_sparse(const T* subarray_tile_coords);




 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The book-keeping of the fragment the read state belongs to. */
  BookKeeping* book_keeping_;
  // TODO
  bool done_;
  /** The fragment the read state belongs to. */
  const Fragment* fragment_;
  /** 
   * The tile coordinates of the currently investigated tile. Applicable
   * only to **dense** fragments.
   */
  void* tile_coords_;
  /** A buffer for each attribute used by mmap for mapping a tile from disk. */
  std::vector<void*> map_addr_;
  /** The corresponding lengths of the buffers in map_addr_. */
  std::vector<size_t> map_addr_lengths_;
  /** A buffer mapping a compressed tile from disk. */
  void* map_addr_compressed_;
  /** The corresponding length of the map_addr_compressed_ buffer. */
  size_t map_addr_compressed_length_;
  /** 
   * A buffer for each attribute used by mmap for mapping a variable tile from
   * disk. 
   */
  std::vector<void*> map_addr_var_;
  /** The corresponding lengths of the buffers in map_addr_var_. */
  std::vector<size_t> map_addr_var_lengths_;
  /** Indicates buffer overflow for each attribute. */ 
  std::vector<bool> overflow_;
  /** Internal buffer used in the case of compression. */
  void* tile_compressed_;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_;
  /** Local tile buffers (one per attribute). */
  std::vector<void*> tiles_;
  // TODO
  std::vector<bool> fetched_tile_;
  // TODO
  int64_t search_tile_pos_;
  /** Current offsets in tiles_ (one per attribute). */
  std::vector<size_t> tiles_offsets_;
  /** Sizes of tiles_ (one per attribute). */
  std::vector<size_t> tiles_sizes_;
  /** Local variable tile buffers (one per attribute). */
  std::vector<void*> tiles_var_;
  /** Allocated sizes for the local varible tile buffers. */
  std::vector<size_t> tiles_var_allocated_size_;
  /** Current offsets in tiles_var_ (one per attribute). */
  std::vector<size_t> tiles_var_offsets_;
  /** Sizes of tiles_var_ (one per attribute). */
  std::vector<size_t> tiles_var_sizes_;




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
   * @return void.
   */
  void compute_bytes_to_copy(
      int attribute_id,
      int64_t start_cell_pos,
      int64_t& end_cell_pos,
      size_t buffer_free_space, 
      size_t buffer_var_free_space,
      size_t& bytes_to_copy,
      size_t& bytes_var_to_copy) const;

  /**
   * Reads/maps a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case there is GZIP compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @param tile_i The position of the tile to be read from the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_cmp_gzip(int attribute_id, int64_t tile_i);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case there is no compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @param tile_i The position of the tile to be read from the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_cmp_none(int attribute_id, int64_t tile_i);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case of variable-sized tiles with GZIP compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @param tile_i The position of the tile to be read from the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_var_cmp_gzip(int attribute_id, int64_t tile_i);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case of variable-sized tiles with no compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @param tile_i The position of the tile to be read from the disk.
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_var_cmp_none(int attribute_id, int64_t tile_i);

  /** Returns *true* if the file of the input attribute is empty. */
  bool is_empty_attribute(int attribute_id) const;

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case there is GZIP compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_cmp_gzip(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case there is no compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function is invoked in place of
   * ReadState::read_tile_from_file_cmp_gzip if _TILEDB_USE_MMAP is defined.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_with_mmap_cmp_gzip(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function is invoked in place of
   * ReadState::read_tile_from_file_cmp_none if _TILEDB_USE_MMAP is defined.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_with_mmap_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case of variable-sized tiles and GZIP compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_var_cmp_gzip(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer. This
   * function focuses on the case of variable-sized tiles and no compression. 
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_var_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function is invoked in place of
   * ReadState::read_tile_from_file_var_cmp_gzip if _TILEDB_USE_MMAP is defined.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size. 
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_with_mmap_var_cmp_gzip(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /** 
   * Reads a tile from the disk for an attribute into a local buffer, using 
   * memory map (mmap). This function is invoked in place of
   * ReadState::read_tile_from_file_var_cmp_none if _TILEDB_USE_MMAP is defined.
   *
   * @param attribute_id The id of the attribute the read occurs for.
   * @param offset The offset at which the tile starts in the file.
   * @param tile_size The tile size.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int read_tile_from_file_with_mmap_var_cmp_none(
      int attribute_id,
      off_t offset,
      size_t tile_size);

  /**
   * Shifts the offsets stored in the tile buffer of the input attribute, such
   * that the first starts from 0 and the rest are relative to the first one.
   *
   * @param attribute_id The id of the attribute the tile corresponds to.
   * @return void.
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
   * @return void.
   */
  void shift_var_offsets(
      void* buffer, 
      int64_t offset_num, 
      size_t new_start_offset);
};

#endif
