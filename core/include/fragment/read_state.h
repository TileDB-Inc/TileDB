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
  // TYPE DEFINITIONS

  // TODO
  typedef std::pair<int64_t, int64_t> CellPosRange;
  // TODO
  typedef std::pair<int, int64_t> FragmentInfo;
  // TODO
  typedef std::pair<FragmentInfo, CellPosRange> FragmentCellPosRange;
  // TODO
  typedef std::vector<FragmentCellPosRange> FragmentCellPosRanges;
  // TODO
  typedef std::pair<FragmentInfo, void*> FragmentCellRange;
  // TODO
  typedef std::vector<FragmentCellRange> FragmentCellRanges;

  /** 
   * Type of tile overlap with the query range. PARTIAL_CONTIG means that all
   * the qualifying cells are all contiguous on the disk; PARTIAL_NON_CONTIG
   * means the contrary.  
   */
  enum Overlap {NONE, FULL, PARTIAL_NON_CONTIG, PARTIAL_CONTIG};

  /** An overlapping tile with the query range. */
  struct OverlappingTile {
    // TODO
    bool coords_tile_fetched_reset_;
    /** Number of cells in this tile. */
    int64_t cell_num_;
    /**
     * Ranges of positions of qualifying cells in the range.
     * Applicable only to sparse arrays.
     */
    std::vector<std::pair<int64_t,int64_t> > cell_pos_ranges_;
    /** 
     * The coordinates of the tile in the tile domain. Applicable only to the
     * dense case.
     */
    void* coords_;
    // TODO
    std::vector<bool> tile_fetched_;
    // TODO
    void* global_coords_;
    // TODO
    void* mbr_coords_;
    // TODO
    void* mbr_in_tile_domain_;
    /** The type of the overlap of the tile with the query range. */
    Overlap overlap_;
    /** 
     * The overlapping range inside the tile. In the dense case, it is expressed
     * in relative terms, i.e., within tile domain (0, tile_extent_#1-1), 
     * (0, tile_extent_#2-1), etc. In the sparse case, it is expressed in 
     * absolute terms, i.e., within the array domain.
     */
    void* overlap_range_;
    /** The position of the tile in the global tile order. */
    int64_t pos_;
// TODO    /** The global position of the overlapping tile. */
// TODO    int64_t global_pos_;
  };

  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the write state belongs to.
   * @param book_keeping The book-keeping structures for this fragment.
   */
  ReadState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~ReadState();

  // ACCESSORS

  // TODO
  bool dense() const;

  // TODO
  int64_t search_tile_pos() const;

  // TODO
  Overlap overlap() const;

  // TODO
  bool overlaps() const;

  // TODO
  void get_bounding_coords(void* bounding_coords) const;

  // TODO
  template<class T>
  int get_fragment_cell_pos_range_sparse(
      const FragmentInfo& fragment_info,
      const T* cell_range,
      FragmentCellPosRange& fragment_cell_pos_range);

  // TODO
  bool overflow(int attribute_id) const;

  // TODO
  template<class T>
  bool max_overlap(const T* max_overlap_range) const;

  // TODO
  template<class T>
  void compute_fragment_cell_ranges(
      const FragmentInfo& fragment_info,
      FragmentCellRanges& fragment_cell_ranges) const;

  // TODO
  template<class T>
  void compute_fragment_cell_ranges_dense(
      const FragmentInfo& fragment_info,
      FragmentCellRanges& fragment_cell_ranges) const;

  // TODO
  template<class T>
  void compute_fragment_cell_ranges_sparse(
      const FragmentInfo& fragment_info,
      FragmentCellRanges& fragment_cell_ranges) const;

  // TODO
  const void* get_global_tile_coords() const;

  // TODO
  template<class T>
  int copy_cell_range(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      const CellPosRange& cell_pos_range);

  // TODO
  template<class T>
  int copy_cell_range_var(
      int attribute_id,
      int tile_i,
      void* buffer,
      size_t buffer_size,
      size_t& buffer_offset,
      void* buffer_var,
      size_t buffer_var_size,
      size_t& buffer_var_offset,
      const CellPosRange& cell_pos_range);


  // MUTATORS

  /**
   * Computes the next tile that overlaps with the range given in Array::init.
   * Applicable only to the sparse case.
   *
   * @return void 
   */
  // TODO
  void get_next_overlapping_tile_sparse();

  /**
   * Computes the next tile that overlaps with the range given in Array::init.
   * Applicable only to the sparse case.
   *
   * @template T The coordinates type.
   * @return void 
   */

  // TODO
  template<class T>
  void get_next_overlapping_tile_dense(const T* subarray_tile_coords);

  // TODO
  template<class T>
  void get_next_overlapping_tile_sparse(const T* subarray_tile_coords);

  // TODO
  template<class T>
  void get_next_overlapping_tile_sparse();


  // TODO
  template<class T>
  void tile_done(int attribute_id);

  /** 
   * Resets the overflow flag of every attribute to *false*. 
   *
   * @return void.
   */
  void reset_overflow();

  // TODO
  int64_t overlapping_tiles_num() const;



  // TODO
  template<class T>
  int get_first_two_coords(
      int tile_i,
      const T* start_coords,
      const T* end_coords,
      T* first_coords,
      T* second_coords,
      int& coords_num);

  // TODO
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      const T* start_coords,
      const T* end_coords,
      FragmentCellRanges& fragment_cell_ranges); 

  // TODO
  template<class T>
  int get_fragment_cell_ranges_sparse(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

  // TODO
  template<class T>
  int get_fragment_cell_ranges_dense(
      int fragment_i,
      FragmentCellRanges& fragment_cell_ranges); 

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
      int tile_i,
      T* start_coords_after,
      T* first_coords);

  // TODO
  template<class T>
  int get_first_coords_after(
      T* start_coords_after,
      T* first_coords,
      bool& coords_retrieved);


 private:
  // PRIVATE ATTRIBUTES

  // TODO
  std::vector<int64_t> tile_pos_;

  // TODO
  int64_t search_tile_pos_;

  /** The book-keeping structure of the fragment the read state belongs to. */
  BookKeeping* book_keeping_;
  /** 
   * For each attribute, this holds the position of the cell position ranges
   * in the current OverlappingTile object. Applicable only to the sparse case.
   */
  std::vector<int64_t> cell_pos_range_pos_;
  /** The fragment the read state belongs to. */
  const Fragment* fragment_;
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
  // TODO
  bool overlap_;
  /** 
   * A list of tiles overlapping the query range. Each attribute points to a
   * tile in this list.
   */
  std::vector<OverlappingTile> overlapping_tiles_;
  /** 
   * Current position under investigation in overlapping_tiles_ for each 
   * attribute. 
   */
  std::vector<int64_t> overlapping_tiles_pos_;
  /** 
   * The query range mapped to the tile domain. In other words, it contains
   * the coordinates of the tiles (in the tile domain) that the range overlaps
   * with.
   */
  void* range_in_tile_domain_;
  /** Internal buffer used in the case of compression. */
  void* tile_compressed_;
  /** Allocated size for internal buffer used in the case of compression. */
  size_t tile_compressed_allocated_size_;
  /** 
   * A range indicating the positions of the adjacent tiles to be searched. 
   * Applicable only to the sparse case.
   */
  int64_t tile_search_range_[2];
  /** Local tile buffers (one per attribute). */
  std::vector<void*> tiles_;
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

  // PRIVATE METHODS
  /** 
   * Cleans up processed overlapping tiles with the range across all attributed
   * specified in Array::init, properly freeing up the allocated memory.
   */
  void clean_up_processed_overlapping_tiles();

  /**
   * Computes the number of bytes to copy from the local tile buffers of a given
   * attribute in the case of variable-sized cells. It takes as input a range
   * of cells that should be copied ideally if the buffer sizes permit it.
   * Then, based on the available buffer sizes, this range is adjusted and the
   * final bytes to be copied for the two buffers are returned.
   *
   * @param attribute_id The id of the attribute.
   * @param start_cell_pos The starting position of the cell range.
   * @param end_cell_pos The ending position of the cell range.
   * @param buffer_free_space The free space in the buffer will stores the
   *     starting offsets of the variable-sized cells.
   * @param buffer_var_free_space The free space in the buffer will stores the
   *     actual variable-sized cells.
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
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range.
   */
  template<class T>
  void compute_cell_pos_ranges(); 

  /** 
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range. This function focuses on the case the tile overlap
   * tile is partial contiguous.
   */
  template<class T>
  void compute_cell_pos_ranges_contig(); 

  /** 
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range. This function focuses on the case the tile overlap
   * tile is partial contiguous, and specifically on the column-major
   * cell order.
   */
  template<class T>
  void compute_cell_pos_ranges_contig_col(); 

  /** 
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range. This function focuses on the case the tile overlap
   * tile is partial contiguous, and specifically on the row-major
   * cell order.
   */
  template<class T>
  void compute_cell_pos_ranges_contig_row(); 

  /** 
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range. This function focuses on the case the tile overlap
   * tile is partial non-contiguous.
   */
  template<class T>
  void compute_cell_pos_ranges_non_contig(); 

  /** 
   * Computes the ranges of positions of cells in a tile that overlap with
   * the query range. This function focuses on the case the tile overlap
   * tile is partial, and computes the ranges by scanning the cells between
   * the input start and end positions.
   *
   * @param start_pos The starting cell position of the scan.
   * @param end_pos The ending cell position of the scan.
   * @return void.
   */
  template<class T>
  void compute_cell_pos_ranges_scan(int64_t start_pos, int64_t end_pos); 

  /** 
   * Searches for the cell with coordinates that are included in the
   * unary query range.
   */
  template<class T>
  void compute_cell_pos_ranges_unary(); 

  /** 
   * Searches for the cell with coordinates that are included in the
   * unary query range. This function focuses on column-major cell order.
   */
  template<class T>
  void compute_cell_pos_ranges_unary_col(); 

  /** 
   * Searches for the cell with coordinates that are included in the
   * unary query range. This function focuses on Hilbert cell order.
   */
  template<class T>
  void compute_cell_pos_ranges_unary_hil(); 

  /** 
   * Searches for the cell with coordinates that are included in the
   * unary query range. This function focuses on row-major cell order.
   */
  template<class T>
  void compute_cell_pos_ranges_unary_row(); 

  /**
   * Computes the size of a variable tile of a given attribute.
   *
   * @param attribute_id The id of the attribute.
   * @param tile_pos The position of the tile in the global tile order.
   * @param tile_size The size of the tile to be returned.
   * @return TILEDB_RS_OK for success, and TILEDB_RS_ERR for error.
   */
  int compute_tile_var_size(
      int attribute_id, 
      int tile_pos,
      size_t& tile_size);



















  /**
   * Computes the next tile that overlaps with the range given in Array::init.
   * Applicable only to the dense case.
   *
   * @return void 
   */
  void get_next_overlapping_tile_dense();

  /**
   * Computes the next tile that overlaps with the range given in Array::init.
   * Applicable only to the dense case.
   *
   * @template T The coordinates type.
   * @return void 
   */
  template<class T>
  void get_next_overlapping_tile_dense();


  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case there is GZIP compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
//TODO
  int get_tile_from_disk_cmp_gzip(int attribute_id);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case there is no compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_cmp_none(int attribute_id);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case of variable-sized tiles with GZIP compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_var_cmp_gzip(int attribute_id);

  /**
   * Reads a tile from the disk into a local buffer for an attribute. This
   * function focuses on the case of variable-sized tiles with no compression.
   *
   * @param attribute_id The id of the attribute the tile is read for. 
   * @return TILEDB_RS_OK for success and TILEDB_RS_ERR for error.
   */
  int get_tile_from_disk_var_cmp_none(int attribute_id);

  /** 
   * Maps the query range into the tile domain, which is oriented by tile
   * coordinates instead of cell coordinates. Applicable only to the dense
   * case.
   */
  void init_range_in_tile_domain();

  /** 
   * Maps the query range into the tile domain, which is oriented by tile
   * coordinates instead of cell coordinates. Applicable only to the dense
   * case.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_range_in_tile_domain();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case.
   */
  void init_tile_search_range();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case. It also
   * focuses on column-major cell order.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range_col();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case. It also
   * focuses on Hilbert cell order.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range_hil();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case. It also
   * focuses on row-major cell order.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range_row();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case. It also
   * focuses on orders that rely on some tile decomposition and column-major
   * cell order.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range_id_col();

  /** 
   * Computes a range of tile positions (along the global tile order), which
   * the read will focus on (since these tiles may overlap with the query
   * range). This function is applicable only to the sparse case. It also
   * focuses on orders that rely on some tile decomposition and row-major
   * cell order.
   *
   * @template T The coordinates type.
   * @return void.
   */
  template<class T>
  void init_tile_search_range_id_row();

  /** True if the file of the input attribute is empty. */
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
   * Shifts the offsets stored in the input tile buffer such that the first one
   * starts from 0, and the rest are shifted accordingly.
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
