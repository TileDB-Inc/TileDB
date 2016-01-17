/**
 * @file   read_state.h
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

#define TILEDB_RS_OK     0
#define TILEDB_RS_ERR   -1

class BookKeeping;

/** Stores the state necessary when reading cells from a fragment. */
class ReadState {
 public:
  // TYPE DEFINITIONS
  /** 
   * Type of tile overlap with the query range. PARTIAL_SPECIAL means that
   * the qualifying cells are all contiguous on the disk. 
   */
  enum Overlap {NONE, FULL, PARTIAL, PARTIAL_SPECIAL};

  /** An overlapping tile with the query range. */
  struct OverlappingTile {
    /** The coordinates of the tile in the tile domain. */
    void* coords_;
    /** The position of the tile in the global tile order. */
    int64_t pos_;
    /** The type of the overlap of the tile with the query range. */
    Overlap overlap_;
    /** 
     * The overlapping range inside the tile, expressed in relative terms, i.e.,
     * within domain (0, tile_extent_#1-1), (0, tile_extent_#2-1), etc.
     */
    void* overlap_range_;
  };

  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the write state belongs to.
   */
  ReadState(const Fragment* fragment, BookKeeping* book_keeping);

  /** Destructor. */
  ~ReadState();

  // READ FUNCTIONS
  
  // TODO
  int read(void** buffers, size_t* buffer_sizes);

 private:
  // PRIVATE ATTRIBUTES

  /** The book-keeping structure of the fragment the write state belongs to. */
  BookKeeping* book_keeping_;
  /** The fragment the write state belongs to. */
  const Fragment* fragment_;
  /** Specifies which tiles overlap the query range. */
  std::vector<OverlappingTile> overlapping_tiles_;
  /** 
   * Current position under investigation in overlapping_tiles_ 
   * (one per attribute). 
   */
  std::vector<int64_t> overlapping_tile_pos_;
  /** 
   * The query range mapped to the tile domain. In other words, it contains
   * the coordinates of the tiles (in the tile domain) that the range overlaps
   * with.
   */
  void* range_in_tile_domain_;
  /** Local tile buffers (one per attribute). */
  std::vector<void*> tiles_;
  /** Current offsets in tiles_ (one per attribute). */
  std::vector<size_t> tile_offsets_;
  /** Sizes of tiles_ (one per attribute). */
  std::vector<size_t> tile_sizes_;

  // PRIVATE METHODS

  // TODO
  template<class T>
  void get_next_overlapping_tile();

  // TODO
  template<class T>
  void copy_from_tile_buffer(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  void copy_from_tile_buffer_full(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  template<class T>
  void copy_from_tile_buffer_partial(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  template<class T>
  void copy_from_tile_buffer_partial_special(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);


  // TODO
  int copy_tile_full(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  int copy_tile_full_direct(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  template<class T>
  int copy_tile_partial(
      int attribute_id,
      void* buffer, 
      size_t buffer_size, 
      size_t& buffer_offset);

  // TODO
  int get_tile_from_disk(int attribute_id);

  // TODO
  template<class T>
  void init_range_in_tile_domain();

  /** True if the file of the input attribute is empty. */
  bool is_empty_attribute(int attribute_id) const;

  // TODO
  int read_dense(
      void** buffers, 
      size_t* buffer_sizes);

  // TODO
  int read_dense_attr(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);

  // TODO
  int read_dense_attr_cmp_none(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);

  // TODO
  template<class T>
  int read_dense_attr_cmp_none(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);

  // TODO
  int read_dense_attr_cmp_gzip(
      int attribute_id,
      void* buffer, 
      size_t& buffer_size);
};


#endif
