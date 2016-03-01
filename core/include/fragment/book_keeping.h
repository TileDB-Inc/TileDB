/**
 * @file   book_keeping.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class BookKeeping. 
 */

#ifndef __BOOK_KEEPING_H__
#define __BOOK_KEEPING_H__

#include "fragment.h"
#include <vector>
#include <zlib.h>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_BK_OK     0
#define TILEDB_BK_ERR   -1

#define TILEDB_BK_BUFFER_SIZE 10000

class Fragment;

/** Stores the book-keeping structures of a fragment. */
class BookKeeping {
 public:
  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the book-keeping structure belongs to.
   */
  BookKeeping(const Fragment* fragment);

  /** Destructor. */
  ~BookKeeping();

  // ACCESSORS

  /** Returns the bounding coordinates. */
  const std::vector<void*>& bounding_coords() const; 

  /** Returns the number of cells in the last tile. */
  int64_t last_tile_cell_num() const;

  /** Returns the MBRs. */
  const std::vector<void*>& mbrs() const; 

  /** Returns the domain in which the fragment is constrained. */
  const void* domain() const;

  /** Returns the non-empty domain in which the fragment is constrained. */
  const void* non_empty_domain() const;

  /** Returns the number of tiles in the fragment. */
  int64_t tile_num() const;

  /** Returns the tile offsets. */
  const std::vector<std::vector<size_t> >& tile_offsets() const;

  /** Returns the variable tile offsets. */
  const std::vector<std::vector<size_t> >& tile_var_offsets() const;

  /** Returns the variable tile sizes. */
  const std::vector<std::vector<size_t> >& tile_var_sizes() const;

  // MUTATORS 

  /** Appends the tile bounding coordinates to the book-keeping structure. */
  void append_bounding_coords(const void* bounding_coords);

  /** Appends the input MBR to the book-keeping structure. */
  void append_mbr(const void* mbr);
 
  /** Appends a tile offset for the input attribute. */
  void append_tile_offset(int attribute_id, size_t offset);

  /** Appends a variable tile offset for the input attribute. */
  void append_tile_var_offset(int attribute_id, size_t offset);

  /** Appends a variable tile size for the input attribute. */
  void append_tile_var_size(int attribute_id, size_t size);

  /*
   * Initializes the book-keeping structure.
   * 
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   * @return TILEDB_BK_OK for success, and TILEDB_OK_ERR for error.
   */
  int init(const void* non_empty_domain);

  // TODO
  int load();

  // TODO
  void set_last_tile_cell_num(int64_t cell_num);

  // MISC

  // TODO
  int finalize();

 private:
  // PRIVATE ATTRIBUTES

  /** The first and last coordinates of each tile. */
  std::vector<void*> bounding_coords_;
  /**
   * The (expanded) domain in which the fragment is constrained. Note that
   * the type of the domain must be the same as the type of the array
   * coordinates.
   */
  void* domain_;
  /** The fragment the book-keeping belongs to. */
  const Fragment* fragment_;
  /** Number of cells in the last tile (meaningful only in the sparse case. */
  int64_t last_tile_cell_num_;
  /** The MBRs (applicable only to the sparse case with irregular tiles). */
  std::vector<void*> mbrs_;
  /** The offsets of the next tile for each attribute. */
  std::vector<size_t> next_tile_offsets_;
  /** The offsets of the next variable tile for each attribute. */
  std::vector<size_t> next_tile_var_offsets_;
  /**
   * The non-empty domain in which the fragment is constrained. Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* non_empty_domain_;
  /** The tile offsets in their corresponding attribute files. */
  std::vector<std::vector<size_t> > tile_offsets_;
  /** The variable tile offsets in their corresponding attribute files. */
  std::vector<std::vector<size_t> > tile_var_offsets_;
  /** The sizes of the variable tiles. */
  std::vector<std::vector<size_t> > tile_var_sizes_;

  // PRIVATE METHODS

  // TODO
  int flush_bounding_coords(gzFile fd) const;

  // TODO
  int flush_last_tile_cell_num(gzFile fd) const;

  // TODO
  int flush_mbrs(gzFile fd) const;

  // TODO
  int flush_non_empty_domain(gzFile fd) const;

  // TODO
  int flush_tile_offsets(gzFile fd) const;

  // TODO
  int flush_tile_var_offsets(gzFile fd) const;

  // TODO
  int flush_tile_var_sizes(gzFile fd) const;

  // TODO
  int load_bounding_coords(gzFile fd);

  // TODO
  int load_last_tile_cell_num(gzFile fd);

  // TODO
  int load_mbrs(gzFile fd);

  // TODO
  int load_non_empty_domain(gzFile fd);

  // TODO
  int load_tile_offsets(gzFile fd);

  // TODO
  int load_tile_var_offsets(gzFile fd);

  // TODO
  int load_tile_var_sizes(gzFile fd);
};

#endif
