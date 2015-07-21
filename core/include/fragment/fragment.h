/**
 * @file   fragment.h
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
 * This file defines class Fragment. 
 */

#ifndef FRAGMENT_H
#define FRAGMENT_H

#include "array_schema.h"
#include "book_keeping.h"
#include "fragment_const_tile_iterator.h"
#include "fragment_const_reverse_tile_iterator.h"
#include "read_state.h"
#include "tile.h"
#include "write_state.h"
#include <string>

class FragmentConstTileIterator;
class FragmentConstReverseTileIterator;

/** Contains information about a fragment. */
class Fragment {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  Fragment(const std::string& workspace, size_t segment_size,
           size_t write_state_max_size,
           const ArraySchema* array_schema, const std::string& fragment_name);
  /** Destructor. */
  ~Fragment();

  // ACCESSORS
  /** Returns the array schema. */ 
  const ArraySchema* array_schema() const; 
  /** Begin tile iterator. */
  FragmentConstTileIterator begin(int attribute_id) const;
  /** Returns the bounding coordinates of the tile at the input position. */
  Tile::BoundingCoordinatesPair bounding_coordinates(int64_t pos) const;
  /** Returns the fragment name. */
  const std::string& fragment_name() const; 
  /** Returns a tile for a given attribute and tile position. */
  const Tile* get_tile_by_pos(int attribute_id, int64_t pos) const;  
  /** Returns the MBR of the tile at the input position. */
  Tile::MBR mbr(int64_t pos) const;
  /** Begin reverse tile iterator. */
  FragmentConstReverseTileIterator rbegin(int attribute_id) const;
  /** 
   * Returns a tile for a given attribute and tile position, when traversing
   * tiles in reverse order. This is important so that the segments are
   * retrieved from the disk such that the tile that triggeres the 
   * segment retrieval appears in the end of the segment, rather than
   * in the beginning. 
   */
  const Tile* rget_tile_by_pos(int attribute_id, int64_t pos) const;  

  /** Returns the id of the tile at the input position. */
  int64_t tile_id(int64_t pos) const;
  /** Returns the number of tiles in the fragment. */ 
  int64_t tile_num() const;

  // MUTATORS
  /** Clears the book keeping structures from main memory. */
  void clear_book_keeping();
  /** Clears the write state. */
  void clear_write_state();
  /** 
   * Writes the book keeping structures on the disk, but does not clear
   * them from main memory. 
   */
  void commit_book_keeping();
  /** Flushes the write state onto the disk. */
  void flush_write_state();
  /** 
   * Initializes the read state of the fragment (i.e., prepares it for 
   * reading).
   */
  void init_read_state();
  /**  
   * Writes a cell to the fragment. It takes as input a cell and its size. 
   * The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  template<class T>
  void write_cell(const void* cell) const; 
  /** 
   * Writes a cell into the fragment, respecting the global cell order. 
   * The input cell carries no ids.
   */
  template<class T>
  void write_cell_sorted(const void* cell); 
  /** 
   * Writes a cell into the fragment, respecting the global cell order. 
   * The input cell carries a single (tile) id.
   */
  template<class T>
  void write_cell_sorted_with_id(const void* cell); 
  /** 
   * Writes a cell into the fragment, respecting the global cell order. 
   * The input cell carries a tile and a cell id.
   */
  template<class T>
  void write_cell_sorted_with_2_ids(const void* cell); 
 
 private:
  // PRIVATE ATTRIBUTES
  /** The array schema (see ArraySchema). */
  const ArraySchema* array_schema_;
  /** The book-keeping structures. */
  BookKeeping* book_keeping_;
  /** The fragment name. */
  std::string fragment_name_;
  /** The read state. */ 
  ReadState* read_state_;
  /** The segment size */
  size_t segment_size_;
  /** Temporary directory. */
  std::string temp_dirname_;
  /** The workspace where the array data are created. */
  std::string workspace_; 
  /** The write state. */ 
  WriteState* write_state_;
};

#endif
