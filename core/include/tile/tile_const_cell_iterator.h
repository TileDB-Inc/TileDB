/**
 * @file   tile_const_cell_iterator.h
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
 * This file defines class TileConstCellIterator.
 */

#ifndef TILE_CONST_CELL_ITERATOR_H
#define TILE_CONST_CELL_ITERATOR_H

#include "tile.h"
#include <inttypes.h>
#include <string.h>

class Tile;

/** This class implements a constant cell iterator. */
class TileConstCellIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Empty iterator constuctor. */
  TileConstCellIterator();
  /** 
   * Constuctor that takes as input the tile for which the 
   * iterator is created, and a cell position in the tile payload. 
   */
  TileConstCellIterator(const Tile* tile, int64_t pos);
   
  // ACCESSORS
  /** 
   * Returns true if the coordinates pointed by the iterator fall 
   * inside the input range. It applies only to coordinate tiles.
   * The range is in the form (dim#1_lower, dim#1_upper, ...).
   * 
   * Make sure that type T is the same as the cell type. Otherwise, in debug
   * mode an assert is triggered, whereas in release mode the behavior is
   * unpredictable.
   */
  template<class T>
  bool cell_inside_range(const T* range) const;
  /** Returns the number of cells in the tile. */
  int64_t cell_num() const; 
  /** Returns the (potentially variable) size of the current cell. */
  size_t cell_size() const;
  /** Returns the cell type of the tile. */
  const std::type_info* cell_type() const;
  /** Returns the number of dimensions of the tile. */
  int dim_num() const;
  /** Returns true if the end of the iterator is reached. */
  bool end() const;
  /** True if the iterator points to a cell representing a deletion. */
  bool is_del() const;
  /** True if the iterator points to a NULL cell. */
  bool is_null() const;
  /** Returns the current payload position of the cell iterator. */
  int64_t pos() const;
  /** Returns the tile the cell iterator belongs to. */
  const Tile* tile() const;
  /** Returns the tile the cell iterator belongs to. */
  int64_t tile_id() const;

  // OPERATORS
  /** Assignment operator. */
  void operator=(const TileConstCellIterator& rhs);
  /** Addition operator. */
  TileConstCellIterator operator+(int64_t step);
  /** Addition-assignment operator. */
  void operator+=(int64_t step);
  /** Pre-increment operator. */
  TileConstCellIterator operator++();
  /** Post-increment operator. */
  TileConstCellIterator operator++(int junk);
  /**
   * Returns true if the operands belong to the same tile and they point to
   * the same cell. 
   */
  bool operator==(const TileConstCellIterator& rhs) const;
  /**
   * Returns true if either the operands belong to different tiles, or the
   * they point to different cells. 
   */
  bool operator!=(const TileConstCellIterator& rhs) const;
  /** Returns the pointer in the tile payload of the cell it points to. */ 
  const void* operator*() const;

 private:
  /** The current cell. */
  const void* cell_;
  /** True if the end of the iterator is reached. */
  bool end_;
  /** 
   * The position of the cell in the tile payload the iterator
   * currently is pointing to. 
   */
  int64_t pos_;  
  /** The tile object the iterator is created for. */
  const Tile* tile_;
};

#endif
