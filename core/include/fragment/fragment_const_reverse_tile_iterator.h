/**
 * @file   fragment_const_reverse_tile_iterator.h
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
 * This file defines class FragmentConstReverseTileIterator. 
 */

#ifndef FRAGMENT_CONST_REVERSE_TILE_ITERATOR_H
#define FRAGMENT_CONST_REVERSE_TILE_ITERATOR_H

#include "array_schema.h"
#include "fragment.h"
#include "tile.h"

class Fragment;

/** This class implements a constant reverse tile iterator. */
class FragmentConstReverseTileIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Iterator constuctor. */
  FragmentConstReverseTileIterator();
  /** Iterator constuctor. */
  FragmentConstReverseTileIterator(
      const Fragment* fragment, int attribute_id, int64_t pos); 

  // ACCESSORS
  /** Returns the array schema associated with this tile. */
  const ArraySchema* array_schema() const;
  /** Returns the bounding coordinates of the tile. */
  Tile::BoundingCoordinatesPair bounding_coordinates() const;
  /** True if the iterators has reached its end. */
  bool end() const;
  /** Returns the MBR of the tile. */
  Tile::MBR mbr() const;
  /** Returns the position. */
  int64_t pos() const;
  /** Returns the id of the tile. */
  int64_t tile_id() const;
  /** Number of tiles in the fragment. */
  int64_t tile_num() const;

  // OPERATORS
  /** Assignment operator. */
  void operator=(const FragmentConstReverseTileIterator& rhs);
  /** Addition operator. */
  FragmentConstReverseTileIterator operator+(int64_t step);
  /** Addition-assignment operator. */
  void operator+=(int64_t step);
  /** Pre-increment operator. */
  FragmentConstReverseTileIterator operator++();
  /** Post-increment operator. */
  FragmentConstReverseTileIterator operator++(int junk);
  /** 
   * Returns true if the iterator is equal to that in the
   * right hand side (rhs) of the operator. 
   */
  bool operator==(const FragmentConstReverseTileIterator& rhs) const;
  /** 
   * Returns true if the iterator is equal to that in the
   * right hand side (rhs) of the operator. 
   */
  bool operator!=(const FragmentConstReverseTileIterator& rhs) const;
  /** Returns the tile pointed by the iterator. */
  const Tile* operator*() const; 

 private:
  // PRIVATE ATTRIBUTES
  /** The attribute id corresponding to this iterator. */
  int attribute_id_;
  /** True if the iterators has reached its end. */
  bool end_;
  /** The array fragment corresponding to this iterator. */
  const Fragment* fragment_;
  /** The position of the current tile in the book-keeping structures. */
  int64_t pos_;
};

#endif
