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

#ifndef BOOK_KEEPING_H
#define BOOK_KEEPING_H

#include "tile.h"

/** Stores the book-keeping structures of a fragment. */
class BookKeeping {
 public:
  // Friends can manipulate the private members of this class.
  friend class FragmentConstTileIterator;
  friend class FragmentConstReverseTileIterator;

  // TYPE DEFINITIONS
  /** Mnemonic: <bound_coord_pair#1, bound_coord_pair#2, ...> */
  typedef std::vector<Tile::BoundingCoordinatesPair> BoundingCoordinates;
  /** Mnemonic: <MBR#1, MBR#2, ...> */
  typedef std::vector<Tile::MBR> MBRs;
  /** Mnemonic: <offset#1, offset#2, ...> */
  typedef std::vector<int64_t> OffsetList;
  /** Mnemonic: [attribute_id] --> <offset#1, offset#2, ...> */
  typedef std::vector<OffsetList> Offsets;
  /** Mnemonic: <tile_id#1, tile_id#2, ...> */
  typedef std::vector<int64_t> TileIds;

  // ACCESSORS
  /** Returns the tile offset for the given attribute and position. */
  int64_t offset(int attribute_id, int64_t pos) const; 
  /** Returns the number of tiles. */
  int64_t tile_num() const;

 private:
  // PRIVATE ATTRIBUTES
  /** 
   * Stores the bounding coordinates of every (coordinate) tile, i.e., the 
   * first and last cell of the tile (see Tile::bounding_coordinates).
   */
  BoundingCoordinates bounding_coordinates_;   
  /** Stores the MBR of every (coordinate) tile. */
  MBRs mbrs_; 
  /** 
   * Stores the offset (i.e., starting position) of every tile of every 
   * attribute in the respective data file. 
   */
  Offsets offsets_;
  /** Stores all the tile ids of the fragment. */
  TileIds tile_ids_;
};

#endif
