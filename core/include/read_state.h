/**
 * @file   read_state.h
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
 * This file defines class ReadState. 
 */

#ifndef READ_STATE_H
#define READ_STATE_H

/** Stores the state necessary when reading tiles from a fragment. */
class ReadState {
 public:
  // TYPE DEFINITIONS
  /** Mnemonic: (pos_lower, pos_upper) */
  typedef std::pair<int64_t, int64_t> PosRange;
  /** Mnemonic: [attribute_id] --> (pos_lower, pos_upper) */
  typedef std::vector<PosRange> PosRanges;
  /** Mnemonic: [attribute_id] --> segment */
  typedef std::vector<void*> Segments;
  /** Mnemonic: <tile#1, tile#2, ...> */
  typedef std::vector<const Tile*> TileList;
  /** Mnemonic: [attribute_id] --> <tile#1, tile#2, ...> */
  typedef std::vector<TileList> Tiles;

 public:
  // PRIVATE ATTRIBUTES
  /** 
   * Stores the range of the position of the tiles currently in main memory,
   * for each attribute. The position of a tile is a sequence number 
   * indicating the order in which it was appended to the fragment with 
   * respect to the the other tiles appended to the fragment for the same
   * attribute (e.g., 0 means that it was appended first, 1 second, etc.).
   * The position helps in efficiently browsing tile info in the
   * book-keeping structures.
   */
  PosRanges pos_ranges_;
  /** Stores one segment per attribute. */
  Segments segments_;
  /** Stores the tiles of every attribute currently in main memory. */
  Tiles tiles_;
};

#endif
