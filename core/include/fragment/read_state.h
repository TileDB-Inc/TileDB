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

#include "array_schema.h"
#include "book_keeping.h"
#include "tile.h"
#include <inttypes.h>
#include <vector>

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

  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  ReadState(
      const ArraySchema* array_schema, 
      const BookKeeping* book_keeping,
      const std::string* fragment_name,
      const std::string* workspace,
      size_t segment_size);
  /** Destructor. */
  ~ReadState();

  // TILE FUNCTIONS
  /** Returns a tile for a given attribute and tile position. */
  const Tile* get_tile_by_pos(int attribute_id, int64_t pos);  
  /** 
   * Returns a tile for a given attribute and tile position, when traversing
   * tiles in reverse order. This is important so that the segments are
   * retrieved from the disk such that the tile that triggeres the 
   * segment retrieval appears in the end of the segment, rather than
   * in the beginning. 
   */
  const Tile* rget_tile_by_pos(int attribute_id, int64_t pos);  

 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;
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
  /** The array book-keeping structures. */
  const BookKeeping* book_keeping_;
  /** The fragment name. */
  const std::string* fragment_name_;
  /** The segment size */
  size_t segment_size_;
  /** Stores one segment per attribute. */
  Segments segments_;
  /** Stores the tiles of every attribute currently in main memory. */
  Tiles tiles_;
  /** The workspace. */
  const std::string* workspace_;

  // PRIVATE METHODS
  /** Deletes the tiles of an attribute from main memory. */
  void delete_tiles(int attribute_id);
  /** 
   * Loads the payloads of the tiles of a given attribute from disk and into
   * the corresponding segment in the read state, starting from the tile at
   * position pos. Returns the segment utilization after the load, and the
   * number of tiles loaded.
   */
  std::pair<size_t, int64_t> load_payloads_into_segment(
      int attribute_id, int64_t pos);
  /** 
   * Loads tiles of a given attribute from disk, starting from the tile at 
   * position pos. 
   */
  void load_tiles_from_disk(int attribute_id, int64_t pos);
  /** 
   * Loads the tiles of an attribute from the corresponding segment and 
   * stores them into the read state. 
   */
  void load_tiles_from_segment(
      int attribute_id, int64_t pos, 
      size_t segment_utilization, int64_t tiles_in_segment);

};

#endif
