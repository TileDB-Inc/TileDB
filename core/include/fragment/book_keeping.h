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

#include "array_schema.h"
#include "tile.h"

/** Stores the book-keeping structures of a fragment. */
class BookKeeping {
 public:
  // Friend classes that can manipulate the private attributes and methods
  friend class WriteState;

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

  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  BookKeeping(
      const ArraySchema* array_schema,
      const std::string* fragment_name,
      const std::string* workspace);  
  /** Destructor. */
  ~BookKeeping();

  // ACCESSORS
  /** Returns the bounding coordinates of the tile at the input position. */
  Tile::BoundingCoordinatesPair bounding_coordinates(int64_t pos) const;
  /** Returns the MBR of the tile at the input position. */
  Tile::MBR mbr(int64_t pos) const;
  /** Returns the tile offset for the given attribute and position. */
  int64_t offset(int attribute_id, int64_t pos) const; 
  /** Returns the id of the tile at the input position. */
  int64_t tile_id(int64_t pos) const;
  /** Returns the number of tiles. */
  int64_t tile_num() const;
  /** 
   * Returns the size of the tile for the input attribute at the input 
   * position. 
   */
  size_t tile_size(int attribute_id, int64_t pos) const;

  // MUTATORS
  /** Writes the book keeping structures on the disk. */
  void commit();
  /** Intiializes the book keeping structures for writing. */
  void init();
  /** Loads the book-keeping structures from the disk. */
  void load();

 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** 
   * Stores the bounding coordinates of every (coordinate) tile, i.e., the 
   * first and last cell of the tile (see Tile::bounding_coordinates).
   */
  BoundingCoordinates bounding_coordinates_;   
  /** The fragment name. */
  const std::string* fragment_name_;
  /** Stores the MBR of every (coordinate) tile. */
  MBRs mbrs_; 
  /** 
   * Stores the offset (i.e., starting position) of every tile of every 
   * attribute in the respective data file. 
   */
  Offsets offsets_;
  /** Stores all the tile ids of the fragment. */
  TileIds tile_ids_;
  /** The workspace. */
  const std::string* workspace_;

  // PRIVATE METHODS
  /** Writes the bounding coordinates on the disk. */
  void commit_bounding_coordinates();
  /** Writes the MBRs on the disk. */
  void commit_mbrs();
  /** Writes the offsets on the disk. */
  void commit_offsets();
  /** Writes the tile ids on the disk. */
  void commit_tile_ids();
  /** Loads the bounding coordinates. */
  void load_bounding_coordinates();
  /** Loads the tile MBRs. */
  void load_mbrs();
  /** Loads the tile offsets. */
  void load_offsets();
  /** Loads the tile ids. */
  void load_tile_ids();
};

#endif
