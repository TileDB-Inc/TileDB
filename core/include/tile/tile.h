/**
 * @file   tile.h
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
 * This file defines class Tile.
 */

#ifndef TILE_H
#define TILE_H

#include <typeinfo>
#include <limits>
#include <string>
#include <vector>
#include <inttypes.h>
#include "special_values.h"
#include "tile_const_cell_iterator.h"
#include "tile_const_reverse_cell_iterator.h"

/** Default payload capacity. */
#define TL_PAYLOAD_CAPACITY 100

class TileConstCellIterator;
class TileConstReverseCellIterator;

/** 
 * The tile is the central notion of TileDB. A tile can be a coordinate tile
 * or an attribute tile. In both cases, the cell values are stored sequentially
 * in main memory. We collectively call the cell values as the tile payload.
 *
 * Each tile has a particular cell type, which is one of char, int, int64_t,
 * float, and double for attribute tiles, and int, int64_t, float, and double
 * for coordinate tiles. In order to avoid the tedious templates and for
 * performance purposes, we use a generic void* variable for pointing 
 * to the payload, namely Tile::payload_. The same stands for other member
 * attributes, such as Tile::mbr_. In other words, we make the Tile object
 * oblivious of the cell type. We store the cell type in Tile::cell_type_,
 * and properly cast the void pointers whenever necessary.
 */
class Tile {
 public:
  // TYPE DEFINITIONS
  /** A tile can be either an attribute or a coordinate tile. */
  enum TileType {ATTRIBUTE, COORDINATE};
  /** Mnemonic: (first_bound_coord, last_bound_coord) */
  typedef std::pair<void*, void*> BoundingCoordinatesPair;
  /** 
   * A hyper-rectangle in the logical space, including all the coordinates
   * of a tile. It is a list of lower/upper values across each dimension, i.e.,
   * (dim#1_lower, dim#1_upper, dim#2_lower, dim#2_upper, ...).
   */
  typedef void* MBR; 

  // CONSTRUCTORS AND DESTRUCTORS
  /**
   * Constructor. If dim_num is 0, then this is an attribute tile; otherwise,
   * it is a coordinate tile. The val_num indicates how many values are 
   * stored per cell. It is equal to VAR_SIZE if the cell size is variable. 
   */
  Tile(int64_t tile_id, int dim_num, 
       const std::type_info* cell_type, int val_num); 
  /** Destructor. */
  ~Tile(); 

  // ACCESSORS
  /** Returns a cell iterator pointing to the first cell of the tile. */
  TileConstCellIterator begin() const;
  /** 
   * Returns the bounding coordinates, i.e., the first and 
   * last coordinates that were appended to the tile. It applies only to
   * coordinate tiles. The bounding coordinates are typically useful
   * when the cells in the tile are sorted in a certain order. 
   */
  BoundingCoordinatesPair bounding_coordinates() const;
  /** Returns the pointer to the pos-th cell. */
  const void* cell(int64_t pos) const;
  /** Returns the cell size. Applicable only to fixed-sized cells. */
  size_t cell_size() const;
  /** Returns the number of cells in the tile. */
  int64_t cell_num() const;
  /** Returns the cell type. */
  const std::type_info* cell_type() const;
  /** Copies the tile payload into buffer. */
  void copy_payload(void* buffer) const;
  /** Returns the number of dimensions. It returns 0 for attribute tiles. */
  int dim_num() const;
  /** Returns a cell iterator signifying the end of the tile. */
  static TileConstCellIterator end();
  /** Returns true if the cell at position pos represents a deletion. */
  bool is_del(int64_t pos) const;
  /** Returns true if the cell at position pos is NULL. */
  bool is_null(int64_t pos) const;
  /** Returns the MBR (see Tile::mbr_). */
  MBR mbr() const;
  /** Returns the tile id. */
  int64_t tile_id() const;
  /** Returns the tile size (in bytes). */
  size_t tile_size() const;
  /** Returns the tile type. */
  TileType tile_type() const;
  /** Returns the cell type size. */
  size_t type_size() const; 
  /** True if the cells are variable-sized. */
  bool var_size() const;
 
  // MUTATORS
  /** Clears the tile. */
  void clear();
  /** MBR setter. Applicable only to coordinate tiles. */
  void set_mbr(const void* mbr);
  /** Payload setter. */
  void set_payload(void* payload, size_t payload_size); 

  // MISC
  /** 
   * Returns true if the pos-th coordinates fall inside the input range. 
   * The range is in the form (dim#1_lower, dim#1_upper, ...).
   * It applies only to coordinate tiles.
   * 
   * Make sure that type T is the same as the cell type. Otherwise, in debug
   * mode an assert is triggered, whereas in release mode the behavior is
   * unpredictable.
   */
  template<class T>
  bool cell_inside_range(int64_t pos, const T* range) const;
  /** Prints the details of the tile on the standard output. */
  void print() const;

  /** Returns a cell iterator pointing to the first cell of the tile. */
  TileConstReverseCellIterator rbegin() const;
  /** Returns a cell iterator signifying the end of the tile. */
  static TileConstReverseCellIterator rend();

 private:
  // PRIVATE ATTRIBUTES
  /** The number of cells in the tile. */
  int64_t cell_num_;
  /** The cell size. For variable-sized cells, it is equal to VAR_SIZE. */
  size_t cell_size_;
  /** The cell type. */
  const std::type_info* cell_type_;
  /** The number of dimensions. It is equal to 0 for attribute tiles. */
  int dim_num_; 
  /** 
   * The tile MBR (minimum bounding rectangle), i.e., the tightest 
   * hyper-rectangle in the logical space that contains all the 
   * coordinates in the tile. The MBR is represented as a vector
   * of lower/upper pairs of values in each dimension, i.e., 
   * (dim#1_lower, dim#1_upper, dim#2_lower, dim#2_upper, ...). Applicable
   * only for coordinate tiles (otherwise, it is set to NULL).
   */
  void* mbr_;
  /** 
   * The payload stores the cell (attribute/coordinate) values. 
   * The coordinates are serialized, i.e., the payload first stores
   * the coordinates for dimension 1, then for dimensions 2, etc.
   */
  void* payload_;
  /** The tile id. */
  int64_t tile_id_;
  /** The tile size (in bytes). */
  size_t tile_size_;
  /** The tile type. */
  TileType tile_type_;
  /** The size of the cell type. */
  size_t type_size_;
  /** 
   * Number of cell values. It is equal to VAR_SIZE, if the cell has a
   * variable number of values.
   */
  int val_num_;

  // PRIVATE METHODS
  /** Deletes the MBR. */
  void clear_mbr();
  /** Deletes the payload. */
  void clear_payload();
  /** Expands the tile MBR bounds. Applicable only to coordinate tiles. */
  template<class T>
  void expand_mbr(const T* coords);  
  /** 
   * This is populated only in the case of variable-sized cells. It is a list
   * of offsets where each cell begins in the payload.
   */
  std::vector<size_t> offsets_;
  /** Prints the bounding coordinates on the standard output. */
  template<class T>
  void print_bounding_coordinates() const;
  /** Prints the MBR on the standard output. */
  template<class T>
  void print_mbr() const;
  /** Prints the payload on the standard output. */
  template<class T>
  void print_payload() const;
};

#endif
