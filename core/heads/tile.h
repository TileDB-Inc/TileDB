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

#include <inttypes.h>
#include <string>
#include <sstream>
#include <vector>
#include <typeinfo>
#include <limits>
#include "csv_file.h"

/** Missing char. */
#define TL_NULL_CHAR '$'
/** Missing int. */
#define TL_NULL_INT std::numeric_limits<int>::max()
/** Missing int64_t. */
#define TL_NULL_INT64_T std::numeric_limits<int64_t>::max()
/** Missing uint64_t. */
#define TL_NULL_UINT64_T std::numeric_limits<uint64_t>::max()
/** Missing float. */
#define TL_NULL_FLOAT std::numeric_limits<float>::max()
/** Missing double. */
#define TL_NULL_DOUBLE std::numeric_limits<double>::max()

class CSVLine;

/**
 * The tile is the central notion in TileDB. A tile can be an attribute tile or
 * a coordinate tile.
 */
class Tile {
 public:
  class const_cell_iterator;

  // TYPE DEFINITIONS
  /** A tile can be either an attribute or a coordinate tile. */
  enum TileType {ATTRIBUTE, COORDINATE};

  // CONSTRUCTORS AND DESTRUCTORS
  /** Simple constructor that takes as input the tile and cell types. */
  Tile(TileType tile_type, const std::type_info* cell_type, 
       uint64_t tile_id, uint64_t cell_num, uint64_t cell_size) 
      : tile_type_(tile_type), cell_type_(cell_type), 
        tile_id_(tile_id), cell_num_(cell_num), cell_size_(cell_size) {}
  /** Empty virtual destructor. */
  virtual ~Tile() {}

  // ACCESSORS
  /** 
   * Returns the bounding coordinates, i.e., the first and 
   * last coordinates that were appended to the tile. It applies only to
   * coordinate tiles. The bounding coordinates are typically useful
   * when the cells in the tile are sorted in a certain order. 
   */
  std::pair<const void*, const void*> bounding_coordinates();
  /** Returns a pointer to the pos-th cell. */
  void* cell(int64_t pos);
  /** Returns the number of cells in the tile. */
  uint64_t cell_num() const { return cell_num_; }
  /** Returns the cell size (in bytes). */
  uint64_t cell_size() const { return cell_size_; } 
  /** Returns the cell type. */
  const std::type_info* cell_type() const { return cell_type_; } 
  /** Copies the tile payload (i.e., all the cell values) into buffer. */
  void copy_payload(char* buffer);
  /** Returns the number of dimensions. It applies only to coordinate tiles. */
  int dim_num();
  /** 
   * Returns the MBR (minimum bounding rectangle) of the coordinates in the
   * logical multi-dimensional space. It applies only to coordinate tiles. 
   * The MBR is a list of low/high values in each dimension, i.e.,
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
   */
  const void* mbr();
  /** Returns the tile id. */
  uint64_t tile_id() const { return tile_id_; }
  /** Returns the tile size (in bytes). */
  uint64_t tile_size() const { return tile_num_ * cell_size_; } 
  /** Returns the tile type (see Tile::TileType). */
  TileType tile_type() const { return tile_type_; } 
 
  // MUTATORS
  /** 
   * Appends a cell from various sources. Returns true if the retrieval from
   * the input was successful.
   */
  template<class T>
  bool append_cell(T* value);
  /** 
   * Appends a cell from various sources. Returns true if the retrieval from
   * the input was successful.
   */
  template<class T>
  bool append_cell(const T& value);
  /** MBR setter. Applicable only to coordinate tiles. */
  void set_mbr(const void* mbr);
  /** Payload setter. */
  void set_payload(const void* payload, size_t payload_size); 

  // OPERATORS
  /** Appends a cell value to (the end of) a tile. */
  template<typename T>
  void operator<<(const T& value); 
  /** 
   * Appends a cell value to (the end of) a tile, retrieved from the input CSV
   * line. It returns false if it fails to retrieve from the CSV line.
   */
  bool operator<<(CSVLine& csv_line); 
  /** 
   * Appends a cell value to (the end of) a tile, retrieved from the input cell
   * iterator.
   */
  void operator<<(const Tile::const_cell_iterator& cell_it); 

  // CELL ITERATORS
  /** This class implements a constant cell iterator. */
  class const_cell_iterator {
   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Empty iterator constuctor. */
    const_cell_iterator();
    /** 
     * Constuctor that takes as input the tile for which the 
     * iterator is created, and the current cell position in the tile. 
     */
    const_cell_iterator(const Tile* tile, int64_t pos);
    
    // ACCESSORS
    /** Returns the current position of the cell iterator. */
    int64_t pos() const { return pos_; }
    /** Returns the tile the cell iterator belongs to. */
    const Tile* tile() const { return tile_; }

    // MISC
    /** Returns true if the iterator points to a NULL cell. */
    bool is_null() const;

    // TODO: is_deleted

    // OPERATORS
    /** Assignment operator. */
    void operator=(const const_cell_iterator& rhs);
    /** Addition operator. */
    const_cell_iterator operator+(int64_t step);
    /** Addition-assignment operator. */
    void operator+=(int64_t step);
    /** Pre-increment operator. */
    const_cell_iterator operator++();
    /** Post-increment operator. */
    const_cell_iterator operator++(int junk);
    /**
     * Returns true if the operands belong to the same tile and the cells 
     * they point to have the same values. 
     */
    bool operator==(const const_cell_iterator& rhs) const;
    /**
     * Returns true if either the operands belong to different tiles, or the
     * cells they point to have different values. 
     */
    bool operator!=(const const_cell_iterator& rhs) const;
    /** Returns the pointer to the cell the iterator points to. */ 
    const void* operator*() const;
    /** 
     * Appends to the input CSV line the value of the cell the iterator
     * points to. 
     */
    void operator>>(CSVLine& csv_line) const;

    // MISC
    /** 
     * Returns true if the coordinates pointed by the iterator fall 
     * inside the input range. It applies only to coordinate tiles.
     * The range is in the form (dim#1_low, dim#1_high, ...).
     */
    template<class T>
    bool cell_inside_range(const T* range) const;

   private:
    /** The current position of the iterator in the payload vector. */
    int64_t pos_;  
    /** The tile object the iterator is created for. */
    const Tile* tile_;
  };
  /** Returns a cell iterator pointing to the first cell of the tile. */
  const_cell_iterator begin() const;
  /**
   * Returns a cell iterator pointing to one position after the last 
   * cell of the tile. 
   */
  const_cell_iterator end() const;

  // MISC
  /** 
   * Appends the pos-th cell value to the input CSV line. 
   */
  void append_cell_to_csv_line(int64_t pos, CSVLine& csv_line);
  /** 
   * Returns true if the pos-th coordinates fall inside the input range. 
   * It applies only to coordinate tiles.
   */
  bool cell_inside_range(int64_t pos, const T* range);
  /** Prints the details of the tile on the standard output. */
  void print();
 
 private:
  // PRIVATE ATTRIBUTES
  /** The number of cells in the tile. */
  int64_t cell_num_;
  /** The cell size (in bytes). */
  int64_t cell_size_;
  /** The number of dimensions. It is equal to -1 for attribute tiles. */
  int dim_num; 
  /** The cell type. */
  const std::type_info* cell_type_;
  /** 
   * The tile MBR (minimum bounding rectangle), i.e., the tightest 
   * hyper-rectangle in the logical space that contains all the 
   * coordinates in the tile. The MBR is represented as a vector
   * of low/high pairs of values in each dimension, i.e., 
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...). Applicable
   * only for coordinate tiles (otherwise, it is set to NULL);
   */
  void* mbr_;
  /** 
   * The payload stores the cell (attribute/coordinate) values. 
   * The coordinates are serialized (i.e., the payload first stores
   * the coordinates for dimension 1, then for dimensions 2, etc.
   */
  void* payload_;
  /** 
   * The currently allocated payload size. NOTE: this may not be equal to
   * to the tile size. 
   */
  size_t payload_size_;
  /** The tile id. */
  int64_t tile_id_;
  /** The tile type (see Tile::TileType). */
  TileType tile_type_;

  // PRIVATE METHODS
  /** Updates the tile MBR bounds. Applicable only to coordinate tiles. */
  void update_mbr(const void* coords);  
};

#endif
