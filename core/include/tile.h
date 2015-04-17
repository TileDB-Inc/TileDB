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
#include <limits>
#include <string>
#include <typeinfo>
#include <vector>

/** Deleted char. */
#define TL_DEL_CHAR 127
/** Deleted int. */
#define TL_DEL_INT std::numeric_limits<int>::min()
/** Deleted int64_t. */
#define TL_DEL_INT64_T std::numeric_limits<int64_t>::min()
/** Deleted float. */
#define TL_DEL_FLOAT std::numeric_limits<float>::min()
/** Deleted double. */
#define TL_DEL_DOUBLE std::numeric_limits<double>::min()
/** Missing char. */
#define TL_NULL_CHAR 0
/** Missing int. */
#define TL_NULL_INT std::numeric_limits<int>::max()
/** Missing int64_t. */
#define TL_NULL_INT64_T std::numeric_limits<int64_t>::max()
/** Missing float. */
#define TL_NULL_FLOAT std::numeric_limits<float>::max()
/** Missing double. */
#define TL_NULL_DOUBLE std::numeric_limits<double>::max()
/** Default payload capacity. */
#define TL_PAYLOAD_CAPACITY 100

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

  // CONSTRUCTORS AND DESTRUCTORS
  /**
   * Constructor. If dim_num is 0, then this is an attribute tile; otherwise,
   * it is a coordinate tile. The payload_capacity argument practically 
   * determines the amount of memory to be reserved for the payload of the
   * tile. It is expressed as the number of cells (of type cell_type) that
   * can be stored in the payload. Note though that this does not limit
   * the number of cells that can be stored in the tile; if more than
   * payload_capacity cells are appended to the tile, the payload
   * will be properly expanded. This is merely an optimization to prevent
   * numerous (potentially unnecessary) payload expansions as multiple
   * cells are appended to it.
   */
  Tile(int64_t tile_id, int dim_num, const std::type_info* cell_type, 
       int64_t payload_capacity = TL_PAYLOAD_CAPACITY); 
  /** 
   * Destructor. We distinguish two cases: (i) the tile was created by
   * appending cells one by one, or (ii) the tile was created by
   * assigning to it an existing payload. In the former case, the tile 
   * dynamically allocates mempory for storing the payload (and
   * other information). In the latter case, the memory where the payload
   * resides does not belong to the tile. Therefore, the destructor frees
   * only the dynamically allocated memory in the first case.
   */
  ~Tile(); 

  // ACCESSORS
  /** 
   * Returns the bounding coordinates, i.e., the first and 
   * last coordinates that were appended to the tile. It applies only to
   * coordinate tiles. The bounding coordinates are typically useful
   * when the cells in the tile are sorted in a certain order. 
   */
  std::pair<const void*, const void*> bounding_coordinates() const;
  /** Returns a pointer to the pos-th cell in the payload. */
  const void* cell(int64_t pos) const;
  /** Returns the number of cells in the tile. */
  int64_t cell_num() const { return cell_num_; }
  /** Returns the cell size (in bytes). */
  size_t cell_size() const { return cell_size_; } 
  /** Returns the cell type. */
  const std::type_info* cell_type() const { return cell_type_; } 
  /** Copies the tile payload into buffer. */
  void copy_payload(void* buffer) const;
  /** Returns the number of dimensions. It returns 0 for attribute tiles. */
  int dim_num() const { return dim_num_; }
  /** Returns the MBR (see Tile::mbr_). */
  const void* mbr() const { return mbr_; }
  /** Returns the tile id. */
  int64_t tile_id() const { return tile_id_; }
  /** Returns the tile size (in bytes). */
  size_t tile_size() const { return tile_size_; } 
  /** Returns the tile type. */
  TileType tile_type() const { return tile_type_; } 
 
  // MUTATORS
  /** 
   * Clears the tile. We distinguish two cases: (i) the tile was created by
   * appending cells one by one, or (ii) the tile was created by
   * assigning to it an existing payload. In the former case, the tile 
   * dynamically allocates mempory for storing the payload (and
   * other information). In the latter case, the memory where the payload
   * resides does not belong to the tile. Therefore, the destructor frees
   * only the dynamically allocated memory in the first case.
   */
  void clear();
  /** MBR setter. Applicable only to coordinate tiles. */
  void set_mbr(const void* mbr);
  /** Payload setter. */
  void set_payload(void* payload, size_t payload_size); 

  // OPERATORS
  /** Appends a cell value to (the end of) a tile. */
  void operator<<(void* value); 
  /** Appends a cell value to (the end of) a tile. */
  void operator<<(const void* value); 
  /** 
   * Appends a cell value to (the end of) a tile.
   * 
   * Make sure that type T is the same as the cell type. Otherwise, in debug
   * mode an assert is triggered, whereas in release mode the behavior is
   * unpredictable.
   */
  template<class T>
  void operator<<(T* value); 
  /** 
   * Appends a cell value to (the end of) a tile.
   * 
   * Make sure that type T is the same as the cell type. Otherwise, in debug
   * mode an assert is triggered, whereas in release mode the behavior is
   * unpredictable.
   */
  template<class T>
  void operator<<(const T* value); 
  /** 
   * Appends a cell value to (the end of) a tile.
   * 
   * Make sure that type T is the same as the cell type. Otherwise, in debug
   * mode an assert is triggered, whereas in release mode the behavior is
   * unpredictable.
   */
  template<class T>
  void operator<<(const T& value); 

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

  // CELL ITERATOR
  /** This class implements a constant cell iterator. */
  class const_cell_iterator {
   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Empty iterator constuctor. */
    const_cell_iterator();
    /** 
     * Constuctor that takes as input the tile for which the 
     * iterator is created, and a cell position in the tile payload. 
     */
    const_cell_iterator(const Tile* tile, int64_t pos);
    
    // ACCESSORS
    /** Returns the cell type of the tile. */
    const std::type_info* cell_type() const { return tile_->cell_type(); }
    /** Returns the number of dimensions of the tile. */
    int dim_num() const { return tile_->dim_num(); }
    /** Returns true if the end of the iterator is reached. */
    bool end() const { return end_; }
    /** Returns the current payload position of the cell iterator. */
    int64_t pos() const { return pos_; }
    /** Returns the tile the cell iterator belongs to. */
    const Tile* tile() const { return tile_; }

    // MISC
    /** True if the iterator points to a cell representing a deletion. */
    bool is_del() const;
    /** True if the iterator points to a NULL cell. */
    bool is_null() const;

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
     * Returns true if the operands belong to the same tile and they point to
     * the same cell. 
     */
    bool operator==(const const_cell_iterator& rhs) const;
    /**
     * Returns true if either the operands belong to different tiles, or the
     * they point to different cells. 
     */
    bool operator!=(const const_cell_iterator& rhs) const;
    /** Returns the pointer in the tile payload of the cell it points to. */ 
    const void* operator*() const;

    // MISC
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
  /** Returns a cell iterator pointing to the first cell of the tile. */
  const_cell_iterator begin() const;
  static const_cell_iterator end();
 
 private:
  // PRIVATE ATTRIBUTES
  /** The number of cells in the tile. */
  int64_t cell_num_;
  /** The cell size (in bytes). */
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
  /** 
   * It is true if the memory for storing the payload was allocated by the
   * Tile object. In other words, it is true if the payload was created
   * by the Tile object, and false if it is set via Tile::set_payload.
   */ 
  bool payload_alloc_;
  /**  
   * The currently allocated payload capacity. 
   * 
   * NOTE #1: This is different from the tile capacity (i.e., Tile::cell_num_). 
   * The payload capacity indicates how many cells fit in the currently
   * allocated memory for the payload. When a newly appended cell causes
   * the payload to overflow, the memory allocated for the payload doubles
   * (via Tile::expand_payload). The payload capacity helps keeping track when
   * the payload expansion should occur.
   *
   * NOTE #2: If the allocated payload capacity is equal to 0, then no 
   * memory has been dynamically allocated by the object. In this case,
   * if Tile::payload_ is not NULL, it means that it has been set to
   * point to a memory location not owned by the Tile object 
   * (via Tile::set_payload). 
   */
  int64_t payload_capacity_;
  /** The tile id. */
  int64_t tile_id_;
  /** The tile size (in bytes). */
  size_t tile_size_;
  /** The tile type. */
  TileType tile_type_;

  // PRIVATE METHODS
  /** Deletes the MBR (if it has been created by the Tile object). */
  void clear_mbr();
  /** Deletes the payload (if it has been created by the Tile object). */
  void clear_payload();
  /** Expands the tile MBR bounds. Applicable only to coordinate tiles. */
  template<class T>
  void expand_mbr(const T* coords);  
  /** 
   * Expands the payload (doubling its capacity) if a newly appended cell does
   * not fit in the currently allocated one. 
   */
  void expand_payload();
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
