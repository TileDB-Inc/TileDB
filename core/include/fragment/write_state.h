/**
 * @file   write_state.h
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
 * This file defines class WriteState. 
 */

#ifndef WRITE_STATE_H
#define WRITE_STATE_H

#include "array_schema.h"
#include "book_keeping.h"
#include "tile.h"
#include <iostream>

/** Initial buffer size (in bytes). It will keep on doubling. */
#define BUFFER_INITIAL_SIZE 40000

/** Stores the state necessary when writing cells to a fragment. */
class WriteState {
 public:
  // TYPE DEFINITIONS & DECLARATIONS
  /** Mnemonic: [attribute_id] --> segment */
  typedef std::vector<void*> Segments;
  /** Mnemonic: [attribute_id] --> segment_utilziation */
  typedef std::vector<size_t> SegmentUtilization;
  struct Cell;
  struct CellWithId;
  struct CellWith2Ids;  
  template<typename T> struct SmallerCol;
  template<typename T> struct SmallerColWithId;
  template<typename T> struct SmallerRow;
  template<typename T> struct SmallerRowWithId;
  template<typename T> struct SmallerWith2Ids;

  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  WriteState(
      const ArraySchema* array_schema, 
      const std::string* fragment_name,
      const std::string* temp_dirname,
      const std::string* workspace,
      BookKeeping* book_keeping,
      size_t segment_size,
      size_t write_state_max_size);  
  /** Destructor. */
  ~WriteState();

  // ACCESSORS
  /** Returns the number of cells currently in the write state. */
  int64_t cell_num() const;

  // MUTATORS
  /** Flushes the write state onto the disk. */
  void flush();
  /**  
   * Writes a cell to the write state. It takes as input a cell and its size
   * The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  template<class T>
  void write_cell(const void* cell);
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
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The array book-keeping structures. */
  BookKeeping* book_keeping_;
  /** The bounding coordinates of the currently populated tile. */
  Tile::BoundingCoordinatesPair bounding_coordinates_;
  /** Buffers that store the cells. */
  std::vector<void*> buffers_;
  /** Sizes of buffers that store the cells. */
  std::vector<size_t> buffers_sizes_;
  /** Utilization of buffers that store the cells. */
  std::vector<size_t> buffers_utilization_;
  /** Stores logical cells. */
  std::vector<Cell> cells_;
  /** Stores logical cells. */
  std::vector<CellWithId> cells_with_id_;
  /** Stores logical cells. */
  std::vector<CellWith2Ids> cells_with_2_ids_;
  /** The number of cell in the tile currently being populated. */
  int64_t cell_num_;
  /** 
   * Keeping track of the offsets of the attribute files (plus coordinates),
   * when writing cells in a sorted manner to create the tiles.
   */
  std::vector<int64_t> file_offsets_;
  /** The fragment name. */
  const std::string* fragment_name_;
  /** The MBR of the currently populated tile. */
  void* mbr_;
  /** Stores the offset in the run buffer for the next write. */
  size_t run_offset_;
  /** Total memory consumption of the current run. */
  size_t run_size_;
  /** Counts the number of sorted runs. */
  int runs_num_;
  /** The segment size. */
  size_t segment_size_;
  /** Stores one segment per attribute. */
  Segments segments_;
  /** Stores the segment utilization. */
  SegmentUtilization segment_utilization_;
  /** Temporary directory name. */
  const std::string* temp_dirname_;
  /** The id of the tile being currently populated. */
  int64_t tile_id_;
  /** The workspace. */
  const std::string* workspace_; 
  /** Max memory size of the write state when creating an array fragment. */
  size_t write_state_max_size_;

  // PRIVATE METHODS
  /** 
   * Appends an attribute value to the corresponding segment, and returns (by
   * reference) the (potentially variable) attribute value size. 
   */
  void append_attribute_to_segment(const char* attr, int attribute_id,
                                   size_t& attr_size);
  /** Appends the coordinate to the corresponding segment. */
  void append_coordinates_to_segment(const char* coords);
  /** Copies a cell in the buffers of the write state. */
  void* copy_cell(const void* cell, size_t cell_size);
  /** Sorts and writes the last run on the disk. */
  void finalize_last_run();
  /** Flushes a segment to its corresponding file. */
  void flush_segment(int attribute_id);
  /** Flushes all segments to their corresponding files. */
  void flush_segments();
  /** Writes a sorted run on the disk. */
  void flush_sorted_run();
  /** Writes a sorted run on the disk. */
  void flush_sorted_run_with_id();
  /** Writes a sorted run on the disk. */
  void flush_sorted_run_with_2_ids();
  /** 
   * Writes the info about the lastly populated tile to the book keeping
   * structures. 
   */
  void flush_tile_info_to_book_keeping();
  /** Makes tiles from existing sorted runs, stored in dirname. */
  void make_tiles(const std::string& dirname);
  /** Makes tiles from existing sorted runs, stored in dirname. */
  template<class T>
  void make_tiles(const std::string& dirname);
  /** Makes tiles from existing sorted runs, stored in dirname. */
  template<class T>
  void make_tiles_with_id(const std::string& dirname);
  /** Makes tiles from existing sorted runs, stored in dirname. */
  template<class T>
  void make_tiles_with_2_ids(const std::string& dirname);
  /** Sorts a run in main memory. */
  void sort_run();
  /** Sorts a run in main memory. */
  void sort_run_with_id();
  /** Sorts a run in main memory. */
  void sort_run_with_2_ids();

  /**
   * Updates the info of the currently populated tile with the input
   * coordinates,  tile id, and sizes of all attribute values in the cell. 
   */
  template<class T>
  void update_tile_info(const T* coords, int64_t tile_id, 
                        const std::vector<size_t>& attr_sizes);
};

/**  A logical cell. */
struct WriteState::Cell {
  /** The cell buffer. */ 
  void* cell_; 
};

/** A logical cell with a tile or cell id. */
struct WriteState::CellWithId {
  /** The cell buffer. */ 
  void* cell_; 
  /** An id. */
  int64_t id_;
};

/** A logical cell with a tile and a cell id. */
struct WriteState::CellWith2Ids {
  /** The cell buffer. */ 
  void* cell_; 
  /** A tile id. */
  int64_t tile_id_;
  /** A cell id. */
  int64_t cell_id_;
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct WriteState::SmallerCol {
  /** Constructor. */
  SmallerCol(int dim_num) { dim_num_ = dim_num; }

  /** Comparison operator. */
  bool operator () (const Cell& a, const Cell& b) {
    const T* coords_a = static_cast<const T*>(a.cell_);
    const T* coords_b = static_cast<const T*>(b.cell_);

    for(int i=dim_num_-1; i>=0; --i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

  /** Number of dimension. */
  int dim_num_;
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct WriteState::SmallerColWithId {
  /** Constructor. */
  SmallerColWithId(int dim_num) { dim_num_ = dim_num; }

  /** Comparison operator. */
  bool operator () (const CellWithId& a, 
                    const CellWithId& b) {
    if(a.id_ < b.id_)
      return true;

    if(a.id_ > b.id_)
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = static_cast<const T*>(a.cell_);
    const T* coords_b = static_cast<const T*>(b.cell_);

    for(int i=dim_num_-1; i>=0; --i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

  /** Number of dimensions. */
  int dim_num_;
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct WriteState::SmallerRow {
  /** Constructor. */
  SmallerRow(int dim_num) { dim_num_ = dim_num; }

  /** Comparison operator. */
  bool operator () (const Cell& a, const Cell& b) {
    const T* coords_a = static_cast<const T*>(a.cell_);
    const T* coords_b = static_cast<const T*>(b.cell_);

    for(int i=0; i<dim_num_; ++i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

  /** Number of dimension. */
  int dim_num_;
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct WriteState::SmallerRowWithId {
  /** Constructor. */
  SmallerRowWithId(int dim_num) { dim_num_ = dim_num; }

  /** Comparison operator. */
  bool operator () (const CellWithId& a, 
                    const CellWithId& b) {

    if(a.id_ < b.id_)
      return true;

    if(a.id_ > b.id_)
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = static_cast<const T*>(a.cell_);
    const T* coords_b = static_cast<const T*>(b.cell_);

    for(int i=0; i<dim_num_; ++i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

  /** Number of dimension. */
  int dim_num_;
};

/** Wrapper of comparison function for sorting cells. */
template<typename T>
struct WriteState::SmallerWith2Ids {
  /** Constructor. */
  SmallerWith2Ids(int dim_num) { dim_num_ = dim_num; }

  /** Comparison operator. */
  bool operator () (const CellWith2Ids& a, 
                    const CellWith2Ids& b) {
    if(a.tile_id_ < b.tile_id_)
      return true;

    if(a.tile_id_ > b.tile_id_)
      return false;

    if(a.cell_id_ < b.cell_id_)
      return true;

    if(a.cell_id_ > b.cell_id_)
      return false;

    // a.tile_id_ == b.tile_id_ && 
    // a.cell_id_ == b.cell_id_     --> check coordinates (row major)
    const T* coords_a = static_cast<const T*>(a.cell_);
    const T* coords_b = static_cast<const T*>(b.cell_);

    for(int i=0; i<dim_num_; ++i) 
      if(coords_a[i] < coords_b[i]) 
        return true;
      else if(coords_a[i] > coords_b[i]) 
        return false;
      // else coords_a[i] == coords_b[i] --> continue

    return false;
  }

  /** Number of dimension. */
  int dim_num_;
};

#endif
