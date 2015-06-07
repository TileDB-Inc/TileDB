/**
 * @file   fragment.h
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
 * This file defines class Fragment. 
 */

#ifndef FRAGMENT_H
#define FRAGMENT_H

#include "array_schema.h"
#include "book_keeping.h"
#include "fragment_const_tile_iterator.h"
#include "fragment_const_reverse_tile_iterator.h"
#include "read_state.h"
#include "sorted_run.h"
#include "tile.h"
#include "write_state.h"
#include <string>

class FragmentConstTileIterator;
class FragmentConstReverseTileIterator;

/** Contains information about a fragment. */
class Fragment {
 public:
  // Friend classes that can manipulate the private attributes and methods
  friend class Array;
  friend class FragmentConstTileIterator;
  friend class FragmentConstReverseTileIterator;

  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  Fragment(const std::string& workspace, size_t segment_size,
           size_t write_state_max_size,
           const ArraySchema* array_schema, const std::string& fragment_name);
  /** Destructor. */
  ~Fragment();

  // ACCESSORS
  /** Returns the array schema. */ 
  const ArraySchema* array_schema() const; 
  /** Returns the fragment name. */
  const std::string& fragment_name() const; 
  /** Returns the number of tiles in the fragment. */ 
  int64_t tile_num() const;
  /** 
   * Returns the size of the tile for the input attribute at the input 
   * position. 
   */
  size_t tile_size(int attribute_id, int64_t pos) const;

  // CELL FUNCTIONS
  /**  
   * Writes a cell to the fragment. It takes as input a cell and its size. 
   * The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  template<class T>
  void write_cell(const void* cell, size_t cell_size) const; 


  /** Writes a cell into the fragment. */ // TODO: change this
//  void write_cell(const WriteState::Cell& cell, size_t cell_size);
  /** Writes a cell into the fragment. */ // TODO: change this
//  void write_cell(const WriteState::CellWithId& cell, size_t cell_size);
  /** Writes a cell into the fragment. */ // TODO: change this
//  void write_cell(const WriteState::CellWith2Ids& cell, size_t cell_size);


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

  // TILE FUNCTIONS
  /** Begin tile iterator. */
  FragmentConstTileIterator begin(int attribute_id);
  /** Returns a tile for a given attribute and tile position. */
  const Tile* get_tile_by_pos(int attribute_id, int64_t pos);  
  /** Begin reverse tile iterator. */
  FragmentConstReverseTileIterator rbegin(int attribute_id);
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
  /** The array schema (see ArraySchema). */
  const ArraySchema* array_schema_;
  /** The book-keeping structures. */
  BookKeeping* book_keeping_;
  /** The fragment name. */
  std::string fragment_name_;
  /** The read state. */ 
  ReadState* read_state_;
  /** The segment size */
  size_t segment_size_;
  /** Temporary directory. */
  std::string temp_dirname_;
  /** The workspace where the array data are created. */
  std::string workspace_; 
  /** The write state. */ 
  WriteState* write_state_;

  // PRIVATE METHODS
  // --- READ STATE FUNCTIONS
  /** Clears the read state. */
  void clear_read_state();
  /** Deletes the tiles of an attribute from main memory. */
  void delete_tiles(int attribute_id);
  /** Initializes the read state. */
  void init_read_state();
  /** 
   * Loads data into the fragment, which are stored in files inside the input
   * directory. Each file stores the cells in binary form, sorted based
   * on the global cell order specified in the array schema. Each cell
   * must have the same binary format as that used when creating sorted
   * runs triggered by StorageManager::write_cell.
   */
  void load_sorted_bin(const std::string& dirname);
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
  /** 
   * Loads the payloads of the tiles of a given attribute from disk and into
   * the corresponding segment in the read state, starting from the tile at
   * position pos. Returns the segment utilization after the load, and the
   * number of tiles loaded.
   */
  std::pair<size_t, int64_t> load_payloads_into_segment(
      int attribute_id, int64_t pos);

  // --- WRITE STATE FUNCTIONS
  /** 
   * Appends an attribute value to the corresponding segment, and returns (by
   * reference) the (potentially variable) attribute value size. 
   */
  void append_attribute_to_segment(const char* attr, int attribute_id,
                                   size_t& attr_size);
  /** 
   * Appends the coordinate to the corresponding segment. 
   */
  void append_coordinates_to_segment(const char* coords);
  /** Clears the write state. */
  void clear_write_state();
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
  /** Flushes the write state onto the disk. */
  void flush_write_state();
  /** 
   * Gets the next cell from the input runs that precedes in the global
   * cell order indicated by the input array schema. If the cell is
   * variable-sized, the function will return into cell_size
   * (passed by reference) the cell size.
   */
  template<class T>
  void* get_next_cell(
      SortedRun** runs, int runs_num, size_t& cell_size) const;
  /** 
   * Gets the next cell from the input runs that precedes in the global
   * cell order indicated by the input array schema.
   */
  template<class T>
  void* get_next_cell_with_id(SortedRun** runs, int runs_num, 
                              size_t& cell_size) const;
  /** 
   * Gets the next cell from the input runs that precedes in the global
   * cell order indicated by the input array schema.
   */
  template<class T>
  void* get_next_cell_with_2_ids(SortedRun** runs, int runs_num, 
                                 size_t& cell_size) const;
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
  /** 
   * Merges existing sorted runs. The dirname is the directory where the
   * initial sorted runs are stored.
   */
  bool merge_sorted_runs(const std::string& dirname);
  /** 
   * Merges existing sorted runs. The dirname is the directory where the
   * initial sorted runs are stored.
   */
  template<class T>
  bool merge_sorted_runs(const std::string& dirname);
  /**
   * Each run is named after an integer identifier. This function 
   * merges runs [first_run, last_run] into a new run called new_run in the 
   * next merge operation.
   */
  template<class T>
  void merge_sorted_runs(
      const std::string& dirname, const std::vector<std::string>& filenames, 
      int first_run, int last_run, int new_run);
  /** 
   * Merges existing sorted runs. The dirname is the directory where the
   * initial sorted runs are stored.
   */
  template<class T>
  bool merge_sorted_runs_with_id(const std::string& dirname); 
  /**
   * Each run is named after an integer identifier. This function 
   * merges runs [first_run, last_run] into a new run called new_run in the 
   * next merge operation.
   */
  template<class T>
  void merge_sorted_runs_with_id(
      const std::string& dirname, const std::vector<std::string>& filenames, 
      int first_run, int last_run, int new_run);
  /** 
   * Merges existing sorted runs. The dirname is the directory where the
   * initial sorted runs are stored.
   */
  template<class T>
  bool merge_sorted_runs_with_2_ids(const std::string& dirname); 
  /**
   * Each run is named after an integer identifier. This function 
   * merges runs [first_run, last_run] into a new run called new_run in the 
   * next merge operation.
   */
  template<class T>
  void merge_sorted_runs_with_2_ids(
      const std::string& dirname, const std::vector<std::string>& filenames, 
      int first_run, int last_run, int new_run);
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

  // --- BOOK-KEEPING FUNCTIONS
  /** Clears the book keeping structures from main memory. */
  void clear_book_keeping();
  /** 
   * Writes the book keeping structures on the disk, but does not clear
   * them from main memory. 
   */
  void commit_book_keeping();
  /** 
   * Writes the bounding coordinates on the disk, but does not clear
   * them from main memory. 
   */
  void commit_bounding_coordinates();
  /** 
   * Writes the MBRs on the disk, but does not clear
   * them from main memory. 
   */
  void commit_mbrs();
  /** 
   * Writes the offsets on the disk, but does not clear
   * them from main memory. 
   */
  void commit_offsets();
  /** 
   * Writes the tile ids on the disk, but does not clear
   * them from main memory. 
   */
  void commit_tile_ids();
  /** Initializes the book-keeping structures. */
  void init_book_keeping();
  /** Loads the book-keeping structures. */
  void load_book_keeping();
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
