/**
 * @file   storage_manager.h
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
 * This file defines classes StorageManager. It also defines 
 * StorageManagerException, which is thrown by StorageManager.
 */  

#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H

#include "tile.h"
#include "array_schema.h"
#include "mpi_handler.h"
#include <unistd.h>
#include <vector>
#include <string>
#include <map>
#include <limits>
#include <set>

/** Name of the file storing the array schema. */
#define SM_ARRAY_SCHEMA_FILENAME "array_schema"
/** Suffix of all book-keeping files. */
#define SM_BOOK_KEEPING_FILE_SUFFIX ".bkp"
/** Name of the file storing the bounding coordinates of each tile. */
#define SM_BOUNDING_COORDINATES_FILENAME "bounding_coordinates"
/** Name of the file storing the fragment book-keeping info. */
#define SM_FRAGMENT_TREE_FILENAME "fragment_tree"
/** Indicates an invalid tile position. */
#define SM_INVALID_TILE_POS -1
/** Indicates an invalid tile id. */
#define SM_INVALID_TILE_ID -1
/** Maximum number of arrays that can be simultaneously open. */
#define SM_MAX_OPEN_ARRAYS 100
/** Name of the file storing the MBR of each tile. */
#define SM_MBRS_FILENAME "mbrs"
/** Name of the file storing the offset of each tile in its data file. */
#define SM_OFFSETS_FILENAME "offsets"
/** 
 * Determines the mount of data that can be exchanged between the hard disk and
 * the main memory in a single I/O operation. 
 */
#define SM_SEGMENT_SIZE 10000000
/** Name for temp (usually used in directory paths). */
#define SM_TEMP "temp"
/** Name of the file storing the id of each tile. */
#define SM_TILE_IDS_FILENAME "tile_ids"
/** Suffix of all tile data files. */
#define SM_TILE_DATA_FILE_SUFFIX ".tdt"
/** Max memory size (in bytes) used when creating a new array fragment. */
#define SM_WRITE_STATE_MAX_SIZE 1*1073741824 // 1GB

/** 
 * A storage manager object is responsible for storing/fetching tiles to/from 
 * the disk. It maintains book-keeping structures in main memory to efficiently
 * locate the tile data on disk. 
 */
class StorageManager {
 public:
  // TYPE DEFINITIONS
  class Fragment;

  /** Mnemonic: (first_bound_coord, last_bound_coord) */
  typedef std::pair<void*, void*> BoundingCoordinatesPair;
  /** Mnemonic: <bound_coord_pair#1, bound_coord_pair#2, ...> */
  typedef std::vector<BoundingCoordinatesPair> BoundingCoordinates;
  /** Mnemonic: (level, number of nodes) */
  typedef std::pair<int, int> FragmentTreeLevel;
  /** Menmonic <(level, number of nodes), ...> */
  typedef std::vector<FragmentTreeLevel> FragmentTree;
  /** 
   * A hyper-rectangle in the logical space, including all the coordinates
   * of a tile. It is a list of lower/upper values across each dimension, i.e.,
   * (dim#1_lower, dim#1_upper, dim#2_lower, dim#2_upper, ...).
   */
  typedef void* MBR; 
  /** Mnemonic: <MBR#1, MBR#2, ...> */
  typedef std::vector<MBR> MBRs;
  /** Mnemonic: <offset#1, offset#2, ...> */
  typedef std::vector<int64_t> OffsetList;
  /** Mnemonic: [attribute_id] --> <offset#1, offset#2, ...> */
  typedef std::vector<OffsetList> Offsets;
  /** Mnemonic: [array_name + "_" + array_name] --> array_descriptor */
  typedef std::map<std::string, int> OpenArrays;
  /** Mnemonic: [attribute_id] --> segment_utilziation */
  typedef std::vector<size_t> SegmentUtilization;
  /** Mnemonic: (pos_lower, pos_upper) */
  typedef std::pair<int64_t, int64_t> PosRange;
  /** Mnemonic: [attribute_id] --> (pos_lower, pos_upper) */
  typedef std::vector<PosRange> PosRanges;
  /** Mnemonic: [attribute_id] --> segment */
  typedef std::vector<void*> Segments;
  /** Mnemonic: <tile_id#1, tile_id#2, ...> */
  typedef std::vector<int64_t> TileIds;
  /** Mnemonic: <tile#1, tile#2, ...> */
  typedef std::vector<const Tile*> TileList;
  /** Mnemonic: [attribute_id] --> <tile#1, tile#2, ...> */
  typedef std::vector<TileList> Tiles;

  /**  A logical cell. */
  struct Cell {
    /** The cell buffer. */ 
    void* cell_; 
  };

  /** A logical cell with a tile or cell id. */
  struct CellWithId {
    /** The cell buffer. */ 
    void* cell_; 
    /** An id. */
    int64_t id_;
  };

  /** A logical cell with a tile and a cell id. */
  struct CellWith2Ids {
    /** The cell buffer. */ 
    void* cell_; 
    /** A tile id. */
    int64_t tile_id_;
    /** A cell id. */
    int64_t cell_id_;
  };

  /** 
   * Fragment cells are sorted using a traditional external sortin algorithm.
   * This algorithm produces 'sorted runs', i.e., sorted sequences of cells,
   * during its 'sort' phase. Subsequently, during a 'merge' phase, 
   * multiple runs are merged into a single one (potentially recursively).
    A SortedRun object stores information about a sorted run.
   */
  class SortedRun {
    /** StorageManager objects can manipulate private members of this class. */
    friend class StorageManager; 

   public:
    // CONSTRUCTORS AND DESTRUCTORS
    /** 
     * Takes as input the name of the file of the run, as well as the size of
     * each cell that it stores.
     */
    SortedRun(const std::string& filename, 
              size_t cell_size, size_t segment_size);
    /** Closes the run file and deletes the main memory segment. */
    ~SortedRun();

    // ACCESSORS
    /** Returns the next cell in the main memory segment with the given size. */
    void* current_cell() const;

    // MUTATORS
    /** 
     * Advances the offset in the segment by cell_size to point to the next, 
     * logical cell, and potentially fetches a new segment from the file.
     */
    void advance_cell();
    /** Loads the next segment from the file. */
    void load_next_segment();

   private:
    /** The cell size. */
    size_t cell_size_;
    /** File descriptor of the run. */
    std::string filename_;
    /** Current offset in the file. */
    size_t offset_in_file_;
    /** Current offset in the main memory segment. */
    size_t offset_in_segment_;
    /** Stores cells currently in main memory. */
    char* segment_;
    /** The size of the segment. */
    size_t segment_size_;
    /** The segment utilization. */
    ssize_t segment_utilization_;
  };

  /** Stores the book-keeping structures of a fragment. */
  struct BookKeeping {
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

  /** Stores the state necessary when reading tiles from a fragment. */
  struct ReadState {
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

  /** Stores the state necessary when writing cells to a fragment. */
  struct WriteState {
    /** The bounding coordinates of the currently populated tile. */
    BoundingCoordinatesPair bounding_coordinates_;
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
    /** The MBR of the currently populated tile. */
    void* mbr_;
    /** Stores the cells to be sorted in the current run. */
    void* run_buffer_;
    /** Stores the run buffer size. */
    size_t run_buffer_size_;
    /** Stores the offset in the run buffer for the next write. */
    size_t run_offset_;
    /** Total memory consumption of the current run. */
    size_t run_size_;
    /** Counts the number of sorted runs. */
    int runs_num_;
    /** Stores one segment per attribute. */
    Segments segments_;
    /** Stores the segment utilization. */
    SegmentUtilization segment_utilization_;
    /** The id of the tile being currently populated. */
    int64_t tile_id_;
  };

  /** Contains information about a fragment. */
  class Fragment {
    // StorageManager objects can manipulate the private members of this class.
    friend class StorageManager;

   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Constructor. */
    Fragment(const std::string& workspace, size_t segment_size,
             size_t write_state_max_size,
             const ArraySchema* array_schema, const std::string& fragment_name);
    /** Destructor. */
    ~Fragment();

    // ACCESSORS
    /** Returns the array schema. */ 
    const ArraySchema* array_schema() const 
        { return array_schema_; } 
    /** Returns the fragment name. */
    const std::string& fragment_name() const 
        { return fragment_name_; } 

    // CELL FUNCTIONS
    /** Writes a cell into the fragment. */
    void write_cell(const StorageManager::Cell& cell);
    /** Writes a cell into the fragment. */
    void write_cell(const StorageManager::CellWithId& cell);
    /** Writes a cell into the fragment. */
    void write_cell(const StorageManager::CellWith2Ids& cell); 
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
    /** Returns a tile for a given attribute and tile position. */
    const Tile* get_tile_by_pos(int attribute_id, int64_t pos);  

    // TILE ITERATORS
    /** This class implements a constant tile iterator. */
    class const_tile_iterator {
     public:
      // CONSTRUCTORS & DESTRUCTORS
      /** Iterator constuctor. */
      const_tile_iterator();
      /** Iterator constuctor. */
      const_tile_iterator(Fragment* fragment, int attribute_id, int64_t pos); 

      // OPERATORS
      /** Assignment operator. */
      void operator=(const const_tile_iterator& rhs);
      /** Addition operator. */
      const_tile_iterator operator+(int64_t step);
      /** Addition-assignment operator. */
      void operator+=(int64_t step);
      /** Pre-increment operator. */
      const_tile_iterator operator++();
      /** Post-increment operator. */
      const_tile_iterator operator++(int junk);
      /** 
       * Returns true if the iterator is equal to that in the
       * right hand side (rhs) of the operator. 
       */
      bool operator==(const const_tile_iterator& rhs) const;
      /** 
       * Returns true if the iterator is equal to that in the
       * right hand side (rhs) of the operator. 
       */
      bool operator!=(const const_tile_iterator& rhs) const;
      /** Returns the tile pointed by the iterator. */
      const Tile* operator*() const; 

       // ACCESSORS
      /** Returns the array schema associated with this tile. */
      const ArraySchema* array_schema() const;
      /** Returns the bounding coordinates of the tile. */
      BoundingCoordinatesPair bounding_coordinates() const;
      /** True if the iterators has reached its end. */
      bool end() const { return end_; }
      /** Returns the MBR of the tile. */
      MBR mbr() const;
      /** Returns the position. */
      int64_t pos() const { return pos_; };
      /** Returns the id of the tile. */
      int64_t tile_id() const;

     private:
      // PRIVATE ATTRIBUTES
      /** The attribute id corresponding to this iterator. */
      int attribute_id_;
      /** True if the iterators has reached its end. */
      bool end_;
      /** The array fragment corresponding to this iterator. */
      Fragment* fragment_;
      /** The position of the current tile in the book-keeping structures. */
      int64_t pos_;

      // PRIVATE METHODS
      /** Finds the position of the next tile. */
      void advance_tile();
      /** Finds the position of the next tile inside the stored range. */
      void advance_tile_in_range();
      /** Finds the position of the next tile inside the stored range. */
      template<class T>
      void advance_tile_in_range();
    };

   private:
    // PRIVATE ATTRIBUTES
    /** The array schema (see ArraySchema). */
    const ArraySchema* array_schema_;
    /** The book-keeping structures. */
    BookKeeping book_keeping_;
    /** The fragment name. */
    std::string fragment_name_;
    /** The read state. */ 
    ReadState* read_state_;
    /** The segment size */
     size_t segment_size_;
    /** The workspace where the array data are created. */
    std::string workspace_; 
    /** The write state. */ 
    WriteState* write_state_;
    /** Max memory size of the write state when creating an array fragment. */
    size_t write_state_max_size_;

    // PRIVATE METHODS
    /** 
     * Override the delete operator so that only a StorageManager object can
     * delete dynamically created Fragment objects.
     */
    void operator delete(void* ptr) { ::operator delete(ptr); }
    /** 
     * Override the delete[] operator so that only a StorageManager object can
     * delete dynamically created Fragment objects.
     */
    void operator delete[](void* ptr) { ::operator delete[](ptr); }

    // READ STATE FUNCTIONS
    /** Deletes the tiles of an attribute from main memory. */
    void delete_tiles(int attribute_id);
    /** Initializes the read state. */
    void init_read_state();
    /** Flushes the read state. */
    void flush_read_state();
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

    // WRITE STATE FUNCTIONS
    /** 
     * Appends a (coordinate or attribute) cell to its corresponding segment. 
     */
    void append_cell_to_segment(const void* cell, int attribute_id);
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
    /** Flushes the write state. */
    void flush_write_state();
    /** 
     * Gets the next cell from the input runs that precedes in the global
     * cell order indicated by the input array schema.
     */
    template<class T>
    void* get_next_cell(SortedRun** runs, int runs_num) const;
    /** 
     * Gets the next cell from the input runs that precedes in the global
     * cell order indicated by the input array schema.
     */
    template<class T>
    void* get_next_cell_with_id(SortedRun** runs, int runs_num) const;
    /** 
     * Gets the next cell from the input runs that precedes in the global
     * cell order indicated by the input array schema.
     */
    template<class T>
    void* get_next_cell_with_2_ids(SortedRun** runs, int runs_num) const;
    /** Initializes the write state. */
    void init_write_state();
    /** Makes tiles from existing sorted runs. */
    void make_tiles();
    /** Makes tiles from existing sorted runs. */
    template<class T>
    void make_tiles();
    /** Makes tiles from existing sorted runs. */
    template<class T>
    void make_tiles_with_id();
    /** Makes tiles from existing sorted runs. */
    template<class T>
    void make_tiles_with_2_ids();
    /** Merges existing sorted runs. */
    void merge_sorted_runs();
    /** Merges existing sorted runs. */
    template<class T>
    void merge_sorted_runs();
    /**
     * Each run is named after an integer identifier. This function 
     * merges runs [first_run, last_run] into a new run called new_run in the 
     * next merge operation.
     */
    template<class T>
    void merge_sorted_runs(int first_run, int last_run, int new_run);
    /** Merges existing sorted runs. */
    template<class T>
    void merge_sorted_runs_with_id();
    /**
     * Each run is named after an integer identifier. This function 
     * merges runs [first_run, last_run] into a new run called new_run in the 
     * next merge operation.
     */
    template<class T>
    void merge_sorted_runs_with_id(int first_run, int last_run, int new_run);
    /** Merges existing sorted runs. */
    template<class T>
    void merge_sorted_runs_with_2_ids();
    /**
     * Each run is named after an integer identifier. This function 
     * merges runs [first_run, last_run] into a new run called new_run in the 
     * next merge operation.
     */
    template<class T>
    void merge_sorted_runs_with_2_ids(int first_run, int last_run, int new_run);
    /** Sorts a run in main memory. */
    void sort_run();
    /** Sorts a run in main memory. */
    void sort_run_with_id();
    /** Sorts a run in main memory. */
    void sort_run_with_2_ids();
    /**
     * Updates the info of the currently populated tile with the input
     * coordinates and tile id. 
     */
    template<class T>
    void update_tile_info(const T* coords, int64_t tile_id);

    // BOOK-KEEPING FUNCTIONS
    /** Flushes the book-keeping structures. */
    void flush_book_keeping();
    /** Flushes the bounding coordinates. */
    void flush_bounding_coordinates();
    /** Flushes the tile MBRs. */
    void flush_mbrs();
    /** Flushes the tile offsets. */
    void flush_offsets();
    /** Flushes the tile ids. */
    void flush_tile_ids();
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

  /** 
   * This object holds a vector of Fragment objects and the array schema. It 
   * essentially includes all the information necessary to process an array.
   */
  class Array {
    // StorageManager objects can manipulate the private members of this class.
    friend class StorageManager;

   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Constructor. */
    Array(const std::string& workspace_, size_t segment_size, 
          size_t write_state_max_size,
          const ArraySchema* array_schema, const char* mode); 
    /** Destructor. */
    ~Array(); 

    // ACCESSORS
    /** Returns the array name. */
    const std::string& array_name() const 
        { return array_schema_->array_name(); } 
    /** Returns the array schema. */
    const ArraySchema* array_schema() const 
        { return array_schema_; } 
    /** Checks if the array is empty. */
    bool empty() const { return !fragments_.size(); }

    // CELL FUNCTIONS
    /** Writes a cell into the array. */
    void write_cell(const Cell& cell);
    /** Writes a cell into the array. */
    void write_cell(const CellWithId& cell);
    /** Writes a cell into the array. */
    void write_cell(const CellWith2Ids& cell);
    /** Writes a cell into the array, respecting the global cell order. */
    template<class T>
    void write_cell_sorted(const void* cell);

    // CELL ITERATORS
    /** 
     * A constant cell iterator that iterators over the cells of all the 
     * fragments of the array in the global cell order as specified by the
     * array schema.
     */
    template<class T>
    class const_cell_iterator {
     public:
      // CONSTRUCTORS & DESTRUCTORS
      /** Constructor. */
      const_cell_iterator();
      /** Constructor. */
      const_cell_iterator(Array* array);       
      /** 
       * Constructor. Takes as input also a multi-dimensional range. The
       * iterator will iterate only on the cells of the array whose coordinates
       * fall into the input range.
       */
      const_cell_iterator(Array* array, const T* range); 
      /** Destructor. */
      ~const_cell_iterator();

      // ACCESSORS
      /** Returns true if the iterator has reached the end of the cells. */
      bool end() const;

      // OPERATORS
      /** Increment. */
      void operator++();
      /** Dereference. */
      const void* operator*();

     private:
      // PRIVATE ATTRIBUTES
      /** The array the cell iterator was created for. */
      Array* array_;
      /** Number of attributes. */
      int attribute_num_;
      /** 
       * The current cell. Contains pointers to physical cells of all attibutes.
       */
      void* cell_;
      /** Stores one cell iterator per fragment per attribute. */
      Tile::const_cell_iterator** cell_its_;
      /** Number of dimensions. */
      int dim_num_;
      /** True if the iterator has reached the end of all cells. */
      bool end_;
      /** Number of fragments. */
      int fragment_num_;
      /** 
       * Stores a value per fragment. It is used when iterating cells that fall
       * inside the stored range. It indicates whether the current logical tile
       * under investigation is completely contained in the range or not. 
       */
      bool* full_overlap_;
      /*
       * A multi-dimensional range. If not NULL, the iterator will iterate only
       * on the cells of the array whose coordinates fall into the input range.
       */
      T* range_;
      /** Stores one tile iterator per fragment per attribute. */
      Fragment::const_tile_iterator** tile_its_;

      // PRIVATE METHODS
      /** 
       * Advances the cell iterators of all attributes of the fragment with the
       * input id. 
       */
      void advance_cell(int fragment_id);
      /** 
       * Advances the cell iterators of all attributes of the fragment with the
       * input id, until. 
       */
      void advance_cell_in_range(int fragment_id);
      /** 
       * Finds the next cell from the input fragment along the global cell
       * order, which falls inside the range stored upon initialization of the
       * iterator (see StorageManager::Array::const_cell_iterator::range_). 
       */
      void find_next_cell_in_range(int fragment_id);
      /** 
       * Extracts the next cell from all the fragments along the global cell
       * order. It returns the id of the fragment the cell was extracted from.
       * If the end of the cells is reached, it returns -1.
       */
      int get_next_cell();
      /** Initializes tile and cell iterators. */
      void init_iterators();
      /** 
       * Initializes tile and cell iterators that will irerate over tiles and
       * cells that overlap with the stored range. 
       */
      void init_iterators_in_range();
    };

    // TILE ITERATORS
    /** Begin tile iterator. */
    Fragment::const_tile_iterator begin(
        Fragment* fragment, int attribute_id) const;
 
   private:
    // PRIVATE ATTRIBUTES
    /** The array schema. */
    const ArraySchema* array_schema_;
    /** The array fragments. */
    std::vector<Fragment*> fragments_;
    /** The fragment tree of the array (book-keeping about all fragments). */
    FragmentTree fragment_tree_;  
    /** 
     * The array mode. The following modes are supported:
     *
     * "r": Read mode
     *
     * "w": Write mode (if the array exists, it is deleted)
     *
     * "a": Append mode
     */
     char mode_[2];
     /** The next fragment sequence. */
     int64_t next_fragment_seq_;
     /** The segment size. */
     size_t segment_size_;
     /** The workspace where the array data are created. */
     std::string workspace_; 
     /** Max memory size of the write state when creating an array fragment. */
     size_t write_state_max_size_;

    // PRIVATE METHODS
    /** Closes all the array fragments. */
    void close_fragments();
    /** 
     * Flushes the fragment tree, i.e., the book-keeping structure about the
     * array fragments, to the disk.
     */
    void flush_fragment_tree();
    /** Returns all the existing fragment names. */
    std::vector<std::string> get_fragment_names() const;
    /** 
     * Loads the fragment tree, i.e., the book-keeping structure about the
     * array fragments.
     */
    void load_fragment_tree();
    /** Initializes a new fragment. */
    void new_fragment();
    /** Opens all the existing array fragments. */
    void open_fragments();
    /** 
     * Override the delete operator so that only a StorageManager object can
     * delete dynamically created Array objects.
     */
    void operator delete(void* ptr) { ::operator delete(ptr); }
    /** 
     * Override the delete[] operator so that only a StorageManager object can
     * delete dynamically created Array objects.
     */
    void operator delete[](void* ptr) { ::operator delete[](ptr); }
  };
 
  // CONSTRUCTORS & DESTRUCTORS
  /**
   * Upon its creation, a storage manager object needs a workspace path. The 
   * latter is a folder in the disk where the storage manager creates all the 
   * tile and book-keeping data. Note that the input path must 
   * exist. If the workspace folder exists, the function does nothing, 
   * otherwise it creates it. The segment size determines the amount of data 
   * exchanged in an I/O operation between the disk and the main memory. 
   * The MPI handler takes care of the MPI communication in the distributed
   * setting where there are multiple TileDB processes runnign simultaneously.
   */
  StorageManager(const std::string& path, 
                 const MPIHandler* mpi_handler = NULL,
                 size_t segment_size = SM_SEGMENT_SIZE);
  /** When a storage manager object is deleted, it closes all open arrays. */
  ~StorageManager();
 
  // MUTATORS
  /** Changes the default segment size. */
  void set_segment_size(size_t segment_size) { segment_size_ = segment_size; }
   
  // ARRAY FUNCTIONS
  /** Returns true if the array has been defined. */
  bool array_defined(const std::string& array_name) const;
  /** Returns true if the array is empty. */
  bool array_empty(const std::string& array_name) const;
  /** Returns the schema of an array. The input is an array descriptor */
  const ArraySchema* get_array_schema(int ad) const;
  /** Deletes all the fragments of an array. */
  void clear_array(const std::string& array_name);
  /** Closes an array. */
  void close_array(int ad);
  /** Defines an array (stores its array schema). */
  void define_array(const ArraySchema* array_schema) const;
  /** It deletes an array (regardless of whether it is open or not). */
  void delete_array(const std::string& array_name);
  /** Stores a new schema for an array on the disk. */
  void modify_array_schema(const ArraySchema* array_schema) const;
  /** 
   * Opens an array in the input mode. It returns an 'array descriptor',
   * which is used in subsequent array operations. Currently, the following
   * modes are supported:
   *
   * "r": Read mode
   *
   * "w": Write mode (if the array exists, it is deleted)
   *
   * "a": Append mode
   */
  int open_array(const std::string& array_name, const char* mode);

  // CELL FUNCTIONS
  /** 
   * Takes as input an array descriptor and returns an array begin constant 
   * cell iterator.
   */
  template<class T>
  Array::const_cell_iterator<T> begin(int ad) const;
  /** 
   * Takes as input an array descriptor and a range and returns an array begin 
   * constant cell iterator. The iterator iterates only over the cells whose
   * coordinates lie within the input range, following the global cell order.
   */
  template<class T>
  Array::const_cell_iterator<T> begin(int ad, const T* range) const;
  /**
   * Takes as input an array descriptor and a multi-dimensional range, and 
   * returns the cells whose coordinates fall inside the range, as well as
   * their number (last two arguments).
   */
  void read_cells(int ad, const void* range, 
                  void*& cells, int64_t& cell_num) const;
  /**
   * Takes as input an array descriptor and a multi-dimensional range, and 
   * returns the cells whose coordinates fall inside the range, as well as
   * their number (last two arguments).
   */
  template<class T>
  void read_cells(int ad, const T* range, 
                  void*& cells, int64_t& cell_num) const;
  /**
   * Takes as input an array descriptor, a multi-dimensional range and 
   * the rank of the process that will receive the data. It returns form
   * all the processes the cells whose coordinates fall inside the input range,
   * as well as their number (last two arguments).
   */
  void read_cells(int ad, const void* range, 
                  void*& cells, int64_t& cell_num,
                  int rcv_rank) const;
  /**
   * Takes as input an array descriptor, a multi-dimensional range and 
   * the rank of the process that will receive the data. It returns form
   * all the processes the cells whose coordinates fall inside the input range,
   * as well as their number (last two arguments).
   */
  template<class T>
  void read_cells(int ad, const T* range,
                  void*& cells, int64_t& cell_num,
                  int rcv_rank) const;
  /**  
   * Writes a cell to an array. It takes as input an array descriptor, and
   * a cell pointer. The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  void write_cell(int ad, const void* cell) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema.
   */
  void write_cells(int ad, const void* cells, int64_t cell_num) const; 
  /**  
   * Writes a cell to an array. It takes as input an array descriptor, and
   * a cell pointer. The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema. This function
   * is used only when it is guaranteed that the cells are written
   * respecting the global cell order as specified in the array schema.
   */
  template<class T>
  void write_cell_sorted(int ad, const void* cell) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema. This function is used only when it is guaranteed that the
   * cells are written respecting the global cell order as specified in the
   * array schema.
   */
  void write_cells_sorted(int ad, const void* cells, int64_t cell_num) const; 
  /**  
   * Writes a set of cells to an array. It takes as input an array descriptor,
   * and a pointer to cells, which are serialized one after the other. Each cell
   * has the following format: The coordinates appear first, and then the
   * attribute values in the same order as the attributes are defined in the
   * array schema. This function is used only when it is guaranteed that the
   * cells are written respecting the global cell order as specified in the
   * array schema.
   */
  template<class T>
  void write_cells_sorted(int ad, const void* cells, int64_t cell_num) const; 

 private: 
  // PRIVATE ATTRIBUTES
  /** Keeps track of the descriptors of the currently open arrays. */
  OpenArrays open_arrays_;
  /** Stores all the open arrays. */
  Array** arrays_; 
  /** The MPI communication handler. */
  const MPIHandler* mpi_handler_; 
  /** 
   * Determines the amount of data that can be exchanged between the 
   * hard disk and the main memory in a single I/O operation. 
   */
  size_t segment_size_;
  /** 
   * Is a folder in the disk where the storage manager creates 
   * all the array data (i.e., tile and index files). 
   */
  std::string workspace_;
  /** Max memory size of the write state when creating an array fragment. */
  size_t write_state_max_size_;
  
  // PRIVATE METHODS
  /** Checks when opening an array. */
  void check_on_open_array(const std::string& array_name, 
                           const char* mode) const;
  /** Returns the array schema. */
  const ArraySchema* get_array_schema(const std::string& array_name) const; 
  /** Checks the validity of the array mode. */
  bool invalid_array_mode(const char* mode) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Stores an array object and returns an array descriptor. */
  int store_array(Array* array);
}; 

/** This exception is thrown by StorageManager. */
class StorageManagerException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  StorageManagerException(const std::string& msg) 
      : msg_(msg) {}
  /** Empty destructor. */
  ~StorageManagerException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
