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
 * This file defines class StorageManager.
 */  

#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H

#include "tile.h"
#include "array_schema.h"
#include <vector>
#include <string>
#include <map>
#include <limits>
#include <set>

/** Name of the file storing the array schema. */
#define SM_ARRAY_SCHEMA_FILENAME "array_schema"
/** Name of the file storing the bounding coordinates of each tile. */
#define SM_BOUNDING_COORDINATES_FILENAME "bounding_coordinates"
/** Suffix of all book-keeping files. */
#define SM_BOOK_KEEPING_FILE_SUFFIX ".bkp"
/** Name of the file storing the MBR of each tile. */
#define SM_MBRS_FILENAME "mbrs"
/** Name of the file storing the offset of each tile in its data file. */
#define SM_OFFSETS_FILENAME "offsets"
/** 
 * The segment size determines the minimum amount of data that can be exchanged
 * between the hard disk and the main memory in a single I/O operation. 
 * Unless otherwise defined, this default size is used. 
 */
#define SM_SEGMENT_SIZE 10000000
/** Name of the file storing the id of each tile. */
#define SM_TILE_IDS_FILENAME "tile_ids"
/** Suffix of all tile data files. */
#define SM_TILE_DATA_FILE_SUFFIX ".tdt"
/** Special value returned by StorageManager::tile_rank.  */
#define SM_INVALID_RANK std::numeric_limits<uint64_t>::max()
/** Special value used in ArrayInfo::lastly_appended_tile_ids_.  */
#define SM_INVALID_TILE_ID std::numeric_limits<uint64_t>::max()

/** 
 * A storage manager object is responsible for storing/fetching tiles
 * to/from the disk, and managing the tiles in main memory. It
 * maintains all the book-keeping structures and data files for the 
 * created arrays. 
 *
 * If there are m attributes in an array, a logical tile in the 
 * multi-dimensional space corresponds to m+1 physical tiles on the disk;
 * one for each of the m attributes, and one for the coordinates which
 * is regarded as an extra (m+1)-th attribute.
 * The storage manager stores the physical tiles of each attribute into
 * a separate file on the disk.
 */
class StorageManager {
 public:
  // TYPE DEFINITIONS
  class ArrayInfo;

  /** 
   * An array is opened either to be created (CREATE mode) or to be read 
   * (READ mode), but not both. After an array is created, no more tiles
   * can be inserted to it, and no modifications of existing tiles are allowed
   * (array updates are handled by the consolidation module).
   */
  enum ArrayMode {READ, CREATE};

  /** Mnemonic: (first_bound_coord, last_bound_coord) */
  typedef std::pair<std::vector<double>, std::vector<double> > 
      BoundingCoordinatesPair;
  /** Mnemonic: <bound_coord_pair#1, bound_coord_pair#2, ...> */
  typedef std::vector<BoundingCoordinatesPair> BoundingCoordinates;
  /** 
   * A hyper-rectangle in the logical space, including all the coordinates
   * of a tile. It is a list of low/high values across each dimension, i.e.,
   * (dim#1_low, dim#1_high, dim#2_low, dim#2_high, ...).
   */
  typedef std::vector<double> MBR; 
  /** Mnemonic: <MBR#1, MBR#2, ...> */
  typedef std::vector<MBR> MBRs;
  /** Mnemonic: <offset#1, offset#2, ...> */
  typedef std::vector<uint64_t> OffsetList;
  /** Mnemonic: [attribute_id] --> <offset#1, offset#2, ...> */
  typedef std::vector<OffsetList> Offsets;
  /** Mnemonic: [array_name] --> ArrayInfo */
  typedef std::map<std::string, ArrayInfo> OpenArrays;
  /** Mnemonic: [attribute_id] --> payload_size */
  typedef std::vector<uint64_t> PayloadSizes;
  /** Mnemonic: (rank_low, rank_high) */
  typedef std::pair<uint64_t, uint64_t> RankRange;
  /** Mnemonic: [attribute_id] --> (rank_low, rank_high) */
  typedef std::vector<RankRange> RankRanges;
  /** Mnemonic: <tile_id#1, tile_id#2, ...> */
  typedef std::vector<uint64_t> TileIds;
  /** Mnemonic: <tile#1, tile#2, ...> */
  typedef std::vector<const Tile*> TileList;
  /** Mnemonic: [attribute_id] --> <tile#1, tile#2, ...> */
  typedef std::vector<TileList> Tiles;

  /** 
   * This struct groups info about an array (e.g., schema, book-keeping 
   * structures, etc.).
   */  
  struct ArrayInfo {
    /** The array mode (see StorageManager::ArrayMode). */
    ArrayMode array_mode_;
    /** The array schema (see class ArraySchema). */
    ArraySchema array_schema_; 
    /** 
     * Stores the bounding coordinates of every (coordinate) tile, i.e., the first
     * and last cell of the tile (see Tile::bounding_coordinates).
     */
    BoundingCoordinates bounding_coordinates_;   
    /** 
     * Unique ArrayInfo object id, for debugging purposes when using 
     * StorageManager::ArrayDescriptor objects 
     * (see StorageManager::ArrayDescriptor::array_info_id_).
     */
    uint64_t id_;
    /** 
     * It keeps the id of the lastly appended tile for each attribute. It is
     * used for debugging purposes to ensure the array "correctness" in 
     * StorageManager::check_on_append_tile.
     */
    std::vector<uint64_t> lastly_appended_tile_ids_;
    /** Stores the MBR of every (coordinate) tile. */
    MBRs mbrs_; 
    /** 
     * Stores the offset (i.e., starting position) of every tile of every 
     * attribute in the respective data file. 
     */
    Offsets offsets_;
    /** 
     * Stores the aggregate payload size of the tiles currently stored in main 
     * memory for each attribute. 
     */
    PayloadSizes payload_sizes_;
    /** 
     * Stores the range of the ranks of the tiles currently in main memory,
     * for each attribute. The rank of a tile is a sequence number indicating
     * the order in which it was appended to the array with respect to the
     * the other tiles appended to the array for the same attribute (e.g.,
     * 0 means that it was appended first, 1 second, etc.).
     */
    RankRanges rank_ranges_;
    /** Stores all the tile ids of the array. */
    TileIds tile_ids_;
    /** Stores the tiles of every attribute currently in main memory. */
    Tiles tiles_;
  };

  /** 
   * This class is a wrapper for a ArrayInfo object. It is 
   * returned by StorageManager::open_array, and used to append/get tiles
   * to/from an array. Its purpose is to eliminate the cost of finding the
   * array info in the book-keeping structures (and specifically in 
   * StorageManager::open_arrays_) every time an operation must be
   * executed for this array (e.g., StorageManager::get_tile). It contains
   * a pointer to an ArrayInfo object in StorageManager::open_arrays_,
   * along with ArrayDescriptor::array_info_id_ that is used for debugging 
   * purposes to check if the stored ArrayInfo object is obsolete (i.e., if 
   * it has been deleted by the storage manager when closing the array).
   */
  class ArrayDescriptor {
    // StorageManager objects can manipulate the private members of this class.
    friend class StorageManager;

   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Simple constructor. */
    explicit ArrayDescriptor(ArrayInfo* array_info) { 
      array_info_ = array_info;
      array_info_id_ = array_info->id_;
      array_name_ = array_info->array_schema_.array_name(); 
    }
    /** Empty destructor. */
    ~ArrayDescriptor() {}

    // ACCESSORS
    /** Returns the array info. */
    const ArrayInfo* array_info() const { return array_info_; } 
    /** Returns the array schema. */
    const ArraySchema& array_schema() const 
        { return array_info_->array_schema_; } 
 
   private:
    // PRIVATE ATTRIBUTES
    /** The array info. */
    ArrayInfo* array_info_;
    /** The array name. */
    std::string array_name_;
    /** 
     * The id of the ArrayDescriptor::array_info_ object. This is used for 
     * debugging purposes to check if the stored ArrayInfo object is obsolete 
     * (i.e., if it has been deleted by the storage manager from 
     * StorageManager::open_arrays_ when closing the array).
     */
    uint64_t array_info_id_;
    
    // PRIVATE METHODS
    /** 
     * Override the delete operator so that only a StorageManager object can
     * delete dynamically created ArrayDescriptor objects.
     */
    void operator delete(void* ptr) { ::operator delete(ptr); }
    /** 
     * Override the delete[] operator so that only a StorageManager object can
     * delete dynamically created ArrayDescriptor objects.
     */
    void operator delete[](void* ptr) { ::operator delete(ptr); }
  };
 
  // CONSTRUCTORS & DESTRUCTORS
  /**
   * Upon its creation, a storage manager object needs a workspace path. The 
   * latter is a folder in the disk where the storage manager creates all the 
   * array data (i.e., tile and index files). Note that the input path must 
   * exist. If the workspace folder exists, the function does nothing, 
   * otherwise it creates it. The segment size determines the amount of data 
   * exchanged in an I/O operation between the disk and the main memory. 
   */
  StorageManager(const std::string& path, 
                 uint64_t segment_size = SM_SEGMENT_SIZE);
  /** When a storage manager object is deleted, it closes all open arrays. */
  ~StorageManager();
 
  // MUTATORS
  /** Changes the default segment size. */
  void set_segment_size(uint64_t segment_size) { segment_size_ = segment_size; }
   
  // ARRAY FUNCTIONS
  /** 
   * Closes an array. 
   * Note: A rule must be satisfied before closing the array. 
   * Across all attributes, the lastly appended tile must have the same id.
   */
  void close_array(const ArrayDescriptor* array_descriptor);
  /** It deletes an array (regardless of whether it is open or not). */
  void delete_array(const std::string& array_name);
  /** Returns true if the array is empty. */
  bool is_empty(const ArrayDescriptor* array_descriptor) const;
  /** Opens an array in READ mode.*/
  ArrayDescriptor* open_array(const std::string& array_name);
  /** Opens an array in CREATE mode. */
  ArrayDescriptor* open_array(const ArraySchema& array_schema);

  // TILE FUNCTIONS
  /**
   * Inserts a tile into the array. Note that tiles are always 
   * appended in the end of the corresponding attribute file.
   * Note: Two rules must be followed: (i) For each attribute,
   * tiles must be appended in strictly ascending order of tile ids.
   * (ii) If a tile with a certain id is appended for an attribute A,
   * a tile with the same id must be appended across all attributes
   * before appending a new tile with a different tile id for A.
   */
  void append_tile(const Tile* tile, 
                   const ArrayDescriptor* array_descriptor,
                   unsigned int attribute_id); 
  /**  Returns a tile of an array with the specified attribute and tile id. */
  const Tile* get_tile(
      const ArrayDescriptor* array_descriptor,
      unsigned int attribute_id, uint64_t tile_id);  
  /** 
   * Creates an empty tile for a specific array and attribute, with reserved 
   * capacity equal to cell_num (note though that there are no constraints on
   * the number of cells the tile will actually accommodate - this is only
   * some initial reservation of memory to avoid mutliple memory expansions
   * as new cells are appended to the tile). 
   */
  Tile* new_tile(const ArraySchema& array_schema, unsigned int attribute_id, 
                 uint64_t tile_id, uint64_t cell_num) const; 

  // TILE ITERATORS
  /** This class implements a constant tile iterator. */
  class const_iterator {
   public:
    /** Iterator constuctor. */
    const_iterator();
    /** Iterator constuctor. */
    const_iterator(
        StorageManager* storage_manager,
        const ArrayDescriptor* array_descriptor, 
        unsigned int attribute_id,
        uint64_t rank); 
    /** Assignment operator. */
    void operator=(const const_iterator& rhs);
    /** Addition-assignment operator. */
    void operator+=(int64_t step);
    /** Pre-increment operator. */
    const_iterator operator++();
    /** Post-increment operator. */
    const_iterator operator++(int junk);
    /** 
     * Returns true if the iterator is equal to that in the
     * right hand side (rhs) of the operator. 
     */
    bool operator==(const const_iterator& rhs) const;
    /** 
     * Returns true if the iterator is equal to that in the
     * right hand side (rhs) of the operator. 
     */
    bool operator!=(const const_iterator& rhs) const;
    /** Returns the tile pointed by the iterator. */
    const Tile& operator*() const; 
    /** 
     * We distinguish two cases: (i) If the operands correspond to the same 
     * array, then it is true if the rank of the left-hand side is smaller 
     * than that of the right-hand side. (ii) Otherwise, it is true if the 
     * tile of the first operand precedes that of the right one along the 
     * (common) global cell order. A tile precedes another in the global
     * order if its upper bounding coordinate precedes that of the
     * other tile along the global order.  
     */
    bool operator<(const const_iterator& it_R) const; 
    /** Returns the array schema associated with this tile. */
    const ArraySchema& array_schema() const;
    /** Returns the bounding coordinates of the tile. */
    BoundingCoordinatesPair bounding_coordinates() const;
    /** Returns the MBR of the tile. */
    MBR mbr() const;
    /** Returns the rank. */
    uint64_t rank() const { return rank_; };
    /** Returns the id of the tile. */
    uint64_t tile_id() const;

   private:
    /** The array descriptor corresponding to this iterator. */
    const ArrayDescriptor* array_descriptor_;
    /** The attribute id corresponding to this iterator. */
    unsigned int attribute_id_;
    /** The rank of the current tile in the book-keeping structures. */
    uint64_t rank_;
    /** The storage manager object that created the iterator. */ 
    StorageManager* storage_manager_;
  };
  /** Begin tile iterator. */
  const_iterator begin(const ArrayDescriptor* array_descriptor, 
                       unsigned int attribute_id);
  /** End tile iterator. */
  const_iterator end(const ArrayDescriptor* array_descriptor,
                     unsigned int attribute_id);
  /** 
   * Returns the begin iterator to the StorageManager::MBRList that 
   * contains the MBRs of the intput array. 
   */ 
  MBRs::const_iterator MBR_begin(
      const ArrayDescriptor* const array_descriptor) const; 
  /** 
   * Returns the end iterator to the StorageManager::MBRList that 
   * contains the MBRs of the intput array. 
   */ 
  MBRs::const_iterator MBR_end(const ArrayDescriptor* array_descriptor) const; 

  // MISC
  /** 
   * Returns the ids of the tiles whose MBR overlaps with the input range.
   * The bool variable in overlapping_tile_ids indicates whether the overlap
   * is full (i.e., if the tile MBR is completely in the range) or not.
   */
  void get_overlapping_tile_ids(
      const ArrayDescriptor* array_descriptor, const Tile::Range& range, 
      std::vector<std::pair<uint64_t, bool> >* overlapping_tile_ids) const;

 private: 
  // PRIVATE ATTRIBUTES
  /** Used in ArrayInfo and ArrayDescriptor for debugging purposes. */
  static uint64_t array_info_id_;
  /** 
   * Stores info (e.g., book-keeping structures) about all currently open
   * arrays.
   */
  OpenArrays open_arrays_;
   /** 
   * Determines the minimum amount of data that can be exchanged between the 
   * hard disk and the main memory in a single I/O operation. 
   */
  uint64_t segment_size_;
  /** 
   * Is a folder in the disk where the storage manager creates 
   * all the array data (i.e., tile and index files). 
   */
  std::string workspace_;
  
  // PRIVATE METHODS
  /** Checks on the array descriptor. */
  bool check_array_descriptor(const ArrayDescriptor& array_descriptor) const;
  /** Checks upon appending a tile. */
  bool check_on_append_tile(const ArrayDescriptor& array_descriptor,
                            unsigned int attribute_id,
                            const Tile* tile) const;
  /** Checks upon closing an array. */
  bool check_on_close_array(
      const ArrayDescriptor& array_descriptor) const;
  /** Checks upon getting a tile. */
  bool check_on_get_tile(const ArrayDescriptor& array_descriptor,
                         unsigned int attribute_id,
                         uint64_t tile_id) const;
  /** Checks upon opening an array. */
  bool check_on_open_array(const std::string& array_name, 
                           ArrayMode array_mode) const;
  /** Creates the array folder in the workspace. */
  void create_array_directory(const std::string& array_name) const;
  /** Creates the workspace folder. */
  void create_workspace();
  /** Deletes the directory of the array, along with all its files. */
  void delete_directory(const std::string& array_name) const;
  /** Deletes all the main-memory tiles of the array. */
  void delete_tiles(ArrayInfo& array_info) const;
  /** Deletes the main-memory tiles of a specific attribute of an array. */
  void delete_tiles(ArrayInfo& array_info, 
                    unsigned int attribute_id) const;
  /** Writes the array info on the disk. */
  void flush_array_info(ArrayInfo& array_info) const;
  /** Writes the array schema on the disk. */
  void flush_array_schema(const ArrayInfo& array_info) const;
  /** Writes the bounding coordinates of each tile on the disk. */
  void flush_bounding_coordinates(const ArrayInfo& array_info) const;
  /** Writes the MBR of each tile on the disk. */
  void flush_MBRs(const ArrayInfo& array_info) const;
  /** Writes the tile offsets on the disk. */
  void flush_offsets(const ArrayInfo& array_info) const;
  /** Writes the tile ids on the disk. */
  void flush_tile_ids(const ArrayInfo& array_info) const;
  /** Writes the main-memory tiles of the array on the disk. */
  void flush_tiles(ArrayInfo& array_info) const;
  /** 
   * Writes the main-memory tiles of a specific attribute of an array
   * on the disk. 
   */
  void flush_tiles(ArrayInfo& array_info, unsigned int attribute_id) const;
  /**
   * Gets a tile of an attribute of an array using the tile rank. 
   * The rank of a tile is a sequence number indicating
   * the order in which it was appended to the array with respect to the
   * the other tiles appended to the array for the same attribute (e.g.,
   * 0 means that it was appended first, 1 second, etc.).
   */
  const Tile* get_tile_by_rank(ArrayInfo& array_info, 
                               unsigned int attribute_id, uint64_t rank) const;
  /** Initializes the array info using the input mode and schema. */
  void init_array_info(ArrayInfo& array_info, 
                       const ArrayMode array_mode,
                       const ArraySchema& array_schema) const;
  /** Loads the array info into main memory from the disk. */
  void load_array_info(const std::string& array_name);
  /** Loads the schema of an array into main memory from the disk. */
  void load_array_schema(const std::string& array_name, 
                         ArraySchema& array_schema) const;
  /** Loads the bounding coordinates into main memory from the disk. */
  void load_bounding_coordinates(ArrayInfo& array_info);
  /** Loads the MBRs into main memory from the disk. */
  void load_MBRs(ArrayInfo& array_info);
  /** Loads the offsets into main memory from the disk. */
  void load_offsets(ArrayInfo& array_info);
  /** 
   * Fetches tiles from the disk into main memory. Specifically, it loads their
   * payloads into a buffer. The aggregate payload size of the tiles is equal
   * to the smallest number that exceeds StorageManager::segment_size_.
   * NOTE: Care must be taken after the call of this function to delete
   * the created buffer.
   * \param array_info The array info.
   * \param attribute_id The attribute id.
   * \param start_rank The position in the index of the id of the tile 
   * the loading will start from.
   * \param buffer The buffer that will store the payloads of the loaded tiles.
   * \return A pair (buffer_size, tiles_in_buffer), where buffer_size is the
   * size of the created buffer, and tiles_in_buffer is the number of tiles 
   * loaded in the buffer.
   */
  std::pair<uint64_t, uint64_t> load_payloads_into_buffer(
      const ArrayInfo& array_info, unsigned int attribute_id,
      uint64_t start_rank, char*& buffer) const;
  /** Loads the tile ids into main memory from the disk. */
  void load_tile_ids(ArrayInfo& array_info) const;
  /** 
   * Creates tiles_in_buffer tiles for an attribute of an array from the 
   * payloads stored in buffer.
   */
  void load_tiles_from_buffer(
      ArrayInfo& array_info, unsigned int attribute_id,
      uint64_t start_rank, char* buffer, uint64_t buffer_size,
      uint64_t tiles_in_buffer) const;
  /** 
   * Loads tiles from the disk for a specific attribute of an array.
   * The loading starts from start_rank (recall that the tiles
   * are stored on disk in increasing id order). The number of tiles
   * to be loaded is determined by StorageManager::segment_size_ (namely,
   * the function loads the minimum number of tiles whose aggregate
   * payload exceeds StorageManager::segment_size_).
   */
  void load_tiles_from_disk(ArrayInfo& array_info, 
      unsigned int attribute_id, uint64_t start_rank) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** 
   * Copies the payloads of the tiles of the input array and attribute
   * currently in main memory into the segment buffer.
   * The file_offset is the offset in the file where the segment buffer
   * will eventually be written to.
   */
  void prepare_segment(
      ArrayInfo& array_info, unsigned int attribute_id, 
      uint64_t file_offset, uint64_t segment_size, 
      char *segment) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** 
   * Returns the position of tile_id in StorageManager::ArrayInfo::tile_ids_.
   * If tile_id does not exist in the book-keeping structure, it returns
   * SM_INVALID_RANK. 
   */
  uint64_t tile_rank(const ArrayInfo& array_info, uint64_t tile_id) const;
}; 

#endif
