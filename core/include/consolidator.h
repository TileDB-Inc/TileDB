/**   
 * @file   consolidator.h 
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
 * This file defines class Consolidator.
 */

#ifndef CONSOLIDATOR_H
#define CONSOLIDATOR_H

#include "storage_manager.h"
#include <string>
#include <map>

/** A default value for the consolidation step. */
#define CN_DEFAULT_CONSOLIDATION_STEP 3
/** 
 * Suffix of the file that stores book-keeping information about the array 
 * fragments. 
 */
#define CN_SUFFIX ".frg"

/**
 * The consolidator is responsible for merging 'array fragments'. The rationale
 * behind the array fragments is the following. When data are loaded into the
 * array for the first time, a new array fragment is created. This is a fully 
 * functional TileDB array, which abides by a praticular array schema (see also
 * ArraySchema). Updates to an array come in batches. Each update batch creates
 * a new array fragment. We will hereadter distinguish between terms 'array' 
 * and 'fragment'; an array has a unique name (using which the user may invoke
 * update and query operations), but at any given instance it may be comprised
 * of multiple array fragments. Each array fragment is named after the array it
 * belongs to, appending a suffix that indicates which update batches it
 * encompasses (more on this below). The consolidator is enforced with 
 * occassionally merging a set of array fragments into a single one, based on a
 * parameter called 'consolidation step'. The consolidation takes place in a
 * hierarchical manner, conceptually visualized as a tree, called 'fragment
 * tree'. The Consolidator encodes the tree in a concise manner (more on this
 * below).
 *
 * The consolidation works as follows. Let c denote the consolidation step. 
 * Imagine a complete c-ary tree structure, where each node is initially colored
 * white and represents nothing. When a new fragment is created, the left-most 
 * leaf node (at level 0) is colored grey and represents this new fragment. 
 * After c fragment insertions the c left-most leaves become grey. The 
 * consolidator then merges the c fragments into a single one, colors as grey
 * parent of the corresponding leaves (at level 1), and colors black the leaves.
 * The grey node at level 1 represents the newly created fragment. Its (now
 * black) children are completely disregarded from now on. When a new fragment
 * arrives, the first leaf (at level 0) on the right of the grey node at level 1
 * becomes grey and represents the new fragment. The process continues until
 * there are c leaves with the same parent, and then the consolidator merges
 * these leaves, colors them black (i.e., disregards them) and colors their
 * parents grey. Assume that that the process continues until there are
 * c-1 grey nodes at level 1, and c-1 grey nodes at level 0. Also assume
 * that a new fragment is inserted. The consolidator merges the c leaf fragments
 * creating the c-th grey node at level 1. At this point the consolidator must
 * continue recursively and merge the c level-1 fragments, creating a grey node
 * at level 2 and coloring the c nodes at level 1 as black. The process
 * continues in the same fashion. This algorithm, for c>1, leads to an
 * amortized logarithmic update cost.
 *
 * The Consolidator encodes the merge tree in a very simple manner, storing
 * essentially only the grey nodes. Specifically, it stores a vector of pairs
 * (level, number of grey nodes at this level). The pairs in the vector are in
 * non-increasing order of first element (i.e., level). Moreover, there
 * may be up to c-1 nodes per level. 
 *
 * Note that, upon consolidating fragments, a cell on the same coordinates
 * may appear in multiple fragments. In that case, the cell of the latest
 * (i.e., most recent) fragment will be appended to the consolidated 
 * fragment. Moreover, a deletion is represented by a cell that has valid
 * coordinate values, but all its attribute values have special NULL
 * values.
 *
 * A final remark concerns the names of the fragments. There is a sequence
 * of increasing fragment numbers (starting from 0), which indicates when an
 * udpate took place. Every new fragment, say for array "A", gets its name
 * by appending a suffix that indicates the range of updates
 * encompassed by the merged fragments. For instance, the very first fragment
 * of "A" is called "A_0_0", after merging the first 5 fragments, the resulting
 * fragment gets name "A_0_4", while merging fragments "A_0_4" and "A_5_9", the
 * resulting fragment is named "A_0_9".
 */
class Consolidator {
 public:
  // TYPE DEFINITIONS
  class ArrayInfo;

  /** Mnemonic: (vector of fragment suffixes, result fragment suffix) */
  typedef std::pair<std::vector<std::string>, std::string> ConsolidationInfo;
  /** Mnemonic: (level, number of nodes) */
  typedef std::pair<unsigned int, unsigned int> FragmentTreeLevel;
  /** Menmonic <(level, number of nodes), ...> */
  typedef std::vector<FragmentTreeLevel> FragmentTree;
  /** Menmonic [array_name] --> array_info */
  typedef std::map<std::string, ArrayInfo> OpenArrays;

  /** This struct groups consolidation book-keeping info about an array. */  
  struct ArrayInfo {
    /** The array schema (see class ArraySchema). */
    ArraySchema array_schema_; 
    /** The fragment tree of the array. */
    FragmentTree fragment_tree_;  
    /** 
     * Unique ArrayInfo object id, for debugging purposes when using 
     * Consolidator::ArrayDescriptor objects (see 
     * Consolidator::ArrayDescriptor::array_info_id_).
     */
    uint64_t id_;
    /**
     * Each array fragment has a sequence number. This variable holds the next 
     * sequence number to be assigned to the next incoming array fragment.
     */
    uint64_t next_fragment_seq_;
  };

  /** 
   * This class is a wrapper for a ArrayInfo object. It is returned by 
   * Consolidator::open_array, and used for the various functions of the 
   * consolidator. It contains a pointer to an ArrayInfo object in 
   * Consolidator::open_arrays_, along with ArrayDescriptor::array_info_id_ 
   * that is used for debugging purposes to check if the stored ArrayInfo 
   * object is obsolete (i.e., if it has been deleted by the consolidator when 
   * closing the array).
   */
  class ArrayDescriptor {
    // Consolidator objects can manipulate the private members of this class.
    friend class Consolidator;

   public:
    // CONSTRUCTORS & DESTRUCTORS
    /** Simple constructor. */
    explicit ArrayDescriptor(ArrayInfo* array_info) { 
      array_info_ = array_info;
      array_info_id_ = array_info->id_;
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
    /** 
     * The id of the ArrayDescriptor::array_info_ object. This is used for 
     * debugging purposes to check if the stored ArrayInfo object is obsolete 
     * (i.e., if it has been deleted by the consolidator from 
     * Consolidator::open_arrays_ when closing the array).
     */
    uint64_t array_info_id_;

    // PRIVATE METHODS
    /** 
     * Override the delete operator so that only a Consolidator object can
     * delete dynamically created ArrayDescriptor objects.
     */
    void operator delete(void* ptr) { ::operator delete(ptr); }
    /** 
     * Override the delete[] operator so that only a Consolidator object can
     * delete dynamically created ArrayDescriptor objects.
     */
    void operator delete[](void* ptr) { ::operator delete(ptr); }

  };

  // CONSTRUCTORS & DESTRUCTORS
  /** Empty constructor. */
  Consolidator();
  /** Simple constructor. */ 
  Consolidator(const std::string& workspace, 
               StorageManager& storage_manager,
               unsigned int consolidation_step = CN_DEFAULT_CONSOLIDATION_STEP);

  // ARRAY FUNCTIONS 
  /** 
   * Updates the fragment information about this array, and performs 
   * consolidation if necessary.
   */
  void add_fragment(const ArrayDescriptor* ad) const;
  /** Returns true if there is a book-keeping file about the input array. */
  bool array_exists(const std::string& array_name) const;
  /** It deletes the book-keeping consolidation info of an array from memory. */
  void close_array(const ArrayDescriptor* ad);
  /** Deletes the fragment book-keeping info of an array. */
  void delete_array(const std::string& array_name) const;
  /** Returns the suffixes corresponding to all existing array fragments. */
  std::vector<std::string> get_all_fragment_suffixes(
      const ArrayDescriptor* ad) const;
  /** Returns the next fragment name (which appends the proper suffix). */
  std::string get_next_fragment_name(const ArrayDescriptor* ad) const;
  /** Returns the next fragment sequence number. */
  uint64_t get_next_fragment_seq(const ArrayDescriptor* ad) const;
  /** It loads the book-keeping consolidation info for an array into memory. */
  const ArrayDescriptor* open_array(const ArraySchema& array_schema);

 private:
  // PRIVATE ATTRIBUTES
  /** Used in ArrayInfo and ArrayDescriptor for debugging purposes. */
  static uint64_t array_info_id_;
  /** This determines when consolidation must take place. */
  unsigned int consolidation_step_;
  /** Keeps track the arrays whose book-keeping info is in memory. */
  OpenArrays open_arrays_;
  /** The StorageManager object the Consolidator will be interfacing with. */
  StorageManager& storage_manager_;
  /** A folder in the disk where the Consolidator creates all its data. */
  std::string workspace_;
  
  // PRIVATE METHODS
  /** Advances all the cell iterators by 1. */
  void advance_cell_its(unsigned int attribute_num,
                        Tile::const_iterator* cell_its) const;
  /** 
   * Advances all the cell iterators by 1. If the cell iterators are equal to
   * the end iterator, the tile iterators are advanced. If the tile iterators
   * are not equal to the end tile iterator, then new cell iterators are
   * initialized.
   */
  void advance_cell_tile_its(unsigned int attribute_num,
                             Tile::const_iterator* cell_its, 
                             Tile::const_iterator& cell_it_end,
                             StorageManager::const_iterator* tile_its,
                             StorageManager::const_iterator& tile_it_end) const;
  /** Advances all the tile iterators by 1. */
  void advance_tile_its(unsigned int attribute_num,
                        StorageManager::const_iterator* tile_its) const; 
  /** 
   * Appends a logical cell of an array (comprised of attribute values and 
   * coordinates held in the input cell iterators) into
   * another array (in the corresponding tiles held in input variable 'tiles').
   */
  void append_cell(const Tile::const_iterator* cell_its,
                   Tile** tiles,
                   unsigned int attribute_num) const;
  /**
   * Returns true if the cell represents a deletion, i.e., when all its
   * attribute values are NULL.
   */
  bool is_null(const Tile::const_iterator* cell_its,
               unsigned int attribute_num) const;
  /** 
   * Consolidates the input fragments (described by a vector of suffixes) for
   * the case of irregular tiles. 
   */
  void consolidate_irregular(const std::vector<std::string>& fragment_suffixes,
                             const std::string& result_fragment_suffix,
                             const ArraySchema& array_schema) const;
  /** 
   * Consolidates the input fragments (described by a vector of suffixes) for
   * the case of regular tiles. 
   */
  void consolidate_regular(const std::vector<std::string>& fragment_suffixes,
                           const std::string& result_fragment_suffix,
                           const ArraySchema& array_schema) const;
  /** Creates the workspace folder. */
  void create_workspace() const;
  /** Flushes the fragment tree of an array to the disk. */
  void flush_fragment_tree(const std::string& array_name,
                           const FragmentTree& fragment_tree) const;
  /** 
   * Returns the index of the fragment from which we will get the next
   * cell.
   */
  int get_next_fragment_index(StorageManager::const_iterator** tile_its,
                              StorageManager::const_iterator* tile_it_end,
                              unsigned int fragment_num,
                              Tile::const_iterator** cell_its,
                              Tile::const_iterator* cell_it_end,
                              const ArraySchema& array_schema) const;
  /** Initializes cell iterators. */
  void initialize_cell_its(const StorageManager::const_iterator* tile_its,
                           unsigned int attribute_num,
                           Tile::const_iterator* cell_its, 
                           Tile::const_iterator& cell_it_end) const; 
  /** Initializes tile iterators. */
  void initialize_tile_its(const StorageManager::ArrayDescriptor* ad,
                           StorageManager::const_iterator* tile_its,
                           StorageManager::const_iterator& tile_it_end) const;
  /** 
   * Loads the fragment tree of an array from the disk, and returns the next
   * sequnce number to be assigned to a new fragment. 
   */
  uint64_t load_fragment_tree(const std::string& array_name,
                              FragmentTree& fragment_tree) const;
  /** 
   * Creates an array of Tile objects with the input tile id based on the input 
   * array schema. 
   */
  void new_tiles(const ArraySchema& array_schema,
                 uint64_t tile_id, Tile** tiles) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Sends the input tiles to the storage manager. */
  void store_tiles(const StorageManager::ArrayDescriptor* ad, 
                   Tile** tiles) const;
};

#endif
