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

/**
 * The consolidator is responsible for merging 'array fragments'. The rationale
 * behind the array fragments is the following. When data are loaded into the
 * array for the first time, a new array fragment is created. This is a fully 
 * functional TileDB array, which abides by a praticular array schema (see also
 * ArraySchema). Updates to an array come in batches. Each update batch creates
 * a new array fragment. We will hereadter distinguish between terms 'array' 
 * and 'fragment'; an array has a unique name (using which the user may invoke
 * update and query operations), but at any given instance it may be comprised
 * of multiple array fragments. Each array fragment is named after the batches
 * it encompasses (more on this below). The consolidator is enforced with 
 * occassionally merging a set of array fragments into a single one, based on a
 * parameter called 'consolidation step'. The latter is stored in the array
 * schema, i.e., it is specific to the array. The consolidation takes place in a
 * hierarchical manner, conceptually visualized as a tree, called 'fragment
 * tree'. The Consolidator encodes the tree in a concise manner (more on this
 * below).
 *
 * The consolidation works as follows. Let C denote the consolidation step. 
 * Imagine a complete C-ary tree structure, where each node is initially colored
 * white and represents nothing. When a new fragment is created, the left-most 
 * leaf node (at level 0) is colored grey and represents this new fragment. 
 * After C fragment insertions the C left-most leaves become grey. The 
 * consolidator then merges the C fragments into a single one, colors as grey
 * the parent at level 1 of the respective leaves, and colors black the leaves.
 * The grey node at level 1 represents the newly created fragment. Its (now
 * black) children are completely disregarded from now on. When a new fragment
 * arrives, the first leaf (at level 0) on the right of the grey node at level 1
 * becomes grey and represents the new fragment. The process continues until
 * there are c leaves with the same parent, and then the consolidator merges
 * these leaves, colors them black (i.e., disregards them) and colors their
 * parents grey. Assume that that the process continues until there are
 * C-1 grey nodes at level 1, and C-1 grey nodes at level 0. Also assume
 * that a new fragment is inserted. The consolidator merges the C leaf fragments
 * creating the C-th grey node at level 1. At this point the consolidator must
 * continue recursively and merge the C level-1 fragments, creating a grey node
 * at level 2 and coloring the C nodes at level 1 as black. The process
 * continues in the same fashion. This algorithm, for C>1, leads to an
 * amortized logarithmic update cost. For C=1, the algorithm always
 * consolidates the newly incoming batch with the (single) current fragment.
 *
 * The Consolidator encodes the merge tree in a very simple manner, storing
 * essentially only the grey nodes. Specifically, it stores a vector of pairs
 * (level, number of grey nodes at this level). The pairs in the vector are in
 * non-increasing order of first element (i.e., level). Moreover, there
 * may be up to C-1 nodes per level. 
 *
 * Note that, upon consolidating fragments, a cell on the same coordinates
 * may appear in multiple fragments. In that case, the cell of the latest
 * (i.e., most recent) fragment will be appended to the consolidated 
 * fragment. Moreover, a deletion is represented by a cell that has valid
 * coordinate values, but all its attribute values have special NULL
 * values.
 *
 * A final remark concerns the names of the fragments. There is a sequence
 * of increasing update numbers (starting from 0), which indicates when an
 * udpate took place. Every fragment gets its name after the range of updates
 * it encompasses. For instance, the very first fragment is called "0_0", 
 * indicating that it encompasses only the first update. After merging the first
 * 3 fragments (e.g., in case C=3), the resulting fragment gets name "0_2",
 * indicating that it covers updates 0, 1 and 2. Merging fragments "0_2", "3_5"
 * and "6_8", the resulting fragment is named "0_8".
 */
class Consolidator {
 public:
  // TYPE DEFINITIONS
  class ArrayInfo;
  /** 
   * An array can be opened in READ or WRITE mode. In WRITE mode, the fragment
   * book-keeping info may be updated, whereas in READ mode it cannot.
   */
  enum Mode {READ, WRITE}; 

  /** Mnemonic: (vector of fragment names, result fragment name) */
  typedef std::pair<std::vector<std::string>, std::string> ConsolidationInfo;
  /** Mnemonic: (level, number of nodes) */
  typedef std::pair<int, int> FragmentTreeLevel;
  /** Menmonic <(level, number of nodes), ...> */
  typedef std::vector<FragmentTreeLevel> FragmentTree;
  /** Menmonic [array_name] --> array_info */
  typedef std::map<std::string, ArrayInfo> OpenArrays;

  /** This struct groups consolidation book-keeping info about an array. */  
  struct ArrayInfo {
    /** The array mode. */
    Mode array_mode_;
    /** The array schema. */
    const ArraySchema* array_schema_; 
    /** The fragment tree of the array. */
    FragmentTree fragment_tree_;  
    /** 
     * Unique ArrayInfo object id, for debugging purposes when using 
     * Consolidator::ArrayDescriptor objects (see 
     * Consolidator::ArrayDescriptor::array_info_id_).
     */
    int64_t id_;
    /**
     * Each update has a sequence number. This variable holds the next 
     * sequence number to be assigned to the next created fragment.
     */
    int64_t next_update_seq_;
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
    int64_t array_info_id_;

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
    void operator delete[](void* ptr) { ::operator delete[](ptr); }

  };

  // CONSTRUCTORS & DESTRUCTORS
  /** Empty constructor. */
  Consolidator();
  /** Simple constructor. */ 
  Consolidator(const std::string& workspace, 
               StorageManager* storage_manager);

  // ARRAY FUNCTIONS 
  /** 
   * Updates the fragment information about this array, and performs 
   * consolidation if necessary.
   */
  void add_fragment(const ArrayDescriptor* ad) const;
  /** It deletes the book-keeping consolidation info of an array from memory. */
  void close_array(const ArrayDescriptor* ad);
  /** 
   * Eager consolidation when the consolidation step is equal to 1. It
   * consolidates every new fragment with the current (single) one.
   */
  void eagerly_consolidate(const ArrayDescriptor* ad) const;
  /** Returns the name corresponding to all existing array fragments. */
  std::vector<std::string> get_all_fragment_names(
      const ArrayDescriptor* ad) const;
  /** Returns the next fragment name. */
  std::string get_next_fragment_name(const ArrayDescriptor* ad) const;
  /** Returns the next update sequence number. */
  int64_t get_next_update_seq(const ArrayDescriptor* ad) const;
  /** Lazy consolidation, when the consolidation step is greater than 1. */
  void lazily_consolidate(const ArrayDescriptor* ad) const;
  /** It loads the book-keeping consolidation info for an array into memory. */
  const ArrayDescriptor* open_array(const ArraySchema* array_schema, Mode mode);
         
 private:
  // PRIVATE ATTRIBUTES
  /** Used in ArrayInfo and ArrayDescriptor for debugging purposes. */
  static int64_t array_info_id_;
  /** Keeps track the arrays whose book-keeping info is in memory. */
  OpenArrays open_arrays_;
  /** The StorageManager object the Consolidator will be interfacing with. */
  StorageManager* storage_manager_;
  /** A folder in the disk where the Consolidator creates all its data. */
  std::string workspace_;
  
  // PRIVATE METHODS
  /** Advances all the cell iterators by 1. */
  void advance_cell_its(int attribute_num, 
                        Tile::const_cell_iterator* cell_its) const;
  /** 
   * Advances all the cell iterators by 1. If the cell iterators are equal to
   * the end iterator, the tile iterators are advanced. If the tile iterators
   * are not equal to the end tile iterator, then new cell iterators are
   * initialized.
   */
  void advance_cell_tile_its(
      int attribute_num,
      Tile::const_cell_iterator* cell_its, 
      Tile::const_cell_iterator& cell_it_end,
      StorageManager::const_tile_iterator* tile_its,
      StorageManager::const_tile_iterator& tile_it_end) const;
  /** Advances all the tile iterators by 1. */
  void advance_tile_its(int attribute_num,
                        StorageManager::const_tile_iterator* tile_its) const; 
  /** 
   * Appends a logical cell of an array (comprised of attribute values and 
   * coordinates held in the input cell iterators) into
   * another array (in the corresponding tiles held in input variable 'tiles').
   */
  void append_cell(const Tile::const_cell_iterator* cell_its,
                   Tile** tiles, int attribute_num) const;
  /** Consolidates the input fragments. */
  void consolidate(const ArraySchema* array_schema,
                   const std::vector<std::string>& fragment_names,
                   const std::string& result_fragment_name) const;
  /**  Consolidates the input fragments for the case of irregular tiles. */
  void consolidate_irregular(
      const StorageManager::ArrayDescriptor* ad,
      const std::string& result_fragment_name) const;
  /**  Consolidates the input fragments for the case of regular tiles. */
  void consolidate_regular(
      const StorageManager::ArrayDescriptor* ad,
      const std::string& result_fragment_name) const;
  /** Flushes the fragment tree of an array to the disk. */
  void flush_fragment_tree(const std::string& array_name,
                           const FragmentTree& fragment_tree) const;
  /** 
   * Returns the index of the fragment from which we will get the next
   * cell.
   */
  int get_next_fragment_index(StorageManager::const_tile_iterator** tile_its,
                              StorageManager::const_tile_iterator* tile_it_end,
                              int fragment_num,
                              Tile::const_cell_iterator** cell_its,
                              Tile::const_cell_iterator* cell_it_end,
                              const ArraySchema* array_schema,
                              const std::string& result_fragment_suffix) const;
  /** Initializes cell iterators. */
  void initialize_cell_its(const StorageManager::const_tile_iterator* tile_its,
                           int attribute_num,
                           Tile::const_cell_iterator* cell_its, 
                           Tile::const_cell_iterator& cell_it_end) const; 
  /** Initializes tile iterators. */
  void initialize_tile_its(
      const StorageManager::FragmentDescriptor* fd,
      StorageManager::const_tile_iterator* tile_its,
      StorageManager::const_tile_iterator& tile_it_end) const;
  /** Returns true if the cell represents a deletion. */
  bool is_del(const Tile::const_cell_iterator& cell_it) const;
  /** 
   * Loads the fragment tree of an array from the disk, and returns the next
   * sequnce number to be assigned to a new fragment. 
   */
  int64_t load_fragment_tree(const ArraySchema* array_schema,
                             FragmentTree& fragment_tree) const;
  /** 
   * Creates an array of Tile objects with the input tile id based on the input 
   * array schema. 
   */
  void new_tiles(const ArraySchema* array_schema,
                 int64_t tile_id, Tile** tiles) const;
  /** Returns true if the input path is an existing directory. */
  bool path_exists(const std::string& path) const;
  /** Simply sets the workspace. */
  void set_workspace(const std::string& path);
  /** Sends the input tiles to the storage manager. */
  void store_tiles(const StorageManager::FragmentDescriptor* fd,
                   Tile** tiles) const;
};

#endif
