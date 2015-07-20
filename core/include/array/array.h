/**
 * @file   array.h
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
 * This file defines class Array. 
 */

#ifndef ARRAY_H
#define ARRAY_H

#include "array_const_cell_iterator.h"
#include "array_const_reverse_cell_iterator.h"
#include "fragment.h"
#include <inttypes.h>
#include <vector>
#include <string>

/**  An array is a collection of Fragment objects. */
class Array {
 public:
  // TYPE DEFINITIONS
  /** An array can be opened in READ, WRITE or APPEND mode. */
  enum Mode {READ, WRITE, APPEND};
  /** Mnemonic: (level, number of nodes) */
  typedef std::pair<int, int> FragmentTreeLevel;
  /** Menmonic <(level, number of nodes), ...> */
  typedef std::vector<FragmentTreeLevel> FragmentTree;

  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  Array(const std::string& workspace_, size_t segment_size, 
        size_t write_state_max_size,
        const ArraySchema* array_schema, Mode mode); 
  /** Destructor. */
  ~Array(); 

  // ACCESSORS
  /** Returns the array name. */
  const std::string& array_name() const; 
  /** Returns the array schema. */
  const ArraySchema* array_schema() const;
  /** Returns a tile iterator for the input fragment and attribute. */
  FragmentConstTileIterator begin(int fragment_id, int attribute_id) const;
  /** Checks if the array is empty. */
  bool empty() const; 
  /** Returns the number of fragments. */
  int fragment_num() const;
  /** Returns the array mode. */
  Mode mode() const;
  /** Returns a reverse tile iterator for the input fragment and attribute. */
  FragmentConstReverseTileIterator rbegin(
      int fragment_id, int attribute_id) const;

  //  MUTATORS
  /** 
   * Forces the array to close, during abnormal execution. If the array was
   * opened in write or append mode, the last fragment is deleted (since
   * its creation procedure was interrupted). 
   */
  void forced_close();
  /** Initializes a new fragment. */
  void new_fragment();
  /**  
   * Writes a cell to the array. It takes as input a cell and its size. 
   * The cell has the following format: The coordinates
   * appear first, and then the attribute values in the same order
   * as the attributes are defined in the array schema.
   */
  template<class T>
  void write_cell(const void* cell) const; 
  /** Writes a cell into the array, respecting the global cell order. */ 
  template<class T>
  void write_cell_sorted(const void* cell);
 
 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The array fragments. */
  std::vector<Fragment*> fragments_;
  /** The fragment tree of the array (book-keeping about all fragments). */
  FragmentTree fragment_tree_;  
  /** The mode in which the array was opened. */
  Mode mode_;
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
   * Consolidates fragments based on the consolidation step defined in the
   * array schema. 
   */
  void consolidate();
  /** 
   * Consolidates the array fragments using the eager algorithm. Specifically,
   * it always consolidates the (two) existing fragments.
   */
  void consolidate_eagerly();
  /** 
   * Consolidates the array fragments using the eager algorithm. Specifically,
   * it always consolidates the (two) existing fragments.
   */
  template<class T>
  void consolidate_eagerly();
  /** 
   * Consolidates the array fragments using the lazy algorithm. Specifically,
   * it always consolidates the latest c fragments, where c is the
   * consolidation step.
   */
  void consolidate_lazily();
  /** 
   * Consolidates the array fragments using the lazy algorithm. Specifically,
   * it always consolidates the latest c fragments, where c is the
   * consolidation step.
   */
  template<class T>
  void consolidate_lazily();
  /** Deletes the i-th fragment from the array. */
  void delete_fragment(int i);
  /** Deletes all fragments in the array. */
  void delete_fragments();
  /** Deletes the fragments whose ids are included in the input. */
  void delete_fragments(const std::vector<int>& fragment_ids);
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
  /** Returns true if consolidation must take place. */
  bool must_consolidate();
  /** Opens all the existing array fragments. */
  void open_fragments();
};

#endif
