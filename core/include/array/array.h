/**
 * @file   array.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#ifndef __ARRAY_H__
#define __ARRAY_H__

#include "array_const_cell_iterator.h"
#include "array_const_dense_cell_iterator.h"
#include "array_const_reverse_cell_iterator.h"
#include "fragment.h"
#include <inttypes.h>
#include <string>
#include <vector>


#define METADATA_SCHEMA_FILENAME           "metadata_schema"

class Fragment;

/**  
 * An array is a collection of Fragment objects. Each fragment is created for
 * a *batch* of cell updates (i.e., insertions, deletions, modifications) and
 * is practically perceived as a separate array. Every fragment is stored in
 * a sub-directory inside the array directory. Initially, the name of the
 * directory of a newly created fragment is generated based on the process id
 * and the current timestamp. When the array gets consolidated, all the
 * temporary names are converted to names of the form "0_1", "0_2", etc.
 * This indicates that the newly created fragments are nodes "1", "2", etc.
 * of level 0 in the merge tree structure described in tiledb_array_define().
 * When fragments get consolidated, a produced fragment receives a special name
 * that implies its position in the merge tree structure. For instance, name 
 * "2_1" means that this fragment corresponds to node "1" at level "2" in the
 * merge tree. There is a *special case* when the consolidation step is 1.
 * In this case, a name of the form "0_y" implies a fragment that was created
 * for a single batch (never consolidated before), whereas a name of the 
 * form "1_y" implies that "y" batches have been previously consolidated to
 * create this fragment. This way we can keep track of the number of batches
 * that each fragment encompasses at all times.
 */
class Array {
 public:
  // ----- TYPE DEFINITIONS ----- //
  /** An array can be opened in **READ**, **WRITE** or **CONSOLIDATE** mode. */
  enum Mode {READ, WRITE, CONSOLIDATE};

  // ----- CONSTRUCTORS & DESTRUCTORS ----- //
  /** 
   * Constructor. 
   * @param dirname The directory where the array data is stored. It is of the
   * form "workspace/group/array_name".
   * @param array_schema The schema of the array.
   * @param mode The array mode (see Mode).
   */
  Array(const std::string& dirname, const ArraySchema* array_schema, Mode mode);

  /**
   * Checks if the constructor of the object was executed successfully.
   * Always check this function after creating an Array object.
   *
   * @return *True* for successfull creation and *false* otherwise. 
   */
  bool created_successfully() const;

  /** 
   * Finalizes an Array object. Always execute this function before deleting
   * an Array object (otherwise a warning will be printed in the destructor,
   * if compiled in VERBOSE mode). 
   *
   * @return **0** for success and <b>-1</b> for error.
   */
  int finalize();

  /** 
   * Destructor. Note that the destructor will delete *array_schema* given as
   * input to the constructor.
   */
  ~Array(); 

  // ----- ACCESSORS ----- //
  /** Returns the array name. */
  const std::string& array_name() const; 
  /** Returns the array schema. */
  const ArraySchema* array_schema() const;
  /** Returns the directory name. */
  const std::string& dirname() const;
  /** Checks if the array is empty. */
  bool is_empty() const; 
  /** Returns the number of fragments. */
  int fragment_num() const;
  /** Returns the array mode. */
  Mode mode() const;

  // ----- ITERATORS ----- //
  /** Returns a tile iterator for the input fragment and attribute. */
  FragmentConstTileIterator* begin(int fragment_id, int attribute_id) const;
  /** Returns a reverse tile iterator for the input fragment and attribute. */
  FragmentConstReverseTileIterator* rbegin(
      int fragment_id, 
      int attribute_id) const;

  //  ----- MUTATORS ----- //
  /**  
   * Writes a cell to the array. 
   * @tparam T The array coordinates type.
   * @param cell The cell to be written in binary format. For details on the
   * binary format of the cell, see tiledb_array_load(). 
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int cell_write(const void* cell) const; 
  /** 
   * Writes a cell into the array, respecting the global cell order. 
   * @tparam T The array coordinates type.
   * @param cell The cell to be written in binary format. For details on the
   * binary format of the cell, see tiledb_array_load(). 
   * @return **0** for success and <b>-1</b> for error.
   */ 
  template<class T>
  int cell_write_sorted(const void* cell, bool without_coords = false);
  /** 
   * Forces the array to close, during abnormal execution. If the array was
   * opened in WRITE mode, the created fragment is deleted (since its creation
   * procedure was interrupted). 
   * @return **0** for success and <b>-1</b> for error.
   * @return void
   */
  int close_forced();
  /** 
   * Consolidates fragments based on the consolidation step explained in 
   * tiledb_array_define(). 
   * @return **0** for success and <b>-1</b> for error.
   */
  int consolidate();
  /** Initializes a new fragment. */
  int new_fragment_init();
 
 private:
  // ----- PRIVATE ATTRIBUTES ----- //
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** *True* if the constructor succeeded, or *false* otherwise. */
  bool created_successfully_;
  /** 
   * The array (real) directory path. It is in the form
   * "workspace/group/array_name"
   */
  std::string dirname_; 
  /** *True* if the object was finalized, or *false* otherwise. */
  bool finalized_;
  /** The array fragments. */
  std::vector<Fragment*> fragments_;
  /** The mode in which the array was opened. */
  Mode mode_;

  // ----- PRIVATE METHODS ----- //

  // TODO
  bool fragment_is_dense(const std::string& fragment) const;

  /*
   * Consolidates a range of fragments, deleting their directories and creating
   * a new directory with the new fragment's data. 
   * @tparam T The array coordinates type.
   * @param fragments_to_consolidate This is a pair (first,second( that
   * specifies a range of fragments in Array::fragments_ that must be
   * consolidated.
   * @param new_fragment_name The name of the produced fragment.
   * @return **0** for success and <b>-1</b> for error.
   */
  template<class T>
  int consolidate(
      const std::pair<int,int>& fragments_to_consolidate,
      const std::string& new_fragment_name);
  /*
   * The general format of a fragment name is "x_y". This function takes a
   * string fragment name and parses it into an integer pair (x,y).
   * @param fragment_name The fragment name to be parsed.
   * @param x_y The produced integer (x,y) pair.
   * @return **0** for success and <b>-1</b> for error.
   */
  int fragment_name_parse(
      const std::string& fragment_name, 
      std::pair<int,int>& x_y) const;

  /** 
   * Retrieves (by reference) the fragments names. 
   * @param fragment_names It will store the names of the existing array
   * fragments. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int fragment_names_get(std::vector<std::string>& fragment_names) const;
  /** 
   * Sorts the input fragments names based on their format, e.g., "2_2"
   * precedes "2_3" and "3_2", "4_5", etc. Given the general format "x_y" of the
   * fragment names, the sorting is performed by first sorting on "x", breaking
   * ties by sorting on "y".
   * @param fragment_names The fragment names to be sorted. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int fragment_names_sort(std::vector<std::string>& fragment_names) const;
  /** Closes all the existing array fragments. */
  void fragments_close();
  /** 
   * Opens all the existing array fragments. 
   * @return **0** for success and <b>-1</b> for error.
   */
  int fragments_open();
  /*
   * Decides which fragments to consolidate.
   * @param fragments_to_consolidate This will hold the list of pairs
   * (first,second), which specifies a range of fragments in Array::fragments_ 
   * that must be consolidated. Multiple ranges imply that multiple
   * consolidations will take place.
   * @param new_fragment_names A list that holds the names of
   * the fragments that will result from each consolidation. Note that there
   * is a one-to-one correspondence betweem fragment_names and the pairs
   * in fragments_to_consolidate.
   * @return **0** for success and <b>-1</b> for error.
   */
  int what_fragments_to_consolidate(
      std::vector<std::pair<int,int> >& fragments_to_consolidate,
      std::vector<std::string>& new_fragment_names) const;
  /*
   * TODO
   */
  int fragments_rename_level_0() const;

  /*
   * Renames fragments with temporary names starting with "__".
   * @return **0** for success and <b>-1</b> for error.
   */
  int fragments_rename_tmp() const;


  // MISC
  /** TODO */
  static bool greater_smaller(
      const std::pair<int, std::pair<int, std::string> >& a,
      const std::pair<int, std::pair<int, std::string> >& b);

  // TODO: this has to move
  bool metadata_exists(
      const std::string& metadata_name,
      bool real_paths = false) const;

  // TODO
  bool has_fragments() const;
};

#endif
