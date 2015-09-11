/**
 * @file   array_const_dense_cell_iterator.h
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
 * This file defines class ArrayConstDenseCellIterator. 
 */

#ifndef ARRAY_CONST_DENSE_CELL_ITERATOR_H
#define ARRAY_CONST_DENSE_CELL_ITERATOR_H

#include "array.h"
#include "fragment_const_tile_iterator.h"
#include "tile.h"
#include "tile_const_cell_iterator.h"
#include <vector>

/** 
 * Initial cell buffer size (in bytes) for variable-lengthed cells. 
 * It will keep on doubling. 
 */
#define CELL_BUFFER_INITIAL_SIZE 40000

class Array;

/** 
 * A constant cell iterator that iterates over the cells of all the 
 * fragments of the array in the global cell order as specified by the
 * array schema. An important difference to ArrayConstCellIterator is
 * that ArrayConstDenseCellIterator simulates a dense array. That is,
 * it will return a cell even for an empty cell (i.e., not explicitly
 * stored by TileDB), assigning a zero value to it.
 */
template<class T>
class ArrayConstDenseCellIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  ArrayConstDenseCellIterator();
  /** Constructor. */
  ArrayConstDenseCellIterator(Array* array); 
  /** 
   * Constructor. The second argument specifies the fragments the iterator
   * will focus on. If the list is empty, then the iterator iterates over
   * all fragments. The last argument indicates whether
   * a cell representing a deletion must be returned or suppressed.
   */
  ArrayConstDenseCellIterator(
      Array* array, 
      const std::vector<int>& fragment_ids,
      bool return_del); 
  /** 
   * Constructor. The second argument determines the attribute
   * the iterator will focus on.
   */
  ArrayConstDenseCellIterator(
      Array* array, 
      const std::vector<int>& attribute_ids); 
  /** 
   * Constructor. Takes as input also a multi-dimensional 
   * range. The iterator will iterate only on the cells of the array whose
   * coordinates fall into the input range. 
   */
  ArrayConstDenseCellIterator(Array* array, const T* range);
  /** 
   * Constructor. Takes as input also a multi-dimensional 
   * range. The iterator will iterate only on the cells of the array whose
   * coordinates fall into the input range. The third argument determines 
   * the attributes the iterator will focus on. 
   */
  ArrayConstDenseCellIterator(
      Array* array, const T* range, const std::vector<int>& attribute_ids);
  /** Destructor. */
  ~ArrayConstDenseCellIterator();

  // ACCESSORS
  /** The array schema. */
  const ArraySchema* array_schema() const;
  /** Return the ids of the attributes the iterator iterates on. */
  const std::vector<int>& attribute_ids() const;
  /** Returns the size of the current cell. */
  size_t cell_size() const;
  /** 
   * Returns the size of the current cell pointed by the iterators of the
   * fragment with id equal to fragment_id. 
   */
  size_t cell_size(int fragment_id) const;
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
  /** The ids of the attributes the iterator iterates over. */
  std::vector<int> attribute_ids_;
  /** Number of attributes. */
  int attribute_num_;
  /** 
   * The current cell. Contains pointers to physical cells of all attibutes.
   */
  void* cell_;
  /** Stores one cell iterator per fragment per attribute. */
  TileConstCellIterator** cell_its_;
  /** The size of the current buffer that holds a cell. */
  size_t cell_buffer_size_;
  /** The size of the current cell. */
  size_t cell_size_;
  /** 
   * True if the coordinates of a retrieved cell from the sparse array, match
   * with the current_coords_ ones that simulate the dense array.
   */
  bool coords_match_;
  /** 
   * Goes through all the coordinates simulating a dense array, following the 
   * specified cell order.
   */
  T* current_coords_;
  /** Number of dimensions. */
  int dim_num_;
  /** True if the iterator has reached the end of all cells. */
  bool end_;
  /** The ids of the fragments the iterator iterates over. */
  std::vector<int> fragment_ids_;
  /** The number of fragments. */
  int fragment_num_;
  /** 
   * Stores a value per fragment. It is used when iterating cells that fall
   * inside the stored range. It indicates whether the current logical tile
   * under investigation is completely contained in the range or not. 
   */
  bool* full_overlap_;
  /** 
   * True if the cell currently pointed to by the iterator represents a 
   * deletion.
   */
  bool is_del_;
  /**
   * A multi-dimensional range. If not NULL, the iterator will iterate only
   * on the cells of the array whose coordinates fall into the input range.
   */
  T* range_;
  /** 
   * If true, a cell representing a deletion must be returned, otherwise it
   * is suppressed. 
   */
  bool return_del_;
  /** Stores one tile iterator per fragment per attribute. */
  FragmentConstTileIterator** tile_its_;
  /** True if the iterator iterates over variable-sized cells. */
  bool var_size_;
  /** 
   * A zero cell has specific coordinates, all its numerical attributes are 0,
   * and all non-numerical values are NULL. 
   */
  void* zero_cell_;

  // PRIVATE METHODS
  /** 
   * Advances the cell iterators of all attributes of the fragment with the
   * input id. 
   */
  void advance_cell(int fragment_id);
  /** 
   * Advances the cell iterators of all attributes of the fragment with the
   * input id, until the next cell in range is found. 
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
  /** 
   * Initializes tile and cell iterators for the input fragments and
   * attributes. 
   */
  void init_iterators();
  /** 
   * Initializes tile and cell iterators that will irerate over tiles and
   * cells that overlap with the stored range. 
   */
  void init_iterators_in_range();
  /** 
   * True if the cell pointed by the first iterator precedes that of the
   * second on the global cell order.
   */
  bool precedes(const TileConstCellIterator& it_A, 
                const TileConstCellIterator& it_B) const;
};

#endif
