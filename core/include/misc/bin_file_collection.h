/**
 * @file   bin_file_collection.h
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
 * This file defines class BINFileCollection, which enables retrieving cells
 * from a set of binary files (in a sorted or unsorted order). */

#ifndef BIN_FILE_COLLECTION_H
#define BIN_FILE_COLLECTION_H

#include "array_schema.h"
#include "bin_file.h"
#include "cell.h"

/** 
 * Encompasses a set of binary files containing array cells. Its purpose
 * is to read cells from this set in a **sorted** or **unsorted** order
 * with respect to the cell order defined in the array schema. The 
 * case of unsorted is simple; it just iterates over the files separately
 * one by one, serving the next cell until all files are read. The
 * case of sorted is more complex. First, each binary file must individually 
 * have its own cells sorted along the array cell order. Second, these cells
 * must be traversed in a synchronized (sort-merge) manner, so that
 * it is guaranteed that the next retrieved cell is indeed the next cell in the
 * order across all files. 
 */
template<class T>
class BINFileCollection {
 public: 
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  BINFileCollection();
  /** Destructor. */
  ~BINFileCollection();

  // BASIC METHODS
  /** Clear the file collection data from main memory. */
  int close(); 
  /** Prepares the file collection. */
  int open( 
      const ArraySchema* array_schema,
      int id_num,
      const std::string& path,
      bool sorted);

  // OPERATORS
  /** Retrieves the next cell from the collection. */
  bool operator>>(Cell& cell);

 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The BIN file collection. */
  std::vector<BINFile*> bin_files_;
  /** Stores the next cell in each file in the collection. */
  std::vector<Cell*> cells_;
  /** The names of the files included in the collection. */
  std::vector<std::string> filenames_;
  /** File id from which we lastly accessed a cell. */
  int last_accessed_file_;
  /** Priority queue for efficient retrieval of the next cell .*/
  void* pq_; 
  /** Number of ids in each cell. */
  int id_num_;
  /** True if this is a sorted file collection. */
  bool sorted_;
};

#endif
