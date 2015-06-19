/**
 * @file   sorted_run.h
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
 * This file defines class SortedRun. 
 */

#ifndef SORTED_RUN_H
#define SORTED_RUN_H

#include <string>

/** 
 * Fragment cells are sorted using a traditional external sortin algorithm.
 * This algorithm produces 'sorted runs', i.e., sorted sequences of cells,
 * during its 'sort' phase. Subsequently, during a 'merge' phase, 
 * multiple runs are merged into a single one (potentially recursively).
  A SortedRun object stores information about a sorted run.
 */
class SortedRun {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** 
   * Takes as input the name of the file of the run, as well as a boolean 
   * variable indicating whether each stored cell is of variable size or not.
   */
  SortedRun(const std::string& filename, 
            bool var_size, size_t segment_size);
  /** Closes the run file and deletes the main memory segment. */
  ~SortedRun();

  // ACCESSORS
  /** Returns the file name of the run. */
  const std::string& filename() const;
  /** True if the run stores variable-sized cells. */
  bool var_size() const; 

  // MUTATORS
  /** 
   * Advances the offset in the segment by cell_size to point to the next, 
   * logical cell.  The input cell_size indicates the size of the lastly
   * investigated cell, so that the object knows how many bytes to "jump" in
   * the segment. Note that cell_size also encompasses the size of the
   * potential id(s) the cell may carry.
   */
  void advance_cell(size_t cell_size);
  /** 
   * Returns the next cell in the main memory segment with the given size. 
   * Potentially fetches a new segment from the disk.
   */
  void* current_cell();

 private:
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
  /** True if the cell size is variable. */
  bool var_size_;

  // PRIVATE METHODS
  /** Loads the next segment from the file. */
  void load_next_segment();
};

#endif
