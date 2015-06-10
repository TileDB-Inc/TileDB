/**
 * @file   sorted_run.cc
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
 * This file implements the SortedRun class.
 */

#include "sorted_run.h"
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

SortedRun::SortedRun(
    const std::string& filename, bool var_size, size_t segment_size) 
    : filename_(filename), 
      var_size_(var_size), 
      segment_size_ (segment_size) { 
  // Load the first segment
  offset_in_file_ = 0; 
  offset_in_segment_ = 0;
  segment_ = new char[segment_size_];
  load_next_segment();
  assert(segment_ != NULL);
}

SortedRun::~SortedRun() {
  delete [] segment_;
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

const std::string& SortedRun::filename() const {
  return filename_;
}

bool SortedRun::var_size() const {
  return var_size_;
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

void SortedRun::advance_cell(size_t cell_size) {
  assert(offset_in_segment_ < segment_utilization_);

  // Advance offset in segment
  offset_in_segment_ += cell_size;
}

void* SortedRun::current_cell() {
  // Potentially fetch a new segment from the file
  if(offset_in_segment_ >= segment_utilization_)
     load_next_segment();

  if(segment_utilization_ == 0) // End of file
    return NULL;
  else // Return the cell
    return segment_ + offset_in_segment_;
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void SortedRun::load_next_segment() {
  assert(segment_ != NULL);

  // Find the offset in file where the read should start
  offset_in_file_ += offset_in_segment_;

  // Read the segment
  int fd = open(filename_.c_str(), O_RDONLY);
  assert(fd != -1);
  lseek(fd, offset_in_file_, SEEK_SET);
  segment_utilization_ = read(fd, segment_, segment_size_);
  assert(segment_utilization_ != -1);

  // Update offset
  offset_in_segment_ = 0; 

  // Close file
  close(fd);
}
