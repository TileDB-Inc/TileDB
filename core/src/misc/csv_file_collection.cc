/**
 * @file   csv_file_collection.cc
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
 * This file implements the CSVFileCollection class.
 */

#include "csv_file_collection.h"
#include "utils.h"
#include <assert.h>
#include <iostream>
#include <queue>
#include <time.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

template<class T>
CSVFileCollection<T>::CSVFileCollection() {
  pq_ = NULL;
}

template<class T>
CSVFileCollection<T>::~CSVFileCollection() {
  close();
}

/******************************************************
******************** BASIC METHODS ********************
******************************************************/

template<class T>
int CSVFileCollection<T>::close() {
  // Clear CSV files
  int csv_files_num = int(csv_files_.size());
  for(int i=0; i<csv_files_num; ++i) {
    csv_files_[i]->close();
    delete csv_files_[i]; 
  }
  csv_files_.clear();

  // Clear cells
  for(int i=0; i<cells_.size(); ++i)
    delete cells_[i];
  cells_.clear();

  if(pq_ != NULL) { 
    std::priority_queue<std::pair<const Cell*, int>, 
                    std::vector<std::pair<const Cell*, int> >, 
                    Cell::Succeeds<T> >* 
    pq = static_cast<std::priority_queue<
                         std::pair<const Cell*, int>, 
                         std::vector<std::pair<const Cell*, int> >,
                         Cell::Succeeds<T> >*>(pq_);
    delete pq;
    pq_ = NULL;
  }

  return 0;

}

template<class T>
int CSVFileCollection<T>::open(
    const ArraySchema* array_schema,
    const std::string& path,
    bool sorted) {
  // Initialization
  array_schema_ = array_schema;
  sorted_ = sorted;
  last_accessed_file_ = -1;

  // Gather all files in path
  if(is_file(path)) {
    filenames_.push_back(path);
  } else if(is_dir(path)) {
    filenames_ = get_filenames(path);
  } else {
    std::cerr << ERROR_MSG_HEADER 
              << " Path '" << path << "' does not exists.\n";
    return TILEDB_EFILE;
  }

  // Create priority queue
  std::priority_queue<std::pair<const Cell*, int>, 
                      std::vector<std::pair<const Cell*, int> >, 
                      Cell::Succeeds<T> >* pq;
  if(sorted) {
    pq = new std::priority_queue<std::pair<const Cell*, int>,
                                 std::vector<std::pair<const Cell*, int> >,
                                 Cell::Succeeds<T> >;
    pq_ = pq;
  }

  // Open files and prepare first cells
  int file_num = int(filenames_.size());
  CSVFile* csv_file;
  Cell* cell;
  for(int i=0; i<file_num; ++i) {
    csv_file = new CSVFile(array_schema_);  
    if(is_file(filenames_[i]))
      csv_file->open(filenames_[i], "r");
    else 
      csv_file->open(path + "/" + filenames_[i], "r");

    csv_files_.push_back(csv_file);

    cell = new Cell(array_schema_);
    *csv_file >> *cell;
    cells_.push_back(cell);
    if(sorted)
      pq->push(std::pair<const Cell*, int>(cell,i)); 
  }

  return 0;
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T>
bool CSVFileCollection<T>::operator>>(Cell& cell) {
  assert(csv_files_.size());

  cell.set_cell(NULL);
  std::priority_queue<std::pair<const Cell*, int>, 
                        std::vector<std::pair<const Cell*, int> >, 
                        Cell::Succeeds<T> >* 
        pq = static_cast<std::priority_queue<
                             std::pair<const Cell*, int>, 
                             std::vector<std::pair<const Cell*, int> >,
                             Cell::Succeeds<T> >*>(pq_);

  // Update the cell of the lastly accessed file
  if(last_accessed_file_ != -1) { 
    *csv_files_[last_accessed_file_] >> *cells_[last_accessed_file_];
    if(sorted_ && cells_[last_accessed_file_]->cell() != NULL)
        pq->push(std::pair<const Cell*, int>(
                     cells_[last_accessed_file_],last_accessed_file_));
  }

  // In the case of 'sorted', we need to search for the 
  // the cell that appears first in the cell order
  if(!sorted_) { // UNSORTED
    if(last_accessed_file_ == -1) 
      last_accessed_file_ = 0;
    if(last_accessed_file_ < csv_files_.size()) { 
      // Get the next cell
      if(cells_[last_accessed_file_]->cell() != NULL) {
        cell = *cells_[last_accessed_file_];
      } else {
        ++last_accessed_file_;
        if(last_accessed_file_ < csv_files_.size())      
          cell = *cells_[last_accessed_file_];
        else
          cell.set_cell(NULL);
      }
    } else {
      cell.set_cell(NULL);
    }
  } else {       // SORTED
    if(!pq->empty()) {
      cell = *(pq->top()).first;
      last_accessed_file_ = (pq->top()).second;
      pq->pop();
    }  
  }

  if(cell.cell() != NULL) 
    return true;
  else 
    return false;
}


// Explicit template instantiations
template class CSVFileCollection<int>;
template class CSVFileCollection<int64_t>;
template class CSVFileCollection<float>;
template class CSVFileCollection<double>;
