/**
 * @file   array_const_reverse_cell_iterator.cc
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
 * This file implements the ArrayConstReverseCellIterator class.
 */

#include "array_const_reverse_cell_iterator.h"
#include "utils.h"
#include <assert.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator() {
  array_ = NULL;
  cell_ = NULL;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  cell_its_ = NULL;
  end_ = false;
  range_ = NULL;
  tile_its_ = NULL;
  full_overlap_ = NULL;
  is_del_ = false;
  var_size_ = false;
  cell_size_ = 0;
}

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator(
    Array* array) : array_(array) {
  // Initialize private attributes
  const ArraySchema* array_schema = array_->array_schema();
  attribute_num_ = array_schema->attribute_num();
  dim_num_ = array_schema->dim_num();
  fragment_num_ = array_->fragment_num();
  end_ = false;
  range_ = NULL;
  full_overlap_ = NULL;
  return_del_ = false;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  cell_its_ = NULL;
  tile_its_ = NULL; 

  // Prepare the ids of the fragments the iterator will iterate on
  for(int i=0; i<fragment_num_; ++i)
    fragment_ids_.push_back(i);

  // Prepare the ids of the attributes the iterator will iterate on
  for(int i=0; i<=attribute_num_; ++i)
    attribute_ids_.push_back(i);

  // Case where the array is empty
  if(array_->empty()) {
    cell_size_ = 0;
    var_size_ = false;
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return;
  }

  // Allocate space for the cell
  cell_size_ = array_schema->cell_size(attribute_ids_);
  var_size_ = (cell_size_ == VAR_SIZE);
  if(!var_size_)
    cell_ = malloc(cell_size_);
  else 
    cell_ = NULL;

  // Get first cell
  init_iterators();
  int fragment_id = get_next_cell(); 
  if(fragment_id != -1)
    advance_cell(fragment_id); 
}

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator(
    Array* array, 
    const std::vector<int>& fragment_ids,
    bool return_del) : array_(array), return_del_(return_del) {
  // Checks
  assert(fragment_ids.size() != 0);

  // Initialize private attributes
  const ArraySchema* array_schema = array_->array_schema();
  attribute_num_ = array_schema->attribute_num();
  dim_num_ = array_schema->dim_num();
  fragment_num_ = array_->fragment_num();
  end_ = false;
  range_ = NULL;
  full_overlap_ = NULL;
  cell_its_ = NULL;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  tile_its_ = NULL;

  // Prepare the ids of the fragments the iterator will iterate on
  fragment_ids_ = fragment_ids;

  // Prepare the ids of the attributes the iterator will iterate on
  for(int i=0; i<=attribute_num_; ++i)
    attribute_ids_.push_back(i);

  // Case where the array is empty
  if(array_->empty()) {
    cell_size_ = 0;
    var_size_ = false;
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return;
  }

  // Allocate space for the cell
  cell_size_ = array_schema->cell_size(attribute_ids_);
  var_size_ = (cell_size_ == VAR_SIZE);
  if(!var_size_)
    cell_ = malloc(cell_size_);
  else 
    cell_ = NULL;

  // Get first cell
  init_iterators();
  int fragment_id = get_next_cell(); 
  if(fragment_id != -1)
    advance_cell(fragment_id); 
}

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator(
    Array* array, 
    const std::vector<int>& attribute_ids) : array_(array) {
  // Check fragment and attribute ids
  assert(array_->array_schema()->valid_attribute_ids(attribute_ids));

  // Initialize private attributes
  const ArraySchema* array_schema = array_->array_schema();
  attribute_num_ = array_schema->attribute_num();
  dim_num_ = array_schema->dim_num();
  fragment_num_ = array_->fragment_num();
  end_ = false;
  range_ = NULL;
  full_overlap_ = NULL;
  return_del_ = false;
  cell_its_ = NULL;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  tile_its_ = NULL;

  // Prepare the ids of the fragments the iterator will iterate on
  for(int i=0; i<fragment_num_; ++i)
    fragment_ids_.push_back(i);

  // Prepare the ids of the attributes the iterator will iterate on
  // The coordinates must always be part of the attributes to be
  // iterated on (to allow visitng cell of multiple fragments in order)
  attribute_ids_ = attribute_ids;
  // In case the attribute_ids is empty, include a dummy attribute to
  // gracefully handle deletions.
  if(attribute_ids_.size() == 0) 
    attribute_ids_.push_back(array_schema->smallest_attribute());
  attribute_ids_.push_back(attribute_num_);
  attribute_ids_ = rdedup(attribute_ids_);

  // Case where the array is empty
  if(array_->empty()) {
    cell_size_ = 0;
    var_size_ = false;
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return;
  }

  // Allocate space for the cell
  cell_size_ = array_schema->cell_size(attribute_ids_);
  var_size_ = (cell_size_ == VAR_SIZE);
  if(!var_size_)
    cell_ = malloc(cell_size_);
  else 
    cell_ = NULL;

  // Get first cell
  init_iterators();
  int fragment_id = get_next_cell(); 
  if(fragment_id != -1)
    advance_cell(fragment_id); 
}

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator(
    Array* array, const T* multi_D_obj, bool is_range) : array_(array) {
  // Initialize private attributes
  const ArraySchema* array_schema = array_->array_schema();
  attribute_num_ = array_schema->attribute_num();
  dim_num_ = array_schema->dim_num();
  fragment_num_ = array_->fragment_num();
  end_ = false;
  cell_its_ = NULL;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  tile_its_ = NULL;
  return_del_ = false;

  // Decide if the input is a range or cell coordinates
  if(is_range) {
    range_ = new T[2*dim_num_];
    memcpy(range_, multi_D_obj, 2*dim_num_*sizeof(T)); 
    full_overlap_ = new bool[fragment_num_];
  } else {
    range_ = NULL;
    full_overlap_ = NULL;
  } 

  // Prepare the ids of the fragments the iterator will iterate on
  // By default it is all the fragments, but this may change in the future
  for(int i=0; i<fragment_num_; ++i)
    fragment_ids_.push_back(i);

  // Prepare the ids of the attributes the iterator will iterate on
  for(int i=0; i<=attribute_num_; ++i)
    attribute_ids_.push_back(i);

  // Case where the array is empty
  if(array_->empty()) {
    cell_size_ = 0;
    var_size_ = false;
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return;
  }

  // Allocate space for the cell
  cell_size_ = array_schema->cell_size(attribute_ids_);
  var_size_ = (cell_size_ == VAR_SIZE);
  if(!var_size_)
    cell_ = malloc(cell_size_);
  else 
    cell_ = NULL;

  // Get first cell
  if(is_range) { // Range
    init_iterators_in_range();
    for(int i=0; i<fragment_num_; ++i)
      find_next_cell_in_range(fragment_ids_[i]);
    int fragment_id = get_next_cell(); 
    if(fragment_id != -1)
      advance_cell_in_range(fragment_id); 
  } else { // Cell coordinates
    init_iterators_at_coords(multi_D_obj);
    for(int i=0; i<fragment_num_; ++i)
      find_cell_at_coords(fragment_ids_[i], multi_D_obj);
    int fragment_id = get_next_cell(); 
    if(fragment_id != -1)
      advance_cell(fragment_id); 
  }
}

template<class T>
ArrayConstReverseCellIterator<T>::ArrayConstReverseCellIterator(
    Array* array, 
    const T* multi_D_obj,
    const std::vector<int>& attribute_ids,
    bool is_range) : array_(array) {
  // Check fragment and attribute ids
  assert(array_->array_schema()->valid_attribute_ids(attribute_ids));

  // Initialize private attributes
  const ArraySchema* array_schema = array_->array_schema();
  attribute_num_ = array_schema->attribute_num();
  dim_num_ = array_schema->dim_num();
  fragment_num_ = array_->fragment_num();
  end_ = false;
  return_del_ = false;
  cell_its_ = NULL;
  cell_buffer_size_ = CELL_BUFFER_INITIAL_SIZE;
  tile_its_ = NULL;

  // Decide if the input is a range or cell coordinates
  if(is_range) {
    range_ = new T[2*dim_num_];
    memcpy(range_, multi_D_obj, 2*dim_num_*sizeof(T)); 
    full_overlap_ = new bool[fragment_num_];
  } else {
    range_ = NULL;
    full_overlap_ = NULL;
  } 

  // Prepare the ids of the fragments the iterator will iterate on
  // By default it is all the fragments, but this may change in the future
  for(int i=0; i<fragment_num_; ++i)
    fragment_ids_.push_back(i);

  // Prepare the ids of the attributes the iterator will iterate on
  // The coordinates must always be part of the attributes to be
  // iterated on (to allow visitng cell of multiple fragments in order)
  attribute_ids_ = attribute_ids;
  // In case the attribute_ids is empty, include a dummy attribute to
  // gracefully handle deletions.
  if(attribute_ids_.size() == 0) 
    attribute_ids_.push_back(array_schema->smallest_attribute());
  attribute_ids_.push_back(attribute_num_);
  attribute_ids_ = rdedup(attribute_ids_);

  // Case where the array is empty
  if(array_->empty()) {
    cell_size_ = 0;
    var_size_ = false;
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return;
  }

  // Allocate space for the cell
  cell_size_ = array_schema->cell_size(attribute_ids_);
  var_size_ = (cell_size_ == VAR_SIZE);
  if(!var_size_)
    cell_ = malloc(cell_size_);
  else 
    cell_ = NULL;

  // Get first cell
  if(is_range) { // Range
    init_iterators_in_range();
    for(int i=0; i<fragment_num_; ++i)
      find_next_cell_in_range(fragment_ids_[i]);
    int fragment_id = get_next_cell(); 
    if(fragment_id != -1)
      advance_cell_in_range(fragment_id); 
  } else { // Cell coordinates
    init_iterators_at_coords(multi_D_obj);
    for(int i=0; i<fragment_num_; ++i)
      find_cell_at_coords(fragment_ids_[i], multi_D_obj);
    int fragment_id = get_next_cell(); 
    if(fragment_id != -1)
      advance_cell(fragment_id); 
  }
}

template<class T>
ArrayConstReverseCellIterator<T>::~ArrayConstReverseCellIterator() {
  if(cell_ != NULL) 
    free(cell_);

  if(cell_its_ != NULL) {
    for(int i=0; i<fragment_num_; ++i)
      delete [] cell_its_[i];
    delete [] cell_its_; 
  }

  if(tile_its_ != NULL) {
    for(int i=0; i<fragment_num_; ++i) {
      delete [] tile_its_[i];
    }
    delete [] tile_its_; 
  }

  if(range_ != NULL)
    delete [] range_;

  if(full_overlap_ != NULL)
    delete [] full_overlap_;
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

template<class T>
const ArraySchema* ArrayConstReverseCellIterator<T>::array_schema() const {
  return array_->array_schema();
}


template<class T>
const std::vector<int>& 
ArrayConstReverseCellIterator<T>::attribute_ids() const {
  return attribute_ids_;
}

template<class T>
size_t ArrayConstReverseCellIterator<T>::cell_size() const {
  assert(!end_);

  if(!var_size_) {
    return cell_size_;
  } else {
    size_t cell_size;
    size_t coords_size = array_->array_schema()->cell_size(attribute_num_);
     memcpy(&cell_size, static_cast<char*>(cell_) + coords_size, 
            sizeof(size_t));
    return cell_size;
  }
}

template<class T>
size_t ArrayConstReverseCellIterator<T>::cell_size(int fragment_id) const {

  if(!var_size_) {
    return cell_size_;
  } else {
    size_t cell_size = sizeof(size_t); 
    for(int i=0; i<attribute_ids_.size(); ++i)
      cell_size += cell_its_[fragment_id][attribute_ids_[i]].cell_size();
    return cell_size; 
  }
}

template<class T>
bool ArrayConstReverseCellIterator<T>::end() const {
  return end_;
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

template<class T>
void ArrayConstReverseCellIterator<T>::operator++() {
  int fragment_id = get_next_cell();

  // Advance cell
  if(fragment_id != -1) {
    if(range_ != NULL) 
      advance_cell_in_range(fragment_id);
    else 
      advance_cell(fragment_id);
  } 
}

template<class T>
const void* ArrayConstReverseCellIterator<T>::operator*() {
  while(is_del_ && !return_del_ && cell_ != NULL)
    ++(*this);

  return cell_;
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

template<class T>
void ArrayConstReverseCellIterator<T>::advance_cell(
    int fragment_id) {
  // Advance cell iterators
  for(int j=0; j<attribute_ids_.size(); ++j)
    ++cell_its_[fragment_id][attribute_ids_[j]];

  // Potentially advance also tile iterators
  if(cell_its_[fragment_id][attribute_num_].end()) {
    // Advance tile iterators
    for(int j=0; j<attribute_ids_.size(); ++j) 
      ++tile_its_[fragment_id][attribute_ids_[j]];

    // Initialize cell iterators
    if(!tile_its_[fragment_id][attribute_num_].end()) {
      for(int j=0; j<attribute_ids_.size(); ++j) 
        cell_its_[fragment_id][attribute_ids_[j]] = 
            (*tile_its_[fragment_id][attribute_ids_[j]])->rbegin();
    }
  }
}

template<class T>
void ArrayConstReverseCellIterator<T>::advance_cell_in_range(
    int fragment_id) {
  // Advance cell iterators
  for(int j=0; j<attribute_ids_.size(); ++j)
    ++cell_its_[fragment_id][attribute_ids_[j]];

  find_next_cell_in_range(fragment_id);
}

template<class T>
void ArrayConstReverseCellIterator<T>::find_cell_at_coords(
    int fragment_id, const T* coords) {
  if(tile_its_[fragment_id][attribute_num_].end())
    return;
  // For easy reference
  const ArraySchema* array_schema = this->array_schema();
  Tile::BoundingCoordinatesPair bounding_coords;
  bounding_coords = 
      tile_its_[fragment_id][attribute_num_].bounding_coordinates();

  // If the input coords are not included in the current tile (and
  // hence they succeed its second bounding coordinate) there is nothing
  // to be done
  if(array_schema->succeeds(
      coords, static_cast<const T*>(bounding_coords.second)))
    return;

  // Otherwise, the input coordinates are inside the tile bounding range,
  // so we need to perform a binary search
  int64_t min, max, med; // For the binary search
  int64_t pos; // Cell position inside each tile
  int64_t cell_num = cell_its_[fragment_id][attribute_num_].cell_num();
  const T* cell_coords;

  // Binary search
  min = 0;
  max = cell_num - 1;
  while(min <= max) {
    med = min + ((max - min) / 2);
    cell_coords = static_cast<const T*>( 
        *(cell_its_[fragment_id][attribute_num_] + (cell_num - med - 1)));

    if(array_schema->precedes<T>(coords, cell_coords)) {
      max = med-1;
    } else if(array_schema->succeeds<T>(coords, cell_coords)) {
      min = med+1;
    } else {
      break; 
    }
  }

  if(max < min) // coords preceding max
    pos = max; 
  else // coords included in a tile
    pos = med; 

  // Update the coordinates tile iterator
  cell_its_[fragment_id][attribute_num_] += (cell_num - pos - 1); 

  // Synchronize attribute cell iterators
  for(int j=0; j<attribute_ids_.size()-1; ++j) {
    if(!tile_its_[fragment_id][attribute_ids_[j]].end()) {
      cell_its_[fragment_id][attribute_ids_[j]] += (cell_num - pos - 1);
    } else {
      cell_its_[fragment_id][attribute_ids_[j]] = Tile::rend(); 
    }
  }    
}

template<class T>
void ArrayConstReverseCellIterator<T>::find_next_cell_in_range(
    int fragment_id) {

  const T* point;

  // The loop will be broken when a cell in range is found, or
  // all cells are exhausted
  while(1) { 
    // If not in the end of the tile
    if(!cell_its_[fragment_id][attribute_num_].end() &&
       !full_overlap_[fragment_id]) {
      const void* coords;
      while(!cell_its_[fragment_id][attribute_num_].end()) {
        coords = *cell_its_[fragment_id][attribute_num_];
        point = static_cast<const T*>(coords);
        if(inside_range(point, range_, dim_num_)) 
          break; // cell found
        ++cell_its_[fragment_id][attribute_num_];
      }
    }

    // If the end of the tile is reached (cell not found yet)
    if(cell_its_[fragment_id][attribute_num_].end()) {
      // Advance coordinate tile iterator
      ++tile_its_[fragment_id][attribute_num_];

      // Find the first coordinate tile that overlaps with the range
      const T* mbr;
      std::pair<bool, bool> tile_overlap;
      while(!tile_its_[fragment_id][attribute_num_].end()) {
        mbr = static_cast<const T*>(
                  tile_its_[fragment_id][attribute_num_].mbr());
        tile_overlap = overlap(mbr, range_, dim_num_); 
        if(tile_overlap.first) { 
          full_overlap_[fragment_id] = tile_overlap.second;
          break;  // next tile found
        }
        ++tile_its_[fragment_id][attribute_num_];
      } 

      if(tile_its_[fragment_id][attribute_num_].end())
        break; // cell cannot be found
      else // initialize coordinates cell iterator
        cell_its_[fragment_id][attribute_num_] = 
            (*tile_its_[fragment_id][attribute_num_])->rbegin();

    } else { // Not the end of the cells in this tile
      break; // cell found
    }
  }

  // Synchronize attribute cell and tile iterators
  for(int j=0; j<attribute_ids_.size()-1; ++j) {
    tile_its_[fragment_id][attribute_ids_[j]] = 
        array_->rbegin(fragment_id, attribute_ids_[j]);
    tile_its_[fragment_id][attribute_ids_[j]] +=  
        tile_its_[fragment_id][attribute_num_].tile_num() -
        tile_its_[fragment_id][attribute_num_].pos() - 1;
    if(!tile_its_[fragment_id][attribute_ids_[j]].end()) {
      cell_its_[fragment_id][attribute_ids_[j]] = 
          (*tile_its_[fragment_id][attribute_ids_[j]])->rbegin();
      cell_its_[fragment_id][attribute_ids_[j]] +=  
          cell_its_[fragment_id][attribute_num_].cell_num() -
          cell_its_[fragment_id][attribute_num_].pos() - 1;
    } else {
      cell_its_[fragment_id][attribute_ids_[j]] = Tile::rend(); 
    }
  }    
}

template<class T>
int ArrayConstReverseCellIterator<T>::get_next_cell() {
  // For easy reference
  size_t coords_size = array_->array_schema()->cell_size(attribute_num_);

  // Get the first non-NULL coordinates
  const void *coords, *next_coords;
  int fragment_id;
  int f = 0;
  do {
    next_coords = *cell_its_[fragment_ids_[f]][attribute_num_]; 
    ++f;
  } while((next_coords == NULL) && (f < fragment_ids_.size())); 

  fragment_id = fragment_ids_[f-1];

  // Get the next coordinates in the global cell order
  for(int i=f; i<fragment_ids_.size(); ++i) {
    coords = *cell_its_[fragment_ids_[i]][attribute_num_]; 
    if(coords != NULL) {
      if(memcmp(coords, next_coords, coords_size) == 0) {
        if(range_ != NULL)
          advance_cell_in_range(fragment_id);
        else
          advance_cell(fragment_id); 
        fragment_id = fragment_ids_[i];
      } else if(precedes(cell_its_[fragment_ids_[i]][attribute_num_],
                         cell_its_[fragment_id][attribute_num_])) {
        next_coords = coords;
        fragment_id = fragment_ids_[i];
      }     
    }
  }

  if(next_coords != NULL) { // There are cells
    // --- Prepare cell ---
    // Find cell size and create a new cell for variable-sized cells
    if(var_size_) {
      cell_size_ = this->cell_size(fragment_id);
      size_t new_cell_buffer_size = cell_buffer_size_;
      while(new_cell_buffer_size < cell_size_)
        new_cell_buffer_size *= 2;
      if(cell_ == NULL) {
        cell_ = malloc(new_cell_buffer_size);
      } else if(new_cell_buffer_size > cell_buffer_size_) {
        free(cell_);
        cell_ = malloc(new_cell_buffer_size);
      }  
      cell_buffer_size_ = new_cell_buffer_size;
    }  
    char* cell = static_cast<char*>(cell_);
    size_t offset;

    // Copy coordinates to cell
    memcpy(cell, *(cell_its_[fragment_id][attribute_num_]), coords_size);
    offset = coords_size;
    // Copy cell size for variable-sized cells
    if(var_size_) {
      memcpy(cell + offset, &cell_size_, sizeof(size_t));
      offset += sizeof(size_t);
    }

    // Copy attributes to cell
    size_t attr_size;
    for(int j=0; j<attribute_ids_.size()-1; ++j) { 
      attr_size = cell_its_[fragment_id][attribute_ids_[j]].cell_size();
      memcpy(cell + offset, *(cell_its_[fragment_id][attribute_ids_[j]]), 
             attr_size);
      offset += attr_size;
    }

    // Check if the retrieved cell represents a deletion
    assert(attribute_ids_[0] != attribute_num_);
    is_del_ = cell_its_[fragment_id][attribute_ids_[0]].is_del();

    return fragment_id;
  } else { // No more cells
    if(cell_ != NULL) 
      free(cell_);
    cell_ = NULL;
    end_ = true;
    is_del_ = false;
    return -1;
  }
}

template<class T>
void ArrayConstReverseCellIterator<T>::init_iterators() {
  // Create tile and cell iterators
  tile_its_ = new FragmentConstReverseTileIterator*[fragment_num_];
  cell_its_ = new TileConstReverseCellIterator*[fragment_num_];

  for(int i=0; i<fragment_num_; ++i) {
   tile_its_[i] = new FragmentConstReverseTileIterator[attribute_num_+1];
   cell_its_[i] = new TileConstReverseCellIterator[attribute_num_+1];
  }

  // Initialize iterators
  for(int i=0; i<fragment_ids_.size(); ++i) {
    for(int j=0; j<attribute_ids_.size(); ++j) {
      tile_its_[fragment_ids_[i]][attribute_ids_[j]] = 
          array_->rbegin(fragment_ids_[i], attribute_ids_[j]);
      cell_its_[fragment_ids_[i]][attribute_ids_[j]] = 
          (*tile_its_[fragment_ids_[i]][attribute_ids_[j]])->rbegin();
    }
  }
}

template<class T>
void ArrayConstReverseCellIterator<T>::init_iterators_in_range() {
  // Create tile and cell iterators
  tile_its_ = new FragmentConstReverseTileIterator*[fragment_num_];
  cell_its_ = new TileConstReverseCellIterator*[fragment_num_];

  for(int i=0; i<fragment_num_; ++i) {
    tile_its_[i] = new FragmentConstReverseTileIterator[attribute_num_+1];
    cell_its_[i] = new TileConstReverseCellIterator[attribute_num_+1];
  }

  // Initialize tile and cell iterators 
  for(int i=0; i<fragment_ids_.size(); ++i) { 
    // Initialize coordinate tile iterator
    tile_its_[fragment_ids_[i]][attribute_num_] =  
        array_->rbegin(fragment_ids_[i], attribute_num_);

    // Find the first coordinate tile that overlaps with the range
    const T* mbr;
    std::pair<bool, bool> tile_overlap;
    while(!tile_its_[fragment_ids_[i]][attribute_num_].end()) {
      mbr = static_cast<const T*>
                (tile_its_[fragment_ids_[i]][attribute_num_].mbr());
      tile_overlap = overlap(mbr, range_, dim_num_); 

      if(tile_overlap.first) { 
        full_overlap_[fragment_ids_[i]] = tile_overlap.second;
        break;
      }
      ++tile_its_[fragment_ids_[i]][attribute_num_];
    } 

    // Syncronize attribute tile iterators
    for(int j=0; j<attribute_ids_.size()-1; ++j) {
      tile_its_[fragment_ids_[i]][attribute_ids_[j]] = 
          array_->rbegin(fragment_ids_[i], attribute_ids_[j]);
      tile_its_[fragment_ids_[i]][attribute_ids_[j]] += 
          tile_its_[fragment_ids_[i]][attribute_num_].tile_num() -
          tile_its_[fragment_ids_[i]][attribute_num_].pos() - 1;
    }    

    // Initialize cell iterators
    for(int j=0; j<attribute_ids_.size(); ++j) {
      if(!tile_its_[fragment_ids_[i]][attribute_ids_[j]].end())
        cell_its_[fragment_ids_[i]][attribute_ids_[j]] = 
            (*tile_its_[fragment_ids_[i]][attribute_ids_[j]])->rbegin();
      else
        cell_its_[fragment_ids_[i]][attribute_ids_[j]] = Tile::rend();
    }
  }
}

template<class T>
void ArrayConstReverseCellIterator<T>::init_iterators_at_coords(
    const T* coords) {
  // FIXME: This currently works only for irregular tiles
  assert(array_schema()->has_irregular_tiles());

  // Create tile and cell iterators
  tile_its_ = new FragmentConstReverseTileIterator*[fragment_num_];
  cell_its_ = new TileConstReverseCellIterator*[fragment_num_];

  for(int i=0; i<fragment_num_; ++i) {
    tile_its_[i] = new FragmentConstReverseTileIterator[attribute_num_+1];
    cell_its_[i] = new TileConstReverseCellIterator[attribute_num_+1];
  }

  // For easy reference
  const ArraySchema* array_schema = this->array_schema();
  int64_t pos; // Tile position in each fragment 
  int64_t tile_num; // Number of tiles in each fragment
  int64_t min, max, med; // For the binary search
  Tile::BoundingCoordinatesPair bounding_coords;

  // Binary search for start tile
  for(int i=0; i<fragment_ids_.size(); ++i) { 
    // Initialize coordinate tile iterator
    tile_its_[fragment_ids_[i]][attribute_num_] =  
        array_->rbegin(fragment_ids_[i], attribute_num_);

    // Get number of tiles
    tile_num = tile_its_[fragment_ids_[i]][attribute_num_].tile_num();

    // Binary search
    min = 0;
    max = tile_num - 1;
    while(min <= max) {
      med = min + ((max - min) / 2);
      bounding_coords = 
          (tile_its_[fragment_ids_[i]][attribute_num_] + (tile_num - med - 1)).
              bounding_coordinates(); 

      if(array_schema->precedes<T>(
             coords, static_cast<const T*>(bounding_coords.first))) {
        max = med-1;
      } else if(array_schema->succeeds<T>(
             coords, static_cast<const T*>(bounding_coords.second))) {
        min = med+1;
      } else { // coords included in a tile
        break; 
      }
    } 

    if(max < min) // coords preceding max
      pos = max; 
    else          // coords included in a tile
      pos = med; 

    // Update the coordinates tile iterator
    tile_its_[fragment_ids_[i]][attribute_num_] += (tile_num - pos - 1); 

    // Initialize and syncronize attribute tile iterators
    for(int j=0; j<attribute_ids_.size()-1; ++j) {
      tile_its_[fragment_ids_[i]][attribute_ids_[j]] = 
          array_->rbegin(fragment_ids_[i], attribute_ids_[j]);
      tile_its_[fragment_ids_[i]][attribute_ids_[j]] += (tile_num - pos - 1);
    }    

    // Initialize cell iterators
    for(int j=0; j<attribute_ids_.size(); ++j) {
      if(!tile_its_[fragment_ids_[i]][attribute_ids_[j]].end())
        cell_its_[fragment_ids_[i]][attribute_ids_[j]] = 
            (*tile_its_[fragment_ids_[i]][attribute_ids_[j]])->rbegin();
      else
        cell_its_[fragment_ids_[i]][attribute_ids_[j]] = Tile::rend();
    }
  }
}

template<class T>
bool ArrayConstReverseCellIterator<T>::precedes(
    const TileConstReverseCellIterator& it_A,
    const TileConstReverseCellIterator& it_B) const {
  const void* coords_A = *it_A;
  const void* coords_B = *it_B;
  int64_t tile_id_A = it_A.tile_id();
  int64_t tile_id_B = it_B.tile_id();
  bool regular = array_->array_schema()->has_regular_tiles();

  // NOTE: We are iterating in reverse order

  // Case #1 for true: regular + it_A has smaller tile id
  if(regular && tile_id_A > tile_id_B)
    return true;

  bool coords_A_precede = 
      array_->array_schema()->succeeds(static_cast<const T*>(coords_A), 
                                       static_cast<const T*>(coords_B));

  // Case #2 for true: regular + equal tile ids + coords of it_A precede
  if(regular && tile_id_A == tile_id_B && coords_A_precede)
    return true;

  // Case #3 for true: iregular + coords of it_A precede
  if(!regular && coords_A_precede)
    return true;

  // False in all other cases
  return false;
}

// Explicit template instantiations
template class ArrayConstReverseCellIterator<int>;
template class ArrayConstReverseCellIterator<int64_t>;
template class ArrayConstReverseCellIterator<float>;
template class ArrayConstReverseCellIterator<double>;
