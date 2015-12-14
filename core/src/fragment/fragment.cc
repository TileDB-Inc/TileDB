/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
#include "special_values.h"
#include "utils.h"
#include <algorithm>
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <sys/stat.h>
#include <sstream>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Fragment::Fragment(
    const std::string& dirname, 
    const ArraySchema* array_schema, 
    const std::string& fragment_name,
    bool dense)
    : dirname_(dirname), 
      array_schema_(array_schema),
      fragment_name_(fragment_name), 
      dense_(dense) {
  book_keeping_ = new BookKeeping(array_schema_, &fragment_name_, &dirname_);
  read_state_ = NULL;
  write_state_ = NULL;

  // TODO: flag for error

  temp_dirname_ = dirname_ + "/" + TEMP;

  if(is_dir(dirname_)) {
    book_keeping_->load();
    read_state_ = 
        new ReadState(
            array_schema_, 
            book_keeping_, 
            &fragment_name_, 
            &dirname_,
            dense_);
  } else { 
    // Create directories 
    directory_create(dirname_);
    directory_create(temp_dirname_);

    // Initialize write state and book-keeping structures
    book_keeping_->init();
    write_state_ = 
        new WriteState(
            array_schema, 
            &fragment_name_, 
            &temp_dirname_, 
            &dirname_, 
            book_keeping_,
            dense_);
  }

  assert(is_dir(dirname_));
}

Fragment::~Fragment() {
  // Clear read state
  if(read_state_ != NULL)
    delete read_state_;

  if(write_state_ != NULL) {
    flush_write_state();
    // Commit book keeping
    if(book_keeping_ != NULL)
      book_keeping_->commit();
  }

  // Clear book keeping
  if(book_keeping_ != NULL) {
    delete book_keeping_;
    book_keeping_ = NULL;
  }

  // Delete temp directory
  if(is_dir(temp_dirname_))
    directory_delete(temp_dirname_);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

const ArraySchema* Fragment::array_schema() const {
  return array_schema_;
}

FragmentConstTileIterator* Fragment::begin(int attribute_id) const {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return new FragmentConstTileIterator(this, attribute_id, 0);
  else
    return new FragmentConstTileIterator();
}

Tile::BoundingCoordinatesPair Fragment::bounding_coordinates(
    int64_t pos) const {
  return book_keeping_->bounding_coordinates(pos);
}

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

const std::string& Fragment::dirname() const {
  return dirname_;
}

Tile* Fragment::get_tile_by_pos(int attribute_id, int64_t pos) const {
  return read_state_->get_tile_by_pos(attribute_id, pos);
}

Tile::MBR Fragment::mbr(int64_t pos) const {
  return book_keeping_->mbr(pos);
}

FragmentConstReverseTileIterator* Fragment::rbegin(int attribute_id) const {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return new FragmentConstReverseTileIterator(this, attribute_id, 
                                            book_keeping_->tile_num()-1);
  else
    return new FragmentConstReverseTileIterator();
}

Tile* Fragment::rget_tile_by_pos(int attribute_id, int64_t pos) const {
  return read_state_->rget_tile_by_pos(attribute_id, pos);
}

int64_t Fragment::tile_num() const {
  return book_keeping_->tile_num();
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void Fragment::flush_write_state() {
  if(write_state_ != NULL) {
    write_state_->flush();
    delete write_state_;
    write_state_ = NULL;
  }
}

void Fragment::init_read_state() {
  if(read_state_ != NULL)
    delete read_state_;

  read_state_ = new ReadState(
                     array_schema_, 
                     book_keeping_, 
                     &fragment_name_,
                     &dirname_,
                     dense_);
}

template<class T>
int Fragment::cell_write(const void* cell) const {
  return write_state_->cell_write<T>(cell);
}

template<class T>
int Fragment::cell_write_sorted(const void* cell, bool without_coords) {
  return write_state_->cell_write_sorted<T>(cell, without_coords);
}

template<class T>
int Fragment::cell_write_sorted_with_id(const void* cell) {
  return write_state_->cell_write_sorted_with_id<T>(cell);
}

template<class T>
int Fragment::cell_write_sorted_with_2_ids(const void* cell) {
  return write_state_->cell_write_sorted_with_2_ids<T>(cell);
}

// Explicit template instantiations
template int Fragment::cell_write<int>(const void* cell) const;
template int Fragment::cell_write<int64_t>(const void* cell) const;
template int Fragment::cell_write<float>(const void* cell) const;
template int Fragment::cell_write<double>(const void* cell) const;

template int Fragment::cell_write_sorted<int>(const void* cell, bool without_coords);
template int Fragment::cell_write_sorted<int64_t>(const void* cell, bool without_coords);
template int Fragment::cell_write_sorted<float>(const void* cell, bool without_coords);
template int Fragment::cell_write_sorted<double>(const void* cell, bool without_coords);
