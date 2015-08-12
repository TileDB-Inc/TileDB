/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"
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
    const std::string& workspace, size_t segment_size,
    size_t write_state_max_size,
    const ArraySchema* array_schema, const std::string& fragment_name)
    : workspace_(workspace), array_schema_(array_schema),
      fragment_name_(fragment_name), segment_size_(segment_size) {
  book_keeping_ = new BookKeeping(array_schema_, &fragment_name_, &workspace_);
  read_state_ = NULL;
  write_state_ = NULL;

  temp_dirname_ = workspace_ + "/" + TEMP + "/" + 
                  array_schema->array_name() + "_" + fragment_name + "/";

  std::string fragment_dirname = workspace_ + "/" +
                                 array_schema->array_name() + "/" + 
                                 fragment_name;

  if(is_dir(fragment_dirname)) {
    book_keeping_->load();
    read_state_ = new ReadState(
                      array_schema_, book_keeping_, 
                      &fragment_name_, &workspace_,
                      segment_size_);
  } else { 
    // Create directories 
    create_directory(fragment_dirname);
    create_directory(temp_dirname_);

    // Initialize write state and book-keeping structures
    book_keeping_->init();
    write_state_ = 
        new WriteState(array_schema, &fragment_name_, &temp_dirname_, 
                       &workspace_, book_keeping_, 
                       segment_size_, write_state_max_size);
  }
  assert(is_dir(fragment_dirname));
}

Fragment::~Fragment() {
  // Read state
  if(read_state_ != NULL)
    delete read_state_;

  // Write state
  flush_write_state();

  // Book-keeping
  if(book_keeping_ != NULL) {
    commit_book_keeping();
    clear_book_keeping();
  }

  // Delete temp directory
  delete_directory(temp_dirname_);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

const ArraySchema* Fragment::array_schema() const {
  return array_schema_;
}

FragmentConstTileIterator Fragment::begin(int attribute_id) const {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return FragmentConstTileIterator(this, attribute_id, 0);
  else
    return FragmentConstTileIterator();

}

Tile::BoundingCoordinatesPair Fragment::bounding_coordinates(
    int64_t pos) const {
  return book_keeping_->bounding_coordinates(pos);
}

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

const Tile* Fragment::get_tile_by_pos(int attribute_id, int64_t pos) const {
  return read_state_->get_tile_by_pos(attribute_id, pos);
}

Tile::MBR Fragment::mbr(int64_t pos) const {
  return book_keeping_->mbr(pos);
}

FragmentConstReverseTileIterator Fragment::rbegin(int attribute_id) const {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(book_keeping_->tile_num() > 0) 
    return FragmentConstReverseTileIterator(this, attribute_id, 
                                            book_keeping_->tile_num()-1);
  else
    return FragmentConstReverseTileIterator();
}

const Tile* Fragment::rget_tile_by_pos(int attribute_id, int64_t pos) const {
  return read_state_->rget_tile_by_pos(attribute_id, pos);
}

int64_t Fragment::tile_id(int64_t pos) const {
  return book_keeping_->tile_id(pos);
}

int64_t Fragment::tile_num() const {
  return book_keeping_->tile_num();
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void Fragment::clear_book_keeping() {
  if(book_keeping_ != NULL)
    delete book_keeping_;

  book_keeping_ = NULL;
}

void Fragment::clear_write_state() {
  if(write_state_ != NULL)
    delete write_state_;

  write_state_ = NULL;
}

void Fragment::commit_book_keeping() {
  book_keeping_->commit();
}

void Fragment::flush_write_state() {
  if(write_state_ != NULL) {
    write_state_->flush();
    delete write_state_;
  }
 
  write_state_ = NULL;
}

void Fragment::init_read_state() {
  if(read_state_ != NULL)
    delete read_state_;

  read_state_ = new ReadState(
                     array_schema_, book_keeping_, 
                     &fragment_name_, &workspace_,
                     segment_size_);
}

template<class T>
void Fragment::write_cell(const void* cell) const {
  write_state_->write_cell<T>(cell);
}

template<class T>
void Fragment::write_cell_sorted(const void* cell) {
  write_state_->write_cell_sorted<T>(cell);
}

template<class T>
void Fragment::write_cell_sorted_with_id(const void* cell) {
  write_state_->write_cell_sorted_with_id<T>(cell);
}

template<class T>
void Fragment::write_cell_sorted_with_2_ids(const void* cell) {
  write_state_->write_cell_sorted_with_2_ids<T>(cell);
}

// Explicit template instantiations
template void Fragment::write_cell<int>(const void* cell) const;
template void Fragment::write_cell<int64_t>(const void* cell) const;
template void Fragment::write_cell<float>(const void* cell) const;
template void Fragment::write_cell<double>(const void* cell) const;

template void Fragment::write_cell_sorted<int>(const void* cell);
template void Fragment::write_cell_sorted<int64_t>(const void* cell);
template void Fragment::write_cell_sorted<float>(const void* cell);
template void Fragment::write_cell_sorted<double>(const void* cell);
