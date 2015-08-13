/**
 * @file   array.cc
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
 * This file implements the Array class.
 */

#include "array.h"
#include "string.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <sstream>
#include <sys/stat.h>
#include <cassert>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Array::Array(
    const std::string& workspace, size_t segment_size,
    size_t write_state_max_size,
    const ArraySchema* array_schema, Mode mode) 
    : array_schema_(array_schema), workspace_(workspace),
      mode_(mode),
      segment_size_(segment_size),
      write_state_max_size_(write_state_max_size) {

  load_fragment_tree();
  open_fragments();
}

Array::~Array() {
  if((mode_ == WRITE || mode_ == APPEND) && fragment_tree_.size() > 0) {
    // Commit last fragment
    fragments_.back()->flush_write_state();
    fragments_.back()->commit_book_keeping();

    // Check if the last fragment is empty
    std::string fragment_dirname = workspace_ + "/" +
                                   array_schema_->array_name() + "/" + 
                                   fragments_.back()->fragment_name();

    if(empty_directory(fragment_dirname)) { 
      delete_fragment(fragments_.size()-1);
    } else { 
      if(mode_ == APPEND && must_consolidate()) {
        // Prepare last fragment for reading
        fragments_.back()->init_read_state();
        consolidate();
// TODO: Handle empty array after consolidation.
      }

      flush_fragment_tree();
    }
  }

  close_fragments();

  if(array_schema_ != NULL)
    delete array_schema_;
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

const std::string& Array::array_name() const {
  return array_schema_->array_name();
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

FragmentConstTileIterator Array::begin(
    int fragment_id, int attribute_id) const {
  return fragments_[fragment_id]->begin(attribute_id);
}

bool Array::empty() const {
  return !fragments_.size();
}

int Array::fragment_num() const {
  return fragments_.size();
}

Array::Mode Array::mode() const {
  return mode_;
}

FragmentConstReverseTileIterator Array::rbegin(
    int fragment_id, int attribute_id) const {
  return fragments_[fragment_id]->rbegin(attribute_id);
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

void Array::forced_close() {
  // If in write or append mode, delete the last fragment
  if(mode_ == WRITE || mode_ == APPEND)  
    delete_fragment(fragments_.size()-1);

  // Close the rest of the fragments
  close_fragments(); 

  // Clear the fragment tree, as it became inconsistent
  fragment_tree_.clear();

  // Delete array schema
  if(array_schema_ != NULL) {
    delete array_schema_;
    array_schema_ = NULL;
  }
}

void Array::new_fragment() {
  // Create a new Fragment object
  std::stringstream fragment_name;
  fragment_name << next_fragment_seq_ << "_" << next_fragment_seq_; 
  fragments_.push_back(new Fragment(workspace_, segment_size_,
                                    write_state_max_size_,
                                    array_schema_, fragment_name.str()));

  // Add fragment to tree
  int level_num = fragment_tree_.size();
  if(level_num == 0 || fragment_tree_[level_num-1].first > 0) {
    fragment_tree_.push_back(FragmentTreeLevel(0,1));
  } else {
    ++(fragment_tree_[level_num-1].second);
  }

  // Update the next fragment sequence
  ++next_fragment_seq_;
}

template<class T>
void Array::write_cell(const void* cell) const {
  fragments_.back()->write_cell<T>(cell); 
}

template<class T>
void Array::write_cell_sorted(const void* cell) {
  assert(mode_ == WRITE || mode_ == APPEND);

  fragments_.back()->write_cell_sorted<T>(cell);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void Array::close_fragments() {
  for(int i=0; i<fragments_.size(); ++i)
    delete fragments_[i];

  fragments_.clear();
}

void Array::consolidate() {
  // For easy reference
  int level_num = fragment_tree_.size();
  int consolidation_step = array_schema_->consolidation_step();

  if(consolidation_step == 1) {
    consolidate_eagerly();
  } else {
    while(fragment_tree_[level_num-1].second == consolidation_step) {
      consolidate_lazily();
      level_num = fragment_tree_.size();
    }
  }
}

void Array::consolidate_eagerly() {
  // For easy reference
  const std::type_info& type = *(array_schema_->coords_type());

  if(type == typeid(int))
    consolidate_eagerly<int>();
  else if(type == typeid(int64_t))
    consolidate_eagerly<int64_t>();
  else if(type == typeid(float))
    consolidate_eagerly<float>();
  else if(type == typeid(double))
    consolidate_eagerly<double>();
}

template<class T>
void Array::consolidate_eagerly() {
  // Prepare cell iterator
  ArrayConstCellIterator<T> cell_it = ArrayConstCellIterator<T>(this);

  // Create a new fragment for the cells of the consolidated fragments
  std::stringstream new_fragment_name; 
  new_fragment_name << "0_" << next_fragment_seq_ - 1;
  Fragment* new_fragment = new Fragment(workspace_, segment_size_,
                                        write_state_max_size_,
                                        array_schema_, new_fragment_name.str());

  // Write cells into the new fragment
  for(; !cell_it.end(); ++cell_it) 
    new_fragment->write_cell_sorted<T>(*cell_it);

  // Delete consolidated fragments (i.e., all fragments in the array)
  delete_fragments();

  // Append new fragment 
  new_fragment->flush_write_state();
  new_fragment->commit_book_keeping();
  new_fragment->init_read_state();
  fragments_.push_back(new_fragment);

  // Update fragment tree
  fragment_tree_.clear();
  fragment_tree_.push_back(FragmentTreeLevel(next_fragment_seq_ - 1, 1));
}

void Array::consolidate_lazily() {
  // For easy reference
  const std::type_info& type = *(array_schema_->coords_type());

  if(type == typeid(int))
    consolidate_lazily<int>();
  else if(type == typeid(int64_t))
    consolidate_lazily<int64_t>();
  else if(type == typeid(float))
    consolidate_lazily<float>();
  else if(type == typeid(double))
    consolidate_lazily<double>();
}

template<class T>
void Array::consolidate_lazily() {
  // For easy reference
  int consolidation_step = array_schema_->consolidation_step(); 
  int fragment_num = fragments_.size();
  int level_num = fragment_tree_.size();
  int level = fragment_tree_[level_num-1].first;
  int64_t subtree_size = pow(double(consolidation_step), level);

  // Prepare cell iterator
  bool return_del = (fragment_num != consolidation_step);
  std::vector<int> fragment_ids;
  for(int i=fragment_num-consolidation_step; i<fragment_num; ++i)
    fragment_ids.push_back(i); 
  ArrayConstCellIterator<T> cell_it =
      ArrayConstCellIterator<T>(this, fragment_ids, return_del);

  // Create a new fragment for the cells of the consolidated fragments
  std::stringstream new_fragment_name; 
  new_fragment_name << next_fragment_seq_ - consolidation_step * subtree_size 
                    << "_" << next_fragment_seq_ - 1;
  Fragment* new_fragment = new Fragment(workspace_, segment_size_,
                                        write_state_max_size_,
                                        array_schema_, new_fragment_name.str());

  // Write cells into the new fragment
  for(; !cell_it.end(); ++cell_it) 
    new_fragment->write_cell_sorted<T>(*cell_it);

  // Delete consolidated fragments (i.e., all fragments in the array)
  delete_fragments(fragment_ids);

  // Append new fragment 
  new_fragment->flush_write_state();
  new_fragment->commit_book_keeping();
  new_fragment->init_read_state();
  fragments_.push_back(new_fragment);

  // Update fragment tree
  int max_level = fragment_tree_[0].first; 
  fragment_tree_.pop_back();
  if(fragment_tree_.size() == 0) {
    fragment_tree_.push_back(FragmentTreeLevel(max_level + 1, 1));
  } else {
    ++fragment_tree_.back().second;
  }
}

void Array::delete_fragment(int i) {
  std::vector<Fragment*>::iterator it = fragments_.begin() + i;

  (*it)->clear_write_state();
  (*it)->clear_book_keeping();

  std::string dirname = workspace_ + "/" + array_schema_->array_name() + "/" +
                        (*it)->fragment_name();

  delete (*it);
  fragments_.erase(it);
  delete_directory(dirname);
}

void Array::delete_fragments() {
  while(fragments_.size() > 0)
    delete_fragment(fragments_.size()-1);
}

// NOTE: the input fragment ids must be sorted in ascending order
void Array::delete_fragments(
    const std::vector<int>& fragment_ids) {
  for(int i=0; i<fragment_ids.size(); ++i) {
    delete_fragment(fragment_ids[i] - i);
  }
}

void Array::flush_fragment_tree() {
  // For easy reference
  int level_num = fragment_tree_.size(); 

  // Do nothing if there are not fragments
  if(level_num == 0) 
    return;

  // Initialize buffer
  size_t buffer_size = level_num * 2 * sizeof(int);
  char* buffer = new char[buffer_size];

  // Populate buffer
  for(int i=0; i<level_num; ++i) {
    memcpy(buffer+2*i*sizeof(int), &(fragment_tree_[i].first), sizeof(int));
    memcpy(buffer+(2*i+1)*sizeof(int), 
           &(fragment_tree_[i].second), sizeof(int));
  }

  // Name of the fragment tree file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         FRAGMENT_TREE_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  // Delete fragment tree file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), 
                O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Flush the buffer into the file
  write(fd, buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;
  fragment_tree_.clear();
}

std::vector<std::string> Array::get_fragment_names() const {
  std::vector<std::string> fragment_names;

  // Return nothing if there are no fragments
  if(fragment_tree_.size() == 0)
    return fragment_names;

  // For easy reference
  int level;
  int64_t start_seq = 0, end_seq, subtree_size;
  std::stringstream ss;
  int consolidation_step = array_schema_->consolidation_step();

  // Eager consolidation
  if(consolidation_step == 1) {
    ss << "0_" << next_fragment_seq_-1;
    fragment_names.push_back(ss.str());
  } else { // Lazy consolidation
    for(int i=0; i<fragment_tree_.size(); ++i) {
      level = fragment_tree_[i].first;
      for(int j=0; j < fragment_tree_[i].second; ++j) {
        subtree_size = pow(double(consolidation_step), level); 
        end_seq = start_seq + subtree_size - 1;
        ss.str(std::string());
        ss << start_seq << "_" << end_seq;
        fragment_names.push_back(ss.str());
        start_seq += subtree_size;
      } 
    }
  }

  return fragment_names;
}

void Array::load_fragment_tree() {
  // For easy reference
  const std::string& array_name = array_schema_->array_name();
  int consolidation_step = array_schema_->consolidation_step();

  // Load the fragment book-keeping info from the disk into a buffer
  char* buffer = NULL;
  size_t buffer_size = 0;
  next_fragment_seq_ = 0;

  // Name of the fragment tree file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         FRAGMENT_TREE_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);

  // If the file does not exist, the tree is empty
  if(fd == -1)
    return;

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  buffer_size = st.st_size;
  assert(buffer_size != 0);
  buffer = new char[buffer_size];
 
  // Load contents of the file into the buffer
  read(fd, buffer, buffer_size);
 
  // Create the tree from the buffer and calculate next fragment sequence number
  int level_num = buffer_size / (2 * sizeof(int));
  int level, node_num;
  // Consolidation step == 1
  if(array_schema_->consolidation_step() == 1) {
    memcpy(&level, buffer, sizeof(int));
    memcpy(&node_num, buffer+sizeof(int), sizeof(int));
    fragment_tree_.push_back(FragmentTreeLevel(level, node_num));
    next_fragment_seq_ = level + 1 ;
  } else {
    for(int i=0; i<level_num; ++i) {
      memcpy(&level, buffer+2*i*sizeof(int), sizeof(int));
      memcpy(&node_num, buffer+(2*i+1)*sizeof(int), sizeof(int));
      fragment_tree_.push_back(FragmentTreeLevel(level, node_num));
      next_fragment_seq_ += pow(double(consolidation_step), level) * node_num;
    }
  }

  // Clean up
  close(fd);
  delete [] buffer;
}

bool Array::must_consolidate() {
  assert(fragment_tree_.size() != 0);

  // For easy reference
  int level_num = fragment_tree_.size();
  int consolidation_step = array_schema_->consolidation_step();

  return (consolidation_step == 1 ||
          fragment_tree_[level_num-1].second == consolidation_step);
}

void Array::open_fragments() {
  // Get fragment names
  std::vector<std::string> fragment_names = get_fragment_names();

  // Create a new Fragment object
  for(int i=0; i<fragment_names.size(); ++i) { 
    fragments_.push_back(new Fragment(workspace_, segment_size_, 
                                      write_state_max_size_,
                                      array_schema_, fragment_names[i]));
  }
}

// Explicit template instantiations
template void Array::write_cell<int>(const void* cell) const;
template void Array::write_cell<int64_t>(const void* cell) const;
template void Array::write_cell<float>(const void* cell) const;
template void Array::write_cell<double>(const void* cell) const;

template void Array::write_cell_sorted<int>(const void* cell);
template void Array::write_cell_sorted<int64_t>(const void* cell);
template void Array::write_cell_sorted<float>(const void* cell);
template void Array::write_cell_sorted<double>(const void* cell);
