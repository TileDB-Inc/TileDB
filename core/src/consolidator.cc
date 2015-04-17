/**
 * @file   consolidator.cc
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
 * This file implements the Consolidator class.
 */
  
#include "consolidator.h"
#include "utils.h"
#include <assert.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <math.h>

int64_t Consolidator::array_info_id_ = 0;

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

Consolidator::Consolidator(const std::string& workspace, 
                           StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
  set_workspace(workspace);
  create_directory(workspace_); 
}

/******************************************************
****************** ARRAY FUNCTIONS ********************
******************************************************/

void Consolidator::add_fragment(const ArrayDescriptor* ad) const {
  // For easy reference
  int consolidation_step = ad->array_info_->array_schema_->consolidation_step();

  // Eager consolidation
  if(consolidation_step == 1)
    eagerly_consolidate(ad);
  else 
    lazily_consolidate(ad);

  ++ad->array_info_->next_update_seq_;
}

void Consolidator::close_array(const ArrayDescriptor* ad) {
  // For easy reference
  const std::string& array_name = ad->array_info_->array_schema_->array_name();

  // Check if array is open, and descriptor is valid
  assert(open_arrays_.find(array_name) != open_arrays_.end());
  assert(open_arrays_[array_name].id_ == ad->array_info_id_);

  if(ad->array_info_->array_mode_ == WRITE)
    flush_fragment_tree(array_name, ad->array_info_->fragment_tree_); 
  open_arrays_.erase(array_name); 

  delete ad;
}

void Consolidator::eagerly_consolidate(const ArrayDescriptor* ad) const {
  // For easy reference
  const ArraySchema* array_schema = ad->array_info_->array_schema_;
  const std::string& array_name = array_schema->array_name();
  int64_t cur_update_seq = ad->array_info_->next_update_seq_;
  FragmentTree& fragment_tree = ad->array_info_->fragment_tree_;
  int level_num = fragment_tree.size();
  std::stringstream ss;

  // Add fragment to tree
  // In the case of eager consolidation, after the first fragment is added,
  // the levels of the tree are always equal to 1. The fragment tree is
  // merely used to calculate the next update sequence number.
  if(level_num == 0) { // First fragment
    fragment_tree.push_back(FragmentTreeLevel(0,1));
    ++level_num;
    return; // No need to consolidate first fragment
  } else {
    ++(fragment_tree[level_num-1].second);
  }

  // Get existing fragment name
  std::vector<std::string> fragment_names;
  ss << "0_" << (cur_update_seq -1);
  fragment_names.push_back(ss.str());
  ss.str(std::string());
  // Get incoming fragment name
  ss << cur_update_seq << "_" << cur_update_seq;
  fragment_names.push_back(ss.str());
  ss.str(std::string());
  // Get result fragment name
  ss << "0_" << cur_update_seq;
  std::string result_fragment_name = ss.str();

  // Consolidate
  consolidate(array_schema, fragment_names, result_fragment_name);
}

std::vector<std::string> Consolidator::get_all_fragment_names(
    const ArrayDescriptor* ad) const {
  // For easy reference
  FragmentTree& fragment_tree = ad->array_info_->fragment_tree_;
  int level;
  int64_t start_seq = 0, end_seq, subtree_size;
  std::stringstream ss;
  std::vector<std::string> fragment_names;
  int consolidation_step = ad->array_info_->array_schema_->consolidation_step();

  // Eager consolidation
  if(consolidation_step == 1) {
    ss << "0_" << ad->array_info_->next_update_seq_-1;
    fragment_names.push_back(ss.str());
  } else { // Lazy consolidation
    for(int i=0; i<fragment_tree.size(); ++i) {
      level = fragment_tree[i].first;
      for(int j=0; j < fragment_tree[i].second; ++j) {
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

std::string Consolidator::get_next_fragment_name(
    const ArrayDescriptor* ad) const {
  uint64_t n = get_next_update_seq(ad);
  std::stringstream fragment_name;
  fragment_name << n << "_" << n;

  return  fragment_name.str();
}

int64_t Consolidator::get_next_update_seq(
    const ArrayDescriptor* ad) const {
  return ad->array_info_->next_update_seq_;
}

void Consolidator::lazily_consolidate(const ArrayDescriptor* ad) const {
  // For easy reference
  const ArraySchema* array_schema = ad->array_info_->array_schema_;
  const std::string& array_name = array_schema->array_name();
  FragmentTree& fragment_tree = ad->array_info_->fragment_tree_;
  int level_num = fragment_tree.size();
  int consolidation_step = array_schema->consolidation_step();

  // Add fragment to tree
  if(level_num == 0 || fragment_tree[level_num-1].first > 0) {
    fragment_tree.push_back(FragmentTreeLevel(0,1));
    ++level_num;
  } else {
    ++(fragment_tree[level_num-1].second);
  }
  int64_t cur_update_seq = ad->array_info_->next_update_seq_;

  // Consolidate (recursively) if necessary
  while(fragment_tree[level_num-1].second == consolidation_step) {
    int level = fragment_tree[level_num-1].first;
    int64_t subtree_size = pow(double(consolidation_step), level);
    std::stringstream ss;

    // Get names of fragments under consolidation
    std::vector<std::string> fragment_names;
    for(int i=0; i<consolidation_step; ++i) {
      int64_t start_seq = (cur_update_seq - (consolidation_step - i) * 
                            subtree_size + 1);
      int64_t end_seq = start_seq + subtree_size - 1; 
      ss << start_seq << "_" << end_seq;
      fragment_names.push_back(ss.str());
      ss.str(std::string());
    }

    // Get result fragment name
    ss << cur_update_seq - consolidation_step * subtree_size + 1
       << "_" << cur_update_seq;
    std::string result_fragment_name = ss.str();

    // Consolidate
    consolidate(array_schema, fragment_names, result_fragment_name);

    // Update fragment tree
    fragment_tree.pop_back();
    --level_num;
    if(level_num != 0) {
      ++fragment_tree[level_num-1].second;
    } else {
      fragment_tree.push_back(FragmentTreeLevel(level+1,1));
      ++level_num;
    }
  } 
}

const Consolidator::ArrayDescriptor* Consolidator::open_array(
    const ArraySchema* array_schema, Mode mode) {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  // Check if array is already open
  assert(open_arrays_.find(array_name) == open_arrays_.end());

  // Create a new entry in open_arrays_ 
  std::pair<OpenArrays::iterator, bool> ret = open_arrays_.insert(
      std::pair<std::string, ArrayInfo>(array_name, ArrayInfo()));

  // For easy reference
  ArrayInfo& array_info = ret.first->second;

  // Populate array_info
  array_info.array_mode_ = mode;
  array_info.array_schema_ = array_schema;
  array_info.next_update_seq_ = 
      load_fragment_tree(array_schema, array_info.fragment_tree_);
  array_info.id_ = array_info_id_++;
  
  // Return array descriptor
  return new ArrayDescriptor(&array_info);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

inline
void Consolidator::advance_cell_its(int attribute_num,
                                    Tile::const_cell_iterator* cell_its) const {
  for(int i=0; i<=attribute_num; i++) 
      ++cell_its[i];
}

inline
void Consolidator::advance_cell_tile_its(
    int attribute_num,
    Tile::const_cell_iterator* cell_its,
    Tile::const_cell_iterator& cell_it_end,
    StorageManager::const_tile_iterator* tile_its,
    StorageManager::const_tile_iterator& tile_it_end) const {
  advance_cell_its(attribute_num, cell_its);
  if(cell_its[attribute_num] == cell_it_end) {
    advance_tile_its(attribute_num, tile_its);
    if(tile_its[attribute_num] != tile_it_end)
      initialize_cell_its(tile_its, attribute_num, cell_its, cell_it_end);
  }
}

inline
void Consolidator::advance_tile_its(
    int attribute_num, 
    StorageManager::const_tile_iterator* tile_its) const {
  for(int i=0; i<=attribute_num; ++i) 
    ++tile_its[i];
}

inline
void Consolidator::append_cell(const Tile::const_cell_iterator* cell_its,
                               Tile** tiles,
                               int attribute_num) const {
  for(int i=0; i<=attribute_num; ++i) 
    *tiles[i] << *cell_its[i]; 
}

void Consolidator::consolidate(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::string& result_fragment_name) const {

  // Input fragments
  const StorageManager::ArrayDescriptor* ad =
      storage_manager_->open_array(array_schema, 
                                   fragment_names,
                                   StorageManager::READ);

  if(ad->array_schema()->has_irregular_tiles())
    consolidate_irregular(ad, result_fragment_name);
  else
    consolidate_regular(ad, result_fragment_name);

  // Close array and delete input fragments
  storage_manager_->close_array(ad);
  for(int i=0; i<fragment_names.size(); ++i) 
    storage_manager_->delete_fragment(array_schema->array_name(), 
                                      fragment_names[i]);
}

void Consolidator::consolidate_irregular(
  const StorageManager::ArrayDescriptor* ad,
  const std::string& result_fragment_name) const { 
  // For easy reference
  const ArraySchema* array_schema = ad->array_schema();
  const std::string& array_name = array_schema->array_name();
  int attribute_num = array_schema->attribute_num();
  int fragment_num = ad->fd().size();
  int64_t capacity = array_schema->capacity();
  const std::vector<StorageManager::FragmentDescriptor*>& fd = ad->fd();

  // Open the new result fragment
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(array_schema, 
                                      result_fragment_name, 
                                      StorageManager::CREATE);

  // Create and initialize tile iterators
  StorageManager::const_tile_iterator** tile_its = 
      new StorageManager::const_tile_iterator*[fragment_num];
  StorageManager::const_tile_iterator* tile_it_end = 
      new StorageManager::const_tile_iterator[fragment_num]; 
  for(int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_tile_iterator[attribute_num+1];
    initialize_tile_its(fd[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_cell_iterator** cell_its = 
      new Tile::const_cell_iterator*[fragment_num];
  Tile::const_cell_iterator* cell_it_end = 
      new Tile::const_cell_iterator[fragment_num];
  for(int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_cell_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], attribute_num, 
                        cell_its[i], cell_it_end[i]);
  }

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the index of the fragment from which we will get the next cell 
  // and append it to the consolidation result.
  int next_fragment_index = 
      get_next_fragment_index(tile_its, tile_it_end, fragment_num, 
                              cell_its, cell_it_end, array_schema,
                              result_fragment_name);

  int64_t tile_id = 0;
  new_tiles(array_schema, tile_id, result_tiles);

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    StorageManager::const_tile_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_tile_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];
    Tile::const_cell_iterator* next_cell_its = 
        cell_its[next_fragment_index];
    Tile::const_cell_iterator& next_cell_it_end = 
        cell_it_end[next_fragment_index];

    if(result_tiles[attribute_num]->cell_num() == capacity) {
      // Store tiles and create new ones
      store_tiles(result_fd, result_tiles);
      new_tiles(array_schema, ++tile_id, result_tiles);
    } 

    // Append cell
    append_cell(next_cell_its, result_tiles, attribute_num);

    // Advance cell (and potentially tile) iterators
    advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                          next_tile_its, next_tile_it_end);

    next_fragment_index = get_next_fragment_index(tile_its, tile_it_end, 
                                                  fragment_num, 
                                                  cell_its, cell_it_end, 
                                                  array_schema,
                                                  result_fragment_name);
  } 

  // Store lastly created tiles
  store_tiles(result_fd, result_tiles);

  // Close the result fragment
  storage_manager_->close_fragment(result_fd);

  // Clean up
  for(int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] result_tiles;
}

void Consolidator::consolidate_regular(
    const StorageManager::ArrayDescriptor* ad,
    const std::string& result_fragment_name) const {
  // For easy reference
  const ArraySchema* array_schema = ad->array_schema();
  const std::string& array_name = array_schema->array_name();
  int attribute_num = array_schema->attribute_num();
  int fragment_num = ad->fd().size();
  const std::vector<StorageManager::FragmentDescriptor*>& fd = ad->fd();

  // Open the new result fragment
  StorageManager::FragmentDescriptor* result_fd = 
      storage_manager_->open_fragment(array_schema, 
                                      result_fragment_name, 
                                      StorageManager::CREATE);

  // Create and initialize tile iterators
  StorageManager::const_tile_iterator** tile_its = 
      new StorageManager::const_tile_iterator*[fragment_num];
  StorageManager::const_tile_iterator* tile_it_end = 
      new StorageManager::const_tile_iterator[fragment_num]; 
  for(int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_tile_iterator[attribute_num+1];
    initialize_tile_its(fd[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_cell_iterator** cell_its = 
      new Tile::const_cell_iterator*[fragment_num];
  Tile::const_cell_iterator* cell_it_end = 
      new Tile::const_cell_iterator[fragment_num];
  for(int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_cell_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], attribute_num, 
                        cell_its[i], cell_it_end[i]);
  }

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the index of the fragment from which we will get the next cell 
  // and append it to the consolidation result.
  int next_fragment_index = 
      get_next_fragment_index(tile_its, tile_it_end, fragment_num, 
                              cell_its, cell_it_end, array_schema,
                              result_fragment_name);

  int64_t tile_id;
  bool first_cell = true;

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    StorageManager::const_tile_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_tile_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];
    Tile::const_cell_iterator* next_cell_its = 
        cell_its[next_fragment_index];
    Tile::const_cell_iterator& next_cell_it_end = 
        cell_it_end[next_fragment_index];

    if(first_cell) { // To initialize tile_id and result_tiles
      tile_id = next_cell_its[0].tile()->tile_id();
      first_cell = false;
      new_tiles(array_schema, tile_id, result_tiles);
    }

    if(next_cell_its[0].tile()->tile_id() != tile_id) {
      // Change tile
      tile_id = next_cell_its[0].tile()->tile_id();
      store_tiles(result_fd, result_tiles);
      new_tiles(array_schema, tile_id, result_tiles);
    } 

    // Append cell
    append_cell(next_cell_its, result_tiles, attribute_num);

    // Advance cell (and potentially tile) iterators
    advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                          next_tile_its, next_tile_it_end);

    next_fragment_index = get_next_fragment_index(tile_its, tile_it_end, 
                                                  fragment_num,
                                                  cell_its, cell_it_end, 
                                                  array_schema,
                                                  result_fragment_name);
  } 

  // Store lastly created tiles
  store_tiles(result_fd, result_tiles);

  // Close fragments
  storage_manager_->close_fragment(result_fd);

  // Clean up
  for(int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] result_tiles;
}

void Consolidator::flush_fragment_tree(
    const std::string& array_name,
    const FragmentTree& fragment_tree) const {
  // For easy reference
  int level_num = fragment_tree.size(); 

  // Do nothing if there are not fragments
  if(level_num == 0) 
    return;

  // Initialize buffer
  size_t buffer_size = level_num * 2 * sizeof(int);
  char* buffer = new char[buffer_size];

  // Populate buffer
  for(int i=0; i<level_num; ++i) {
    memcpy(buffer+2*i*sizeof(int), &(fragment_tree[i].first), sizeof(int));
    memcpy(buffer+(2*i+1)*sizeof(int), &(fragment_tree[i].second), sizeof(int));
  }

  // Store the buffer to the disk
  storage_manager_->flush_fragments_bkp(array_name, buffer, buffer_size);

  delete [] buffer;
}

int Consolidator::get_next_fragment_index(
      StorageManager::const_tile_iterator** tile_its,
      StorageManager::const_tile_iterator* tile_it_end,
      int fragment_num,
      Tile::const_cell_iterator** cell_its,
      Tile::const_cell_iterator* cell_it_end,
      const ArraySchema* array_schema,
      const std::string& result_fragment_name) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();

  // Holds the (potentially multiple) indexes of the candidate fragments
  // from which we will get the next cell for consolidation. Note though 
  // that only the largest (last) index will be returned, which corresponds
  // to the latest update.
  std::vector<int> next_fragment_index;
  next_fragment_index.reserve(fragment_num);
  // Loop will be broken upon the following conditions:
  // (1) No more cells
  // (2) Only one cell with the same coordinates
  // (3) More than one cells with the same coordinates, and
  //     if the last one is a deletion, but the resulting fragments in this
  //     array are not 1 (i.e., the resulting fragment name does not start
  //     with '0'. In this case, a deletion persists after consolidation.
  //     This is to prevent discarding a deletion tha may delete cells
  //     in fragments not taking part in this round's consolidation. Only
  //     when the consolidation produces a single fragment (whose name
  //     starts with '0') is it safe to apply the deletions.
  bool del; 
  bool name_starts_with_0 = (result_fragment_name[0] == '0');
  do {
    next_fragment_index.clear();

    for(int i=0; i<fragment_num; ++i) {
      if(cell_its[i][attribute_num] != cell_it_end[i]) {
        if(next_fragment_index.size() == 0 || cell_its[i][attribute_num] == 
           cell_its[next_fragment_index[0]][attribute_num]) {
          next_fragment_index.push_back(i);
        } else if(array_schema->precedes(
                      *cell_its[i][attribute_num], 
                      *cell_its[next_fragment_index[0]][attribute_num])) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int num_of_iterators_to_advance = next_fragment_index.size()-1; 
    del = is_del(cell_its[next_fragment_index.back()][0]);
    if(del && name_starts_with_0)
      ++num_of_iterators_to_advance;

    // Advance cell (and potentially tile) iterators.
    // If the cell is deleted, all iterators are advanced, otherwise all except
    // for the last one.
    for(int i=0; i<num_of_iterators_to_advance; ++i)
      advance_cell_tile_its(attribute_num, 
                            cell_its[next_fragment_index[i]],
                            cell_it_end[next_fragment_index[i]],
                            tile_its[next_fragment_index[i]],
                            tile_it_end[next_fragment_index[i]]);

    if(!del || !name_starts_with_0)
      return next_fragment_index.back();

  } while(1);
}

inline
void Consolidator::initialize_cell_its(
    const StorageManager::const_tile_iterator* tile_its, 
    int attribute_num,
    Tile::const_cell_iterator* cell_its, 
    Tile::const_cell_iterator& cell_it_end) const {

  for(int i=0; i<=attribute_num; ++i) 
    cell_its[i] = (*tile_its[i])->begin();
  cell_it_end = (*tile_its[attribute_num])->end();
}

inline
void Consolidator::initialize_tile_its(
    const StorageManager::FragmentDescriptor* fd,
    StorageManager::const_tile_iterator* tile_its, 
    StorageManager::const_tile_iterator& tile_it_end) const {
  // For easy reference
  int attribute_num = fd->array_schema()->attribute_num();

  for(int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_->begin(fd, i);
  tile_it_end = storage_manager_->end(fd, attribute_num);
}

bool Consolidator::is_del(const Tile::const_cell_iterator& cell_it) const {
  return cell_it.is_del();
}

int64_t Consolidator::load_fragment_tree(const ArraySchema* array_schema,
                                         FragmentTree& fragment_tree) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();
  int consolidation_step = array_schema->consolidation_step();

  // Load the fragment book-keeping info from the disk into a buffer
  char* buffer;
  size_t buffer_size;
  storage_manager_->load_fragments_bkp(array_name, buffer, buffer_size);

  // Empty fragment tree. The update sequence number is 0.
  if(buffer == NULL)
    return 0;

  assert(buffer_size != 0);
  assert(buffer_size % 2 == 0);
 
  // Create the tree from the buffer and calculate next fragment sequence number
  int64_t next_update_seq = 0;
  int level_num = buffer_size / (2 * sizeof(int));
  int level, node_num;
  for(int i=0; i<level_num; ++i) {
    memcpy(&level, buffer+2*i*sizeof(int), sizeof(int));
    memcpy(&node_num, buffer+(2*i+1)*sizeof(int), sizeof(int));
    fragment_tree.push_back(FragmentTreeLevel(level, node_num));
    next_update_seq += 
        pow(double(consolidation_step), double(level)) * node_num;
  }

  // Clean up
  delete [] buffer;

  return next_update_seq;
}

inline
void Consolidator::new_tiles(const ArraySchema* array_schema, 
                             int64_t tile_id, 
                             Tile** tiles) const {
  // For easy reference
  int attribute_num = array_schema->attribute_num();
  int64_t capacity = array_schema->capacity();

  for(int i=0; i<=attribute_num; ++i)
    tiles[i] = storage_manager_->new_tile(array_schema, i, tile_id, capacity);
}

bool Consolidator::path_exists(const std::string& path) const {
  struct stat st;
  return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline
void Consolidator::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  assert(path_exists(workspace_));
 
  workspace_ += "/Consolidator";
}

inline
void Consolidator::store_tiles(const StorageManager::FragmentDescriptor* fd,
                               Tile** tiles) const {
  // For easy reference
  int attribute_num = fd->array_schema()->attribute_num();

  // Append attribute tiles
  for(int i=0; i<=attribute_num; ++i)
    storage_manager_->append_tile(tiles[i], fd, i);
}
