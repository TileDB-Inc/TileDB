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

uint64_t Consolidator::array_info_id_ = 0;

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

Consolidator::Consolidator(const std::string& workspace, 
                           StorageManager& storage_manager,
                           unsigned int consolidation_step) 
    : consolidation_step_(consolidation_step), 
      storage_manager_(storage_manager) {
  assert(consolidation_step_ > 1);
  set_workspace(workspace);
  create_workspace(); 
}

/******************************************************
****************** ARRAY FUNCTIONS ********************
******************************************************/

void Consolidator::add_fragment(const ArrayDescriptor* ad) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_info_->array_schema_;
  const std::string& array_name = array_schema.array_name();
  FragmentTree& fragment_tree = ad->array_info_->fragment_tree_;
  int level_num = fragment_tree.size();

  // Add fragment to tree
  if(level_num == 0 || fragment_tree[level_num-1].first > 0) {
    fragment_tree.push_back(FragmentTreeLevel(0,1));
    ++level_num;
  } else {
    ++(fragment_tree[level_num-1].second);
  }
  uint64_t cur_fragment_seq = ad->array_info_->next_fragment_seq_;

  // Consolidate (recursively) if necessary
  while(fragment_tree[level_num-1].second == consolidation_step_) {
    int level = fragment_tree[level_num-1].first;
    uint64_t subtree_size = pow(double(consolidation_step_), level);
    std::stringstream ss;

    // Get suffixes of fragments under consolidation
    std::vector<std::string> fragment_suffixes;
    for(unsigned int i=0; i<consolidation_step_; ++i) {
      uint64_t start_seq = (cur_fragment_seq - (consolidation_step_ - i) * 
                            subtree_size + 1);
      uint64_t end_seq = start_seq + subtree_size - 1; 
      ss << start_seq << "_" << end_seq;
      fragment_suffixes.push_back(ss.str());
      ss.str(std::string());
    }

    // Get result fragment suffix
    ss << cur_fragment_seq - consolidation_step_ * subtree_size + 1
       << "_" << cur_fragment_seq;
    std::string result_fragment_suffix = ss.str();

    // Consolidate
    if(array_schema.has_regular_tiles())
      consolidate_regular(fragment_suffixes, 
                          result_fragment_suffix, array_schema);
    else
      consolidate_irregular(fragment_suffixes, 
                            result_fragment_suffix, array_schema);

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

  ++ad->array_info_->next_fragment_seq_;
}

bool Consolidator::array_exists(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + CN_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) 
    return false;
  
  close(fd);
  return true;
}

void Consolidator::close_array(const ArrayDescriptor* ad) {
  // For easy reference
  const std::string& array_name = ad->array_info_->array_schema_.array_name();

  // Check if array is open, and descriptor is valid
  assert(open_arrays_.find(array_name) != open_arrays_.end());
  assert(open_arrays_[array_name].id_ == ad->array_info_id_);

  flush_fragment_tree(array_name, ad->array_info_->fragment_tree_); 
  open_arrays_.erase(array_name); 

  delete ad;
}

void Consolidator::delete_array(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + CN_SUFFIX;
  remove(filename.c_str());
}

std::vector<std::string> Consolidator::get_all_fragment_suffixes(
    const ArrayDescriptor* ad) const {
  // For easy reference
  FragmentTree& fragment_tree = ad->array_info_->fragment_tree_;
  unsigned int level;
  uint64_t start_seq = 0, end_seq, subtree_size;
  std::stringstream ss;
  std::vector<std::string> fragment_suffixes;


  for(unsigned int i=0; i<fragment_tree.size(); ++i) {
    level = fragment_tree[i].first;
    for(unsigned int j=0; j < fragment_tree[i].second; ++j) {
      subtree_size = pow(double(consolidation_step_), level); 
      end_seq = start_seq + subtree_size - 1;
      ss.str(std::string());
      ss << start_seq << "_" << end_seq;
      fragment_suffixes.push_back(ss.str());
      start_seq += subtree_size;
    } 
  }

  return fragment_suffixes;
}

std::string Consolidator::get_next_fragment_name(
    const ArrayDescriptor* ad) const {
  // For easy reference
  std::string array_name = ad->array_info_->array_schema_.array_name();

  uint64_t n = get_next_fragment_seq(ad);
  std::stringstream fragment_schema_name;
  fragment_schema_name << array_name << "_" << n << "_" << n;

  return  fragment_schema_name.str();
}

uint64_t Consolidator::get_next_fragment_seq(
    const ArrayDescriptor* ad) const {
  return ad->array_info_->next_fragment_seq_;
}

const Consolidator::ArrayDescriptor* Consolidator::open_array(
    const ArraySchema& array_schema) {
  // For easy reference 
  const std::string& array_name = array_schema.array_name();

  // Check if array is already open
  assert(open_arrays_.find(array_name) == open_arrays_.end());

  // Create a new entry in open_arrays_ 
  std::pair<OpenArrays::iterator, bool> ret = open_arrays_.insert(
      std::pair<std::string, ArrayInfo>(array_name, ArrayInfo()));

  // For easy reference
  ArrayInfo& array_info = ret.first->second;

  // Populate array_info
  array_info.next_fragment_seq_ = 
      load_fragment_tree(array_name, array_info.fragment_tree_);
  array_info.array_schema_ = array_schema;
  array_info.id_ = array_info_id_++;
  
  // Return array descriptor
  return new ArrayDescriptor(&array_info);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

inline
void Consolidator::advance_cell_its(unsigned int attribute_num,
                                      Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
      ++cell_its[i];
}

inline
void Consolidator::advance_cell_tile_its(
    unsigned int attribute_num,
    Tile::const_iterator* cell_its,
    Tile::const_iterator& cell_it_end,
    StorageManager::const_iterator* tile_its,
    StorageManager::const_iterator& tile_it_end) const {
  advance_cell_its(attribute_num, cell_its);
  if(cell_its[attribute_num] == cell_it_end) {
    advance_tile_its(attribute_num, tile_its);
    if(tile_its[attribute_num] != tile_it_end)
      initialize_cell_its(tile_its, attribute_num, cell_its, cell_it_end);
  }
}

inline
void Consolidator::advance_tile_its(
    unsigned int attribute_num, 
    StorageManager::const_iterator* tile_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    ++tile_its[i];
}

inline
void Consolidator::append_cell(const Tile::const_iterator* cell_its,
                               Tile** tiles,
                               unsigned int attribute_num) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    *tiles[i] << cell_its[i]; 
}

bool Consolidator::is_null(const Tile::const_iterator& cell_it) const {
  return cell_it.is_null();
}

void Consolidator::consolidate_irregular(
    const std::vector<std::string>& fragment_suffixes,
    const std::string& result_fragment_suffix,
    const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int fragment_num = consolidation_step_;
  uint64_t capacity = array_schema.capacity();

  // Open fragments under consolidation and result fragment
  const StorageManager::ArrayDescriptor** ad = 
      new const StorageManager::ArrayDescriptor*[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    ad[i] = storage_manager_.open_array(array_name + std::string("_") + 
                                        fragment_suffixes[i]);
  
  ArraySchema result_fragment_schema = array_schema.clone(
      array_name + std::string("_") + result_fragment_suffix);
  const StorageManager::ArrayDescriptor* result_ad = 
      result_ad = storage_manager_.open_array(result_fragment_schema);

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its = 
      new StorageManager::const_iterator*[fragment_num];
  StorageManager::const_iterator* tile_it_end = 
      new StorageManager::const_iterator[fragment_num]; 
  for(unsigned int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_iterator[attribute_num+1];
    initialize_tile_its(ad[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], attribute_num, 
                        cell_its[i], cell_it_end[i]);
  }

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the index of the fragment from which we will get the next cell 
  // and append it to the consolidation result.
  int next_fragment_index = 
      get_next_fragment_index(tile_its, tile_it_end, consolidation_step_, 
                              cell_its, cell_it_end, array_schema,
                              result_fragment_suffix);

  uint64_t tile_id = 0;
  new_tiles(result_fragment_schema, tile_id, result_tiles);

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    StorageManager::const_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];

    if(result_tiles[attribute_num]->cell_num() == capacity) {
      // Store tiles and create new ones
      store_tiles(result_ad, result_tiles);
      new_tiles(result_fragment_schema, ++tile_id, result_tiles);
    } 

    // Append cell
    append_cell(next_cell_its, result_tiles, attribute_num);

    // Advance cell (and potentially tile) iterators
    advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                          next_tile_its, next_tile_it_end);

    next_fragment_index = get_next_fragment_index(tile_its, tile_it_end, 
                                                  consolidation_step_, 
                                                  cell_its, cell_it_end, 
                                                  array_schema,
                                                  result_fragment_suffix);
  } 

  // Store lastly created tiles
  store_tiles(result_ad, result_tiles);

  // Close fragments
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.close_array(ad[i]);
  storage_manager_.close_array(result_ad);

  // Delete fragments
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.delete_array(array_name + std::string("_") + 
                                  fragment_suffixes[i]);
 
  // Clean up
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] ad;
  delete [] result_tiles;
}

void Consolidator::consolidate_regular(
    const std::vector<std::string>& fragment_suffixes,
    const std::string& result_fragment_suffix,
    const ArraySchema& array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema.array_name();
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int fragment_num = consolidation_step_;

  // Open fragments under consolidation and result fragment
  const StorageManager::ArrayDescriptor** ad = 
      new const StorageManager::ArrayDescriptor*[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    ad[i] = storage_manager_.open_array(array_name + std::string("_") + 
                                        fragment_suffixes[i]);
  
  ArraySchema result_fragment_schema = array_schema.clone(
      array_name + std::string("_") + result_fragment_suffix);
  const StorageManager::ArrayDescriptor* result_ad = 
      result_ad = storage_manager_.open_array(result_fragment_schema);

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its = 
      new StorageManager::const_iterator*[fragment_num];
  StorageManager::const_iterator* tile_it_end = 
      new StorageManager::const_iterator[fragment_num]; 
  for(unsigned int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_iterator[attribute_num+1];
    initialize_tile_its(ad[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], attribute_num, 
                        cell_its[i], cell_it_end[i]);
  }

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the index of the fragment from which we will get the next cell 
  // and append it to the consolidation result.
  int next_fragment_index = 
      get_next_fragment_index(tile_its, tile_it_end, consolidation_step_, 
                              cell_its, cell_it_end, array_schema,
                              result_fragment_suffix);

  uint64_t tile_id;
  bool first_cell = true;

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    StorageManager::const_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];

    if(first_cell) { // To initialize tile_id and result_tiles
      tile_id = next_cell_its[0].tile()->tile_id();
      first_cell = false;
      new_tiles(result_fragment_schema, tile_id, result_tiles);
    }

    if(next_cell_its[0].tile()->tile_id() != tile_id) {
      // Change tile
      tile_id = next_cell_its[0].tile()->tile_id();
      store_tiles(result_ad, result_tiles);
      new_tiles(result_fragment_schema, tile_id, result_tiles);
    } 

    // Append cell
    append_cell(next_cell_its, result_tiles, attribute_num);

    // Advance cell (and potentially tile) iterators
    advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                          next_tile_its, next_tile_it_end);

    next_fragment_index = get_next_fragment_index(tile_its, tile_it_end, 
                                                  consolidation_step_,
                                                  cell_its, cell_it_end, 
                                                  array_schema,
                                                  result_fragment_suffix);
  } 

  // Store lastly created tiles
  store_tiles(result_ad, result_tiles);

  // Close fragments
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.close_array(ad[i]);
  storage_manager_.close_array(result_ad);

  // Delete fragments
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.delete_array(array_name + std::string("_") + 
                                  fragment_suffixes[i]);
 
  // Clean up
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] ad;
  delete [] result_tiles;
}

void Consolidator::create_workspace() const {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void Consolidator::flush_fragment_tree(
    const std::string& array_name,
    const FragmentTree& fragment_tree) const {
  // For easy reference
  int level_num = fragment_tree.size(); 
  assert(level_num != 0);

  // Initialize buffer
  uint64_t buffer_size = level_num * 2 * sizeof(int);
  char* buffer = new char[buffer_size];

  // Populate buffer
  for(int i=0; i<level_num; ++i) {
    memcpy(buffer+2*i*sizeof(int), &(fragment_tree[i].first), sizeof(int));
    memcpy(buffer+(2*i+1)*sizeof(int), &(fragment_tree[i].second), sizeof(int));
  }

  // Open file
  std::string filename = workspace_ + "/" + array_name +  CN_SUFFIX;
  int fd = open(filename.c_str(), 
                O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Flush the buffer into the file
  write(fd, buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;
}

int Consolidator::get_next_fragment_index(
      StorageManager::const_iterator** tile_its,
      StorageManager::const_iterator* tile_it_end,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      const ArraySchema& array_schema,
      const std::string& result_fragment_suffix) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();

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
  //     array are not 1 (i.e., the resulting fragment suffix does not start
  //     with '0'. In this case, a deletion persists after consolidation.
  //     This is to prevent discarding a deletion tha may delete cells
  //     in fragments not taking part in this round's consolidation. Only
  //     when the consolidation produces a single fragment (whose suffix
  //     starts with '0') is it safe to apply the deletions.
  bool null; 
  bool suffix_starts_with_0 = (result_fragment_suffix[0] == '0');
  do {
    next_fragment_index.clear();

    for(unsigned int i=0; i<fragment_num; ++i) {
      if(cell_its[i][attribute_num] != cell_it_end[i]) {
        if(next_fragment_index.size() == 0 || cell_its[i][attribute_num] == 
           cell_its[next_fragment_index[0]][attribute_num]) {
          next_fragment_index.push_back(i);
        } else if(array_schema.precedes(
                      cell_its[i][attribute_num], 
                      cell_its[next_fragment_index[0]][attribute_num])) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int num_of_iterators_to_advance = next_fragment_index.size()-1; 
    null = is_null(cell_its[next_fragment_index.back()][0]);
    if(null && suffix_starts_with_0)
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

    if(!null || !suffix_starts_with_0)
      return next_fragment_index.back();

  } while(1);
}

inline
void Consolidator::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, unsigned int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(unsigned int i=0; i<=attribute_num; i++)
    cell_its[i] = (*tile_its[i]).begin();
  cell_it_end = (*tile_its[attribute_num]).end();
}

inline
void Consolidator::initialize_tile_its(
    const StorageManager::ArrayDescriptor* ad,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator& tile_it_end) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  for(unsigned int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_.begin(ad, i);
  tile_it_end = storage_manager_.end(ad, attribute_num);
}

uint64_t Consolidator::load_fragment_tree(const std::string& array_name,
                                          FragmentTree& fragment_tree) const {
  // Open file
  std::string filename = workspace_ + "/" + array_name +  CN_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);

  // If the file does not exist, the tree is empty, and the first fragment
  // sequence number should be 0.
  if(fd == -1)
    return 0;

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  assert(st.st_size != 0);
  assert(st.st_size % 2 == 0);
  uint64_t buffer_size = st.st_size;
  char* buffer = new char[buffer_size];
 
  // Load contents of the file into the buffer
  read(fd, buffer, buffer_size);

  // Create the tree from the buffer and calculate next fragment sequence number
  uint64_t next_fragment_seq = 0;
  int level_num = buffer_size / (2 * sizeof(int));
  unsigned int level, node_num;
  for(int i=0; i<level_num; ++i) {
    memcpy(&level, buffer+2*i*sizeof(int), sizeof(int));
    memcpy(&node_num, buffer+(2*i+1)*sizeof(int), sizeof(int));
    fragment_tree.push_back(FragmentTreeLevel(level, node_num));
    next_fragment_seq += 
        pow(double(consolidation_step_), double(level)) * node_num;
  }

  // Clean up
  close(fd);
  delete [] buffer;

  return next_fragment_seq;
}

inline
void Consolidator::new_tiles(const ArraySchema& array_schema, 
                               uint64_t tile_id, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  for(unsigned int i=0; i<=attribute_num; i++)
    tiles[i] = storage_manager_.new_tile(array_schema, i, tile_id, capacity);
}

bool Consolidator::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
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
void Consolidator::store_tiles(const StorageManager::ArrayDescriptor* ad,
                               Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++)
    storage_manager_.append_tile(tiles[i], ad, i);
}
