/**
 * @file   query_processor.cc
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
 * This file implements the QueryProcessor class.
 */
  
#include "query_processor.h"
#include <stdio.h>
#include <typeinfo>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <assert.h>
#include <algorithm>
#include <parallel/algorithm>
#include <functional>
#include <math.h>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

QueryProcessor::QueryProcessor(const std::string& workspace, 
                               StorageManager& storage_manager) 
    : storage_manager_(storage_manager) {
  set_workspace(workspace);
  create_workspace(); 
}

void QueryProcessor::export_to_CSV(const StorageManager::ArrayDescriptor* ad,
                                   const std::string& filename) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const std::string& array_name = array_schema.array_name();
  const unsigned int attribute_num = array_schema.attribute_num();
  const unsigned int dim_num = array_schema.dim_num();
  
  // Prepare CSV file
  CSVFile csv_file(filename, CSVFile::WRITE);

  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;
  initialize_tile_its(ad, tile_its, tile_it_end);

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Iterate over all tiles
  while(tile_its[attribute_num] != tile_it_end) {
    // Iterate over all cells of each tile
    initialize_cell_its(tile_its, attribute_num, cell_its, cell_it_end);

    while(cell_its[attribute_num] != cell_it_end) { 
      csv_file << cell_to_csv_line(cell_its, attribute_num);
      advance_cell_its(attribute_num, cell_its);
    }
 
    advance_tile_its(attribute_num, tile_its);
  }

  // Clean up 
  delete [] tile_its;
  delete [] cell_its;
}

void QueryProcessor::filter(const StorageManager::ArrayDescriptor* ad,
                            const ExpressionTree* expression,
                            const std::string& result_array_name) const {
  if(ad->array_schema().has_regular_tiles())
    filter_regular(ad, expression, result_array_name);
  else 
    filter_irregular(ad, expression, result_array_name);
} 

void QueryProcessor::join(const StorageManager::ArrayDescriptor* ad_A, 
                          const StorageManager::ArrayDescriptor* ad_B,
                          const std::string& result_array_name) const {
  // For easy reference
  const ArraySchema& array_schema_A = ad_A->array_info()->array_schema_;
  const ArraySchema& array_schema_B = ad_B->array_info()->array_schema_;

  std::pair<bool,std::string> can_join = 
      ArraySchema::join_compatible(array_schema_A, array_schema_B);

  if(!can_join.first)
    throw QueryProcessorException(std::string("[QueryProcessor] Input arrays "
                                  " are not join-compatible.") + 
                                  can_join.second);

  ArraySchema array_schema_C = ArraySchema::create_join_result_schema(
                                   array_schema_A, 
                                   array_schema_B, 
                                   result_array_name);
  
  if(array_schema_A.has_regular_tiles())
    join_regular(ad_A, ad_B, array_schema_C);
  else 
    join_irregular(ad_A, ad_B, array_schema_C);
} 

void QueryProcessor::nearest_neighbors(
    const StorageManager::ArrayDescriptor* ad,
    const std::vector<double>& q,
    uint64_t k,
    const std::string& result_array_name) const { 
  if(ad->array_schema().has_regular_tiles())
    nearest_neighbors_regular(ad, q, k, result_array_name);
  else 
    nearest_neighbors_irregular(ad, q, k, result_array_name);
}

void QueryProcessor::subarray(const StorageManager::ArrayDescriptor* ad,
                              const Tile::Range& range,
                              const std::string& result_array_name) const { 
  if(ad->array_schema().has_regular_tiles())
    subarray_regular(ad, range, result_array_name);
  else 
    subarray_irregular(ad, range, result_array_name);
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

inline
void QueryProcessor::advance_cell_its(unsigned int attribute_num,
                                      Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
      ++cell_its[i];
}

inline
void QueryProcessor::advance_cell_its(
    Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    ++cell_its[attribute_ids[i]];
}

inline
void QueryProcessor::advance_cell_its(unsigned int attribute_num,
                                      Tile::const_iterator* cell_its,
                                      int64_t step) const {
  for(unsigned int i=0; i<attribute_num; i++) 
    cell_its[i] += step;
}

inline
void QueryProcessor::advance_cell_its(
    Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids,
    int64_t step) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    cell_its[attribute_ids[i]] += step;
}

inline
void QueryProcessor::advance_tile_its(
    unsigned int attribute_num, 
    StorageManager::const_iterator* tile_its) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    ++tile_its[i];
}

inline
void QueryProcessor::advance_tile_its(
    unsigned int attribute_num, 
    StorageManager::const_iterator* tile_its, 
    int64_t step) const {
  for(unsigned int i=0; i<attribute_num; i++) 
    tile_its[i] += step;
}

inline
void QueryProcessor::advance_tile_its(
    StorageManager::const_iterator* tile_its,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    ++tile_its[attribute_ids[i]];
}

inline
void QueryProcessor::advance_tile_its(
    StorageManager::const_iterator* tile_its, 
    const std::vector<unsigned int>& attribute_ids,
    int64_t step) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    tile_its[attribute_ids[i]] += step;
}

inline
void QueryProcessor::append_cell(const Tile::const_iterator* cell_its,
                                 Tile** tiles,
                                 unsigned int attribute_num) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    *tiles[i] << cell_its[i]; 
}

inline
void QueryProcessor::append_cell(const Tile::const_iterator* cell_its_A,
                                 const Tile::const_iterator* cell_its_B,
                                 Tile** tiles_C,
                                 unsigned int attribute_num_A,
                                 unsigned int attribute_num_B) const {
  for(unsigned int i=0; i<attribute_num_A; i++) 
    *tiles_C[i] << cell_its_A[i]; 
  for(unsigned int i=0; i<=attribute_num_B; i++)
    *tiles_C[attribute_num_A+i] << cell_its_B[i]; 
}

bool QueryProcessor::cell_satisfies_expression(
    const ArraySchema& array_schema,
    const Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids,
    const ExpressionTree* expression) const {
  // Get the values of the attributes involved in the expression
  std::map<std::string, double> var_values;
  for(unsigned int i=0; i<attribute_ids.size(); ++i) {
    var_values[array_schema.attribute_name(attribute_ids[i])] = 
        *cell_its[attribute_ids[i]];
  }

  // Evaluate the expression
  return expression->evaluate(var_values);
}

inline
CSVLine QueryProcessor::cell_to_csv_line(const Tile::const_iterator* cell_its,
                                         unsigned int attribute_num) const {
  CSVLine csv_line;

  // Append coordinates first
  cell_its[attribute_num] >> csv_line;
  // Append attribute values next
  for(unsigned int i=0; i<attribute_num; i++)
    cell_its[i] >> csv_line;

  return csv_line;
}

std::vector<QueryProcessor::DistRank> QueryProcessor::compute_sorted_dist_ranks(
    const StorageManager::ArrayDescriptor* ad,
    const std::vector<double>& q) const {
  // Initializations
  // We store pairs of the form (dist, rank)
  std::vector<DistRank> dist_ranks;
  double dist;
  uint64_t rank;

  // Retrieve MBR iterators from storage manager
  StorageManager::MBRs::const_iterator mbr_it = 
      storage_manager_.MBR_begin(ad);
  StorageManager::MBRs::const_iterator mbr_it_end = 
      storage_manager_.MBR_end(ad);

  for(rank=0; mbr_it != mbr_it_end; ++mbr_it, ++rank) {
    dist = point_to_mbr_distance(q, *mbr_it);
    dist_ranks.push_back(DistRank(dist, rank));
  }
 
  // Sort ranks on distance and return them
  __gnu_parallel::sort(dist_ranks.begin(), dist_ranks.end());
  return dist_ranks;
}

std::priority_queue<QueryProcessor::RankPosCoord> 
QueryProcessor::compute_sorted_kNN_coords(
    const StorageManager::ArrayDescriptor* ad,
    const std::vector<double>& q,
    uint64_t k,
    const std::vector<DistRank>& sorted_dist_ranks) const {
  std::priority_queue<DistRankPosCoord> kNN_coords;
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  const Tile* tile;
  Tile::const_iterator cell_it, cell_it_end;

  // Find the k nearest neighbor coordinates
  uint64_t rank;
  double dist;
  std::vector<double> coord;
  uint64_t tile_num = sorted_dist_ranks.size();
  // Iterate over the (coordinate) tiles, sorted on their distance to q
  for(uint64_t i=0; i<tile_num; ++i) {
    // Stopping condition
    if(kNN_coords.size() == k && 
       sorted_dist_ranks[i].first > kNN_coords.top().first)
      break;

    // Get new coordinate tile and initalize cell iterators
    rank = sorted_dist_ranks[i].second;
    tile = storage_manager_.get_tile_by_rank(ad, attribute_num, rank); 
    cell_it = tile->begin();
    cell_it_end = tile->end();

    // Scan all (coordinate) cells
    for(uint64_t pos=0; cell_it != cell_it_end; ++cell_it, ++pos) {
      // Find new kNNs
      coord = *cell_it;
      if(kNN_coords.size() < k || 
         (dist = point_to_point_distance(q, coord)) < kNN_coords.top().first) {
        kNN_coords.push(DistRankPosCoord(dist, 
                                         RankPosCoord(rank, 
                                                      PosCoord(pos, coord))));
        if(kNN_coords.size() > k)
          kNN_coords.pop();
      }
    }
  }

  // Make a new priority queue (rank, (pos, coord)), sorted on rank, pos
  std::priority_queue<RankPosCoord> resorted_kNN_coords;
  while(kNN_coords.size() > 0) {
    resorted_kNN_coords.push(kNN_coords.top().second);
    kNN_coords.pop();
  }

  return resorted_kNN_coords;
}

void QueryProcessor::create_workspace() const {
  struct stat st;
  stat(workspace_.c_str(), &st);

  // If the workspace does not exist, create it
  if(!S_ISDIR(st.st_mode)) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }
}

void QueryProcessor::filter_irregular(
    const StorageManager::ArrayDescriptor* ad,
    const ExpressionTree* expression,
    const std::string& result_array_name) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();
  
  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression
  std::vector<unsigned int> expr_attribute_ids;
  std::set<std::string>::const_iterator expr_attr_it = 
      expr_attribute_names.begin();
  std::set<std::string>::const_iterator expr_attr_it_end = 
      expr_attribute_names.end();
  for(; expr_attr_it != expr_attr_it_end; ++expr_attr_it) 
    expr_attribute_ids.push_back(array_schema.attribute_id(*expr_attr_it));
  std::sort(expr_attribute_ids.begin(), expr_attribute_ids.end());
  unsigned int expr_attribute_num = expr_attribute_ids.size();

  // Find the ids of the attributes NOT involved in the expression
  std::vector<unsigned int> non_expr_attribute_ids;
  for(unsigned int j=0; j<expr_attribute_ids[0]; j++)
    non_expr_attribute_ids.push_back(j);
  for(unsigned int i=1; i<expr_attribute_num; ++i) {
    for(unsigned int j=expr_attribute_ids[i-1]+1; j<expr_attribute_ids[i]; ++j)
      non_expr_attribute_ids.push_back(j);
  }
  for(unsigned int j=expr_attribute_ids[expr_attribute_num-1] + 1; 
      j<attribute_num; ++j)
    non_expr_attribute_ids.push_back(j);

  // Initialize tile iterators
  unsigned int end_attribute_id = expr_attribute_ids[0];
  initialize_tile_its(ad, tile_its, tile_it_end, end_attribute_id);
  
  // Auxiliary variable storing the number of skipped tiles when filtering.
  // It is used to advance only the iterators of the attributes involved
  // in the expression when a tile is finished/skipped, and then efficiently 
  // advance the iterators of the rest of the attributes only when a cell 
  // satisfies the expression.
  int64_t skipped_tiles = 0;
    
  // Auxiliary variable storing the number of skipped cells when filtering.
  // It is used to advance only the iterators involved in the expression when 
  // a cell is skipped, and then efficiently advance the iterators for the 
  // rest of the attributes when a cell satisfies the expression.
  uint64_t skipped_cells;

  // Create result tiles
  uint64_t tile_id = 0;
  new_tiles(result_array_schema, tile_id, result_tiles); 

  // Iterate over all tiles
  while(tile_its[end_attribute_id] != tile_it_end) {
    // Initialize cell its for the attributes involved in the expression
    initialize_cell_its(tile_its, cell_its, cell_it_end, expr_attribute_ids);
    skipped_cells = 0;
    bool non_expr_cell_its_initialized = false;

    // Iterate over all cells of each tile
    while(cell_its[end_attribute_id] != cell_it_end) {
      if(cell_satisfies_expression(array_schema, cell_its, 
                                   expr_attribute_ids, expression)) {
        if(skipped_tiles) {
          advance_tile_its(tile_its, non_expr_attribute_ids, skipped_tiles);
          tile_its[attribute_num] += skipped_tiles;
          skipped_tiles = 0;
        }
        if(!non_expr_cell_its_initialized) {
          initialize_cell_its(tile_its, cell_its, non_expr_attribute_ids);
          cell_its[attribute_num] = (*tile_its[attribute_num]).begin();
          non_expr_cell_its_initialized = true;
        }
        if(skipped_cells) {
          advance_cell_its(cell_its, non_expr_attribute_ids, skipped_cells);
          cell_its[attribute_num] += skipped_cells;
          skipped_cells = 0;
        }
        if(result_tiles[attribute_num]->cell_num() == capacity) {
          store_tiles(result_ad, result_tiles);
          new_tiles(result_array_schema, ++tile_id, result_tiles); 
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      } else {
        advance_cell_its(cell_its, expr_attribute_ids);
        ++skipped_cells;
      }
    }
 
    // Advance tile iterators 
    advance_tile_its(tile_its, expr_attribute_ids);
    ++skipped_tiles;
  }
 
  // Send the lastly created tiles to storage manager
  store_tiles(result_ad, result_tiles);

  // Close result array 
  storage_manager_.close_array(result_ad);

  // Clean up
  delete [] result_tiles;
  delete [] tile_its;
  delete [] cell_its;
}

void QueryProcessor::filter_regular(
    const StorageManager::ArrayDescriptor* ad,
    const ExpressionTree* expression,
    const std::string& result_array_name) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  const unsigned int attribute_num = array_schema.attribute_num();
  
  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);

  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression
  std::vector<unsigned int> expr_attribute_ids;
  std::set<std::string>::const_iterator expr_attr_it = 
      expr_attribute_names.begin();
  std::set<std::string>::const_iterator expr_attr_it_end = 
      expr_attribute_names.end();
  for(; expr_attr_it != expr_attr_it_end; ++expr_attr_it) 
    expr_attribute_ids.push_back(array_schema.attribute_id(*expr_attr_it));
  std::sort(expr_attribute_ids.begin(), expr_attribute_ids.end());
  unsigned int expr_attribute_num = expr_attribute_ids.size();

  // Find the ids of the attributes NOT involved in the expression
  std::vector<unsigned int> non_expr_attribute_ids;
  for(unsigned int j=0; j<expr_attribute_ids[0]; j++)
    non_expr_attribute_ids.push_back(j);
  for(unsigned int i=1; i<expr_attribute_num; ++i) {
    for(unsigned int j=expr_attribute_ids[i-1]+1; j<expr_attribute_ids[i]; ++j)
      non_expr_attribute_ids.push_back(j);
  }
  for(unsigned int j=expr_attribute_ids[expr_attribute_num-1] + 1; 
      j<attribute_num; ++j)
    non_expr_attribute_ids.push_back(j);

  // Initialize tile iterators
  unsigned int end_attribute_id = expr_attribute_ids[0];
  initialize_tile_its(ad, tile_its, tile_it_end, end_attribute_id);
  
  // Auxiliary variable storing the number of skipped tiles when filtering.
  // It is used to advance only the iterators of the attributes involved
  // in the expression when a tile is finished/skipped, and then efficiently 
  // advance the iterators of the rest of the attributes only when a cell 
  // satisfies the expression.
  int64_t skipped_tiles = 0;
    
  // Auxiliary variable storing the number of skipped cells when filtering.
  // It is used to advance only the iterators involved in the expression when 
  // a cell is skipped, and then efficiently advance the iterators for the 
  // rest of the attributes when a cell satisfies the expression.
  uint64_t skipped_cells;

  // Iterate over all tiles
  while(tile_its[end_attribute_id] != tile_it_end) {
    // Create new result tiles
    new_tiles(result_array_schema, tile_its[end_attribute_id].tile_id(), 
              result_tiles);
    // Initialize cell its for the attributes involved in the expression
    initialize_cell_its(tile_its, cell_its, cell_it_end, expr_attribute_ids);
    skipped_cells = 0;
    bool non_expr_cell_its_initialized = false;

    // Iterate over all cells of each tile
    while(cell_its[end_attribute_id] != cell_it_end) {
      if(cell_satisfies_expression(array_schema, cell_its, 
                                   expr_attribute_ids, expression)) {
        if(skipped_tiles) {
          advance_tile_its(tile_its, non_expr_attribute_ids, skipped_tiles);
          tile_its[attribute_num] += skipped_tiles;
          skipped_tiles = 0;
        }
        if(!non_expr_cell_its_initialized) {
          initialize_cell_its(tile_its, cell_its, non_expr_attribute_ids);
          cell_its[attribute_num] = (*tile_its[attribute_num]).begin();
          non_expr_cell_its_initialized = false;
        }
        if(skipped_cells) {
          advance_cell_its(cell_its, non_expr_attribute_ids, skipped_cells);
          cell_its[attribute_num] += skipped_cells;
          skipped_cells = 0;
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      } else {
        advance_cell_its(cell_its, expr_attribute_ids);
        ++skipped_cells;
      }
    }
    
    // Send the lastly created tiles to storage manager
    store_tiles(result_ad, result_tiles);
    
    // Advance tile iterators 
    advance_tile_its(tile_its, expr_attribute_ids);
    ++skipped_tiles;
  }
 
  // Close result array 
  storage_manager_.close_array(result_ad);

  // Clean up
  delete [] result_tiles;
  delete [] tile_its;
  delete [] cell_its;
}

inline
void QueryProcessor::get_tiles(
    const StorageManager::ArrayDescriptor* ad, 
    uint64_t tile_id, const Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  // Get attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++) 
   tiles[i] = storage_manager_.get_tile(ad, i, tile_id);
}

bool QueryProcessor::path_exists(const std::string& path) const {
  struct stat st;
  stat(path.c_str(), &st);
  return S_ISDIR(st.st_mode);
}

double QueryProcessor::point_to_mbr_distance(
    const std::vector<double>& q, const std::vector<double>& mbr) const {
  // Check dimensionality
  assert(mbr.size() == 2*q.size());

  unsigned int dim_num = q.size();
  double width, centroid, dq;
  double dist = 0;
  for(unsigned int i=0; i<dim_num; ++i) {
    width = mbr[2*i+1] - mbr[2*i];
    centroid = mbr[2*i] + width/2;
    dq = std::max(abs(q[i]-centroid) - width/2, 0.0);
    dist += dq * dq; 
  }

  return sqrt(dist);
}

double QueryProcessor::point_to_point_distance(
    const std::vector<double>& q, const std::vector<double>& p) const {
  // Check dimensionality
  assert(q.size() == p.size());

  unsigned int dim_num = q.size();
  double dist = 0, diff;
  for(unsigned int i=0; i<dim_num; ++i) {
    diff = q[i] - p[i];
    dist += diff * diff; 
  }

  return sqrt(dist);
}

inline
void QueryProcessor::initialize_cell_its(
    const Tile** tiles, unsigned int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(unsigned int i=0; i<=attribute_num; i++)
    cell_its[i] = tiles[i]->begin();
  cell_it_end = tiles[attribute_num]->end();
}

inline
void QueryProcessor::initialize_cell_its(
    const Tile** tiles, unsigned int attribute_num,
    Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<attribute_num; i++)
    cell_its[i] = tiles[i]->begin();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, unsigned int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(unsigned int i=0; i<=attribute_num; i++)
    cell_its[i] = (*tile_its[i]).begin();
  cell_it_end = (*tile_its[attribute_num]).end();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, 
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++)
    cell_its[attribute_ids[i]] = (*tile_its[attribute_ids[i]]).begin();
  cell_it_end = (*tile_its[attribute_ids[0]]).end();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, 
    Tile::const_iterator* cell_its, 
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++)
    cell_its[attribute_ids[i]] = (*tile_its[attribute_ids[i]]).begin();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, unsigned int attribute_num,
    Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<attribute_num; i++)
    cell_its[i] = (*tile_its[i]).begin();
}

inline
void QueryProcessor::initialize_tile_its(
    const StorageManager::ArrayDescriptor* ad,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator& tile_it_end) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  for(unsigned int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_.begin(ad, i);
  tile_it_end = storage_manager_.end(ad, attribute_num);
}

inline
void QueryProcessor::initialize_tile_its(
    const StorageManager::ArrayDescriptor* ad,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator& tile_it_end,
    unsigned int end_attribute_id) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  for(unsigned int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_.begin(ad, i);
  tile_it_end = storage_manager_.end(ad, end_attribute_id);
}

void QueryProcessor::join_irregular(const StorageManager::ArrayDescriptor* ad_A, 
                                    const StorageManager::ArrayDescriptor* ad_B,
                                    const ArraySchema& array_schema_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = ad_A->array_schema();
  const ArraySchema& array_schema_B = ad_B->array_schema();
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Prepare result array
  const StorageManager::ArrayDescriptor* ad_C = 
      storage_manager_.open_array(array_schema_C);

  // Create tiles 
  const Tile** tiles_A = new const Tile*[attribute_num_A+1];
  const Tile** tiles_B = new const Tile*[attribute_num_B+1];
  Tile** tiles_C = new Tile*[attribute_num_C+1];
  
  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its_A = 
      new StorageManager::const_iterator[attribute_num_A+1];
  StorageManager::const_iterator *tile_its_B = 
      new StorageManager::const_iterator[attribute_num_B+1];
  StorageManager::const_iterator tile_it_end_A;
  StorageManager::const_iterator tile_it_end_B;
  initialize_tile_its(ad_A, tile_its_A, tile_it_end_A);
  initialize_tile_its(ad_B, tile_its_B, tile_it_end_B);
  
  // Create cell iterators
  Tile::const_iterator* cell_its_A = 
      new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator* cell_its_B = 
      new Tile::const_iterator[attribute_num_B+1];
  Tile::const_iterator cell_it_end_A, cell_it_end_B;

  // Auxiliary variables storing the number of skipped tiles when joining.
  // It is used to advance only the coordinates iterator when a tile is
  // finished/skipped, and then efficiently advance the attribute iterators only 
  // when a tile joins.
  int64_t skipped_tiles_A = 0;
  int64_t skipped_tiles_B = 0;
  
  // Note that attribute cell iterators are initialized and advanced only
  // after the first join result is discovered. 
  bool attribute_cell_its_initialized_A = false;
  bool attribute_cell_its_initialized_B = false;

  // To capture the edge case where the first tiles from A and B may join
  bool coordinate_cell_its_initialized_A = false;
  bool coordinate_cell_its_initialized_B = false;

  // Initialize tiles with id 0 for C (result array)
  new_tiles(array_schema_C, 0, tiles_C); 

  // Join algorithm
  while(tile_its_A[attribute_num_A] != tile_it_end_A &&
        tile_its_B[attribute_num_B] != tile_it_end_B) {
    // Potential join result generation
    if(may_join(tile_its_A[attribute_num_A], tile_its_B[attribute_num_B])) { 
      // Update iterators in A
      if(skipped_tiles_A) {
        advance_tile_its(attribute_num_A, tile_its_A, skipped_tiles_A);
        skipped_tiles_A = 0;
        cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
        cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
        coordinate_cell_its_initialized_A = true;
        attribute_cell_its_initialized_A = false;
      } else if(!coordinate_cell_its_initialized_A) {
        cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
        cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
        coordinate_cell_its_initialized_A = true;
      }
      // Update iterators in B
      if(skipped_tiles_B) {
        advance_tile_its(attribute_num_B, tile_its_B, skipped_tiles_B);
        skipped_tiles_B = 0;
        cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
        cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
        coordinate_cell_its_initialized_B = true;
        attribute_cell_its_initialized_B = false;
      } else if(!coordinate_cell_its_initialized_B) {
        cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
        cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
        coordinate_cell_its_initialized_B = true;
      }
      // Join the tiles
      join_tiles_irregular(attribute_num_A, tile_its_A, 
                           cell_its_A, cell_it_end_A, 
                           attribute_num_B, tile_its_B, 
                           cell_its_B, cell_it_end_B,
                           ad_C, tiles_C,
                           attribute_cell_its_initialized_A, 
                           attribute_cell_its_initialized_B);
    }

    // Check which tile precedes the other in the global order
    // Note that operator '<', when the operands are from different
    // arrays, returns true if the first tile precedes the second
    // in the global order by checking their bounding coordinates.
    if(tile_its_A[attribute_num_A] < tile_its_B[attribute_num_B]) {
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
    }
    else {
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    }
  }
  
  // Send the lastly created tiles to storage manager
  store_tiles(ad_C, tiles_C);

  // Close result array
  storage_manager_.close_array(ad_C);

  // Clean up
  delete [] tiles_A;
  delete [] tiles_B;
  delete [] tiles_C;
  delete [] tile_its_A;
  delete [] tile_its_B;
  delete [] cell_its_A;
  delete [] cell_its_B;
}

void QueryProcessor::join_regular(const StorageManager::ArrayDescriptor* ad_A, 
                                  const StorageManager::ArrayDescriptor* ad_B,
                                  const ArraySchema& array_schema_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = ad_A->array_schema();
  const ArraySchema& array_schema_B = ad_B->array_schema();
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();
  uint64_t tile_id_A, tile_id_B;

  // Prepare result array
  const StorageManager::ArrayDescriptor* ad_C = 
      storage_manager_.open_array(array_schema_C);

  // Create tiles 
  const Tile** tiles_A = new const Tile*[attribute_num_A+1];
  const Tile** tiles_B = new const Tile*[attribute_num_B+1];
  Tile** tiles_C = new Tile*[attribute_num_C+1];
  
  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its_A = 
      new StorageManager::const_iterator[attribute_num_A+1];
  StorageManager::const_iterator *tile_its_B = 
      new StorageManager::const_iterator[attribute_num_B+1];
  StorageManager::const_iterator tile_it_end_A;
  StorageManager::const_iterator tile_it_end_B;
  initialize_tile_its(ad_A, tile_its_A, tile_it_end_A);
  initialize_tile_its(ad_B, tile_its_B, tile_it_end_B);
  
  // Create cell iterators
  Tile::const_iterator* cell_its_A = 
      new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator* cell_its_B = 
      new Tile::const_iterator[attribute_num_B+1];
  Tile::const_iterator cell_it_end_A, cell_it_end_B;

  // Auxiliary variables storing the number of skipped tiles when joining.
  // It is used to advance only the coordinates iterator when a tile is
  // finished/skipped, and then efficiently advance the attribute iterators only
  // when a tile joins.
  int64_t skipped_tiles_A = 0;
  int64_t skipped_tiles_B = 0;

  // Join algorithm
  while(tile_its_A[attribute_num_A] != tile_it_end_A &&
        tile_its_B[attribute_num_B] != tile_it_end_B) {
    tile_id_A = tile_its_A[attribute_num_A].tile_id();
    tile_id_B = tile_its_B[attribute_num_B].tile_id();

    // Potential join result generation
    if(tile_id_A == tile_id_B) {
      // Update tile iterators in A
      if(skipped_tiles_A) {
        advance_tile_its(attribute_num_A, tile_its_A, skipped_tiles_A);
        skipped_tiles_A = 0;
      }
      // Initialize cell iterators for A
      cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
      cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
      // Update tile iterators in B
      if(skipped_tiles_B) {
        advance_tile_its(attribute_num_B, tile_its_B, skipped_tiles_B);
        skipped_tiles_B = 0;
      }
      // Initialize cell iterators for B
      cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
      cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
 
      // Initialize tiles for C (result array)
      new_tiles(array_schema_C, tile_id_A, tiles_C);

      // Join the tiles
      join_tiles_regular(attribute_num_A, tile_its_A, cell_its_A, cell_it_end_A,
                         attribute_num_B, tile_its_B, cell_its_B, cell_it_end_B,
                         ad_C, tiles_C);

      // Send the created tiles to storage manager
      store_tiles(ad_C, tiles_C);

      // Advance both tile iterators
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    // Tile precedence in the case of regular tiles is simply determined
    // by the order of the tile ids.
    } else if(tile_id_A < tile_id_B) {
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
    } else { // tile_id_A > tile_id_B
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    }
  } 
  // Close result array
  storage_manager_.close_array(ad_C);

  // Clean up
  delete [] tiles_A;
  delete [] tiles_B;
  delete [] tiles_C;
  delete [] tile_its_A;
  delete [] tile_its_B;
  delete [] cell_its_A;
  delete [] cell_its_B;
}

void QueryProcessor::join_tiles_irregular(
    unsigned int attribute_num_A, 
    const StorageManager::const_iterator* tile_its_A, 
    Tile::const_iterator* cell_its_A,
    Tile::const_iterator& cell_it_end_A, 
    unsigned int attribute_num_B, 
    const StorageManager::const_iterator* tile_its_B, 
    Tile::const_iterator* cell_its_B,
    Tile::const_iterator& cell_it_end_B,
    const StorageManager::ArrayDescriptor* ad_C, Tile** tiles_C,
    bool& attribute_cell_its_initialized_A,
    bool& attribute_cell_its_initialized_B) const {
  // For easy reference
  const ArraySchema& array_schema_C = ad_C->array_schema();
  uint64_t capacity = array_schema_C.capacity();
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Auxiliary variables storing the number of skipped cells when joining.
  // It is used to advance only the coordinates iterator when a cell is
  // finished/skipped, and then efficiently advance the attribute iterators only 
  // when a cell joins.
  int64_t skipped_cells_A;
  int64_t skipped_cells_B;

  if(!attribute_cell_its_initialized_A) 
    skipped_cells_A = cell_its_A[attribute_num_A].pos();
  else
    skipped_cells_A = cell_its_A[attribute_num_A].pos() - cell_its_A[0].pos();

  if(!attribute_cell_its_initialized_B) 
    skipped_cells_B = cell_its_B[attribute_num_B].pos();
  else
    skipped_cells_B = cell_its_B[attribute_num_B].pos() - cell_its_B[0].pos();

  while(cell_its_A[attribute_num_A] != cell_it_end_A &&
        cell_its_B[attribute_num_B] != cell_it_end_B) {
    // If the coordinates are equal
    // Note that operator '==', when the operands correspond to different
    // tiles, returns true if the cell values pointed by the iterators
    // are equal.
    if(cell_its_A[attribute_num_A] == cell_its_B[attribute_num_B]) {      
      if(!attribute_cell_its_initialized_A) {
        initialize_cell_its(tile_its_A, attribute_num_A, cell_its_A);
        attribute_cell_its_initialized_A = true;
      } 
      if(!attribute_cell_its_initialized_B) {
        initialize_cell_its(tile_its_B, attribute_num_B, cell_its_B);
        attribute_cell_its_initialized_B = true;
      }
      if(skipped_cells_A) {
        advance_cell_its(attribute_num_A, cell_its_A, skipped_cells_A);
        skipped_cells_A = 0;
      }
      if(skipped_cells_B) {
        advance_cell_its(attribute_num_B, cell_its_B, skipped_cells_B);
        skipped_cells_B = 0;
      }
      if(tiles_C[attribute_num_C]->cell_num() == capacity) {
        uint64_t new_tile_id = tiles_C[attribute_num_C]->tile_id() + 1;
        store_tiles(ad_C, tiles_C);
        new_tiles(array_schema_C, new_tile_id, tiles_C); 
      }
      append_cell(cell_its_A, cell_its_B, tiles_C, 
                  attribute_num_A, attribute_num_B);
      advance_cell_its(attribute_num_A, cell_its_A);
      advance_cell_its(attribute_num_B, cell_its_B);
    // Otherwise check which cell iterator to advance
    } else {
      if(array_schema_C.precedes(cell_its_A[attribute_num_A],
                                 cell_its_B[attribute_num_B])) {
        ++cell_its_A[attribute_num_A];
        ++skipped_cells_A;
      } else {
        ++cell_its_B[attribute_num_B];
        ++skipped_cells_B;
      }
    }
  }
}

void QueryProcessor::join_tiles_regular(
    unsigned int attribute_num_A, 
    const StorageManager::const_iterator* tile_its_A, 
    Tile::const_iterator* cell_its_A,
    Tile::const_iterator& cell_it_end_A, 
    unsigned int attribute_num_B, 
    const StorageManager::const_iterator* tile_its_B, 
    Tile::const_iterator* cell_its_B,
    Tile::const_iterator& cell_it_end_B,
    const StorageManager::ArrayDescriptor* ad_C, Tile** tiles_C) const {
  // For easy reference
  const ArraySchema& array_schema_C = ad_C->array_schema();
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Auxiliary variables storing the number of skipped cells when joining.
  // It is used to advance only the coordinates iterator when a cell is
  // finished/skipped, and then efficiently advance the attribute iterators only 
  // when a cell joins.
  int64_t skipped_cells_A = 0;
  int64_t skipped_cells_B = 0;
  
  // Note that attribute cell iterators are initialized and advanced only
  // after the first join result is discovered. 
  bool attribute_cell_its_initialized_A = false;
  bool attribute_cell_its_initialized_B = false;

  while(cell_its_A[attribute_num_A] != cell_it_end_A &&
        cell_its_B[attribute_num_B] != cell_it_end_B) {
    // If the coordinates are equal
    // Note that operator '==', when the operands correspond to different
    // tiles, returns true if the cell values pointed by the iterators
    // are equal.
    if(cell_its_A[attribute_num_A] == cell_its_B[attribute_num_B]) {
      if(!attribute_cell_its_initialized_A) {
        initialize_cell_its(tile_its_A, attribute_num_A, cell_its_A);
        attribute_cell_its_initialized_A = true;
      } 
      if(!attribute_cell_its_initialized_B) {
        initialize_cell_its(tile_its_B, attribute_num_B, cell_its_B);
        attribute_cell_its_initialized_B = true;
      }
      if(skipped_cells_A) { 
        advance_cell_its(attribute_num_A, cell_its_A, skipped_cells_A);
        skipped_cells_A = 0;
      }
      if(skipped_cells_B) { 
        advance_cell_its(attribute_num_B, cell_its_B, skipped_cells_B);
        skipped_cells_B = 0;
      }
      append_cell(cell_its_A, cell_its_B, tiles_C, 
                  attribute_num_A, attribute_num_B);
      advance_cell_its(attribute_num_A, cell_its_A);
      advance_cell_its(attribute_num_B, cell_its_B);
    // Otherwise check which cell iterator to advance
    } else {
      if(array_schema_C.precedes(cell_its_A[attribute_num_A],
                                 cell_its_B[attribute_num_B])) {
        ++cell_its_A[attribute_num_A];
        ++skipped_cells_A;
      } else {
        ++cell_its_B[attribute_num_B];
        ++skipped_cells_B;
      }
    }
  }
}

bool QueryProcessor::may_join(
    const StorageManager::const_iterator& it_A,
    const StorageManager::const_iterator& it_B) const {
  // For easy reference
  const ArraySchema& array_schema_A = it_A.array_schema();
  const MBR& mbr_A = it_A.mbr();
  const MBR& mbr_B = it_B.mbr();

  // Check if the tile MBRs overlap
  if(array_schema_A.has_irregular_tiles() && !overlap(mbr_A, mbr_B))
    return false;

  // For easy reference 
  BoundingCoordinatesPair bounding_coordinates_A = it_A.bounding_coordinates();
  BoundingCoordinatesPair bounding_coordinates_B = it_B.bounding_coordinates();

  // Check if the cell id ranges (along the global order) intersect
  if(!overlap(bounding_coordinates_A, bounding_coordinates_B, array_schema_A))
    return false;

  return true;
}

// NOTE: It is assumed that k is small enough for O(k) coordinates to fit
// in main memory. 
void QueryProcessor::nearest_neighbors_irregular(
    const StorageManager::ArrayDescriptor* ad,
    const std::vector<double>& q,
    uint64_t k,
    const std::string& result_array_name) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);

  // Get pairs (dist, rank), sorted on dist
  // rank is a tile rank and dist is its distance to q
  std::vector<DistRank> sorted_dist_ranks = compute_sorted_dist_ranks(ad, q);

  // Compute sorted kNN coordinates of the form (rank, (pos, coord))
  // coord is the cell coordinates, rank is the rank of the tile the
  // cell belongs to, and pos is the position of the cell in the tile
  std::priority_queue<RankPosCoord> sorted_kNN_coords = 
      compute_sorted_kNN_coords(ad, q, k, sorted_dist_ranks); 
    
  // Prepare new result tiles
  uint64_t tile_id = 0; 
  new_tiles(result_array_schema, tile_id, result_tiles); 

  // Retrieve and store the actual k nearest neighbors
  int64_t current_rank = -1;
  uint64_t pos, rank;
  while(sorted_kNN_coords.size() > 0) {
    // For easy reference
    rank = sorted_kNN_coords.top().first; 
    pos = sorted_kNN_coords.top().second.first; 
    const std::vector<double>& coord = sorted_kNN_coords.top().second.second;

    // Retrieve tiles
    if(rank != current_rank) {
      current_rank = rank;
      for(unsigned int i=0; i<attribute_num; i++) 
        tiles[i] = storage_manager_.get_tile_by_rank(ad, i, rank);
    }
   
    // Store result tile if full
    if(result_tiles[attribute_num]->cell_num() == capacity) {
      store_tiles(result_ad, result_tiles);
      new_tiles(result_array_schema, ++tile_id, result_tiles); 
    } 
 
    // Append cell
    *result_tiles[attribute_num] << coord;
    for(unsigned int i=0; i<attribute_num; i++) {
      cell_its[i] = tiles[i]->begin() + pos;
      *result_tiles[i] << cell_its[i];
    }
   
    // Pop the top of the priority queue 
    sorted_kNN_coords.pop();
  }
  
  // Send the lastly created tiles to storage manager
  store_tiles(result_ad, result_tiles);

  // Close result array
  storage_manager_.close_array(result_ad);

  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
} 

// NOTE: It is assumed that k is small enough for O(k) coordinates to fit
// in main memory. 
void QueryProcessor::nearest_neighbors_regular(
    const StorageManager::ArrayDescriptor* ad,
    const std::vector<double>& q,
    uint64_t k,
    const std::string& result_array_name) const {
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  unsigned int attribute_num = array_schema.attribute_num();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);

  // Get pairs (dist, rank), sorted on dist
  // rank is a tile rank and dist is its distance to q
  std::vector<DistRank> sorted_dist_ranks = compute_sorted_dist_ranks(ad, q);

  // Compute sorted kNN coordinates of the form (rank, (pos, coord))
  // coord is the cell coordinates, rank is the rank of the tile the
  // cell belongs to, and pos is the position of the cell in the tile
  std::priority_queue<RankPosCoord> sorted_kNN_coords = 
      compute_sorted_kNN_coords(ad, q, k, sorted_dist_ranks); 
    
  // Retrieve and store the actual k nearest neighbors
  int64_t current_rank = -1;
  uint64_t pos, rank, tile_id;
  while(sorted_kNN_coords.size() > 0) {
    // For easy reference
    rank = sorted_kNN_coords.top().first; 
    pos = sorted_kNN_coords.top().second.first; 
    const std::vector<double>& coord = sorted_kNN_coords.top().second.second;

    // Retrieve tiles and create new result tiles
    if(rank != current_rank) {
      // Store previous result tiles
      if(current_rank != -1)
        store_tiles(result_ad, result_tiles);

      current_rank = rank;
      
      // Retrieve tiles
      for(unsigned int i=0; i<attribute_num; i++) 
        tiles[i] = storage_manager_.get_tile_by_rank(ad, i, rank);
  
      // Prepare new result tiles
      new_tiles(result_array_schema, tiles[0]->tile_id(), result_tiles); 
    }
   
    // Append cell
    *result_tiles[attribute_num] << coord;
    for(unsigned int i=0; i<attribute_num; i++) {
      cell_its[i] = tiles[i]->begin() + pos;
      *result_tiles[i] << cell_its[i];
    }
   
    // Pop the top of the priority queue 
    sorted_kNN_coords.pop();
  }
    
  // Send the lastly created tiles to storage manager
  store_tiles(result_ad, result_tiles);
  
  // Close result array
  storage_manager_.close_array(result_ad);

  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
} 

inline
void QueryProcessor::new_tiles(const ArraySchema& array_schema, 
                               uint64_t tile_id, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  for(unsigned int i=0; i<=attribute_num; i++)
    tiles[i] = storage_manager_.new_tile(array_schema, i, tile_id, capacity);
}

bool QueryProcessor::overlap(const MBR& mbr_A, const MBR& mbr_B) const {
  assert(mbr_A.size() == mbr_B.size());
  assert(mbr_A.size() % 2 == 0);

  // For easy rederence
  unsigned int dim_num = mbr_A.size() / 2;

  for(unsigned int i=0; i<dim_num; i++) 
    if(mbr_A[2*i+1] < mbr_B[2*i] || mbr_A[2*i] > mbr_B[2*i+1])
      return false;

  return true;
}

bool QueryProcessor::overlap(
    const BoundingCoordinatesPair& bounding_coordinates_A,
    const BoundingCoordinatesPair& bounding_coordinates_B, 
    const ArraySchema& array_schema) const {
  if(array_schema.precedes(bounding_coordinates_A.second, 
                           bounding_coordinates_B.first) ||
     array_schema.succeeds(bounding_coordinates_A.first, 
                           bounding_coordinates_B.second))
    return false;
  else
    return true;
}

inline
void QueryProcessor::set_workspace(const std::string& path) {
  workspace_ = path;
  
  // Replace '~' with the absolute path
  if(workspace_[0] == '~') {
    workspace_ = std::string(getenv("HOME")) +
                 workspace_.substr(1, workspace_.size()-1);
  }

  // Check if the input path is an existing directory 
  assert(path_exists(workspace_));
 
  workspace_ += "/QueryProcessor";
}

inline
void QueryProcessor::store_tiles(const StorageManager::ArrayDescriptor* ad,
                                 Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = ad->array_schema().attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++)
    storage_manager_.append_tile(tiles[i], ad, i);
} 

void QueryProcessor::subarray_irregular(
    const StorageManager::ArrayDescriptor* ad,
    const Tile::Range& range, const std::string& result_array_name) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);
  
  // Get the tile ids that overlap with the range
  std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
  storage_manager_.get_overlapping_tile_ids(ad, range, &overlapping_tile_ids);

  // Initialize tile iterators
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
      overlapping_tile_ids.begin();
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
      overlapping_tile_ids.end();
      
  // Create result tiles and load input array tiles 
  uint64_t tile_id = 0;
  new_tiles(result_array_schema, tile_id, result_tiles); 

  // Auxiliary variable storing the number of skipped cells when investigating
  // a tile partially overlapping the range. It is used to advance only the
  // coordinates iterator when a cell is not in range, and then efficiently
  // advance the attribute iterators only when a cell falls in the range.
  int64_t skipped_cells;

  // Iterate over all tiles
  for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
    get_tiles(ad, tile_id_it->first, tiles);
    skipped_cells = 0;

    if(tile_id_it->second) { // Full overlap
      initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 
      while(cell_its[attribute_num] != cell_it_end) {
        if(result_tiles[attribute_num]->cell_num() == capacity) {
          store_tiles(result_ad, result_tiles);
          new_tiles(result_array_schema, ++tile_id, result_tiles); 
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      cell_its[attribute_num] = tiles[attribute_num]->begin();
      cell_it_end = tiles[attribute_num]->end();
      bool attribute_cell_its_initialized = false;
      while(cell_its[attribute_num] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range)) {
          if(result_tiles[attribute_num]->cell_num() == capacity) {
            store_tiles(result_ad, result_tiles);
            new_tiles(result_array_schema, ++tile_id, result_tiles); 
          }
          if(!attribute_cell_its_initialized) { 
            initialize_cell_its(tiles, attribute_num, cell_its);
            attribute_cell_its_initialized = true;
          }
          if(skipped_cells) {
            advance_cell_its(attribute_num, cell_its, skipped_cells);
            skipped_cells = 0;
          }
          append_cell(cell_its, result_tiles, attribute_num);
          advance_cell_its(attribute_num, cell_its);
        } else { // Advance only the coordinates cell iterator
          skipped_cells++;
          ++cell_its[attribute_num];
        }
      }
    }
  } 

  // Send the lastly created tiles to storage manager
  store_tiles(result_ad, result_tiles);
  
  // Close result array
  storage_manager_.close_array(result_ad);

  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

void QueryProcessor::subarray_regular(
    const StorageManager::ArrayDescriptor* ad,
    const Tile::Range& range, const std::string& result_array_name) const { 
  // For easy reference
  const ArraySchema& array_schema = ad->array_schema();
  unsigned int attribute_num = array_schema.attribute_num();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Prepare result array
  ArraySchema result_array_schema = array_schema.clone(result_array_name);
  const StorageManager::ArrayDescriptor* result_ad = 
      storage_manager_.open_array(result_array_schema);
    
  // Get the tile ids that overlap with the range
  std::vector<std::pair<uint64_t, bool> > overlapping_tile_ids;
  storage_manager_.get_overlapping_tile_ids(ad, range, &overlapping_tile_ids);

  // Initialize tile iterators
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it = 
      overlapping_tile_ids.begin();
  std::vector<std::pair<uint64_t, bool> >::const_iterator tile_id_it_end =
      overlapping_tile_ids.end();

  // Auxiliary variable storing the number of skipped cells when investigating
  // a tile partially overlapping the range. It is used to advance only the
  // coordinates iterator when a cell is not in range, and then efficiently
  // advance the attribute iterators only when a cell falls in the range.
  int64_t skipped_cells;

  // Iterate over all overlapping tiles
  for(; tile_id_it != tile_id_it_end; ++tile_id_it) {
    // Create result tiles and load input array tiles 
    new_tiles(result_array_schema, tile_id_it->first, result_tiles); 
    get_tiles(ad, tile_id_it->first, tiles); 
    skipped_cells = 0;
 
    if(tile_id_it->second) { // Full overlap
      initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 
      while(cell_its[attribute_num] != cell_it_end) {
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      cell_its[attribute_num] = tiles[attribute_num]->begin();
      cell_it_end = tiles[attribute_num]->end();
      bool attribute_cell_its_initialized = false;
      while(cell_its[attribute_num] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range)) {
          if(!attribute_cell_its_initialized) { 
            initialize_cell_its(tiles, attribute_num, cell_its);
            attribute_cell_its_initialized = true;
          }
          if(skipped_cells) {
            advance_cell_its(attribute_num, cell_its, skipped_cells);
            skipped_cells = 0;
          }
          append_cell(cell_its, result_tiles, attribute_num);
          advance_cell_its(attribute_num, cell_its);
        } else { // Advance only the coordinates cell iterator
          ++skipped_cells;
          ++cell_its[attribute_num];
        }
      }
    }
      
    // Send new tiles to storage manager
    store_tiles(result_ad, result_tiles);
  } 
  
  // Close result array
  storage_manager_.close_array(result_ad);

  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

