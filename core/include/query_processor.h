/**
 * @file   query_processor.h
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
 * This file defines class QueryProcessor. It also defines 
 * QueryProcessorException, which is thrown by QueryProcessor.
 */

#ifndef QUERY_PROCESSOR_H
#define QUERY_PROCESSOR_H

#include "array_schema.h"
#include "csv_file.h"
#include "storage_manager.h"
#include "expression_tree.h"

/** 
 * This class implements the query processor module, which is responsible
 * for processing the various queries. 
 */
class QueryProcessor {
 public:
  // CONSTRUCTORS AND DESTRUCTORS
  /** 
   * Simple constructor. The storage manager is the module the query processor
   * interefaces with.
   */
  QueryProcessor(StorageManager* storage_manager);

  // QUERY FUNCTIONS
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   */
  void export_to_csv(const std::string& array_name,
                     const std::string& filename) const;
  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array.
   * The function is templated on the coordinates type. 
   * It takes as input an array descriptor instead of its name.
   */
  template<class T>
  void export_to_csv(int ad, const std::string& filename) const;

  /** 
   * Exports an array to a CSV file. Each line in the CSV file represents
   * a logical cell comprised of coordinates and attribute values. The 
   * coordinates are written first, and then the attribute values,
   * following the order as defined in the schema of the array. This function
   * operates on multiple array fragments.
   */
/* TODO
  void export_to_csv(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const std::string& filename) const;
*/
  /** 
   * A filter query creates a new array from the input array descriptor, 
   * containing only the cells whose attribute values satisfy the input 
   * expression (given in the form of an expression tree). 
   * The new array will have the input result name.
   */
/* TODO
  void filter(
      const StorageManager::FragmentDescriptor* fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * A filter query creates a new array from the input array descriptor, 
   * containing only the cells whose attribute values satisfy the input 
   * expression (given in the form of an expression tree). 
   * The new array will have the input result name. This function
   * operates on multiple array fragments.
   */
/* TODO
  void filter(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Joins the two input arrays (say, A and B). The result contains a cell only
   * if both the corresponding cells in A and B are non-empty. The input arrays
   * must be join-compatible (see ArraySchema::join_compatible). Moreover,
   * see ArraySchema::create_join_result_schema to see the schema of the
   * output array.
   */
/* TODO
  void join(const StorageManager::FragmentDescriptor* fd_A,
            const StorageManager::FragmentDescriptor* fd_B,
            const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Joins the two input arrays (say, A and B). The result contains a cell only
   * if both the corresponding cells in A and B are non-empty. The input arrays
   * must be join-compatible (see ArraySchema::join_compatible). Moreover,
   * see ArraySchema::create_join_result_schema to see the schema of the
   * output array. This function operates on multiple array fragments.
   */

/* TODO
  void join(const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
            const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
            const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Returns the k nearest neighbors from query point q. The results (along with
   * all their attribute values) are stored in a new array. The distance metric
   * used to calculate proximity is the Euclidean distance.
   */
/* TODO
  void nearest_neighbors(
      const StorageManager::FragmentDescriptor* fd,
      const std::vector<double>& q,
      uint64_t k,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Copies the coordinates of the non-empty cells of the input array
   * falling inside the input range into the coords buffer, and their
   * corresponding values on the input attribute into buffer values,
   * setting properly the buffer sizes in bytes. 
   */
/* TODO
  void read(const StorageManager::FragmentDescriptor* fd,
            int attribute_id, const void* range,
            void*& coords, size_t& coords_size,
            void*& values, size_t& values_size) const;
*/
  /** 
   * Copies the coordinates of the non-empty cells of the input array
   * falling inside the input range into the coords buffer, and their
   * corresponding values on the input attribute into buffer values,
   * setting properly the buffer sizes in bytes. 
   */
/* TODO
  template<class T>
  void read_irregular(const StorageManager::FragmentDescriptor* fd,
                      int attribute_id, const T* range,
                      void*& coords, size_t& coords_size,
                      void*& values, size_t& values_size) const;
*/
  /** 
   * Retiles an array based on the inputs. If tile extents are provided
   * (i) in the case of regular tiles, if the extents differ from those in the
   * array schema, retiling occurs, (ii) in the case of irregular tiles, the
   * array is retiled so that it has regular tiles. If tile extents are not
   * provided for the case of regular tiles, the array is retiled to one with
   * irregular tiles. If order is provided (different from the existing order)
   * retiling occurs. If a capacity is provided, (i) in the case of regular
   * tiles it has no effect (only the schema changes), (ii) in the case of
   * irregular tiles, only the book-keeping structures and array schema
   * are altered to accommodate the change.
   */
/* TODO
  void retile(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      uint64_t capacity,
      ArraySchema::Order order,
      const std::vector<double>& tile_extents) const;
*/
  /** Retile for the case of irregular tiles. */
/* TODO
  void retile_irregular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      uint64_t capacity,
      ArraySchema::Order order,
      const std::vector<double>& tile_extents) const;
*/
  /** Retile for the case of irregular tiles, where the capacity changes. */
/* TODO
  void retile_irregular_capacity(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      uint64_t capacity) const;
*/
  /** Retile for the case of regular tiles. */
/* TODO
  void retile_regular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      uint64_t capacity,
      ArraySchema::Order order,
      const std::vector<double>& tile_extents) const;
*/
  /** 
   * A subarray query creates a new array from the input array descriptor, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name.
   */
/* TODO
  void subarray(const StorageManager::FragmentDescriptor* fd,
                const Tile::Range& range,
                const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Retrieves the cells contained in the input range. Assigns their
   * coordinates to c_iovec, and their values on the input attribute
   * to v_iovec.
   */
/* TODO
  void subarray(const StorageManager::FragmentDescriptor* fd,
                const Tile::Range& range,
                unsigned int attribute_id,
                struct iovec& c_iovec,
                struct iovec& v_iovec) const;
*/
  /** 
   * A subarray query creates a new array from the input array descriptor, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name. This function
   * operates on multiple array fragments.
   */
/* TODO
  void subarray(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const Tile::Range& range,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * A subarray query creates a new array from the input array, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name. 
   */
  void subarray(const std::string& array_name, 
                const void* range,
                const std::string& result_array_name) const;
  /** 
   * A subarray query creates a new array from the input array, 
   * containing only the cells whose coordinates fall into the input range. 
   * The new array will have the input result name. 
   * The function is templated on the coordinates type. 
   * It takes as input array descriptors instead of names.
   */
  template<class T>
  void subarray(int ad, const T* range, int result_ad) const;

 private:
  // PRIVATE ATTRIBUTES
  /** The StorageManager object the QueryProcessor will be interfacing with. */
  StorageManager* storage_manager_;

  // PRIVATE METHODS
  /** Advances all the cell iterators by 1. */
/* TODO
  void advance_cell_its(int attribute_num,
                        Tile::const_cell_iterator* cell_its) const;
*/
 /** 
   * Advances by one only the cell iterators of the attributes whose id is in 
   * attribute_ids. 
   */
/* TODO
  void advance_cell_its(Tile::const_iterator* cell_its,
                        const std::vector<unsigned int>& attribute_ids) const;
*/
  /** Advances only the attribute cell iterators by step. */
/* TODO
  void advance_cell_its(unsigned int attribute_num,
                        Tile::const_iterator* cell_its,
                        int64_t step) const; 
*/
  /** 
   * Advances by step only the cell iterators of the attributes whose id is in 
   * attribute_ids. 
   */
/* TODO
  void advance_cell_its(Tile::const_iterator* cell_its,
                        const std::vector<unsigned int>& attribute_ids,
                        int64_t step) const; 
*/
  /** 
   * Advances all the cell iterators by 1. If the cell iterators are equal to
   * the end iterator, the tile iterators are advanced. If the tile iterators
   * are not equal to the end tile iterator, then new cell iterators are
   * initialized.
   */
/* TODO
  void advance_cell_tile_its(unsigned int attribute_num,
                             Tile::const_iterator* cell_its, 
                             Tile::const_iterator& cell_it_end,
                             StorageManager::const_iterator* tile_its,
                             StorageManager::const_iterator& tile_it_end) const;
*/
  /** 
   * Advances the cell iterators by 1, focusing only on those described by
   * attribute_ids, plus the extra coordinates attribute. If the cell iterators
   * are equal to the end iterator, the corresponding tile iterators are
   * advanced. If the tile iterators are not equal to the end tile iterator,
   * then new cell iterators are initialized. The last three arguments are
   * updated based on what happens to the iterators:
   *
   * If tile iterators are advanced, skipped_tiles is incremented, skipped_cells
   * is set to 0, and non_cell_iterators_initialized is set to false. This is
   * because we changed tile in a subset of attributes. The above three 
   * changes are essential so that we keep track how to advance the cell and
   * tile iterators of the skipped attributes when the time comes to synchronize
   * them with the tile and cell iterators advanced in this function.
   *
   * If the tile iterators are not advanced, we simply increment skipped_cells.
   */
/* TODO
  void advance_cell_tile_its(
      unsigned int attribute_num,
      Tile::const_iterator* cell_its, 
      Tile::const_iterator& cell_it_end,
      StorageManager::const_iterator* tile_its,
      StorageManager::const_iterator& tile_it_end,
      const std::vector<unsigned int>& attribute_ids, 
      int64_t& skipped_tiles,
      uint64_t& skipped_cells,
      bool& non_cell_its_initialized) const;
*/
  /** 
   * Advances the coordinate cell iterators by 1. If these cell iterators
   * are equal to the end iterator, the corresponding tile iterators are
   * advanced (retrieving the new tiles from the storage manager). If the tile
   * iterators are not equal to the end tile iterator, then new cell iterators
   * are initialized. The skipped_cells argument is updated based on what
   * happens to the iterators:
   *
   * If tile iterators are advanced, skipped_cells is set to 0. If the tile
   * iterators are not advanced, we simply increment skipped_cells.
   */
/* TODO
  void advance_cell_tile_its(
      const StorageManager::FragmentDescriptor* fd,
      unsigned int attribute_num,
      Tile::const_iterator* cell_its, 
      Tile::const_iterator& cell_it_end,
      RankOverlapVector::const_iterator& tile_rank_it,
      RankOverlapVector::const_iterator& tile_rank_it_end,
      unsigned int fragment_num,
      const Tile** tiles,
      uint64_t& skipped_cells) const;
*/
  /** 
   * Advances coordinate cell iterators and potentially coordinate tile
   * iterators. This function is used in joins for multiple fragments.
   */
/* TODO
  void advance_cell_tile_its(
      Tile::const_iterator& cell_it, 
      const Tile::const_iterator& cell_it_end, 
      StorageManager::const_iterator& tile_it,
      uint64_t& skipped_tiles,
      uint64_t& skipped_cells,
      bool& attribute_cell_its_initialized,
      bool& coordinate_cell_its_initialized) const;
*/
  /** Advances all the tile iterators by 1. */
/* TODO
  void advance_tile_its(int attribute_num,
                        StorageManager::const_tile_iterator* tile_its) const; 
*/
  /** Advances only the attribute tile iterators by step. */
/* TODO
  void advance_tile_its(unsigned int attribute_num,
                        StorageManager::const_iterator* tile_its,
                        int64_t step) const; 
*/
  /** 
   * Advances by one only the attribute tile iterators whose ids are in the 
   * last argument. 
   */
/* TODO
  void advance_tile_its(StorageManager::const_iterator* tile_its,
                        const std::vector<unsigned int>& attribute_ids) const;
*/
  /** 
   * Advances by step only the attribute tile iterators whose ids are in the
   * last argument. 
   */
/* TODO
  void advance_tile_its(StorageManager::const_iterator* tile_its,
                        const std::vector<unsigned int>& attribute_ids,
                        int64_t step) const; 
*/
  /** 
   * Appends a logical cell of an array (comprised of attribute values and 
   * coordinates held in the input cell iterators) into
   * another array (in the corresponding tiles held in input variable 'tiles').
   */
/* TODO
  void append_cell(const Tile::const_iterator* cell_its,
                   Tile** tiles,
                   unsigned int attribute_num) const;
*/
  /** 
   * Appends a logical cell to an array C that is the result of joining
   * cells from arrays A and B (see QueryProcessor::join).
   */
/* TODO
  void append_cell(const Tile::const_iterator* cell_its_A,
                   const Tile::const_iterator* cell_its_B,
                   Tile** tiles_C,
                   unsigned int attribute_num_A,
                   unsigned int attribute_num_B) const;
*/
  /** 
   * Returns true if the result of the expression is true on the values of the
   * attributes whose id is in the attribute_ids.
   */
/* TODO
  bool cell_satisfies_expression(const ArraySchema& array_schema,
                                 const Tile::const_iterator* cell_its,
                                 const std::vector<unsigned int>& attribute_ids,
                                 const ExpressionTree* expression) const;
*/
  /** 
   * Converts a logical cell of an array into a CSV line. The cell is 
   * comprised of all coordinates and attribute values, which are contained 
   * in the input array of cell iterators.
   */
  CSVLine cell_to_csv_line(const void* cell, const ArraySchema* array_schema) const;
  /** 
   * Returns true if the input (cell or tile) iterators point to the same
   * coordinates along the global order. Note that for the current tiles,
   * the corresponding coordinate cell iterators may not be initialized.
   * In this case, the check is performed on their first bounding
   * coordinates.
   */
/* TODO
  bool coincides(
    const StorageManager::const_iterator& tile_it_A,
    const Tile::const_iterator& cell_it_A,
    bool coordinate_cell_its_initialized_A,
    const StorageManager::const_iterator& tile_it_B,
    const Tile::const_iterator& cell_it_B,
    bool coordinate_cell_its_initialized_B,
    const ArraySchema& array_schema) const;
*/
  /** 
   * Returns a vector of pairs (dist, rank), sorted on dist, where rank is the
   * rank of a tile (indicating if it was appended first, second, etc., in the 
   * array, and dist is the (Euclidean) distance of its MBR from q. The rank
   * is useful for retrieving later each tile from the storage manager.
   */
/* TODO
  std::vector<DistRank> compute_sorted_dist_ranks(
      const StorageManager::FragmentDescriptor* fd,
      const std::vector<double>& q) const;
*/
  /** 
   * Returns a vector with k tuples of the form (rank, (pos, coord)).
   * Each tuple corresponds to the coordinates of one of the k nearest
   * cells in a nearest neighbor query (see QueryProcessor::nearest_neighbors).
   *
   * coord: the coordinates of the cell.
   *
   * rank: the rank of the tile this cell belongs to.
   *
   * pos: the position of the cell in the tile.
   *
   * q is the query point for the nearest neighbor search.
   *
   * k is the number of nearest neighbors to be found.
   * 
   * sorted_dist_ranks contains pairs of the form (dist, rank), where rank
   * is a tile rank and dist is its distance to the query q.
   */
/* TODO
  std::vector<RankPosCoord> compute_sorted_kNN_coords(
      const StorageManager::FragmentDescriptor* fd,
      const std::vector<double>& q,
      uint64_t k,
      const std::vector<DistRank>& sorted_dist_ranks) const;
*/
  /** 
   * Implementation of QueryProcessor::filter for the case of regular tiles. 
   * This function operators on multiple array fragments.
   */
/* TODO
  void filter_irregular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Implementation of QueryProcessor::filter for the case of irregular tiles.
   */
/* TODO
  void filter_irregular(
      const StorageManager::FragmentDescriptor* fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Implementation of QueryProcessor::filter for the case of irregular tiles.
   * This function operators on multiple array fragments.
   */
/* TODO
  void filter_regular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Implementation of QueryProcessor::filter for the case of regular tiles. 
   */
/* TODO
  void filter_regular(
      const StorageManager::FragmentDescriptor* fd,
      const ExpressionTree* expression,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /**
   * This function focuses on the case of irregular tiles.
   * Returns the indexes of the fragments (from A and B, respectively) that
   * produce a join result (these are the last two arguments passed by
   * reference). During its process, it properly updates the
   * coordinate tile and cell iterators, as well as some auxiliary
   * variables (e.g., skipped_tiles_A) to allow later for synchronization
   * of the attribute tile and cell iterators. In other words, it keeps
   * some state to achieve efficiency. 
   */
/* TODO
  void get_next_join_fragment_indexes_irregular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
      StorageManager::const_iterator** tile_its_A,
      StorageManager::const_iterator* tile_it_end_A,
      Tile::const_iterator** cell_its_A,
      Tile::const_iterator* cell_it_end_A,
      uint64_t* skipped_tiles_A, uint64_t* skipped_cells_A,
      bool* attribute_cell_its_initialized_A,
      bool* coordinate_cell_its_initialized_A,
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
      StorageManager::const_iterator** tile_its_B,
      StorageManager::const_iterator* tile_it_end_B,
      Tile::const_iterator** cell_its_B,
      Tile::const_iterator* cell_it_end_B,
      uint64_t* skipped_tiles_B, uint64_t* skipped_cells_B,
      bool* attribute_cell_its_initialized_B, 
      bool* coordinate_cell_its_initialized_B,
      bool& fragment_indexes_initialized,
      int& next_fragment_index_A, int& next_fragment_index_B) const;
*/
  /**
   * This function focuses on the case of regular tiles.
   * Returns the indexes of the fragments (from A and B, respectively) that
   * produce a join result (these are the last two arguments passed by
   * reference). During its process, it properly updates the
   * coordinate tile and cell iterators, as well as some auxiliary
   * variables (e.g., skipped_tiles_A) to allow later for synchronization
   * of the attribute tile and cell iterators. In other words, it keeps
   * some state to achieve efficiency. 
   */
/* TODO
  void get_next_join_fragment_indexes_regular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
      StorageManager::const_iterator** tile_its_A,
      StorageManager::const_iterator* tile_it_end_A,
      Tile::const_iterator** cell_its_A,
      Tile::const_iterator* cell_it_end_A,
      uint64_t* skipped_tiles_A, uint64_t* skipped_cells_A,
      bool* attribute_cell_its_initialized_A,
      bool* coordinate_cell_its_initialized_A,
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
      StorageManager::const_iterator** tile_its_B,
      StorageManager::const_iterator* tile_it_end_B,
      Tile::const_iterator** cell_its_B,
      Tile::const_iterator* cell_it_end_B,
      uint64_t* skipped_tiles_B, uint64_t* skipped_cells_B,
      bool* attribute_cell_its_initialized_B, 
      bool* coordinate_cell_its_initialized_B,
      bool& fragment_indexes_initialized,
      int& next_fragment_index_A, int& next_fragment_index_B) const;
*/
  /** 
   * Returns the index of the fragment from which we will get the next
   * cell, based on the global order.
   */
/* TODO
  int get_next_fragment_index(StorageManager::const_iterator** tile_its,
                              StorageManager::const_iterator* tile_it_end,
                              unsigned int fragment_num,
                              Tile::const_iterator** cell_its,
                              Tile::const_iterator* cell_it_end,
                              const ArraySchema& array_schema) const;
*/
  /** 
   * Returns the index of the fragment from which we will get the next cell, 
   * based on the global order. During retrieving the next cell, if it needs to
   * advance cell and tile iterators, it only focuses on those described in
   * attribute_ids, plus the extra coordinates attribute. 
   *
   * Since this function may advance cell/tile iterators, we may need to update
   * some information (namely, skipped_tiles, skipped_cells, and
   * non_cell_its_initialized), so that we know how to advance the cell and 
   * tile iterators not being advanced by this function later in another 
   * function (so that the cells across all attributes are synchronized).
   */
/* TODO
  int get_next_fragment_index(
      StorageManager::const_iterator** tile_its,
      StorageManager::const_iterator* tile_it_end,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      const std::vector<unsigned int>& attribute_ids,
      int64_t* skipped_tiles,
      uint64_t* skipped_cells,
      bool* non_cell_its_initialized,
      const ArraySchema& array_schema) const;
*/
  /** 
   * Returns the index of the fragment from which we will get the next cell in
   * QueryProcessor::subarray for multiple array fragments, based on the global
   * order. During retrieving the next cell, it may advance some coordinate cell
   * iterators. 
   *
   * Since this function may advance cell iterators, we may need to update
   * some information (namely, skipped_cells), so that we know how to advance
   * the attribute cell iterators not being advanced by this function later in
   * another function (so that the cells across all attributes are eventually
   * synchronized).
   */
/* TODO
  int get_next_fragment_index(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      RankOverlapVector::const_iterator* tile_rank_it,
      RankOverlapVector::const_iterator* tile_rank_it_end,
      const Tile*** tiles,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      uint64_t* skipped_cells,
      const ArraySchema& array_schema) const;
*/
  /** 
   * Returns the index of the fragment from which we will get the next cell in
   * QueryProcessor::join for multiple array fragments, based on the global
   * order. During retrieving the next cell, it may advance some coordinate cell
   * iterators. 
   */
/* TODO
  int get_next_fragment_index(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      StorageManager::const_iterator** tile_its,
      StorageManager::const_iterator* tile_it_end,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      uint64_t* skipped_tiles, uint64_t* skipped_cells,
      bool* attribute_cell_its_initialized,
      bool* coordinate_cell_its_initialized) const;
*/
  /** 
   * Gets from the storage manager all the (attribute and coordinate) tiles
   * of the input array having the input id. 
   */
/* TODO
  void get_tiles(const StorageManager::FragmentDescriptor* fd,
                 uint64_t tile_id, const Tile** tiles) const;
*/
  /** 
   * Gets from the storage manager all the (attribute and coordinate) tiles
   * of the input array having the input rank. 
   */
/* TODO
  void get_tiles_by_rank(const StorageManager::FragmentDescriptor* fd,
                 uint64_t tile_rank, const Tile** tiles) const;
*/
  /** Initializes cell iterators. */
/* TODO
  void initialize_cell_its(const Tile** tiles,
                           int attribute_num,
                           Tile::const_cell_iterator* cell_its, 
                           Tile::const_cell_iterator& cell_it_end) const; 
*/
  /** Initializes only the attribute cell iterators. */
/* TODO
  void initialize_cell_its(const Tile** tiles,
                           int attribute_num,
                           Tile::const_cell_iterator* cell_its) const; 
*/
  /** Initializes cell iterators. */
/* TODO
  void initialize_cell_its(const StorageManager::const_tile_iterator* tile_its,
                           int attribute_num,
                           Tile::const_cell_iterator* cell_its, 
                           Tile::const_cell_iterator& cell_it_end) const; 
*/
  /** Initializes the cell iterators described in attribute_ids. */
/* TODO
  void initialize_cell_its(
      const StorageManager::const_iterator* tile_its,
      Tile::const_iterator* cell_its, 
      Tile::const_iterator& cell_it_end,
      const std::vector<unsigned int>& attribute_ids) const;
*/
  /** Initializes the cell iterators described in attribute_ids. */
/* TODO
  void initialize_cell_its(
      const StorageManager::const_iterator* tile_its,
      Tile::const_iterator* cell_its, 
      const std::vector<unsigned int>& attribute_ids) const;
*/
  /** Initializes only the attribute cell iterators. */
/* TODO
  void initialize_cell_its(const StorageManager::const_iterator* tile_its,
                           unsigned int attribute_num,
                           Tile::const_iterator* cell_its) const; 
*/
  /** Initializes tile iterators. */
/* TODO
  void initialize_tile_its(
      const StorageManager::FragmentDescriptor* fd,
      StorageManager::const_tile_iterator* tile_its,
      StorageManager::const_tile_iterator& tile_it_end) const;
*/
  /** 
   * Initializes tile iterators. The last argument determines which attribute
   * the end tile iterator will correspond to.
   */
/* TODO
  void initialize_tile_its(const StorageManager::FragmentDescriptor* fd,
                           StorageManager::const_iterator* tile_its,
                           StorageManager::const_iterator& tile_it_end,
                           unsigned int end_attribute_id) const;
*/
  /**
   * Returns true if the cell represents a deletion, i.e., when all its
   * attribute values are NULL.
   */
/* TODO
  bool is_null(const Tile::const_iterator& cell_it) const;
*/
  /** Implements QueryProcessor::join for arrays with irregular tiles. */
/* TODO
  void join_irregular(
      const StorageManager::FragmentDescriptor* fd_A,
      const StorageManager::FragmentDescriptor* fd_B,
      const StorageManager::FragmentDescriptor* fd_C) const;
*/
  /** 
   * Implements QueryProcessor::join for arrays with irregular tiles. This
   * function operates on multiple fragments.
   */
/* TODO
  void join_irregular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
      const StorageManager::FragmentDescriptor* fd_C) const;
*/
  /** Implements QueryProcessor::join for arrays with regular tiles. */
/* TODO
  void join_regular(
      const StorageManager::FragmentDescriptor* fd_A,
      const StorageManager::FragmentDescriptor* fd_B,
      const StorageManager::FragmentDescriptor* fd_C) const;
*/
  /** 
   * Implements QueryProcessor::join for arrays with regular tiles. This
   * function operates on multiple fragments.
   */
/* TODO
  void join_regular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
      const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
      const StorageManager::FragmentDescriptor* fd_C) const;
*/
  /** 
   * Joins two irregular tiles (from A and B respectively) and stores 
   * the result in the tiles of C. 
   */
/* TODO
  void join_tiles_irregular(
      unsigned int attribute_num_A, 
      const StorageManager::const_iterator* tile_its_A,
      Tile::const_iterator* cell_its_A,
      Tile::const_iterator& cell_it_end_A, 
      unsigned int attribute_num_B, 
      const StorageManager::const_iterator* tile_its_B,
      Tile::const_iterator* cell_its_B,
      Tile::const_iterator& cell_it_end_B,
      const StorageManager::FragmentDescriptor* fd_C, Tile** tiles_C,
      bool& attribute_cell_its_initialized_A,
      bool& attribute_cell_its_initialized_B) const;
*/
  /** 
   * Joins two regular tiles (from A and B respectively) and stores 
   * the result in the tiles of C. 
   */
/* TODO
  void join_tiles_regular(
      unsigned int attribute_num_A, 
      const StorageManager::const_iterator* tile_its_A,
      Tile::const_iterator* cell_its_A,
      Tile::const_iterator& cell_it_end_A, 
      unsigned int attribute_num_B, 
      const StorageManager::const_iterator* tile_its_B,
      Tile::const_iterator* cell_its_B,
      Tile::const_iterator& cell_it_end_B,
      const StorageManager::FragmentDescriptor* fd_C, Tile** tiles_C) const;
*/
  /** Returns true if the input tiles may produce join results. */
/* TODO
  bool may_join(const StorageManager::const_iterator& it_A, 
                const StorageManager::const_iterator& it_B) const; 
*/
  /** 
   * Implementation of QueryProcessor::nearest_neighbors for the case of 
   * irregular tiles.
   */
/* TODO
  void nearest_neighbors_irregular(
      const StorageManager::FragmentDescriptor* fd,
      const std::vector<double>& q,
      uint64_t k,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Implementation of QueryProcessor::nearest_neighbors for the case of 
   * regular tiles.
   */
/* TODO
  void nearest_neighbors_regular(
      const StorageManager::FragmentDescriptor* fd,
      const std::vector<double>& q,
      uint64_t k,
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Creates an array of Tile objects with the input tile id based on the input 
   * array schema. 
   */
/* TODO
  void new_tiles(const ArraySchema& array_schema,
                 uint64_t tile_id, Tile** tiles) const;
*/
  /** Returns true if the input MBRs overlap. */
/* TODO
  bool overlap(const MBR& mbr_A, const MBR& mbr_B) const;
*/
  /** 
   * Returns true if the cell id ranges along the global order (derived from 
   * the input bounding coordinates and array schema) intersect. 
   */
/* TODO
  bool overlap(const BoundingCoordinatesPair& bounding_coordinates_A, 
               const BoundingCoordinatesPair& bounding_coordinates_B,
               const ArraySchema& array_schema) const;
*/
  /** Returns the squared Euclidean distance between a point q and an MBR. */
/* TODO
  double point_to_mbr_distance(const std::vector<double>& q,
                          const std::vector<double>& mbr) const;
*/
  /** Returns the squared Euclidean distance between points q and p. */
/* TODO
  double point_to_point_distance(const std::vector<double>& q,
                                 const std::vector<double>& p) const;
*/
  /** 
   * Returns true if the first input (cell or tile) iterators precede
   * the second along the global order. Note that for the current tiles,
   * the corresponding coordinate cell iterators may not be initialized.
   * In this case, the check is performed on their first bounding
   * coordinates.
   */
/* TODO
  bool precedes(
    const StorageManager::const_iterator& tile_it_A,
    const Tile::const_iterator& cell_it_A,
    bool coordinate_cell_its_initialized_A,
    const StorageManager::const_iterator& tile_it_B,
    const Tile::const_iterator& cell_it_B,
    bool coordinate_cell_its_initialized_B,
    const ArraySchema& array_schema) const;
*/
  /** Sends the input tiles to the storage manager. */
/* TODO
  void store_tiles(const StorageManager::FragmentDescriptor* fd, 
                   Tile** tiles) const;
*/
  /** Implements QueryProcessor::subarray for arrays with irregular tiles. */
/* TODO
  void subarray_irregular(
      const StorageManager::FragmentDescriptor* fd,
      const Tile::Range& range, 
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** Implements QueryProcessor::subarray for arrays with irregular tiles. */
/* TODO
  void subarray_irregular(
      const StorageManager::FragmentDescriptor* fd,
      const Tile::Range& range, 
      unsigned int attribute_id,
      struct iovec& c_iovec,
      struct iovec& v_iovec) const;
*/
  /** 
   * Implements QueryProcessor::subarray for arrays with irregular tiles. This
   * function operates on multiple array fragments.
   */
/* TODO
  void subarray_irregular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const Tile::Range& range, 
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** Implements QueryProcessor::subarray for arrays with regular tiles. */
/* TODO
  void subarray_regular(
      const StorageManager::FragmentDescriptor* fd,
      const Tile::Range& range, 
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
  /** 
   * Implements QueryProcessor::subarray for arrays with regular tiles. This
   * function operates on multiple array fragments.
   */
/* TODO
  void subarray_regular(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      const Tile::Range& range, 
      const StorageManager::FragmentDescriptor* result_fd) const;
*/
};

/** This exception is thrown by QueryProcessor. */
class QueryProcessorException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  QueryProcessorException(const std::string& msg) 
      : msg_(msg) {}
  /** Empty destructor. */
  ~QueryProcessorException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};

#endif
