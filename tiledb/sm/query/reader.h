/**
 * @file   reader.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file defines class Reader.
 */

#ifndef TILEDB_READER_H
#define TILEDB_READER_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/result_space_tile.h"
#include "tiledb/sm/query/types.h"
#include "tiledb/sm/query/write_cell_slab_iter.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"
#include "tiledb/sm/tile/tile.h"

#include <future>
#include <list>
#include <memory>

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes read queries. */
class Reader {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** The state for a read query. */
  struct ReadState {
    /**
     * True if the query led to a result that does not fit in
     * the user buffers.
     */
    bool overflowed_ = false;
    /** The subarray partitioner. */
    SubarrayPartitioner partitioner_;
    /**
     * ``true`` if the next partition cannot be retrieved from the
     * partitioner, because it reaches a partition that is unsplittable.
     */
    bool unsplittable_ = false;
    /** True if the reader has been initialized. */
    bool initialized_ = false;

    /** ``true`` if there are no more partitions. */
    bool done() const {
      return partitioner_.done();
    }

    /** Retrieves the next partition from the partitioner. */
    Status next() {
      return partitioner_.next(&unsplittable_);
    }

    /**
     * Splits the current partition and updates the state, retrieving
     * a new current partition. This function is typically called
     * by the reader when the current partition was estimated to fit
     * the results, but that was not eventually true.
     */
    template <class T>
    Status split_current() {
      return partitioner_.split_current<T>(&unsplittable_);
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array_schema The array schema.
   * @param fragment_metadata The fragment metadata.
   */
  Reader();

  /** Destructor. */
  ~Reader();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the array. */
  const Array* array() const;

  /**
   * Adds a range to the (read/write) query on the input dimension,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  Status add_range(
      unsigned dim_idx, const void* start, const void* end, const void* stride);

  /** Retrieves the number of ranges of the subarray for the given dimension. */
  Status get_range_num(unsigned dim_idx, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension in the form (start, end, stride).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @param stride The range stride to retrieve.
   * @return Status
   */
  Status get_range(
      unsigned dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride) const;

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute.
   */
  Status get_est_result_size(const char* attr_name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute.
   */
  Status get_est_result_size(
      const char* attr_name, uint64_t* size_off, uint64_t* size_val);

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /**
   * Return list of attribtues for query
   * @return vector of attributes for query
   */
  std::vector<std::string> attributes() const;

  /** Fetch QueryBuffer for the input attribute/dimension. */
  QueryBuffer buffer(const std::string& name) const;

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /**
   * Retrieves the buffer of a fixed-sized attribute.
   *
   * @param attribute The buffer attribute.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const std::string& attribute,
      void** buffer,
      uint64_t** buffer_size) const;

  /**
   * Retrieves the offsets and values buffers of a var-sized attribute.
   *
   * @param attribute The buffer attribute.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   * retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const std::string& attribute,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size) const;

  /** Returns the first fragment uri. */
  URI first_fragment_uri() const;

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Initializes the reader. */
  Status init();

  /** Returns the cell layout. */
  Layout layout() const;

  /** Returns `true` if no results were retrieved after a query. */
  bool no_results() const;

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Performs a read query using its set members. */
  Status read();

  /** Performs a read query (applicable when setting a Subarray). */
  template <class T>
  Status read();

  /** Sets the array. */
  void set_array(const Array* array);

  /**
   * Sets the array schema. If the array is a kv store, then this
   * function also sets global order as the default layout.
   */
  void set_array_schema(const ArraySchema* array_schema);

  /**
   * Sets the buffer for a fixed-sized attribute.
   *
   * @param attribute The attribute to set the buffer for.
   * @param buffer The buffer that will hold the data to be read.
   * @param buffer_size This initially contains the allocated
   *     size of `buffer`, but after the termination of the function
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute,
      void* buffer,
      uint64_t* buffer_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized attribute.
   *
   * @param attribute The attribute to set the buffer for.
   * @param buffer_off The buffer that will hold the data to be read.
   *     This buffer holds the starting offsets of each cell value in
   *     `buffer_val`.
   * @param buffer_off_size This initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param buffer_val The buffer that will hold the data to be read.
   *     This buffer holds the actual var-sized cell values.
   * @param buffer_val_size This initially contains
   *     the allocated size of `buffer_val`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      bool check_null_buffers = true);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /**
   * This is applicable only to dense arrays (errors out for sparse arrays),
   * and only in the case where the array is opened in a way that all its
   * fragments are sparse. If the input is `true`, then the dense array
   * will be read in "sparse mode", i.e., the sparse read algorithm will
   * be executing, returning results only for the non-empty cells.
   * The sparse mode is useful particularly in consolidation, when the
   * algorithm determines that the subset of fragments to consolidate
   * in the next step are all sparse. In that case, the consolidator
   * needs to read only the non-empty cells of those fragments, which
   * can be achieved by opening the dense array in the sparse mode
   * (otherwise the dense array would return special values for all
   * empty cells).
   *
   * @param sparse_mode This sets the sparse mode.
   * @return Status
   */
  Status set_sparse_mode(bool sparse_mode);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   * @return Status
   */
  Status set_subarray(const void* subarray, bool check_expanded_domain = false);

  /**
   * Sets the query subarray.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const Subarray& subarray);

  /** Returns the query subarray. */
  const Subarray* subarray() const;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Computes a mapping (tile coordinates) -> (result space tile).
   * The produced result space tiles will contain information only
   * about fragments that will contribute results. Specifically, if
   * a fragment is completely covered by a more recent fragment
   * in a particular space tile, then it will certainly not contribute
   * results and, thus, no information about that fragment is included
   * in the space tile.
   *
   * @tparam T The datatype of the tile domains.
   * @param tile_coords The unique coordinates of the tiles that intersect
   *     a subarray.
   * @param array_tile_domain The array tile domain.
   * @param frag_tile_domains The tile domains of each fragment. These
   *     are assumed to be ordered from the most recent to the oldest
   *     fragment.
   * @param result_space_tiles The result space tiles to be produced
   *     by the function.
   */
  template <class T>
  static void compute_result_space_tiles(
      const std::vector<std::vector<uint8_t>>& tile_coords,
      const TileDomain<T>& array_tile_domain,
      const std::vector<TileDomain<T>>& frag_tile_domains,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles);

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   */
  template <class T>
  void compute_result_cell_slabs(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords<T>>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to row-/col-major subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_coords_pos The position in `result_coords` to be
   *     passed to the result cell slab iterator in the function.
   *     This practically keeps track of the sparse coordinate results
   *     already processed in successive calls of this function.
   *     The function updates this value with the current position
   *     returned by the iterator at the end of its process.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param frag_tile_set Stores the unique pairs (frag_idx, tile_idx)
   *     for all result tiles.
   * @param result_cell_slabs The returned result cell slabs.
   */
  template <class T>
  void compute_result_cell_slabs_row_col(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords<T>>* result_coords,
      uint64_t* result_coords_pos,
      std::vector<ResultTile*>* result_tiles,
      std::set<std::pair<unsigned, uint64_t>>* frag_tile_set,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to global order subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   */
  template <class T>
  void compute_result_cell_slabs_global(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords<T>>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array. */
  const Array* array_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The names of the attributes involved in the query. */
  std::vector<std::string> attributes_;

  /** Maps attribute names to their buffers. */
  std::unordered_map<std::string, QueryBuffer> attr_buffers_;

  /** The fragment metadata. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** Read state. */
  ReadState read_state_;

  /**
   * If `true`, then the dense array will be read in "sparse mode", i.e.,
   * the sparse read algorithm will be executing, returning results only
   * for the non-empty cells.
   */
  bool sparse_mode_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The query subarray (initially the whole domain by default). */
  Subarray subarray_;

  /**
   * The memory budget for the fixed-sized attributes and the offsets
   * of the var-sized attributes.
   */
  uint64_t memory_budget_;

  /** The memory budget for the var-sized attributes. */
  uint64_t memory_budget_var_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Deletes the tiles on the input attribute from the result tiles.
   *
   * @param attr The attribute name.
   * @param result_tiles The result tiles to delete from.
   * @return void
   */
  void clear_tiles(
      const std::string& attr,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Compute the maximal cell slabs of contiguous sparse coordinates.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to compute the slabs from.
   * @param result_cell_slabs The result cell slabs to compute.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs(
      const std::vector<ResultCoords<T>>& result_coords,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Retrieves the coordinates that overlap the input N-dimensional range
   * from the input result tile.
   *
   * @tparam T The coords type.
   * @param frag_idx The id of the fragment that the result tile belongs to.
   * @param The result tile.
   * @param range An N-dimensional range (where N is equal to the number
   *     of dimensions of the array).
   * @param result_coords The overlapping coordinates to retrieve.
   * @return Status
   */
  template <class T>
  Status compute_range_result_coords(
      unsigned frag_idx,
      ResultTile* tile,
      const std::vector<const T*>& range,
      std::vector<ResultCoords<T>>* result_coords) const;

  /**
   * Computes the result coordinates for each range of the query
   * subarray.
   *
   * @tparam T The domain type.
   * @param single_fragment For each range, it indicates whether all
   *     result coordinates come from a single fragment.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result tiles of each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  template <class T>
  Status compute_range_result_coords(
      const std::vector<bool>& single_fragment,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<std::vector<ResultCoords<T>>>* range_result_coords);

  /**
   * Computes the result coordinates of a given range of the query
   * subarray.
   *
   * @tparam T The domain type.
   * @param range_idx The range to focus on.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result_tiles overlapping with each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  template <class T>
  Status compute_range_result_coords(
      uint64_t range_idx,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords<T>>* range_result_coords);

  /**
   * Computes the final subarray result coordinates, which will be
   * deduplicated and sorted on the specified subarray layout.
   *
   * @tparam T The domain type.
   * @param range_result_coords The result coordinates for each subarray range.
   * @param tile_coords If the subarray layout is global order, this
   *     function will store the unique tile coordinates of the subarray
   *     coordinates in `tile_coords`. Then the element of `result_coords` will
   *     store only pointers to the unique tile coordinates.
   * @param result_coords The final (subarray) result coordinates to be
   *     retrieved.
   * @return Status
   *
   * @note the function will try to gradually clean up ``range_result_coords``
   *     as it is done processing its elements to quickly reclaim memory.
   */
  template <class T>
  Status compute_subarray_coords(
      std::vector<std::vector<ResultCoords<T>>>* range_result_coords,
      std::vector<std::vector<T>>* tile_coords,
      std::vector<ResultCoords<T>>* result_coords);

  /**
   * Computes info about the sparse result tiles, such as which fragment they
   * belong to, the tile index and the type of overlap. The tile vector
   * contains unique info about the tiles. The function also computes
   * a map from fragment index and tile id to a result tile to keep
   * track of the unique result  tile info for subarray ranges that overlap
   * with common tiles.
   *
   * @tparam T The coords type.
   * @param result_tiles The result tiles to be computed.
   * @param result_tile_map The result tile map to be computed.
   * @param single_fragment Each element corresponds to a range of the
   *     subarray and is set to ``true`` if all the overlapping
   *     tiles come from a single fragment for that range.
   * @return Status
   */
  template <class T>
  Status compute_sparse_result_tiles(
      std::vector<ResultTile>* result_tiles,
      std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
      std::vector<bool>* single_fragment);

  /**
   * Computes the sparse tile coordinates. It stores the unique tile
   * coordinates in `tile_coords`, and then it stores pointers to
   * those tile coordinates in the elements of`result_coords`.
   *
   * @tparam T The domain data type.
   * @param result_coords The result coordinates.
   * @param tile_coords The unique tile coordinates of the result
   *     coordinates.
   * @return
   */
  template <class T>
  Status compute_sparse_tile_coords(
      std::vector<ResultCoords<T>>* result_coords,
      std::vector<std::vector<T>>* tile_coords) const;

  /**
   * Copies the cells for the input attribute and result cell slabs, into
   * the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @return Status
   */
  Status copy_cells(
      const std::string& attribute,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Copies the cells for the input **fixed-sized** attribute and result
   * cell slabs, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @return Status
   */
  Status copy_fixed_cells(
      const std::string& attribute,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Copies the cells for the input **var-sized** attribute and result
   * cell slabs, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @return Status
   */
  Status copy_var_cells(
      const std::string& attribute,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Computes offsets into destination buffers for the given attribute's offset
   * and variable-length data, for the given list of result cell slabs.
   *
   * @param attribute The variable-length attribute
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to compute destinations for.
   * @param offset_offsets_per_cs Output to hold one vector per result cell
   *    slab, and one element per cell in the slab. The elements are the
   *    destination offsets for the attribute's offsets.
   * @param var_offsets_per_cs Output to hold one vector per result cell slab,
   *    and one element per cell in the slab. The elements are the destination
   *    offsets for the attribute's variable-length data.
   * @param total_offset_size Output set to the total size in bytes of the
   *    offsets in the given list of result cell slabs.
   * @param total_var_size Output set to the total size in bytes of the
   *    attribute's variable-length in the given list of result cell slabs.
   * @return Status
   */
  Status compute_var_cell_destinations(
      const std::string& attribute,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<std::vector<uint64_t>>* offset_offsets_per_cs,
      std::vector<std::vector<uint64_t>>* var_offsets_per_cs,
      uint64_t* total_offset_size,
      uint64_t* total_var_size) const;

  /**
   * Computes the result space tiles based on the input subarray.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles to be computed.
   */
  template <class T>
  void compute_result_space_tiles(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const;

  /**
   * Computes the result coordinates from the sparse fragments.
   *
   * @param result_tiles This will store the unique result tiles.
   * @param tile_coords If the subarray layout is global order, this
   *     function will store the unique tile coordinates of the subarray
   *     coordinates in `tile_coords`. Then the element of `result_coords`
   *     will store only pointers to the unique tile coordinates.
   * @param result_coords This will store the result coordinates.
   */
  template <class T>
  Status compute_result_coords(
      std::vector<ResultTile>* result_tiles,
      std::vector<std::vector<T>>* tile_coords,
      std::vector<ResultCoords<T>>* result_coords);

  /**
   * Deduplicates the input result coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @tparam T The coords type.
   * @param result_coords The result coordinates to dedup.
   * @return Status
   */
  template <class T>
  Status dedup_result_coords(std::vector<ResultCoords<T>>* result_coords) const;

  /**
   * Performs a read on a dense array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read();

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords(const Subarray& subarray);

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to global order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param coords_buff The coordinates buffer to be filled.
   * @param coords_buff_size The size of the coordinates buffer.
   * @param coords_buff_offset The offset in the coordinates buffer the filling
   *     will start from.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_global(
      const Subarray& subarray,
      void* coords_buff,
      uint64_t coords_buff_size,
      uint64_t* coords_buff_offset);

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to row-/col-major order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param coords_buff The coordinates buffer to be filled.
   * @param coords_buff_size The size of the coordinates buffer.
   * @param coords_buff_offset The offset in the coordinates buffer the filling
   *     will start from.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_row_col(
      const Subarray& subarray,
      void* coords_buff,
      uint64_t coords_buff_size,
      uint64_t* coords_buff_offset);

  /**
   * Fills coordinates in the input buffer for a particular cell slab, following
   * a row-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [3, 1], [3, 2], and [3, 3].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param buff The buffer to write the coordinates into.
   * @param offset The offset in `buff` where the write will begin.
   */
  template <class T>
  void fill_dense_coords_row_slab(
      const T* start, uint64_t num, void* buff, uint64_t* offset) const;

  /**
   * Fills coordinates in the input buffer for a particular cell slab, following
   * a col-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [4, 1], [5, 1], and [6, 1].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param buff The buffer to write the coordinates into.
   * @param offset The offset in `buff` where the write will begin.
   */
  template <class T>
  void fill_dense_coords_col_slab(
      const T* start, uint64_t num, void* buff, uint64_t* offset) const;

  /**
   * Filters the tiles on a particular attribute from all input fragments
   * based on the tile info in `result_tiles`.
   *
   * @param attribute Attribute whose tiles will be filtered
   * @param result_tiles Vector containing the tiles to be filtered
   * @return Status
   */
  Status filter_tiles(
      const std::string& attribute,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Runs the input tile for the input attribute through the filter pipeline.
   * The tile buffer is modified to contain the output of the pipeline.
   *
   * @param attribute The attribute the tile belong to.
   * @param tile The tile to be filtered.
   * @param offsets True if the tile to be filtered contains offsets for a
   *    var-sized attribute.
   * @return Status
   */
  Status filter_tile(
      const std::string& attribute, Tile* tile, bool offsets) const;

  /**
   * Gets all the result coordinates of the input tile into `result_coords`.
   *
   * @tparam T The coords type.
   * @param result_tile The result tile to read the coordinates from.
   * @param result_coords The result coordinates to copy into.
   * @return Status
   */
  template <class T>
  Status get_all_result_coords(
      ResultTile* tile, std::vector<ResultCoords<T>>* result_coords) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /** Initializes the read state. */
  Status init_read_state();

  /**
   * Initializes a fixed-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param attribute The attribute the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version, const std::string& attribute, Tile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param attribute The attribute the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version,
      const std::string& attribute,
      Tile* tile,
      Tile* tile_var) const;

  /**
   * Retrieves the tiles on a particular attribute and stores it in the
   * appropriate result tile.
   *
   * @param attr The attribute name.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_tiles(
      const std::string& attr,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Retrieves the tiles on for the given regions.
   *
   * @param tile_to_chunks Maps a tile pointer to the number of read chunks.
   * @param tile_to_buffer Maps a tile pointer to the buffer it will read into.
   * @param uri_to_region Maps a URI to its region.
   * @return Status
   */
  Status read_tiles(
      std::unordered_map<Tile*, uint64_t>* tile_to_chunks,
      const std::unordered_map<Tile*, std::unique_ptr<Buffer>>& tile_to_buffer,
      const std::unordered_map<
          URI,
          std::vector<std::pair<uint64_t, Tile*>>,
          URIHasher>& uri_to_region,
      std::vector<std::future<Status>>* tasks) const;

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /**
   * Sorts the input result coordinates according to the subarray layout.
   *
   * @tparam T The coords type.
   * @param result_coords The coordinates to sort.
   * @param layout The layout to sort into.
   * @return Status
   */
  template <class T>
  Status sort_result_coords(
      std::vector<ResultCoords<T>>* result_coords, Layout layout) const;

  /**
   * Performs a read on a sparse array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status sparse_read();

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /**
   * Returns true if the input tile's MBR of the input fragment is fully
   * covered by the non-empty domain of a more recent fragment.
   */
  template <class T>
  bool sparse_tile_overwritten(unsigned frag_idx, uint64_t tile_idx) const;

  /**
   * Returns true if the input coordinates of the input fragment is
   * covered by the non-empty domain of a more recent fragment.
   */
  template <class T>
  bool coords_overwritten(unsigned frag_idx, const T* coords) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_H
