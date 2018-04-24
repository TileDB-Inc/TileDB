/**
 * @file   query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/tile.h"

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace tiledb {
namespace sm {

class StorageManager;

/** Processes a (read/write) query. */
class Query {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  struct GlobalWriteState {
    /**
     * Stores the last tile of each attribute for each write operation.
     * For fixed-sized attributes, the second tile is ignored. For
     * var-sized attributes, the first tile is the offsets tile, whereas
     * the second tile is the values tile.
     */
    std::unordered_map<std::string, std::pair<Tile, Tile>> last_tiles_;

    /**
     * Stores the number of cells written for each attribute across the
     * write operations.
     */
    std::unordered_map<std::string, uint64_t> cell_written_;

    /** The fragment metadata. */
    std::shared_ptr<FragmentMetadata> frag_meta_;
  };

  /**
   * For each fixed-sized attributes, the second tile in the pair is
   * ignored. For var-sized attributes, the first is a pointer to the
   * offsets tile and the second is a pointer to the var-sized values tile.
   */
  typedef std::pair<std::shared_ptr<Tile>, std::shared_ptr<Tile>> TilePair;

  /** Information about a tile (across multiple attributes). */
  struct OverlappingTile {
    /** A fragment index. */
    unsigned fragment_idx_;
    /** The tile index in the fragment. */
    uint64_t tile_idx_;
    /** `true` if the overlap is full, and `false` if it is partial. */
    bool full_overlap_;
    /**
     * Maps attribute names to attribute tiles. Note that the coordinates
     * are a special attribute as well.
     */
    std::unordered_map<std::string, TilePair> attr_tiles_;

    /** Constructor. */
    OverlappingTile(
        unsigned fragment_idx, uint64_t tile_idx, bool full_overlap = false)
        : fragment_idx_(fragment_idx)
        , tile_idx_(tile_idx)
        , full_overlap_(full_overlap) {
    }
  };

  /** A vector of overlapping tiles. */
  typedef std::vector<std::shared_ptr<OverlappingTile>> OverlappingTileVec;

  /** A cell range belonging to a particular overlapping tile. */
  struct OverlappingCellRange {
    /**
     * The tile the cell range belongs to. If `nullptr`, then this is
     * an "empty" cell range, to be filled with the default empty
     * values.
     */
    std::shared_ptr<OverlappingTile> tile_;
    /** The starting cell in the range. */
    uint64_t start_;
    /** The ending cell in the range. */
    uint64_t end_;

    /** Constructor. */
    OverlappingCellRange(
        std::shared_ptr<OverlappingTile> tile, uint64_t start, uint64_t end)
        : tile_(std::move(tile))
        , start_(start)
        , end_(end) {
    }
  };

  /** A list of cell ranges. */
  typedef std::list<std::shared_ptr<OverlappingCellRange>>
      OverlappingCellRangeList;

  /**
   * Records the overlapping tile and position of the coordinates
   * in that tile.
   *
   * @tparam T The coords type
   */
  template <class T>
  struct OverlappingCoords {
    /** The overlapping tile the coords belong to. */
    std::shared_ptr<OverlappingTile> tile_;
    /** The coordinates. */
    const T* coords_;
    /** The position of the coordinates in the tile. */
    uint64_t pos_;

    /** Constructor. */
    OverlappingCoords(
        std::shared_ptr<OverlappingTile> tile, const T* coords, uint64_t pos)
        : tile_(std::move(tile))
        , coords_(coords)
        , pos_(pos) {
    }
  };

  /** A cell range produced by the dense read algorithm. */
  template <class T>
  struct DenseCellRange {
    /**
     * The fragment index. `-1` stands for no fragment, which means
     * that the cell range must be filled with the fill value.
     */
    int fragment_idx_;
    /** The tile coordinates of the range. */
    const T* tile_coords_;
    /** The starting cell in the range. */
    uint64_t start_;
    /** The ending cell in the range. */
    uint64_t end_;

    /** Constructor. */
    DenseCellRange(
        int fragment_idx, const T* tile_coords, uint64_t start, uint64_t end)
        : fragment_idx_(fragment_idx)
        , tile_coords_(tile_coords)
        , start_(start)
        , end_(end) {
    }
  };

  /** Cell range to be written. */
  struct WriteCellRange {
    /** The position in the tile where the range will be copied. */
    uint64_t pos_;
    /** The starting cell in the user buffers. */
    uint64_t start_;
    /** The ending cell in the user buffers. */
    uint64_t end_;

    /** Constructor. */
    WriteCellRange(uint64_t pos, uint64_t start, uint64_t end)
        : pos_(pos)
        , start_(start)
        , end_(end) {
    }
  };

  /** A vector of write cell ranges. */
  typedef std::vector<WriteCellRange> WriteCellRangeVec;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Query();

  /** Destructor. */
  ~Query();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Computes info about the overlapping tiles, such as which fragment they
   * belong to, the tile index and the type of overlap.
   *
   * @tparam T The coords type.
   * @param tiles The tiles to be computed.
   * @return Status
   */
  template <class T>
  Status compute_overlapping_tiles(OverlappingTileVec* tiles) const;

  /**
   * Retrieves the tiles on a particular attribute from all input fragments
   * based on the tile info in `tiles`.
   *
   * @param attr_name The attribute name.
   * @param tiles The retrieved tiles will be stored in `tiles`.
   * @return Status
   */
  Status read_tiles(
      const std::string& attr_name, OverlappingTileVec* tiles) const;

  /**
   * Computes the overlapping coordinates for a given subarray.
   *
   * @tparam T The coords type.
   * @param tiles The tiles to get the overlapping coordinates from.
   * @param coords The coordinates to be retrieved.
   * @return Status
   */
  template <class T>
  Status compute_overlapping_coords(
      const OverlappingTileVec& tiles,
      std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const;

  /**
   * Retrieves the coordinates that overlap the subarray from the input
   * overlapping tile.
   *
   * @tparam T The coords type.
   * @param The overlapping tile.
   * @param coords The overlapping coordinates to retrieve.
   * @return Status
   */
  template <class T>
  Status compute_overlapping_coords(
      const std::shared_ptr<OverlappingTile>& tile,
      std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const;

  /**
   * Gets all the coordinates of the input tile into `coords`.
   *
   * @tparam T The coords type.
   * @param tile The overlapping tile to read the coordinates from.
   * @param coords The overlapping coordinates to copy into.
   * @return Status
   */
  template <class T>
  Status get_all_coords(
      const std::shared_ptr<OverlappingTile>& tile,
      std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const;

  /**
   * Sorts the input coordinates according to the input layout.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to sort.
   * @return Status
   */
  template <class T>
  Status sort_coords(
      std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const;

  /**
   * Deduplicates the input coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @tparam T The coords type.
   * @param coords The coordinates to dedup.
   * @return Status
   */
  template <class T>
  Status dedup_coords(
      std::list<std::shared_ptr<OverlappingCoords<T>>>* coords) const;

  /**
   * Compute the maximal cell ranges of contiguous cell positions.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to compute the ranges from.
   * @param cell_ranges The cell ranges to compute.
   * @return Status
   */
  template <class T>
  Status compute_cell_ranges(
      const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
      OverlappingCellRangeList* cell_ranges) const;

  /**
   * Copies the cells for the input attribute and cell ranges, into
   * the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges) const;

  /**
   * Copies the cells for the input **fixed-sized** attribute and cell
   * ranges, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_fixed_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges) const;

  /**
   * Copies the cells for the input **var-sized** attribute and cell
   * ranges, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_var_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges) const;

  /**
   * Checks whether two hyper-rectangles overlap, and determines whether
   * the first rectangle contains the second.
   *
   * @tparam T The domain type.
   * @param a The first rectangle.
   * @param b The second rectangle.
   * @param dim_num The number of dimensions.
   * @param a_contains_b Determines whether the first rectangle contains the
   *     second.
   * @return `True` if the rectangles overlap, and `false` otherwise.
   */
  template <class T>
  bool overlap(
      const T* a, const T* b, unsigned dim_num, bool* a_contains_b) const;

  /** Returns the array schema.*/
  const ArraySchema* array_schema() const;

  /** Processes a query. */
  Status process();

  /** Returns the list of ids of attributes involved in the query. */
  const std::vector<unsigned int>& attribute_ids() const;

  /** Retrieves the index of the buffer corresponding to the input attribute. */
  Status buffer_idx(const std::string& attribute, unsigned* bid) const;

  /**
   * Retrieves the index of the coordinates buffer in the specified query
   * buffers.
   *
   * @param coords_buffer_i The index of the coordinates buffer to be retrieved.
   * @return Status
   */
  Status coords_buffer_i(int* coords_buffer_i) const;

  /**
   * Computes a vector of `subarrays` into which `subarray` must be partitioned,
   * such that each subarray in `subarrays` can be saferly answered by the
   * query without a memory overflow.
   *
   * @param subarray The input subarray.
   * @param subarrays The vector of subarray partitions to be retrieved.
   * @return Status
   */
  Status compute_subarrays(void* subarray, std::vector<void*>* subarrays) const;

  /**
   * Finalizes the query, properly finalizing and deleting the involved
   * fragments.
   */
  Status finalize();

  /** Returns the metadata of the fragments involved in the query. */
  const std::vector<FragmentMetadata*>& fragment_metadata() const;

  /** Returns a vector with the fragment URIs. */
  std::vector<URI> fragment_uris() const;

  /** Returns the number of fragments involved in the query. */
  unsigned int fragment_num() const;

  /**
   * Initializes the query states. This must be called before the query is
   * submitted.
   */
  Status init();

  /**
   * Initializes the query.
   *
   * @param storage_manager The storage manager.
   * @param array_schema The array schema.
   * @param fragment_metadata The metadata of the involved fragments.
   * @param type The query type.
   * @param layout The cell layout.
   * @param subarray The subarray the query is constrained on. A nuullptr
   *     indicates the full domain.
   * @param attributes The names of the attributes involved in the query.
   * @param attribute_num The number of attributes.
   * @param buffers The query buffers with a one-to-one correspondences with
   *     the specified attributes. In a read query, the buffers will be
   *     populated with the query results. In a write query, the buffer
   *     contents will be appropriately written in a new fragment.
   * @param buffer_sizes The corresponding buffer sizes.
   * @return Status
   */
  Status init(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_metadata,
      QueryType type,
      Layout layout,
      const void* subarray,
      const char** attributes,
      unsigned int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  /** Returns the lastly created fragment uri. */
  URI last_fragment_uri() const;

  /** Returns the cell layout. */
  Layout layout() const;

  /**
   * Returns true if the query cannot write to some buffer due to
   * an overflow.
   */
  bool overflow() const;

  /**
   * Checks if a particular query buffer (corresponding to some attribute)
   * led to an overflow based on an attribute id.
   */
  bool overflow(unsigned int attribute_id) const;

  /**
   * Checks if a particular query buffer (corresponding to some attribute)
   * led to an overflow based on an attribute name.
   *
   * @param attribute_name The attribute whose overflow to retrieve.
   * @param overflow The overflow status to be retieved.
   * @return Status (error is attribute is not involved in the query).
   */
  Status overflow(const char* attribute_name, unsigned int* overflow) const;

  /** Sets the array schema. */
  void set_array_schema(const ArraySchema* array_schema);

  /**
   * Sets the buffers to the query for a set of attributes.
   *
   * @param attributes The attributes the query will focus on.
   * @param attribute_num The number of attributes.
   * @param buffers The buffers that either have the input data to be written,
   *     or will hold the data to be read. Note that there is one buffer per
   *     fixed-sized attribute, and two buffers for each variable-sized
   *     attribute (the first holds the offsets, and the second the actual
   *     values).
   * @param buffer_sizes There must be an one-to-one correspondence with
   *     *buffers*. In the case of writes, they contain the sizes of *buffers*.
   *     In the case of reads, they initially contain the allocated sizes of
   *     *buffers*, but after the termination of the function they will contain
   *     the sizes of the useful (read) data in the buffers.
   * @return Status
   */
  Status set_buffers(
      const char** attributes,
      unsigned int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  /** Sets the query buffers. */
  void set_buffers(void** buffers, uint64_t* buffer_sizes);

  /**
   * Sets the callback function and its data input that will be called
   * upon the completion of an asynchronous query.
   */
  void set_callback(
      const std::function<void(void*)>& callback, void* callback_data);

  /** Sets and initializes the fragment metadata. */
  Status set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout.
   */
  Status set_layout(Layout layout);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const void* subarray);

  /** Sets the query type. */
  void set_type(QueryType type);

  /** Returns the query status. */
  QueryStatus status() const;

  /** Returns the storage manager. */
  StorageManager* storage_manager() const;

  /** Returns the subarray in which the query is constrained. */
  const void* subarray() const;

  /** Returns the query type. */
  QueryType type() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The state associated with global writes. */
  std::unique_ptr<GlobalWriteState> global_write_state_;

  /** The names of the attributes involved in the query. */
  std::vector<std::string> attributes_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The ids of the attributes involved in the query. */
  std::vector<unsigned int> attribute_ids_;

  /**
   * The query buffers (one per involved attribute, two per variable-sized
   * attribute.
   */
  void** buffers_;

  /** The corresponding buffer sizes. */
  uint64_t* buffer_sizes_;

  /** Number of buffers. */
  unsigned buffer_num_;

  /** A function that will be called upon the completion of an async query. */
  std::function<void(void*)> callback_;

  /** The data input to the callback function. */
  void* callback_data_;

  /** The query status. */
  QueryStatus status_;

  /** The metadata of the fragments involved in the query. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** The cell layout. */
  Layout layout_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /**
   * The subarray the query is constrained on. A nullptr implies the
   * entire domain.
   */
  void* subarray_;

  /** The query type. */
  QueryType type_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Adds the coordinates attribute if it does not exist. */
  void add_coords();

  /** Checks if attributes has been appropriately set for a query. */
  Status check_attributes();

  /**
   * Checks if the buffer sizes are correct in the case of writing
   * in a dense array in an ordered layout.
   *
   * @return Status
   */
  Status check_buffer_sizes_ordered() const;

  /** Checks if `subarray` falls inside the array domain. */
  Status check_subarray(const void* subarray) const;

  /** Checks if `subarray` falls inside the array domain. */
  template <class T>
  Status check_subarray(const T* subarray) const;

  /**
   * For the given cell range, it computes all the result dense cell ranges
   * across fragments, given precedence to more recent fragments.
   *
   * @tparam T The domain type.
   * @param tile_coords The tile coordinates in the array domain.
   * @param frag_its The fragment dence cell range iterators.
   * @param start The start position of the range this function focuses on.
   * @param end The end position of the range this function focuses on.
   * @param dense_cell_ranges The cell ranges where the results are appended to.
   * @return Status
   *
   * @note The input dense cell range iterators will be appropriately
   *     incremented.
   */
  template <class T>
  Status compute_dense_cell_ranges(
      const T* tile_coords,
      std::vector<DenseCellRangeIter<T>>& frag_its,
      uint64_t start,
      uint64_t end,
      std::list<DenseCellRange<T>>* dense_cell_ranges);

  /**
   * Computes the dense overlapping tiles and cell ranges based on the
   * input dense cell ranges. Note that the function also computes
   * the maximal ranges of contiguous cells for each fragment/tile pair.
   *
   * @tparam T The domain type.
   * @param dense_cell_ranges The dense cell ranges the overlapping tiles
   *     and cell ranges will be derived from.
   * @param tiles The overlapping tiles to be computed.
   * @param overlapping_cell_ranges The overlapping cell ranges to be
   *     computed.
   * @return Status
   */
  template <class T>
  Status compute_dense_overlapping_tiles_and_cell_ranges(
      const std::list<DenseCellRange<T>>& dense_cell_ranges,
      const std::list<std::shared_ptr<OverlappingCoords<T>>>& coords,
      OverlappingTileVec* tiles,
      OverlappingCellRangeList* overlapping_cell_ranges);

  /** Returns the empty fill value based on the input datatype. */
  const void* fill_value(Datatype type) const;

  /**
   * Initializes the fragment dense cell range iterators. There is one vector
   * per tile overlapping with the query subarray, which stores one cell range
   * iterator per fragment.
   *
   * @tparam T The domain type.
   * @param iters The iterators to be initialized.
   * @param overlapping_tile_idx_coords A map from global tile index to a pair
   *     (overlapping tile index, overlapping tile coords).
   */
  template <class T>
  Status init_tile_fragment_dense_cell_range_iters(
      std::vector<std::vector<DenseCellRangeIter<T>>>* iters,
      std::unordered_map<uint64_t, std::pair<uint64_t, std::vector<T>>>*
          overlapping_tile_idx_coords);

  /**
   * Returns a new fragment name, which is in the form: <br>
   * .__thread-id_timestamp. For instance,
   *  __6426153_1458759561320
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character.
   *
   * @return A new special fragment name on success, or "" (empty string) on
   *     error.
   */
  std::string new_fragment_name() const;

  /** Sets the query attributes. */
  Status set_attributes(const char** attributes, unsigned int attribute_num);

  /**
   * Sets the input buffer sizes to zero. The function assumes that the buffer
   * sizes correspond to the attribute buffers specified upon query creation.
   */
  void zero_out_buffer_sizes(uint64_t* buffer_sizes) const;

  /** Memsets all set buffers to zero. Used only in read queries. */
  void zero_out_buffers();

  /**
   * Initializes dense cell range iterators for the subarray to be writte,
   * one per overlapping tile.
   *
   * @tparam T The domain type.
   * @param iters The dense cell range iterators to be created.
   * @return Status
   */
  template <class T>
  Status init_tile_dense_cell_range_iters(
      std::vector<DenseCellRangeIter<T>>* iters) const;

  /**
   * Computes the cell ranges to be written, derived from a
   * dense cell range iterator for a specific tile.
   *
   * @tparam T The domain type.
   * @param iter The dense cell range iterator for one
   *     tile overlapping with the write subarray.
   * @param write_cell_ranges The write cell ranges to be created.
   * @return Status
   */
  template <class T>
  Status compute_write_cell_ranges(
      DenseCellRangeIter<T>* iters, WriteCellRangeVec* write_cell_ranges) const;

  /**
   * Sorts the coordinates of the user buffers, creating a vector with
   * the sorted positions.
   *
   * @tparam T The domain type.
   * @param cell_pos The sorted cell positions to be created.
   * @return Status
   */
  template <class T>
  Status sort_coords(std::vector<uint64_t>* cell_pos) const;

  /** Initializes the global write state. */
  Status init_global_write_state();

  /** Finalizes the global write state. */
  Status finalize_global_write_state();

  /**
   * Finalizes the global write state.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status finalize_global_write_state();

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `last_tiles_` as part of the state to be used in the next
   * write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `last_tiles_` as part of the state to be used in the next
   * write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to fixed-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_fixed(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `last_tiles_` as part of the state to be used in the next
   * write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to var-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_var(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, copying from the user buffers into the tiles
   * the values based on the input write cell ranges, focusing on the
   * input attribute.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param write_cell_ranges The write cell ranges.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& attribute,
      const std::vector<WriteCellRangeVec>& write_cell_ranges,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions. Applicable only
   * to fixed-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_fixed(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions. Applicable only
   * to var-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_var(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /**
   * Computes the coordinates metadata (e.g., MBRs).
   *
   * @tparam T The domain type.
   * @param tiles The tiles to calculate the coords metadata from.
   * @param meta The fragment metadata that will store the coords metadata.
   * @return Status
   */
  template <class T>
  Status compute_coords_metadata(
      const std::vector<Tile>& tiles, FragmentMetadata* meta) const;

  /**
   * Writes the input tiles for the input attribute to storage.
   *
   * @param attribute The attribute the tiles belong to.
   * @param frag_meta The fragment metadata.
   * @param tiles The tiles to be written.
   * @return Status
   */
  Status write_tiles(
      const std::string& attribute,
      FragmentMetadata* frag_meta,
      std::vector<Tile>& tiles) const;

  /**
   * Initializes the tiles for writing for the input attribute.
   *
   * @param attribute_id The id of the attribute the tiles belong to.
   * @param tile_num The number of tiles.
   * @param tiles The tiles to be initialized. Note that the vector
   *     has been already preallocated.
   * @return Status
   */
  Status init_tiles(
      unsigned attribute_id, uint64_t tile_num, std::vector<Tile>* tiles) const;

  /**
   * Writes the input cell range to the input tile, for a particular
   * buffer. Applicable to **fixed-sized** attributes.
   *
   * @param buff The write buffer where the cells will be copied from.
   * @param start The start element in the write buffer.
   * @param end The end element in the write buffer.
   * @param tile The tile to write to.
   * @return Status
   */
  Status write_cell_range_to_tile(
      ConstBuffer* buff, uint64_t start, uint64_t end, Tile* tile) const;

  /**
   * Writes the input cell range to the input tile, for a particular
   * buffer. Applicable to **variable-sized** attributes.
   *
   * @param buff The write buffer where the cell offsets will be copied from.
   * @param buff_var The write buffer where the cell values will be copied from.
   * @param start The start element in the write buffer.
   * @param end The end element in the write buffer.
   * @param tile The tile offsets to write to.
   * @param tile_var The tile with the var-sized cells to write to.
   * @return Status
   */
  Status write_cell_range_to_tile_var(
      ConstBuffer* buff,
      ConstBuffer* buff_var,
      uint64_t start,
      uint64_t end,
      Tile* tile,
      Tile* tile_var) const;

  /**
   * Applicable only to global writes. Writes the last tiles for each
   * attribute remaining in the state, and records the metadata for
   * the coordinates attribute (if present).
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status global_write_handle_last_tile();

  /**
   * Writes an empty cell range to the input tile.
   * Applicable to **fixed-sized** attributes.
   *
   * @param num Number of empty values to write.
   * @param tile The tile to write to.
   * @return Status
   */
  Status write_empty_cell_range_to_tile(uint64_t num, Tile* tile) const;

  /**
   * Writes an empty cell range to the input tile.
   * Applicable to **variable-sized** attributes.
   *
   * @param num Number of empty values to write.
   * @param tile The tile offsets to write to.
   * @param tile_var The tile with the var-sized cells to write to.
   * @return Status
   */
  Status write_empty_cell_range_to_tile_var(
      uint64_t num, Tile* tile, Tile* tile_var) const;

  /**
   * Creates a new fragment.
   *
   * @param dense Whether the fragment is dense or not.
   * @param frag_meta The fragment metadata to be generated.
   * @return Status
   */
  Status create_fragment(
      bool dense, std::shared_ptr<FragmentMetadata>* frag_meta) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /** Closes all attribute files, flushing their state to storage. */
  Status close_files(FragmentMetadata* meta) const;

  /** Perform a dense read */
  Status dense_read();

  /**
   * Perform a dense read.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read();

  /**
   * Writes in the global layout. Applicable to both dense and sparse
   * arrays.
   */
  Status global_write();

  /**
   * Writes in the global layout. Applicable to both dense and sparse
   * arrays.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status global_write();

  /**
   * Writes in an ordered layout (col- or row-major order). Applicable only
   * to dense arrays.
   */
  Status ordered_write();

  /**
   * Writes in an ordered layout (col- or row-major order). Applicable only
   * to dense arrays.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status ordered_write();

  /**
   * Writes in an unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   */
  Status unordered_write();

  /**
   * Writes in an unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status unordered_write();

  /** Performs a read on a sparse array. */
  Status sparse_read();

  /**
   * Performs a read on a sparse array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status sparse_read();

  /** Executes a read query. */
  Status read();

  /** Executes a write query. */
  Status write();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_QUERY_H
