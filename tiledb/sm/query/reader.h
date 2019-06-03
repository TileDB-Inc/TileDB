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
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/result_space_tile.h"
#include "tiledb/sm/query/types.h"
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

  /**
   * For a read query, the user sets a subarray and buffers that will
   * hold the results. For some subarray, the user buffers may not be
   * able to hold the entire result. Given a subarray and the buffer sizes,
   * TileDB knows how to decompose the subarray into partitions, such
   * that the results of each partition can certainly fit in the user
   * buffers. The user can perform successive calls to `submit` in order
   * to incrementally perform each subarray partition. The query is
   * "incomplete" until all partitions are processed.
   *
   * The read state maintains a vector with all the subarray partitions,
   * along with an index `idx_` that indicates the parition to be processed
   * next.
   */
  struct ReadState {
    /** The current subarray the query is constrained on. */
    void* cur_subarray_partition_;
    /** The original subarray set by the user. */
    void* subarray_;
    /**
     * A list of subarray partitions. The head of the list is the partition
     * to be split next.
     */
    std::list<void*> subarray_partitions_;
    /** True if the reader has been initialized. */
    bool initialized_;
    /**
     * `True` if the query produced results that could not fit in
     * some buffer.
     */
    bool overflowed_;
    /** True if the current subarray partition is unsplittable. */
    bool unsplittable_;
  };

  struct ReadState2 {
    /**
     * True if the query led to a result that does not fit in
     * the user buffers.
     */
    bool overflowed_ = false;
    /**
     * ``true`` if a ``Subarray`` object has been set to the reader,
     * which indicates that the new (multi-range subarray) read
     * algorithm will be used.
     */
    bool set_ = false;
    /** The subarray partitioner. */
    SubarrayPartitioner partitioner_;
    /**
     * ``true`` if the next partition cannot be retrieved from the
     * partitioner, because it reaches a partition that is unsplittable.
     */
    bool unsplittable_ = false;
    /** A single-range subarray. */
    void* subarray_ = nullptr;
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

  /**
   * For each fixed-sized attributes, the second tile in the pair is ignored.
   * For var-sized attributes, the first is the offsets tile and the second is
   * the var-sized values tile.
   */
  typedef std::pair<Tile, Tile> TilePair;

  /** Information about a tile (across multiple attributes). */
  struct OverlappingTile {
    /** A fragment index. */
    unsigned fragment_idx_;
    /** The tile index in the fragment. */
    uint64_t tile_idx_;
    /** `true` if the overlap is full, and `false` if it is partial. */
    bool full_overlap_;  // TODO: this will probably be unnecessary
    /**
     * Maps attribute names to attribute tiles. Note that the coordinates
     * are a special attribute as well.
     */
    std::unordered_map<std::string, TilePair> attr_tiles_;

    /** Constructor. */
    OverlappingTile(
        unsigned fragment_idx,
        uint64_t tile_idx,
        const std::vector<std::string>& attributes,
        bool full_overlap = false)
        : fragment_idx_(fragment_idx)
        , tile_idx_(tile_idx)
        , full_overlap_(full_overlap) {
      attr_tiles_[constants::coords] = std::make_pair(Tile(), Tile());
      for (const auto& attr : attributes) {
        if (attr != constants::coords)
          attr_tiles_[attr] = std::make_pair(Tile(), Tile());
      }
    }
  };

  /** A vector of overlapping tiles. */
  typedef std::vector<std::unique_ptr<OverlappingTile>> OverlappingTileVec;

  /** A map (fragment_idx, tile_idx) -> pos in OverlappingTileVec. */
  typedef std::map<std::pair<unsigned, uint64_t>, size_t> OverlappingTileMap;

  /** A cell range belonging to a particular overlapping tile. */
  struct OverlappingCellRange {  // TODO: remove
    /**
     * The tile the cell range belongs to. If `nullptr`, then this is
     * an "empty" cell range, to be filled with the default empty
     * values.
     *
     * Note that the tile this points to is allocated and freed in
     * sparse_read/dense_read, so the lifetime of this struct must not exceed
     * the scope of those functions.
     */
    const OverlappingTile* tile_;
    /** The starting cell in the range. */
    uint64_t start_;
    /** The ending cell in the range. */
    uint64_t end_;

    /** Constructor. */
    OverlappingCellRange(
        const OverlappingTile* tile, uint64_t start, uint64_t end)
        : tile_(tile)
        , start_(start)
        , end_(end) {
    }
  };

  /** A list of cell ranges. */
  typedef std::vector<OverlappingCellRange> OverlappingCellRangeList;

  /**
   * Records the overlapping tile and position of the coordinates
   * in that tile.
   *
   * @tparam T The coords type
   */
  template <class T>
  struct OverlappingCoords {
    /**
     * The overlapping tile the coords belong to.
     *
     * Note that the tile this points to is allocated and freed in
     * sparse_read/dense_read, so the lifetime of this struct must not exceed
     * the scope of those functions.
     */
    const OverlappingTile* tile_;
    /** The coordinates. */
    const T* coords_;
    /** The coordinates of the tile. */
    const T* tile_coords_;
    /** The position of the coordinates in the tile. */
    uint64_t pos_;
    /** Whether this instance is "valid". */
    bool valid_;

    /** Constructor. */
    OverlappingCoords(
        const OverlappingTile* tile, const T* coords, uint64_t pos)
        : tile_(tile)
        , coords_(coords)
        , tile_coords_(nullptr)
        , pos_(pos)
        , valid_(true) {
    }

    /** Invalidate this instance. */
    void invalidate() {
      valid_ = false;
    }

    /** Return true if this instance is valid. */
    bool valid() const {
      return valid_;
    }
  };

  /**
   * Type alias for a list of OverlappingCoords.
   */
  template <typename T>
  using OverlappingCoordsVec = std::vector<OverlappingCoords<T>>;

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
    /**
     * The coordinates of the start of the range. This is needed for
     * comparing precednece of ranges. If it is `nullptr` it means
     * that the precedence check will be based solely on `start_`
     * and `end_`.
     */
    const T* coords_start_;
    /**
     * The coordinates of the end of the range. This is needed for
     * comparing precednece of ranges. If it is `nullptr` it means
     * that the precedence check will be based solely on `start_`
     * and `end_`.
     */
    const T* coords_end_;

    /** Constructor. */
    DenseCellRange(
        int fragment_idx,
        const T* tile_coords,
        uint64_t start,
        uint64_t end,
        const T* coords_start,
        const T* coords_end)
        : fragment_idx_(fragment_idx)
        , tile_coords_(tile_coords)
        , start_(start)
        , end_(end)
        , coords_start_(coords_start)
        , coords_end_(coords_end) {
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

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /**
   * Return list of attribtues for query
   * @return vector of attributes for query
   */
  std::vector<std::string> attributes() const;

  /**
   * Fetch AttributeBuffer for attribute
   * @param attribute to fetch
   * @return AttributeBuffer for attribute
   */
  AttributeBuffer buffer(const std::string& attribute) const;

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

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Initializes the reader. */
  Status init();

  /** Returns the cell layout. */
  Layout layout() const;

  /**
   * Advances the read state to the next subarray partition. It splits the
   * head of the subarray partition list (re-inserting at the front the
   * potentially derived partitons) and copies the first partition that
   * fits in the user buffers to `read_state_.cur_subarray_`. If there
   * is no next subarray, `read_state_.cur_subarray_` is freed and
   * set to `nullptr`.
   */
  Status next_subarray_partition();

  /** Returns `true` if no results were retrieved after a query. */
  bool no_results() const;

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Performs a read query using its set members. */
  Status read();

  /** Performs a read query (applicable when setting a Subarray). */
  Status read_2();

  /** Performs a read query (applicable when setting a Subarray). */
  template <class T>
  Status read_2();

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
   * @return Status
   */
  Status set_subarray(const void* subarray);

  /**
   * Sets the query subarray.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const Subarray& subarray);

  /** Returns the subarray. */
  void* subarray() const;

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
  std::unordered_map<std::string, AttributeBuffer> attr_buffers_;

  /** The fragment metadata. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /**
   * The layout of the cells in the result of the subarray. Note
   * that this may not be the same as what the user set to the
   * query, as the Reader may calibrate it to boost performance.
   */
  Layout layout_;

  /** To handle incomplete read queries. */
  ReadState read_state_;

  /** Read state for Subarray queries. */
  ReadState2 read_state_2_;

  /**
   * If `true`, then the dense array will be read in "sparse mode", i.e.,
   * the sparse read algorithm will be executing, returning results only
   * for the non-empty cells.
   */
  bool sparse_mode_;

  /** The storage manager. */
  StorageManager* storage_manager_;

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

  /** Clears the read state. */
  void clear_read_state();

  /**
   * Deletes the tiles on a particular attribute from all input fragments
   * based on the tile info in `tiles`.
   *
   * @param attr The attribute name.
   * @param tiles The tiles will be deleted from `tiles`.
   * @return void
   */
  void clear_tiles(const std::string& attr, OverlappingTileVec* tiles) const;

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
   * Compute the maximal cell ranges of contiguous cell positions.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to compute the ranges from.
   * @param cell_ranges The cell ranges to compute.
   * @return Status
   */
  template <class T>
  Status compute_cell_ranges(
      const OverlappingCoordsVec<T>& coords,
      OverlappingCellRangeList* cell_ranges) const;

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
      const OverlappingCoordsVec<T>& coords,
      OverlappingTileVec* tiles,
      OverlappingCellRangeList* overlapping_cell_ranges);

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
      const OverlappingTileVec& tiles, OverlappingCoordsVec<T>* coords) const;

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
      const OverlappingTile* tile, OverlappingCoordsVec<T>* coords) const;

  /**
   * Retrieves the coordinates that overlap the input N-dimensional range
   * from the input overlapping tile.
   *
   * @tparam T The coords type.
   * @param The overlapping tile.
   * @param range An N-dimensional range (where N is equal to the number
   *     of dimensions of the array).
   * @param coords The overlapping coordinates to retrieve.
   * @return Status
   */
  template <class T>
  Status compute_overlapping_coords_2(
      const OverlappingTile* tile,
      const std::vector<const T*>& range,
      OverlappingCoordsVec<T>* coords) const;

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
   * Computes the coordinates overlapping with each range of the query
   * subarray.
   *
   * @tparam T The domain type.
   * @param single_fragment For each range, it indicates whereas all
   *     overlapping tiles come from a single fragment.
   * @param tiles The tiles to read the coordinates from.
   * @param tile_map This is an auxialiary map that helps finding the
   *     tiles overlapping with each range.
   * @param range_coords The overlapping coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  template <class T>
  Status compute_range_coords(
      const std::vector<bool>& single_fragment,
      const OverlappingTileVec& tiles,
      const OverlappingTileMap& tile_map,
      std::vector<OverlappingCoordsVec<T>>* range_coords);

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
   * Computes the coordinates overlapping with a given range of the query
   * subarray.
   *
   * @tparam T The domain type.
   * @param range_idx The range to focus on.
   * @param tiles The tiles to read the coordinates from.
   * @param tile_map This is an auxialiary map that helps finding the
   *     tiles overlapping with each range.
   * @param range_coords The overlapping coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  template <class T>
  Status compute_range_coords(
      uint64_t range_idx,
      const OverlappingTileVec& tiles,
      const OverlappingTileMap& tile_map,
      OverlappingCoordsVec<T>* range_coords);

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
   * Computes the final subarray overlapping coordinates, which will be
   * deduplicated and sorted on the specified subarray layout.
   *
   * @tparam T The domain type.
   * @param range_coords The overlapping coordinates for each subarray range.
   * @param coords The result coordinates to be retrieved.
   * @return Status
   *
   * @note the function will try to gradually clean up ``range_coords`` as
   *     it is done processing its elements to quickly reclaim memory.
   */
  template <class T>
  Status compute_subarray_coords(
      std::vector<OverlappingCoordsVec<T>>* range_coords,
      OverlappingCoordsVec<T>* coords);

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
   * Computes info about the overlapping tiles, such as which fragment they
   * belong to, the tile index and the type of overlap. The tile vector
   * contains unique info about the tiles. The function also computes
   * a map from fragment index and tile id to an overlapping tile to keep
   * track of the unique tile info for subarray ranges that overlap with
   * common tiles.
   *
   * @tparam T The coords type.
   * @param tiles The tiles to be computed.
   * @param tile_map The tile map to be computed.
   * @param single_fragment Each element corresponds to a range of the
   *     subarray and is set to ``true`` if all the overlapping
   *     tiles come from a single fragment for that range.
   * @return Status
   */
  template <class T>
  Status compute_overlapping_tiles_2(
      OverlappingTileVec* tiles,
      OverlappingTileMap* tile_map,
      std::vector<bool>* single_fragment);

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
   * Computes the tile coordinates for each OverlappingCoords and populates
   * their `tile_coords_` field. The tile coordinates are placed in a
   * newly-allocated array.
   *
   * @tparam T The coords type.
   * @param all_tile_coords Pointer to the memory allocated by this function.
   * @param coords The overlapping coords list
   * @return Status
   */
  template <class T>
  Status compute_tile_coords(
      std::unique_ptr<T[]>* all_tile_coords,
      OverlappingCoordsVec<T>* coords) const;

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
   * Copies the cells for the input attribute and cell ranges, into
   * the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges);

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
   * Copies the cells for the input **fixed-sized** attribute and cell
   * ranges, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_fixed_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges);

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
   * Copies the cells for the input **var-sized** attribute and cell
   * ranges, into the corresponding result buffers.
   *
   * @param attribute The targeted attribute.
   * @param cell_ranges The cell ranges to copy cells for.
   * @return Status
   */
  Status copy_var_cells(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges);

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
   * and variable-length data, for the given list of cell ranges.
   *
   * @param attribute The variable-length attribute
   * @param cell_ranges The cell ranges to compute destinations for.
   * @param offset_offsets_per_cr Output to hold one vector per cell range, and
   *    one element per cell in the range. The elements are the destination
   *    offsets for the attribute's offsets.
   * @param var_offsets_per_cr Output to hold one vector per cell range, and
   *    one element per cell in the range. The elements are the destination
   *    offsets for the attribute's variable-length data.
   * @param total_offset_size Output set to the total size in bytes of the
   *    offsets in the given list of cell ranges.
   * @param total_var_size Output set to the total size in bytes of the
   *    attribute's variable-length in the given list of cell ranges.
   * @return Status
   */
  Status compute_var_cell_destinations(
      const std::string& attribute,
      const OverlappingCellRangeList& cell_ranges,
      std::vector<std::vector<uint64_t>>* offset_offsets_per_cr,
      std::vector<std::vector<uint64_t>>* var_offsets_per_cr,
      uint64_t* total_offset_size,
      uint64_t* total_var_size) const;

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
   * Deduplicates the input coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @tparam T The coords type.
   * @param coords The coordinates to dedup.
   * @return Status
   */
  template <class T>
  Status dedup_coords(OverlappingCoordsVec<T>* coords) const;

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

  /** Performs a read on a dense array. */
  Status dense_read();

  /**
   * Performs a read on a dense array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read();

  /**
   * Performs a read on a dense array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read_2();

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status fill_coords();

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
   * Filters the tiles on all attributes from all input fragments based on the
   * tile info in `tiles`.
   *
   * @param tiles Vector containing tiles to be filtered.
   * @param ensure_coords If true (the default), always filter the coordinate
   *    tiles.
   * @return Status
   */
  Status filter_all_tiles(
      OverlappingTileVec* tiles, bool ensure_coords = true) const;

  /**
   * Filters the tiles on a particular attribute from all input fragments
   * based on the tile info in `tiles`.
   *
   * @param attribute Attribute whose tiles will be filtered
   * @param tiles Vector containing the tiles to be filtered
   * @return Status
   */
  Status filter_tiles(
      const std::string& attribute, OverlappingTileVec* tiles) const;

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
   * Gets all the coordinates of the input tile into `coords`.
   *
   * @tparam T The coords type.
   * @param tile The overlapping tile to read the coordinates from.
   * @param coords The overlapping coordinates to copy into.
   * @return Status
   */
  template <class T>
  Status get_all_coords(
      const OverlappingTile* tile, OverlappingCoordsVec<T>* coords) const;

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

  /**
   * Handles the coordinates that fall between `start` and `end`.
   * This function will either skip the coordinates if they belong to an
   * older fragment than that of the current dense cell range, or include them
   * as results and split the dense cell range.
   *
   * @tparam T The domain type
   * @param cur_tile The current tile.
   * @param cur_tile_coords The current tile coordinates.
   * @param start The start of the dense cell range.
   * @param end The end of the dense cell range.
   * @param coords_size The coordintes size.
   * @param coords The list of coordinates.
   * @param coords_it The iterator pointing at the current coordinates.
   * @param coords_pos The position of the current coordinates in their tile.
   * @param coords_fidx The fragment index of the current coordinates.
   * @param coords_tile_coords The global tile coordinates of the tile the
   *     current cell coordinates belong to
   * @param overlapping_cell_ranges The result cell ranges (to be updated
   *     by inserting a dense cell range for a coordinate result, or by
   *     splitting the current dense cell range).
   * @return Status
   */
  template <class T>
  Status handle_coords_in_dense_cell_range(
      const OverlappingTile* cur_tile,
      const T* cur_tile_coords,
      uint64_t* start,
      uint64_t end,
      uint64_t coords_size,
      const OverlappingCoordsVec<T>& coords,
      typename OverlappingCoordsVec<T>::const_iterator* coords_it,
      uint64_t* coords_pos,
      unsigned* coords_fidx,
      std::vector<T>* coords_tile_coords,
      OverlappingCellRangeList* overlapping_cell_ranges) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /** Initializes the read state. */
  Status init_read_state();

  /** Initializes the read state. */
  Status init_read_state_2();

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
   * Optimize the layout for 1D arrays. Specifically, if the array
   * is 1D, the layout should be global order which produces
   * equivalent results offering faster processing.
   */
  void optimize_layout_for_1D();

  /**
   * Retrieves the tiles on all attributes from all input fragments based on
   * the tile info in `tiles`.
   *
   * @param tiles The retrieved tiles will be stored in `tiles`.
   * @param ensure_coords If true (the default), always read the coordinate
   * tiles.
   * @return Status
   */
  Status read_all_tiles(
      OverlappingTileVec* tiles, bool ensure_coords = true) const;

  /**
   * Retrieves the tiles on a particular attribute from all input fragments
   * based on the tile info in `tiles`.
   *
   * @param attr The attribute name.
   * @param tiles The retrieved tiles will be stored in `tiles`.
   * @return Status
   */
  Status read_tiles(const std::string& attr, OverlappingTileVec* tiles) const;

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
   * Retrieves the tiles on a particular attribute from all input fragments
   * based on the tile info in `tiles`.
   *
   * The reads are done asynchronously, and futures for each read operation are
   * added to the output parameter.
   *
   * @param attribute The attribute name.
   * @param tiles The retrieved tiles will be stored in `tiles`.
   * @param tasks Vector to hold futures for the read tasks.
   * @return Status
   */
  Status read_tiles(
      const std::string& attribute,
      OverlappingTileVec* tiles,
      std::vector<std::future<Status>>* tasks) const;

  /**
   * Retrieves the tiles on a particular attribute and stores it in the
   * appropriate result tile.
   *
   * The reads are done asynchronously, and futures for each read operation are
   * added to the output parameter.
   *
   * @param attribute The attribute name.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param tasks Vector to hold futures for the read tasks.
   * @return Status
   */
  Status read_tiles(
      const std::string& attribute,
      const std::vector<ResultTile*>& result_tiles,
      std::vector<std::future<Status>>* tasks) const;

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /**
   * Sorts the input coordinates according to the subarray layout.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to sort.
   * @return Status
   */
  template <class T>
  Status sort_coords(OverlappingCoordsVec<T>* coords) const;

  /**
   * Sorts the input coordinates according to the subarray layout.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to sort.
   * @return Status
   */
  template <class T>
  Status sort_coords_2(OverlappingCoordsVec<T>* coords) const;

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

  /**
   * Performs a read on a sparse array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status sparse_read_2();

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
