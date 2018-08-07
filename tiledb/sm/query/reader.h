/**
 * @file   reader.h
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
 * This file defines class Reader.
 */

#ifndef TILEDB_READER_H
#define TILEDB_READER_H

#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/query/types.h"
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
    bool full_overlap_;
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

  /** A cell range belonging to a particular overlapping tile. */
  struct OverlappingCellRange {
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
  using OverlappingCoordsList = std::vector<OverlappingCoords<T>>;

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
   * Serialize a writer to capnp format
   * @param writerBuilder
   * @return  Status
   */
  Status capnp(::QueryReader::Builder* queryReaderBuilder) const;

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /** Returns the number of fragments involved in the (read) query. */
  unsigned fragment_num() const;

  /**
   * Deserialize from a capnp message
   * @param writerReader
   * @return Status
   */
  Status from_capnp(::QueryReader::Reader* queryReader);

  /** Returns a vector with the fragment URIs. */
  std::vector<URI> fragment_uris() const;

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

  /** Performs a read query using its set members. */
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
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute, void* buffer, uint64_t* buffer_size);

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
   * @return Status
   */
  Status set_buffer(
      const std::string& attribute,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
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

  /*
   * Return the subarray
   * @return subarray
   */
  void* subarray() const;

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

  /** The storage manager. */
  StorageManager* storage_manager_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Clears the read state. */
  void clear_read_state();

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
      const OverlappingCoordsList<T>& coords,
      OverlappingCellRangeList* cell_ranges) const;

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
      const OverlappingCoordsList<T>& coords,
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
      const OverlappingTileVec& tiles, OverlappingCoordsList<T>* coords) const;

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
      const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const;

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
      OverlappingCoordsList<T>* coords) const;

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
   * Deduplicates the input coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @tparam T The coords type.
   * @param coords The coordinates to dedup.
   * @return Status
   */
  template <class T>
  Status dedup_coords(OverlappingCoordsList<T>* coords) const;

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
  void fill_coords_row_slab(
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
  void fill_coords_col_slab(
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
      const OverlappingTile* tile, OverlappingCoordsList<T>* coords) const;

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
   * @param coords_tile The tile where the current coordinates belong to.
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
      const OverlappingCoordsList<T>& coords,
      typename OverlappingCoordsList<T>::const_iterator* coords_it,
      const OverlappingTile* coords_tile,
      uint64_t* coords_pos,
      unsigned* coords_fidx,
      std::vector<T>* coords_tile_coords,
      OverlappingCellRangeList* overlapping_cell_ranges) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /** Initializes the read state. */
  Status init_read_state();

  /**
   * Initializes a fixed-sized tile.
   *
   * @param attribute The attribute the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(const std::string& attribute, Tile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param attribute The attribute the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      const std::string& attribute, Tile* tile, Tile* tile_var) const;

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
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /**
   * Sorts the input coordinates according to the input layout.
   *
   * @tparam T The coords type.
   * @param coords The coordinates to sort.
   * @return Status
   */
  template <class T>
  Status sort_coords(OverlappingCoordsList<T>* coords) const;

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

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_H
