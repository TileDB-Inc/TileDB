/**
 * @file   sparse_index_reader_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class SparseIndexReaderBase.
 */

#ifndef TILEDB_SPARSE_INDEX_READER_BASE_H
#define TILEDB_SPARSE_INDEX_READER_BASE_H

#include <queue>
#include "reader_base.h"
#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class MemoryTracker;
class Subarray;

class FragIdx {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  FragIdx() = default;

  FragIdx(uint64_t tile_idx, uint64_t cell_idx)
      : tile_idx_(tile_idx)
      , cell_idx_(cell_idx) {
  }

  /** Move constructor. */
  FragIdx(FragIdx&& other) noexcept {
    // Swap with the argument
    swap(other);
  }

  /** Move-assign operator. */
  FragIdx& operator=(FragIdx&& other) {
    // Swap with the argument
    swap(other);

    return *this;
  }

  DISABLE_COPY_AND_COPY_ASSIGN(FragIdx);

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(FragIdx& frag_tile_idx) {
    std::swap(tile_idx_, frag_tile_idx.tile_idx_);
    std::swap(cell_idx_, frag_tile_idx.cell_idx_);
  }

  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Tile index. */
  uint64_t tile_idx_;

  /** Cell index. */
  uint64_t cell_idx_;
};

class MemoryBudget {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  MemoryBudget() = delete;

  MemoryBudget(Config& config, std::string reader_string) {
    refresh_config(config, reader_string);
  }

  DISABLE_COPY_AND_COPY_ASSIGN(MemoryBudget);
  DISABLE_MOVE_AND_MOVE_ASSIGN(MemoryBudget);

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Refresh the configuration.
   *
   * @param config Config object.
   * @param reader_string String to identify the reader settings to load.
   */
  void refresh_config(Config& config, std::string reader_string) {
    bool found = false;
    throw_if_not_ok(
        config.get<uint64_t>("sm.mem.total_budget", &total_budget_, &found));
    assert(found);

    throw_if_not_ok(config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_coords",
        &ratio_coords_,
        &found));
    assert(found);

    throw_if_not_ok(config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_tile_ranges",
        &ratio_tile_ranges_,
        &found));
    assert(found);

    throw_if_not_ok(config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_array_data",
        &ratio_array_data_,
        &found));
    assert(found);

    throw_if_not_ok(config.get<uint64_t>(
        "sm.mem.tile_upper_memory_limit", &tile_upper_memory_limit_, &found));
    assert(found);
  }

  /** @return Total memory budget for the reader. */
  uint64_t total_budget() {
    return total_budget_;
  }

  /**
   * @return Ratio of the budget dedicated to loading coordinate tiles into
   * memory.
   */
  double ratio_coords() {
    return ratio_coords_;
  }

  /**
   * @return Ratio of the budget dedicated to loading tile ranges into
   * memory. Tile ranges contain ranges of tiles, per fragments to consider for
   * results.
   */
  double ratio_tile_ranges() {
    return ratio_tile_ranges_;
  }

  /**
   * @return Ratio of the budget dedicated to loading tile array data into
   * memory. Array data contains rtrees, tile offsets, fragment footers, etc.
   */
  double ratio_array_data() {
    return ratio_array_data_;
  }

  /**
   * @return Returns the tile upper memory limit, which is used to limit the
   * amount of tile data loaded in memory at any given time.
   */
  uint64_t tile_upper_memory_limit() {
    return tile_upper_memory_limit_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Total memory budget. */
  uint64_t total_budget_;

  /** How much of the memory budget is reserved for coords. */
  double ratio_coords_;

  /** How much of the memory budget is reserved for tile ranges. */
  double ratio_tile_ranges_;

  /** How much of the memory budget is reserved for array data. */
  double ratio_array_data_;

  /** Target upper memory limit for tiles. */
  uint64_t tile_upper_memory_limit_;

  /** Mutex protecting memory budget variables. */
  std::mutex used_memory_mtx_;
};

class IgnoredTile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  IgnoredTile() = default;

  IgnoredTile(uint64_t frag_idx, uint64_t tile_idx)
      : frag_idx_(frag_idx)
      , tile_idx_(tile_idx) {
  }

  /** Move constructor. */
  IgnoredTile(IgnoredTile&& other) noexcept {
    // Swap with the argument
    swap(other);
  }

  /** Move-assign operator. */
  IgnoredTile& operator=(IgnoredTile&& other) {
    // Swap with the argument
    swap(other);

    return *this;
  }

  DISABLE_COPY_AND_COPY_ASSIGN(IgnoredTile);

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  inline uint64_t frag_idx() const {
    return frag_idx_;
  }

  inline uint64_t tile_idx() const {
    return tile_idx_;
  }

  bool operator==(const IgnoredTile& v) const {
    return frag_idx_ == v.frag_idx_ && tile_idx_ == v.tile_idx_;
  }

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(IgnoredTile& other) {
    std::swap(frag_idx_, other.frag_idx_);
    std::swap(tile_idx_, other.tile_idx_);
  }

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  // Frag index.
  uint64_t frag_idx_;

  // Tile index.
  uint64_t tile_idx_;
};

struct ignored_tile_hash {
  size_t operator()(IgnoredTile const& v) const {
    std::size_t h1 = std::hash<uint64_t>()(v.frag_idx());
    std::size_t h2 = std::hash<uint64_t>()(v.tile_idx());
    return h1 ^ h2;
  }
};

/** Processes read queries. */
class SparseIndexReaderBase : public ReaderBase {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** The state for a read sparse global order query. */
  struct ReadState {
    /** The tile index inside of each fragments. */
    std::vector<FragIdx> frag_idx_;

    /** Is the reader done with the query. */
    bool done_adding_result_tiles_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseIndexReaderBase(
      std::string reader_string,
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      std::optional<QueryCondition>& condition,
      bool skip_checks_serialization,
      bool include_coords);

  /** Destructor. */
  ~SparseIndexReaderBase() = default;

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Returns the current read state.
   *
   * @return pointer to the read state.
   */
  const ReadState* read_state() const;

  /**
   * Returns the current read state.
   *
   * @return pointer to the read state.
   */
  ReadState* read_state();

  /**
   * Resize the output buffers to the correct size after copying.
   *
   * @param cells_copied Number of cells copied.
   *
   * @return Status.
   */
  Status resize_output_buffers(uint64_t cells_copied);

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** Read state. */
  ReadState read_state_;

  /** Memory budget. */
  MemoryBudget memory_budget_;

  /** Have we loaded all tiles for this fragment. */
  std::vector<uint8_t> all_tiles_loaded_;

  /** Include coordinates when loading tiles. */
  bool include_coords_;

  /** Dimension names. */
  std::vector<std::string> dim_names_;

  /** Are dimensions var sized. */
  std::vector<bool> is_dim_var_size_;

  /**
   * Reverse sorted vector, per fragments, of tiles ranges in the subarray, if
   * set.
   */
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> result_tile_ranges_;

  /** Mutex protecting memory budget variables. */
  std::mutex used_memory_mtx_;

  /** Memory tracker object for the array. */
  MemoryTracker* array_memory_tracker_;

  /** Memory used for coordinates tiles. */
  uint64_t memory_used_for_coords_total_;

  /** Memory used for result tile ranges. */
  uint64_t memory_used_result_tile_ranges_;

  /** Are we in elements mode. */
  bool elements_mode_;

  /** Names of dim/attr loaded for query condition. */
  std::vector<std::string> qc_loaded_attr_names_;

  /* Are the users buffers full. */
  bool buffers_full_;

  /** List of tiles to ignore. */
  std::unordered_set<IgnoredTile, ignored_tile_hash> ignored_tiles_;

  /** Are we doing deletes consolidation (without purge option). */
  bool deletes_consolidation_no_purge_;

  /** Do we allo partial tile offset loading for this query? */
  bool partial_tile_offsets_loading_;

  /** Var dimensions/attributes for which to load tile var sizes. */
  std::vector<std::string> var_size_to_load_;

  /** Attributes for which to load tile offsets. */
  std::vector<std::string> attr_tile_offsets_to_load_;

  /** Per fragment tile offsets memory usage. */
  std::vector<uint64_t> per_frag_tile_offsets_usage_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** @return Available memory. */
  uint64_t available_memory();

  /**
   * Computes the required size for loading tile offsets, per fragments.
   *
   * @return Required memory for loading tile offsets, per fragments.
   */
  std::vector<uint64_t> tile_offset_sizes();

  /**
   * Returns if there is any condition to be applied post deduplication. This
   * will return true if we have:
   *   A query condition.
   *   Delete metadata (delete timestamp condition).
   *   Delete conditions (but not in consolidation mode).
   *
   * @param frag_meta Fragment metadata.
   * @return true if there is any condition to be applied post deduplication.
   */
  bool has_post_deduplication_conditions(FragmentMetadata& frag_meta);

  /**
   * Return how many cells were copied to the users buffers so far.
   *
   * @param names Attribute/dimensions to compute for.
   *
   * @return Number of cells copied.
   */
  uint64_t cells_copied(const std::vector<std::string>& names);

  /**
   * Get the coordinate tiles size for a dimension.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   *
   * @return Tiles size.
   */
  template <class BitmapType>
  uint64_t get_coord_tiles_size(unsigned dim_num, unsigned f, uint64_t t);

  /**
   * Load result tile ranges and dimension/attributes to load tile offsets for.
   *
   * @return Status.
   */
  Status load_initial_data();

  /**
   * Returns the tile offset size for the list of relevant fragments.
   *
   * @param relevant_fragments Relevant fragments to load offsets for.
   * @return Total in memory size.
   */
  uint64_t tile_offsets_size(const RelevantFragments& relevant_fragments);

  /**
   * Load all tile offsets.
   *
   * @param relevant_fragments Relevant fragments to load offsets for.
   */
  void load_tile_offsets_for_fragments(
      const RelevantFragments& relevant_fragments);

  /**
   * Read and unfilter coord tiles.
   *
   * @param result_tiles The result tiles to process.
   *
   * @return Status.
   */
  Status read_and_unfilter_coords(const std::vector<ResultTile*>& result_tiles);

  /**
   * Compute tile bitmaps.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   * */
  template <class BitmapType>
  Status compute_tile_bitmaps(std::vector<ResultTile*>& result_tiles);

  /**
   * Apply query condition.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   */
  template <class ResultTileType, class BitmapType>
  Status apply_query_condition(std::vector<ResultTile*>& result_tiles);

  /**
   * Read and unfilter as many attributes as can fit in the memory budget and
   * return the names loaded in 'names_to_copy'. Also keep the 'buffer_idx'
   * updated to keep track of progress.
   *
   * @param names Attribute/dimensions to compute for.
   * @param mem_usage_per_attr Computed per attribute memory usage.
   * @param buffer_idx Stores/return the current buffer index in process.
   * @param result_tiles Result tiles to process.
   *
   * @return Status, index_to_copy.
   */
  tuple<Status, optional<std::vector<uint64_t>>> read_and_unfilter_attributes(
      const std::vector<std::string>& names,
      const std::vector<uint64_t>& mem_usage_per_attr,
      uint64_t* buffer_idx,
      std::vector<ResultTile*>& result_tiles);

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   *
   * @return Status.
   */
  Status add_extra_offset();

  /**
   * Remove a result tile range for a specific fragment.
   *
   * @param f Fragment index.
   */
  void remove_result_tile_range(uint64_t f);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_INDEX_READER_BASE_H
