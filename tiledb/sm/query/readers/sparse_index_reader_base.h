/**
 * @file   sparse_index_reader_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

namespace tiledb::sm {

class Array;
class ArraySchema;
class MemoryTracker;
class Subarray;

/**
 * Simple class that stores the progress for a fragment as a tile/cell index.
 */
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

  MemoryBudget(
      Config& config,
      std::string reader_string,
      optional<uint64_t> total_budget)
      : total_budget_(total_budget.value_or(0))
      , memory_budget_from_query_(total_budget) {
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
    if (!memory_budget_from_query_.has_value()) {
      total_budget_ =
          config.get<uint64_t>("sm.mem.total_budget", Config::must_find);
    }

    ratio_coords_ = config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_coords", Config::must_find);

    ratio_tile_ranges_ = config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_tile_ranges",
        Config::must_find);

    ratio_array_data_ = config.get<double>(
        "sm.mem.reader." + reader_string + ".ratio_array_data",
        Config::must_find);

    tile_upper_memory_limit_ = config.get<uint64_t>(
        "sm.mem.tile_upper_memory_limit", Config::must_find);
  }

  /** @return Total memory budget for the reader. */
  uint64_t total_budget() {
    return total_budget_;
  }

  /**
   * @return Portion of the total memory budget dedicated to loading coordinate
   * tiles.
   */
  double coordinates_budget() {
    return total_budget_ * ratio_coords_;
  }

  /**
   * @return Ratio of the budget dedicated to loading coordinate tiles into
   * memory.
   */
  double ratio_coords() const {
    return ratio_coords_;
  }

  /**
   * @return Ratio of the budget dedicated to loading tile ranges into
   * memory. Tile ranges contain ranges of tiles, per fragments to consider for
   * results.
   */
  double ratio_tile_ranges() const {
    return ratio_tile_ranges_;
  }

  /**
   * @return Ratio of the budget dedicated to loading tile array data into
   * memory. Array data contains rtrees, tile offsets, fragment footers, etc.
   */
  double ratio_array_data() const {
    return ratio_array_data_;
  }

  /**
   * @return Returns the tile upper memory limit, which is used to limit the
   * amount of tile data loaded in memory at any given time.
   */
  uint64_t tile_upper_memory_limit() const {
    return tile_upper_memory_limit_;
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Total memory budget. */
  uint64_t total_budget_;

  /** Total memory budget if overridden by the query. */
  optional<uint64_t> memory_budget_from_query_;

  /** How much of the memory budget is reserved for coords. */
  double ratio_coords_;

  /** How much of the memory budget is reserved for tile ranges. */
  double ratio_tile_ranges_;

  /** How much of the memory budget is reserved for array data. */
  double ratio_array_data_;

  /** Target upper memory limit for tiles. */
  uint64_t tile_upper_memory_limit_;
};

/**
 * Simple class that stores the fragment/tile index of a tile that will be
 * ignored by further iterations as we deteremined it has no results.
 */
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

/**
 * Identifies an order in which to load result tiles.
 * See `SparseIndexReader::preprocess_tile_order_`.
 */
struct PreprocessTileOrder {
  bool enabled_;
  size_t cursor_;
  std::vector<ResultTileId> tiles_;

  bool has_more_tiles() const {
    return cursor_ < tiles_.size();
  }
};

/**
 * Processes sparse read queries by keeping progress in fragments as indexes.
 */
class SparseIndexReaderBase : public ReaderBase {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * The state for an index query. This read state cannot be reconstructed as it
   * contains progress for data that was copied to the user buffers and returned
   * to the user. The progress is saved, per fragment, as a tile and cell index.
   *
   * TBD: done_adding_result_tiles_ might be moved from here. We have to see if
   * it is really required to determine if a query is incomplete from the client
   * side of a cloud request.
   */
  class ReadState {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /** Delete default constructor. */
    ReadState() = delete;

    /** Constructor.
     * @param frag_idxs_len The length of the fragment index vector.
     */
    ReadState(size_t frag_idxs_len)
        : frag_idx_(frag_idxs_len) {
    }

    /** Constructor used in deserialization. */
    ReadState(std::vector<FragIdx>&& frag_idx, bool done_adding_result_tiles)
        : frag_idx_(std::move(frag_idx))
        , done_adding_result_tiles_(done_adding_result_tiles) {
    }

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /**
     * Return whether the tiles that will be processed are loaded in memory.
     * @return Done adding result tiles.
     */
    inline bool done_adding_result_tiles() const {
      return done_adding_result_tiles_;
    }

    /**
     * Sets the flag that determines whether the tiles that will be processed
     * are loaded in memory.
     * @param done_adding_result_tiles Done adding result tiles.
     */
    inline void set_done_adding_result_tiles(bool done_adding_result_tiles) {
      done_adding_result_tiles_ = done_adding_result_tiles;
    }

    /**
     * Sets a value in the fragment index vector.
     * @param idx The index of the vector.
     * @param val The value to set frag_idx[idx] to.
     */
    inline void set_frag_idx(uint64_t idx, FragIdx val) {
      if (idx >= frag_idx_.size()) {
        throw std::runtime_error(
            "ReadState::set_frag_idx: idx greater than frag_idx_'s size.");
      }
      frag_idx_[idx] = std::move(val);
    }

    /**
     * Returns a read-only version of the fragment index vector.
     * @return The fragment index vector.
     */
    const std::vector<FragIdx>& frag_idx() const {
      return frag_idx_;
    }

    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

   private:
    /** The tile index inside of each fragments. */
    std::vector<FragIdx> frag_idx_;

    /** Have all tiles to be processed been loaded in memory? */
    bool done_adding_result_tiles_;
  };

  /**
   * Temporary read state that can be recomputed using just the read state. This
   * contains information about the tile ranges that still need to be processed
   * if a subarray is set (with memory used) and for which fragments we loaded
   * all tiles into memory. It also contains which tiles contains no results so
   * they should be ignored by further iterations. It should be used in
   * conjunction with the loaded tiles in the respective readers. Eventually,
   * those will also move to this class but that requires the removal of
   * every memory allocated things from result tiles into separate objects so
   * that the list of result tiles is only a fragment/tile index and the objects
   * are not different anymore.
   */
  class TransientReadState : public ITileRange {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    TransientReadState() = delete;

    TransientReadState(uint64_t num_frags)
        : tile_ranges_(num_frags)
        , memory_used_tile_ranges_(0)
        , all_tiles_loaded_(num_frags, false) {
    }

    DISABLE_COPY_AND_COPY_ASSIGN(TransientReadState);
    DISABLE_MOVE_AND_MOVE_ASSIGN(TransientReadState);

    /* ********************************* */
    /*          PUBLIC METHODS           */
    /* ********************************* */

    /**
     * Return the tile ranges vector for a particular fragment.
     *
     * @param f Fragment index.
     * @return Tile ranges.
     */
    std::vector<std::pair<uint64_t, uint64_t>>& tile_ranges(const unsigned f) {
      return tile_ranges_[f];
    }

    /**
     * Return if all tiles are loaded for a fragment.
     *
     * @param f Fragment index.
     * @return All tiles loaded.
     */
    bool all_tiles_loaded(const unsigned f) const {
      return all_tiles_loaded_[f];
    }

    /**
     * Set the all tiles loaded for a fragment.
     *
     * @param f Fragment index.
     */
    void set_all_tiles_loaded(const unsigned f) {
      all_tiles_loaded_[f] = true;
    }

    /** @return Number of fragments left to process. */
    unsigned num_fragments_to_process() const {
      unsigned num = 0;
      for (auto all_loaded : all_tiles_loaded_) {
        num += !all_loaded;
      }

      return num;
    }

    /** @return Are we done adding all result tiles to the list. */
    bool done_adding_result_tiles() const {
      bool ret = true;
      for (auto& b : all_tiles_loaded_) {
        ret &= b != 0;
      }

      return ret;
    }

    /**
     * Remove the last tile range for a fragment.
     *
     * @param f Fragment index.
     */
    void remove_tile_range(unsigned f) {
      tile_ranges_[f].pop_back();
      memory_used_tile_ranges_ -= sizeof(std::pair<uint64_t, uint64_t>);
    }

    /** @ereturn Memory usage for the tile ranges. */
    uint64_t memory_used_tile_ranges() const {
      return memory_used_tile_ranges_;
    }

    /** Clears all tile ranges data. */
    void clear_tile_ranges() override {
      for (auto& tr : tile_ranges_) {
        tr.clear();
      }
      memory_used_tile_ranges_ = 0;
    }

    /**
     * Add a tile range for a fragment.
     *
     * @param f Fragment index.
     * @param min Min tile index for the range.
     * @param max Max tile index for the range.
     */
    void add_tile_range(unsigned f, uint64_t min, uint64_t max) override {
      tile_ranges_[f].emplace_back(min, max);
    }

    /** Signals we are done adding tile ranges. */
    void done_adding_tile_ranges() override {
      // Compute the size of the tile ranges structure and mark empty fragments
      // as fully loaded.
      for (uint64_t i = 0; i < tile_ranges_.size(); i++) {
        memory_used_tile_ranges_ +=
            tile_ranges_[i].size() * sizeof(std::pair<uint64_t, uint64_t>);
        if (tile_ranges_[i].size() == 0) {
          all_tiles_loaded_[i] = true;
        }
      }
    }

    /**
     * Add a tile that should be ignored by later iterations because it contains
     * no results.
     *
     * @param rt Result tile.
     */
    void add_ignored_tile(ResultTile& rt) {
      std::unique_lock<std::mutex> lck(ignored_tiles_mutex_);
      ignored_tiles_.emplace(rt.frag_idx(), rt.tile_idx());
    }

    /**
     * Returns true if the tile should be ignored.
     *
     * @param f Fragment index.
     * @param t Tile index.
     * @return If the tile should be ignored or not.
     */
    bool is_ignored_tile(unsigned f, uint64_t t) {
      return ignored_tiles_.count(IgnoredTile(f, t)) != 0;
    }

    /** @return number of ignored tiles. */
    bool num_ignored_tiles() {
      return ignored_tiles_.size();
    }

   private:
    /* ********************************* */
    /*        PRIVATE ATTRIBUTES         */
    /* ********************************* */

    /**
     * Reverse sorted vector, per fragments, of tiles ranges in the subarray, if
     * set.
     */
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> tile_ranges_;

    /** Memory used for tile ranges. */
    std::atomic<uint64_t> memory_used_tile_ranges_;

    /** Have we loaded all tiles for this fragment. */
    std::vector<uint8_t> all_tiles_loaded_;

    /** List of tiles to ignore. */
    std::unordered_set<IgnoredTile, ignored_tile_hash> ignored_tiles_;

    /** Mutex protecting ignored_tiles_. */
    std::mutex ignored_tiles_mutex_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseIndexReaderBase(
      std::string reader_string,
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StrategyParams& params,
      bool include_coords);

  /** Destructor. */
  ~SparseIndexReaderBase() = default;

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Returns the current read state.
   *
   * @return const reference to the read state.
   */
  const ReadState& read_state() const;

  /**
   * Sets the new read state. Used only for deserialization.
   *
   * @param read_state New read_state value.
   */
  void set_read_state(ReadState read_state);

  const PreprocessTileOrder preprocess_tile_order() const;

  /**
   * Sets the preprocess tile order cursor. Used only for deserialization
   *
   * @param cursor New cursor value.
   */
  virtual void set_preprocess_tile_order_cursor(uint64_t cursor);

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /**
   * State for the optional mode to preprocess the tiles across
   * all fragments and merge them into a single list which identifies
   * the order they should be read in.
   *
   * This is used by SparseGlobalOrderReader to merge the tiles
   * into a single globally-ordered list prior to loading.
   * Tile identifiers in this list are sorted using their starting ranges
   * and have already had the subarray (if any) applied.
   *
   * (this is declared here for serialization purposes)
   */
  PreprocessTileOrder preprocess_tile_order_;

  /** Read state. */
  ReadState read_state_;

  /** Transient read state. */
  TransientReadState tmp_read_state_;

  /** Memory budget. */
  MemoryBudget memory_budget_;

  /** Include coordinates when loading tiles. */
  bool include_coords_;

  /** Dimension names. */
  std::vector<std::string> dim_names_;

  /** Memory used for coordinates tiles. */
  std::atomic<uint64_t> memory_used_for_coords_total_;

  /** Are we in elements mode. */
  bool elements_mode_;

  /** Do we allow partial tile offset loading for this query? */
  bool partial_tile_offsets_loading_;

  /** Var dimensions/attributes for which to load tile var sizes. */
  std::vector<std::string> var_size_to_load_;

  /** Attributes for which to load tile offsets. */
  std::vector<std::string> attr_tile_offsets_to_load_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** @return Available memory. */
  uint64_t available_memory();

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
   */
  template <class BitmapType>
  void compute_tile_bitmaps(std::vector<ResultTile*>& result_tiles);

  /**
   * Apply query condition.
   *
   * @param result_tiles Result tiles to process.
   */
  template <class ResultTileType, class BitmapType>
  void apply_query_condition(std::vector<ResultTile*>& result_tiles);

  /**
   * Read and unfilter as many attributes as can fit in the memory budget and
   * return the names loaded in 'names_to_copy'. Also keep the 'buffer_idx'
   * updated to keep track of progress.
   *
   * @param names Attribute/dimensions to compute for.
   * @param mem_usage_per_attr Computed per attribute memory usage.
   * @param buffer_idx Stores/return the current buffer index in process.
   * @param result_tiles Result tiles to process.
   * @param agg_only Are we loading for aggregates only field.
   *
   * @return names_to_copy.
   */
  std::vector<std::string> read_and_unfilter_attributes(
      const std::vector<std::string>& names,
      const std::vector<uint64_t>& mem_usage_per_attr,
      uint64_t* buffer_idx,
      std::vector<ResultTile*>& result_tiles,
      bool agg_only);

  /**
   * Get the field names to process.
   *
   * The fields are ordered in a manner that will reduce recomputations due to
   * var sized overflows. The order is:
   *  - Var fields with no aggregates that need recompute in case of overflow.
   *  - Var fields with aggregates that need recompute in case of overflow.
   *  - Fixed fields.
   *  - Any aggregate fields with no buffers to copy.
   *
   * This order limits to the maximum the chances we need to recompute an
   * aggregate.
   *
   * @return Field names to process.
   */
  std::vector<std::string> field_names_to_process();

  /**
   * Resize the output buffers to the correct size after copying.
   *
   * @param cells_copied Number of cells copied.
   */
  void resize_output_buffers(uint64_t cells_copied);

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   */
  void add_extra_offset();
};

}  // namespace tiledb::sm

#endif  // TILEDB_SPARSE_INDEX_READER_BASE_H
