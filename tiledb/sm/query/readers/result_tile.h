/**
 * @file   result_tile.h
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
 * This file defines class ResultTile.
 */

#ifndef TILEDB_RESULT_TILE_H
#define TILEDB_RESULT_TILE_H

#include <functional>
#include <iostream>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class Range;
}

namespace tiledb {
namespace sm {

class Domain;
class FragmentMetadata;
class QueryCondition;
class Subarray;

/**
 * Utilitary function to sort result tiles by fragment first then tile index.
 */
bool result_tile_cmp(const ResultTile* a, const ResultTile* b);

struct ResultTileId {
  unsigned fragment_idx_;
  uint64_t tile_idx_;

  ResultTileId() = default;

  ResultTileId(unsigned fragment_idx, uint64_t tile_idx)
      : fragment_idx_(fragment_idx)
      , tile_idx_(tile_idx) {
  }
};

/**
 * Stores information about a logical dense or sparse result tile. Note that it
 * may store the physical tiles across more than one attributes for the same
 * logical tile (space/data tile for dense, data tile oriented by an MBR for
 * sparse).
 */
class ResultTile {
 public:
  /**
   * Class definition for tile sizes.
   *
   */
  class TileSizes {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    TileSizes(
        const std::shared_ptr<tiledb::sm::FragmentMetadata> fragment,
        std::string name,
        const bool var_size,
        const bool nullable,
        const bool validity_only,
        const uint64_t tile_idx)
        : tile_size_(validity_only ? 0 : fragment->tile_size(name, tile_idx))
        , tile_persisted_size_(
              fragment->loaded_metadata()->persisted_tile_size(name, tile_idx))
        , tile_var_size_(
              var_size && !validity_only ?
                  std::optional(fragment->loaded_metadata()->tile_var_size(
                      name, tile_idx)) :
                  std::nullopt)
        , tile_var_persisted_size_(
              var_size ?
                  std::optional(
                      fragment->loaded_metadata()->persisted_tile_var_size(
                          name, tile_idx)) :
                  std::nullopt)
        , tile_validity_size_(
              nullable ? std::optional(
                             fragment->cell_num(tile_idx) *
                             constants::cell_validity_size) :
                         std::nullopt)
        , tile_validity_persisted_size_(
              nullable ?
                  std::optional(
                      fragment->loaded_metadata()->persisted_tile_validity_size(
                          name, tile_idx)) :
                  std::nullopt) {
    }

    TileSizes(
        const uint64_t tile_size,
        const uint64_t tile_persisted_size,
        const optional<uint64_t> tile_var_size,
        const optional<uint64_t> tile_var_persisted_size,
        const optional<uint64_t> tile_validity_size,
        const optional<uint64_t> tile_validity_persisted_size)
        : tile_size_(tile_size)
        , tile_persisted_size_(tile_persisted_size)
        , tile_var_size_(tile_var_size)
        , tile_var_persisted_size_(tile_var_persisted_size)
        , tile_validity_size_(tile_validity_size)
        , tile_validity_persisted_size_(tile_validity_persisted_size) {
    }

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /** @return The fixed tile in memory size. */
    inline uint64_t tile_size() const {
      return tile_size_;
    }

    /** @return The fixed tile in disk size. */
    inline uint64_t tile_persisted_size() const {
      return tile_persisted_size_;
    }

    /** @return If there is a var tile or not. */
    inline bool has_var_tile() const {
      return tile_var_size_.has_value();
    }

    /** @return The fixed tile in memory size. */
    inline uint64_t tile_var_size() const {
      return tile_var_size_.value();
    }

    /** @return The fixed tile in disk size. */
    inline uint64_t tile_var_persisted_size() const {
      return tile_var_persisted_size_.value();
    }

    /** @return If there is a validity tile or not. */
    inline bool has_validity_tile() const {
      return tile_validity_size_.has_value();
    }

    /** @return The validity tile in memory size. */
    inline uint64_t tile_validity_size() const {
      return tile_validity_size_.value();
    }

    /** @return The validity tile in disk size. */
    inline uint64_t tile_validity_persisted_size() const {
      return tile_validity_persisted_size_.value();
    }

   private:
    /* ********************************* */
    /*        PRIVATE ATTRIBUTES         */
    /* ********************************* */

    /** Stores the fixed tile in memory size. */
    const uint64_t tile_size_;

    /** Stores the fixed tile on disk size. */
    const uint64_t tile_persisted_size_;

    /** Stores the var tile in memory size. */
    const optional<uint64_t> tile_var_size_;

    /** Stores the var tile on disk size. */
    const optional<uint64_t> tile_var_persisted_size_;

    /** Stores the validity tile in memory size. */
    const optional<uint64_t> tile_validity_size_;

    /** Stores the validity tile on disk size. */
    const optional<uint64_t> tile_validity_persisted_size_;
  };

  /**
   * Class definition for tile data.
   */
  class TileData {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    TileData(
        std::tuple<void*, shared_ptr<ThreadPool::Task>> fixed_filtered_data,
        std::tuple<void*, shared_ptr<ThreadPool::Task>> var_filtered_data,
        std::tuple<void*, shared_ptr<ThreadPool::Task>> validity_filtered_data)
        : fixed_filtered_data_(std::get<0>(fixed_filtered_data))
        , var_filtered_data_(std::get<0>(var_filtered_data))
        , validity_filtered_data_(std::get<0>(validity_filtered_data))
        , fixed_filtered_data_task_(std::get<1>(fixed_filtered_data))
        , var_filtered_data_task_(std::get<1>(var_filtered_data))
        , validity_filtered_data_task_(std::get<1>(validity_filtered_data)) {
    }

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /** @return The fixed filtered data pointer. */
    inline void* fixed_filtered_data() const {
      return fixed_filtered_data_;
    }

    /** @return The var filtered data pointer. */
    inline void* var_filtered_data() const {
      return var_filtered_data_;
    }

    /** @return The validity filtered data pointer. */
    inline void* validity_filtered_data() const {
      return validity_filtered_data_;
    }

    /** @return The fixed filtered data I/O task. */
    inline shared_ptr<ThreadPool::Task> fixed_filtered_data_task() const {
      return fixed_filtered_data_task_;
    }

    /** @return The var filtered data I/O task. */
    inline shared_ptr<ThreadPool::Task> var_filtered_data_task() const {
      return var_filtered_data_task_;
    }

    /** @return The validity filtered data I/O task. */
    inline shared_ptr<ThreadPool::Task> validity_filtered_data_task() const {
      return validity_filtered_data_task_;
    }

   private:
    /* ********************************* */
    /*        PRIVATE ATTRIBUTES         */
    /* ********************************* */

    /** Stores the fixed filtered data pointer. */
    void* fixed_filtered_data_;

    /** Stores the var filtered data pointer. */
    void* var_filtered_data_;

    /** Stores the validity filtered data pointer. */
    void* validity_filtered_data_;

    /** Stores the fixed filtered data I/O task. */
    shared_ptr<ThreadPool::Task> fixed_filtered_data_task_;

    /** Stores the var filtered data I/O task. */
    shared_ptr<ThreadPool::Task> var_filtered_data_task_;

    /** Stores the validity filtered data I/O task. */
    shared_ptr<ThreadPool::Task> validity_filtered_data_task_;
  };

  /**
   * Class definition for the tile tuple.
   */
  class TileTuple {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /** Default constructor. */
    TileTuple() = delete;

    /** Constructor with var_size and nullable parameters. */
    TileTuple(
        const format_version_t format_version,
        const ArraySchema& array_schema,
        const std::string& name,
        const TileSizes tile_sizes,
        const TileData tile_data,
        shared_ptr<MemoryTracker> memory_tracker)
        : memory_tracker_(memory_tracker)
        , fixed_tile_(
              format_version,
              tile_sizes.has_var_tile() ? constants::cell_var_offset_type :
                                          array_schema.type(name),
              tile_sizes.has_var_tile() ? constants::cell_var_offset_size :
                                          array_schema.cell_size(name),
              (name == constants::coords) ? array_schema.dim_num() : 0,
              tile_sizes.tile_size(),
              tile_data.fixed_filtered_data(),
              tile_sizes.tile_persisted_size(),
              memory_tracker_,
              tile_data.fixed_filtered_data_task()) {
      if (tile_sizes.has_var_tile()) {
        auto type = array_schema.type(name);
        var_tile_.emplace(
            format_version,
            type,
            datatype_size(type),
            0,
            tile_sizes.tile_var_size(),
            tile_data.var_filtered_data(),
            tile_sizes.tile_var_persisted_size(),
            memory_tracker_,
            tile_data.var_filtered_data_task());
      }

      if (tile_sizes.has_validity_tile()) {
        validity_tile_.emplace(
            format_version,
            constants::cell_validity_type,
            constants::cell_validity_size,
            0,
            tile_sizes.tile_validity_size(),
            tile_data.validity_filtered_data(),
            tile_sizes.tile_validity_persisted_size(),
            memory_tracker_,
            tile_data.validity_filtered_data_task());
      }
    }

    /* ********************************* */
    /*                API                */
    /* ********************************* */

    /** @returns Fixed tile. */
    Tile& fixed_tile() {
      return fixed_tile_;
    }

    /** @returns Var tile. */
    Tile& var_tile() {
      return var_tile_.value();
    }

    /** @returns Validity tile. */
    Tile& validity_tile() {
      return validity_tile_.value();
    }

    /** @returns Fixed tile. */
    const Tile& fixed_tile() const {
      return fixed_tile_;
    }

    /** @returns Var tile. */
    const Tile& var_tile() const {
      return var_tile_.value();
    }

    /** @returns Validity tile. */
    const Tile& validity_tile() const {
      return validity_tile_.value();
    }

   private:
    /* ********************************* */
    /*        PRIVATE ATTRIBUTES         */
    /* ********************************* */

    /** The memory tracker. */
    shared_ptr<MemoryTracker> memory_tracker_;

    /** Stores the fixed data tile. */
    Tile fixed_tile_;

    /** Stores the var data tile. */
    optional<Tile> var_tile_;

    /** Stores the validity data tile. */
    optional<Tile> validity_tile_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No Default constructor. */
  ResultTile() = delete;

  /**
   * Constructor.
   *
   * @param memory_tracker The memory tracker to use.
   */
  ResultTile(shared_ptr<MemoryTracker> memory_tracker);

  /**
   * Constructor. The number of dimensions `dim_num` is used to allocate
   * the separate coordinate tiles.
   */
  ResultTile(
      unsigned frag_idx,
      uint64_t tile_idx,
      const FragmentMetadata& frag_md,
      shared_ptr<MemoryTracker> memory_tracker);

  DISABLE_COPY_AND_COPY_ASSIGN(ResultTile);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ResultTile);

  /** Default destructor. */
  ~ResultTile() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Equality operator (mainly for debugging purposes). */
  bool operator==(const ResultTile& rt) const;

  /**
   * Returns the number of cells in the result tile.
   * Should be the same across all attributes.
   */
  uint64_t cell_num() const;

  /** Returns true if it stores zipped coordinates. */
  bool stores_zipped_coords() const;

  /** Returns the zipped coordinates tile. */
  const Tile& zipped_coords_tile() const;

  /** Returns the coordinate tile for the input dimension. */
  const TileTuple& coord_tile(unsigned dim_idx) const;

  /** Returns the stored domain. */
  const Domain* domain() const {
    return domain_;
  }

  /** Erases the tile for the input attribute/dimension. */
  void erase_tile(const std::string& name);

  /** Initializes the result tile for the given attribute. */
  void init_attr_tile(
      const format_version_t format_version,
      const ArraySchema& array_schema,
      const std::string& name,
      const TileSizes tile_sizes,
      const TileData tile_data);

  /** Initializes the result tile for the given dimension name and index. */
  void init_coord_tile(
      const format_version_t format_version,
      const ArraySchema& array_schema,
      const std::string& name,
      const TileSizes tile_sizes,
      const TileData tile_data,
      unsigned dim_idx);

  /** Returns the tile pair for the input attribute or dimension. */
  TileTuple* tile_tuple(const std::string& name);

  /**
   * Returns a constant pointer to the coordinate at position `pos` for
   * dimension `dim_idx`. This will fetch from the zipped `coord_tile_`
   * unless at least one `init_coord_tile()` has been invoked on this
   * instance. The caller must be certain that a coordinate exists at
   * the requested position and dimension.
   */
  inline const void* coord(uint64_t pos, unsigned dim_idx) const {
    return (this->*coord_func_)(pos, dim_idx);
  }

  /**
   * Returns the string coordinate at position `pos` for
   * dimension `dim_idx`. Applicable only to string dimensions.
   */
  std::string_view coord_string(uint64_t pos, unsigned dim_idx) const;

  /** Returns the coordinate size on the input dimension. */
  uint64_t coord_size(unsigned dim_idx) const;

  /**
   * Returns true if the coordinates at position `pos_a` and `pos_b` are
   * the same.
   *
   * This checks for two cells in different tiles.
   */
  bool same_coords(const ResultTile& rt, uint64_t pos_a, uint64_t pos_b) const;

  /**
   * Returns true if the coordinates at position `pos_a` and `pos_b` are
   * the same.
   *
   * This checks for two cells in the same tile.
   */
  bool same_coords(uint64_t pos_a, uint64_t pos_b) const;

  /**
   * Returns the timestamp of the cell at position `pos`.
   */
  uint64_t timestamp(uint64_t pos);

  /**
   * Returns the dimension label value for the `label_name` attribute at
   * position `pos`.
   */
  template <typename LabelType>
  LabelType attribute_value(const std::string& label_name, const uint64_t pos);

  /** Returns the fragment id that this result tile belongs to. */
  unsigned frag_idx() const;

  /** Returns the tile index of this tile in the fragment. */
  uint64_t tile_idx() const;

  /**
   * Reads `len` coordinates the from the tile, starting at the beginning of
   * the coordinates at position `pos`.
   */
  Status read(
      const std::string& name,
      void* buffer,
      uint64_t buffer_offset,
      uint64_t pos,
      uint64_t len,
      const uint64_t timestamp_val = UINT64_MAX);

  /**
   * Reads `len` coordinates the from the tile, starting at the beginning of
   * the coordinates at position `pos`. The associated validity values are
   * stored in the `buffer_validity`.
   */
  Status read_nullable(
      const std::string& name,
      void* buffer,
      uint64_t buffer_offset,
      uint64_t pos,
      uint64_t len,
      void* buffer_validity);

  /**
   * Applicable only to sparse tiles of dense arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   * It also accummulates to an `overwritten_bitmap` that
   * checks if the coordinate is overwritten on that dimension by a
   * future fragment (one with index greater than `frag_idx`). That
   * is computed only for `dim_idx == dim_num -1`, as it needs
   * to know if the the coordinates to be checked are already results.
   */
  template <class T>
  static void compute_results_dense(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const Range& range,
      const std::vector<shared_ptr<FragmentMetadata>> fragment_metadata,
      unsigned frag_idx,
      std::vector<uint8_t>* result_bitmap,
      std::vector<uint8_t>* overwritten_bitmap);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result bitmap for the input dimension for the coordinates that
   * fall in the input range.
   */
  template <class T>
  static void compute_results_sparse(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const Range& range,
      tdb::pmr::vector<uint8_t>* result_bitmap,
      const Layout& cell_order);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input dimension for the coordinates that
   * fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType, class T>
  static void compute_results_count_sparse(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const NDRange& ranges,
      const tdb::pmr::vector<uint64_t>& range_indexes,
      tdb::pmr::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input string dimension for the coordinates
   * that fall in the input ranges and multiply with the previous count. The
   * caller has already determined that the input start-end range needs to be
   * processed fully.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType>
  static void compute_results_count_sparse_string_range(
      const std::vector<std::pair<std::string_view, std::string_view>>
          cached_ranges,
      const char* buff_str,
      const uint64_t* buff_off,
      const uint64_t start,
      const uint64_t end,
      tdb::pmr::vector<BitmapType>& result_count);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input string dimension for the coordinates
   * that fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType>
  static void compute_results_count_sparse_string(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const NDRange& ranges,
      const tdb::pmr::vector<uint64_t>& range_indexes,
      tdb::pmr::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell);

  /**
   * Applicable only to sparse tiles of dense arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   * It also accummulates to an `overwritten_bitmap` that
   * checks if the coordinate is overwritten on that dimension by a
   * future fragment (one with index greater than `frag_idx`). That
   * is computed only for `dim_idx == dim_num -1`, as it needs
   * to know if the the coordinates to be checked are already results.
   */
  Status compute_results_dense(
      unsigned dim_idx,
      const Range& range,
      const std::vector<shared_ptr<FragmentMetadata>> fragment_metadata,
      unsigned frag_idx,
      std::vector<uint8_t>* result_bitmap,
      std::vector<uint8_t>* overwritten_bitmap) const;

  /**
   * Applicable only to sparse arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   */
  Status compute_results_sparse(
      unsigned dim_idx,
      const Range& range,
      tdb::pmr::vector<uint8_t>* result_bitmap,
      const Layout& cell_order) const;

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input dimension for the coordinates that
   * fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType>
  Status compute_results_count_sparse(
      unsigned dim_idx,
      const NDRange& ranges,
      const tdb::pmr::vector<uint64_t>& range_indexes,
      tdb::pmr::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell) const;

 protected:
  /* ********************************* */
  /*        PROTECTED ATTRIBUTES       */
  /* ********************************* */

  /** The memory tracker. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The array domain. */
  const Domain* domain_;

  /** The id of the fragment this tile belongs to. */
  unsigned frag_idx_ = UINT32_MAX;

  /** The id of the tile (which helps locating the physical attribute tiles). */
  uint64_t tile_idx_ = UINT64_MAX;

  /** Number of cells. */
  uint64_t cell_num_;

  /** Attribute names to tiles based on attribute ordering from array schema. */
  std::vector<std::pair<std::string, optional<TileTuple>>> attr_tiles_;

  /** The timestamp attribute tile. */
  optional<TileTuple> timestamps_tile_;

  /** The delete timestamp attribute tile. */
  optional<TileTuple> delete_timestamps_tile_;

  /** The delete condition marker hash attribute tile. */
  optional<TileTuple> delete_condition_index_tile_;

  /** The zipped coordinates tile. */
  optional<TileTuple> coords_tile_;

  /**
   * The separate coordinate tiles along with their names, sorted on the
   * dimension order.
   */
  std::vector<std::pair<std::string, optional<TileTuple>>> coord_tiles_;

  /**
   * Stores the appropriate templated compute_results_dense() function based for
   * each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const Range&,
      const std::vector<shared_ptr<FragmentMetadata>>,
      unsigned,
      std::vector<uint8_t>*,
      std::vector<uint8_t>*)>>
      compute_results_dense_func_;

  /**
   * Implements coord() with either zipped or unzipped coordinates. This
   * is invoked in a critical path and is experimentally faster as a c-style
   * function pointer than a bound `std::function`.
   */
  const void* (ResultTile::*coord_func_)(uint64_t, unsigned) const;

  /**
   * Stores the appropriate templated compute_results_sparse() function based
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const Range&,
      tdb::pmr::vector<uint8_t>*,
      const Layout&)>>
      compute_results_sparse_func_;

  /**
   * Stores the appropriate templated compute_results_count_sparse() function
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const NDRange&,
      const tdb::pmr::vector<uint64_t>&,
      tdb::pmr::vector<uint64_t>&,
      const Layout&,
      const uint64_t,
      const uint64_t)>>
      compute_results_count_sparse_uint64_t_func_;

  /**
   * Stores the appropriate templated compute_results_count_sparse() function
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const NDRange&,
      const tdb::pmr::vector<uint64_t>&,
      tdb::pmr::vector<uint8_t>&,
      const Layout&,
      const uint64_t,
      const uint64_t)>>
      compute_results_count_sparse_uint8_t_func_;

 private:
  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Sets the templated compute_results() function. */
  void set_compute_results_func();

  /** Implements coord() for zipped coordinates. */
  const void* zipped_coord(uint64_t pos, unsigned dim_idx) const;

  /** Implements coord() for unzipped coordinates. */
  const void* unzipped_coord(uint64_t pos, unsigned dim_idx) const;

  /**
   * A helper routine used in `compute_results_sparse<char>` to
   * determine if a given string-valued coordinate intersects
   * the given start and end range.
   *
   * @param c_offset The offset of the coordinate value
   *    within `buff_str`.
   * @param c_cize The size of the coordinate value.
   * @param buff_str The buffer containing the coordinate value
   *    at `c_offset`.
   * @param range_start The starting range value.
   * @param range_end The ending range value.
   * @return false for no intersection, true for instersection.
   */
  static bool str_coord_intersects(
      const uint64_t c_offset,
      const uint64_t c_size,
      const char* const buff_str,
      const std::string_view& range_start,
      const std::string_view& range_end);
};

/** Result tile with bitmap. */
template <class BitmapType>
class ResultTileWithBitmap : public ResultTile {
 protected:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  ResultTileWithBitmap() = delete;

  /**
   * Constructor
   *
   * @param memory_tracker The memory tracker to use.
   */
  ResultTileWithBitmap(shared_ptr<MemoryTracker> memory_tracker)
      : ResultTile(memory_tracker)
      , bitmap_(memory_tracker_->get_resource(MemoryType::RESULT_TILE_BITMAP)) {
  }

  ResultTileWithBitmap(
      unsigned frag_idx,
      uint64_t tile_idx,
      const FragmentMetadata& frag_md,
      shared_ptr<MemoryTracker> memory_tracker)
      : ResultTile(frag_idx, tile_idx, frag_md, memory_tracker)
      , bitmap_(memory_tracker_->get_resource(MemoryType::RESULT_TILE_BITMAP))
      , result_num_(cell_num_) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(ResultTileWithBitmap);
  DISABLE_MOVE_AND_MOVE_ASSIGN(ResultTileWithBitmap);

 public:
  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Returns the number of results in the bitmap.
   *
   * @return Number of results.
   */
  inline uint64_t result_num() {
    return result_num_;
  }

  /**
   * Get the bitmap.
   *
   * @param cell_idx Cell index.
   */
  inline tdb::pmr::vector<BitmapType>& bitmap() {
    return bitmap_;
  }

  /**
   * Accumulates the number of cells in the bitmap.
   */
  void count_cells() {
    result_num_ = std::accumulate(bitmap_.begin(), bitmap_.end(), 0);
  }

  /**
   * Allocate the bitmap.
   */
  void alloc_bitmap() {
    assert(bitmap_.size() == 0);
    bitmap_.resize(cell_num_, 1);
  }

  /**
   * Returns the number of cells that are that are between two cell positions
   * in the bitmap.
   *
   * @param start_pos Starting cell position in the bitmap.
   * @param end_pos End position in the bitmap.
   *
   * @return Result number between the positions.
   */
  uint64_t result_num_between_pos(uint64_t start_pos, uint64_t end_pos) const {
    if (bitmap_.size() == 0) {
      return end_pos - start_pos;
    }

    uint64_t result_num = 0;
    for (uint64_t c = start_pos; c < end_pos; c++) {
      result_num += bitmap_[c];
    }

    return result_num;
  }

  /**
   * Returns the position (index) inside of the bitmap given a number of cells.
   *
   * @param start_pos Starting cell position in the bitmap.
   * @param result_num Number of results to advance.
   *
   * @return Cell position found, or maximum position.
   */
  uint64_t pos_with_given_result_sum(
      uint64_t start_pos, uint64_t result_num) const {
    if (bitmap_.size() == 0) {
      return start_pos + result_num - 1;
    }

    uint64_t sum = 0;
    for (uint64_t c = start_pos; c < bitmap_.size(); c++) {
      sum += bitmap_[c];
      if (sum >= result_num) {
        return c;
      }
    }

    return bitmap_.size() - 1;
  }

  /** Does this tile have a bitmap. */
  inline bool has_bmp() {
    return bitmap_.size() > 0;
  }

  /** Should we copy the whole tile. */
  inline bool copy_full_tile() {
    // No bitmap, copy full tile.
    if (bitmap_.size() == 0) {
      return true;
    }

    // For non overlapping ranges, if result_num == cell_num, copy full tile.
    const bool non_overlapping = std::is_same<BitmapType, uint8_t>::value;
    if (non_overlapping) {
      return cell_num_ == result_num_;
    }

    return false;
  }

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */
  /** Bitmap for this tile. */
  tdb::pmr::vector<BitmapType> bitmap_;

  /** Number of cells in this bitmap. */
  uint64_t result_num_;
};

/** Global order result tile. */
template <class BitmapType>
class GlobalOrderResultTile : public ResultTileWithBitmap<BitmapType> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** No default constructor. */
  GlobalOrderResultTile() = delete;

  GlobalOrderResultTile(
      unsigned frag_idx,
      uint64_t tile_idx,
      bool dups,
      bool include_delete_meta,
      const FragmentMetadata& frag_md,
      shared_ptr<MemoryTracker> memory_tracker)
      : ResultTileWithBitmap<BitmapType>(
            frag_idx, tile_idx, frag_md, memory_tracker)
      , hilbert_values_(this->memory_tracker_->get_resource(
            MemoryType::TILE_HILBERT_VALUES))
      , post_dedup_bitmap_(nullopt)
      , per_cell_delete_condition_(
            this->memory_tracker_->get_resource(MemoryType::QUERY_CONDITION))
      , used_(false) {
    if (!dups || include_delete_meta) {
      post_dedup_bitmap_.emplace(
          this->memory_tracker_->get_resource(MemoryType::RESULT_TILE_BITMAP));
    }
  }

  DISABLE_COPY_AND_COPY_ASSIGN(GlobalOrderResultTile);
  DISABLE_MOVE_AND_MOVE_ASSIGN(GlobalOrderResultTile);

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /** Returns if the tile was used by the merge or not. */
  inline bool used() {
    return used_;
  }

  /** Set the tile as used by the merge. */
  inline void set_used() {
    used_ = true;
  }

  /**
   * Returns whether this tile has a post query condition bitmap. For this
   * tile type, it will either be post_dedup_bitmap_ or the normal bitmap.
   */
  inline bool has_post_dedup_bmp() {
    return ResultTileWithBitmap<BitmapType>::has_bmp() ||
           (post_dedup_bitmap_.has_value() && post_dedup_bitmap_->size() > 0);
  }

  /**
   * Ensures there is a post query condition bitmap allocated. If there is a
   * regular bitmap already for this tile, 'post_dedup_bitmap_' will contain the
   * combination of the existing bitmap with query condition results. Otherwise
   * it will only contain the query condition results.
   */
  void ensure_bitmap_for_query_condition() {
    if (post_dedup_bitmap_.has_value()) {
      if (ResultTileWithBitmap<BitmapType>::has_bmp()) {
        post_dedup_bitmap_->assign(this->bitmap_.begin(), this->bitmap_.end());
      } else {
        post_dedup_bitmap_->resize(ResultTile::cell_num_, 1);
      }
    } else {
      if (ResultTileWithBitmap<BitmapType>::bitmap_.size() == 0) {
        ResultTileWithBitmap<BitmapType>::bitmap_.resize(
            ResultTile::cell_num_, 1);
      }
    }
  }

  /**
   * Returns the bitmap that included query condition results. For this tile
   * type, this is 'post_dedup_bitmap_' if allocated, or the regular bitmap.
   */
  inline tdb::pmr::vector<BitmapType>& post_dedup_bitmap() {
    return post_dedup_bitmap_.has_value() && post_dedup_bitmap_->size() > 0 ?
               post_dedup_bitmap_.value() :
               ResultTileWithBitmap<BitmapType>::bitmap_;
  }

  /**
   * Clear a cell in the bitmap.
   *
   * @param cell_idx Cell index to clear.
   */
  void clear_cell(uint64_t cell_idx) {
    if (cell_idx > ResultTileWithBitmap<BitmapType>::bitmap_.size()) {
      throw std::out_of_range("Cell index out of range");
    }

    ResultTileWithBitmap<BitmapType>::result_num_ -=
        ResultTileWithBitmap<BitmapType>::bitmap_[cell_idx];
    ResultTileWithBitmap<BitmapType>::bitmap_[cell_idx] = 0;

    if (post_dedup_bitmap_.has_value() && post_dedup_bitmap_->size() > 0) {
      post_dedup_bitmap_->at(cell_idx) = 0;
    }
  }

  /** Allocate space for the hilbert values vector. */
  inline void allocate_hilbert_vector() {
    hilbert_values_.resize(ResultTile::cell_num_);
  }

  /** Get the hilbert value at an index. */
  inline uint64_t hilbert_value(uint64_t i) {
    return hilbert_values_[i];
  }

  /** Set a hilbert value. */
  inline void set_hilbert_value(uint64_t i, uint64_t v) {
    hilbert_values_[i] = v;
  }

  /** Return first cell index in bitmap. */
  uint64_t first_cell_in_bitmap() {
    uint64_t ret = 0;
    while (!ResultTileWithBitmap<BitmapType>::bitmap_[ret]) {
      ret++;
    }

    return ret;
  }

  /** Return first cell index in bitmap. */
  uint64_t last_cell_in_bitmap() {
    uint64_t ret = ResultTileWithBitmap<BitmapType>::bitmap_.size() - 1;
    while (!ResultTileWithBitmap<BitmapType>::bitmap_[ret]) {
      ret--;
    }

    return ret;
  }

  /** Allocate space for the delete condition index vector. */
  inline void allocate_per_cell_delete_condition_vector() {
    per_cell_delete_condition_.resize(ResultTile::cell_num_, nullptr);
  }

  /** Compute the delete condition index. */
  inline void compute_per_cell_delete_condition(QueryCondition* ptr) {
    // Go through all cells, if the delete condition cleared the cell, and the
    // index for this cell is still unset, set it to the current condition.
    for (uint64_t c = 0; c < ResultTile::cell_num_; c++) {
      if (post_dedup_bitmap_->at(c) == 0 &&
          per_cell_delete_condition_[c] == nullptr) {
        per_cell_delete_condition_[c] = ptr;
      }
    }

    // Reset the bitmap to be ready for the next condition.
    std::fill(post_dedup_bitmap_->begin(), post_dedup_bitmap_->end(), 1);
  }

  /**
   * Returns the delete timestamp for the cell at `cell_idx`. If there was not
   * any delete condition that deleted this cell, the timestamp is going to be
   * uint64_t max.
   */
  inline uint64_t delete_timestamp(uint64_t cell_idx) {
    auto ptr = per_cell_delete_condition_[cell_idx];
    return ptr == nullptr ? std::numeric_limits<uint64_t>::max() :
                            ptr->condition_timestamp();
  }

  /**
   * Returns the delete condition index for the cell at `cell_idx`. If there
   * was not any delete condition that deleted this cell, the value is going
   * to be uint64_t max.
   */
  inline size_t delete_condition_index(uint64_t cell_idx) {
    auto ptr = per_cell_delete_condition_[cell_idx];
    return ptr == nullptr ? std::numeric_limits<uint64_t>::max() :
                            ptr->condition_index();
  }

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Hilbert values for this tile. */
  tdb::pmr::vector<uint64_t> hilbert_values_;

  /**
   * An extra bitmap will be needed for array with no duplicates. For those,
   * deduplication need to be run before query condition is applied. So bitmap_
   * will contain the results before query condition, and post_dedup_bitmap_
   * will contain results after query condition.
   */
  optional<tdb::pmr::vector<BitmapType>> post_dedup_bitmap_;

  /**
   * Delete condition index that deleted a cell. Used for consolidation with
   * delete metadata.
   */
  tdb::pmr::vector<QueryCondition*> per_cell_delete_condition_;

  /** Was the tile used in the merge. */
  bool used_;
};

template <class BitmapType>
class UnorderedWithDupsResultTile : public ResultTileWithBitmap<BitmapType> {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  /** No default memory tracker. */
  UnorderedWithDupsResultTile() = delete;

  UnorderedWithDupsResultTile(
      unsigned frag_idx,
      uint64_t tile_idx,
      const FragmentMetadata& frag_md,
      shared_ptr<MemoryTracker> memory_tracker)
      : ResultTileWithBitmap<BitmapType>(
            frag_idx, tile_idx, frag_md, memory_tracker) {
  }

  DISABLE_MOVE_AND_MOVE_ASSIGN(UnorderedWithDupsResultTile);
  DISABLE_COPY_AND_COPY_ASSIGN(UnorderedWithDupsResultTile);

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Returns whether this tile has a post query condition bitmap. For this
   * tile type, this is stored in the regular bitmap.
   */
  inline bool has_post_dedup_bmp() {
    return ResultTileWithBitmap<BitmapType>::has_bmp();
  }

  /**
   * Ensures there is a post query condition bitmap allocated. For this tile
   * type, this is stored in the regular bitmap.
   */
  void ensure_bitmap_for_query_condition() {
    if (ResultTileWithBitmap<BitmapType>::bitmap_.size() == 0) {
      ResultTileWithBitmap<BitmapType>::bitmap_.resize(
          ResultTile::cell_num_, 1);
    }
  }

  /**
   * Returns the bitmap that included query condition results. For this tile
   * type, this is stored in the regular bitmap.
   */
  inline tdb::pmr::vector<BitmapType>& post_dedup_bitmap() {
    return ResultTileWithBitmap<BitmapType>::bitmap_;
  }

  /** Not used for this result tile type. */
  inline void allocate_per_cell_delete_condition_vector() {
  }

  /** Not used for this result tile type. */
  inline void compute_per_cell_delete_condition(QueryCondition*) {
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_TILE_H
