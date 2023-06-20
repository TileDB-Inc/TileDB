/**
 * @file unfiltered_data.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Defines the UnfilteredData class and associated classes.
 */

#ifndef TILEDB_UNFILTERED_DATA_H
#define TILEDB_UNFILTERED_DATA_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/sm/query/readers/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ReaderBase;

/** Unfiltered tile type enum. */
enum UnfilteredTileType { FIXED = 0, VAR = 1, NULLABLE = 2 };

/**
 * An unfiltered data block containing unfiltered data for multiple tiles.
 * The block will contain a number of in-memory tiles. A block may contain a
 * combinaiton of fixed data, var data or nullable data.
 *
 * This uses a vector for storage which will be replaced by datablocks when
 * ready.
 */
class UnfilteredDataBlock {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param size Size of the in-memory data for this data block.
   */
  UnfilteredDataBlock(uint64_t size)
      : unfiltered_data_(size) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * @return Pointer to the data at a particular offset in the unfiltered data.
   */
  inline void* data_at(size_t offset) {
    return unfiltered_data_.data() + offset;
  }

  /** @return Pointer to the data inside of the filtered data block. */
  inline void* data() {
    return unfiltered_data_.data();
  }

  /** @return Size of the data block. */
  inline size_t size() const {
    return unfiltered_data_.size();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Data for the data block. */
  std::vector<char> unfiltered_data_;
};

/**
 * Simple class that stores the data block index and offsets for the fixed, var
 * and validity tile.
 */
class TileDataBlockOffsets {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  TileDataBlockOffsets(
      uint64_t data_block_idx,
      uint64_t data_block_offset,
      uint64_t fixed_tile_size,
      uint64_t var_tile_size)
      : data_block_idx_(data_block_idx)
      , fixed_tile_offset_(data_block_offset)
      , var_tile_offset_(data_block_offset + fixed_tile_size)
      , validity_tile_offset_(
            data_block_offset + fixed_tile_size + var_tile_size) {
  }

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  inline uint64_t data_block_idx() const {
    return data_block_idx_;
  }

  inline uint64_t fixed_tile_offset() const {
    return fixed_tile_offset_;
  }

  inline uint64_t var_tile_offset() const {
    return var_tile_offset_;
  }

  inline uint64_t validity_tile_offset() const {
    return validity_tile_offset_;
  }

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  // Data block index.
  uint64_t data_block_idx_;

  // Fixed tile offset.
  uint64_t fixed_tile_offset_;

  // Var tile offset.
  uint64_t var_tile_offset_;

  // Validity tile offset.
  uint64_t validity_tile_offset_;
};

/**
 * Contains unfiltered data blocks for a fragment as well as a map that allows
 * to find the data for each tiles.
 */
class FragmentUnfilteredData {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor using a sorted list of result tiles for a specific fragment.
   *
   * @param f Fragment index to be processed.
   * @param reader Reader object used to know which tile to skip.
   * @param batch_size Batch size we are trying to reach.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result tiles.
   * Only the fragment index and tile index of each result tiles is used here.
   * Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param dups Is the array allowing duplicates?
   * @param rt_idx Index to start adding tiles for, this will be used to return
   * the index that will be used for the next fragment.
   */
  FragmentUnfilteredData(
      const unsigned f,
      const ReaderBase& reader,
      const uint64_t batch_size,
      const shared_ptr<FragmentMetadata>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      const bool dups,
      size_t& rt_idx) {
    add_tiles(
        f,
        reader,
        batch_size,
        fragment_metadata,
        result_tiles,
        name,
        var_sized,
        nullable,
        dups,
        rt_idx);
  }

  /** Destructor. */
  ~FragmentUnfilteredData() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Add tiles to the unfiltered data for a fragment.
   *
   * @param f Fragment index to be processed.
   * @param reader Reader object used to know which tile to skip.
   * @param batch_size Batch size we are trying to reach.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result tiles.
   * Only the fragment index and tile index of each result tiles is used here.
   * Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param dups Is the array allowing duplicates?
   * @param rt_idx Index to start adding tiles for, this will be used to return
   * the index that will be used for the next fragment.
   */
  void add_tiles(
      const unsigned f,
      const ReaderBase& reader,
      const uint64_t batch_size,
      const shared_ptr<FragmentMetadata>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      const bool dups,
      size_t& rt_idx);

  /**
   * Get the tile data for either the fixed, var or validity tile.
   *
   * @param t Tile index.
   * @param type Tile type.
   * @return Tile data or nullptr if it doesn't exist.
   */
  void* tile_data(uint64_t t, UnfilteredTileType type) {
    auto unfiltered_data_offsets{unfiltered_data_offsets_.find(t)};
    if (unfiltered_data_offsets == unfiltered_data_offsets_.end()) {
      return nullptr;
    }

    auto& offsets{unfiltered_data_offsets->second};
    auto& datablock{data_blocks_[offsets.data_block_idx()]};
    switch (type) {
      case UnfilteredTileType::FIXED:
        return datablock.data_at(offsets.fixed_tile_offset());

      case UnfilteredTileType::VAR:
        return datablock.data_at(offsets.var_tile_offset());

      case UnfilteredTileType::NULLABLE:
        return datablock.data_at(offsets.validity_tile_offset());
    }

    return nullptr;
  }

  /**
   * Clear the data for the fragment.
   *
   * @param keep_last_block Option to keep the last block. This is used for
   * global order reads with no duplicates and fragments consolidated with
   * timestamps.
   */
  void clear_data(bool keep_last_block) {
    if (keep_last_block) {
      if (data_blocks_.size() > 1) {
        data_blocks_.erase(
            data_blocks_.begin(),
            data_blocks_.begin() + data_blocks_.size() - 1);

        // The last block should contain only the last tile, search for the
        // highest index item in the index map.
        uint64_t last_tile_index = 0;
        for (auto& item : unfiltered_data_offsets_) {
          if (item.first > last_tile_index) {
            last_tile_index = item.first;
          }
        }

        // Erase all data from the map aside from the data for the last tile.
        for (auto it = unfiltered_data_offsets_.begin();
             it != unfiltered_data_offsets_.end();) {
          if (it->first != last_tile_index) {
            it = unfiltered_data_offsets_.erase(it);
          } else {
            ++it;
          }
        }
      }
    } else {
      data_blocks_.clear();
      unfiltered_data_offsets_.clear();
    }
  }

  /** @return Memory usage for the unfiltered data of this fragment. */
  uint64_t memory_usage() const {
    uint64_t memory_usage = 0;
    for (auto& data_block : data_blocks_) {
      memory_usage += data_block.size();
    }

    return memory_usage;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Unfiltered data blocks. */
  std::vector<UnfilteredDataBlock> data_blocks_;

  /** Map of that allows to get the unfiltered data tuple from a tile index.
   */
  std::unordered_map<uint64_t, TileDataBlockOffsets> unfiltered_data_offsets_;
};

/**
 * Contains the unfiltered data for a dimension or attribute. The data is
 * stored per fragment.
 */
class UnfilteredData {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor using a sorted list of result tiles.
   *
   * @param reader Reader object used to know which tile to skip.
   * @param batch_size Batch size we are trying to reach.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result
   * tiles. Only the fragment index and tile index of each result tiles is
   * used here. Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param dups Is the array allowing duplicates?
   */
  UnfilteredData(
      const ReaderBase& reader,
      const uint64_t batch_size,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      const bool dups) {
    add_tiles(
        reader,
        batch_size,
        fragment_metadata,
        result_tiles,
        name,
        var_sized,
        nullable,
        dups);
  }

  /** Destructor. */
  ~UnfilteredData() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Add tiles to this unfiltered data object.
   *
   * @param reader Reader object used to know which tile to skip.
   * @param batch_size Batch size we are trying to reach.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result tiles.
   * Only the fragment index and tile index of each result tiles is used here.
   * Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param dups Is the array allowing duplicates?
   * @param rt_idx Index to start adding tiles for, this will be used to return
   * the index that will be used for the next fragment.
   */
  void add_tiles(
      const ReaderBase& reader,
      const uint64_t batch_size,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      const bool dups) {
    // Process all tiles in order. The constructor of `FragmentUnfilteredData`
    // will increment `rt_idx` to the index the last tile it processed.
    size_t rt_idx{0};
    while (rt_idx < result_tiles.size()) {
      unsigned f = result_tiles[rt_idx]->frag_idx();
      if (per_fragment_data_.count(f) == 0) {
        per_fragment_data_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(f),
            std::forward_as_tuple(
                f,
                reader,
                batch_size,
                fragment_metadata[f],
                result_tiles,
                name,
                var_sized,
                nullable,
                dups,
                rt_idx));
      } else {
        per_fragment_data_.at(f).add_tiles(
            f,
            reader,
            batch_size,
            fragment_metadata[f],
            result_tiles,
            name,
            var_sized,
            nullable,
            dups,
            rt_idx);
      }
    }

    compute_memory_usage();
  }

  /**
   * Get the tile data for either the fixed, var or validity tile.
   *
   * @param f Fragment index.
   * @param t Tile index.
   * @param type Tile type.
   * @return Tile data or nullptr if it doesn't exist.
   */
  void* tile_data(unsigned f, uint64_t t, UnfilteredTileType type) {
    auto fragment_data{per_fragment_data_.find(f)};
    if (fragment_data == per_fragment_data_.end()) {
      return nullptr;
    }

    return fragment_data->second.tile_data(t, type);
  }

  /**
   * Clear the data for a fragment.
   *
   * @param f Fragment index.
   * @param keep_last_block Option to keep the last block. This is used for
   * global order reads with no duplicates and fragments consolidated with
   * timestamps.
   */
  void clear_fragment_data(unsigned f, bool keep_last_block) {
    auto fragment_data{per_fragment_data_.find(f)};
    if (fragment_data == per_fragment_data_.end()) {
      return;
    }

    fragment_data->second.clear_data(keep_last_block);
    compute_memory_usage();
  }

  /** @return Memory usage for the unfiltered data. */
  uint64_t memory_usage() const {
    return memory_usage_;
  }

 private:
  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Compute memory usage for the unfiltered data. */
  void compute_memory_usage() {
    memory_usage_ = 0;
    for (auto& per_fragment_data : per_fragment_data_) {
      memory_usage_ += per_fragment_data.second.memory_usage();
    }
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Unfiltered data blocks stored in a map. */
  std::unordered_map<unsigned, FragmentUnfilteredData> per_fragment_data_;

  /** Memory usage for the data. */
  uint64_t memory_usage_;
};

/** Class that contains unfiltered data for multiple attributes. */
class UnfilteredDataMap {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  UnfilteredDataMap() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Add tiles for a field to the unfiltered data.
   *
   * @param reader Reader object used to know which tile to skip.
   * @param batch_size Batch size we are trying to reach.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result tiles.
   * Only the fragment index and tile index of each result tiles is used here.
   * Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param dups Is the array allowing duplicates?
   * @param rt_idx Index to start adding tiles for, this will be used to return
   * the index that will be used for the next fragment.
   */
  void add_tiles(
      const ReaderBase& reader,
      const uint64_t batch_size,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      const bool dups) {
    if (unfiltered_data_.count(name) == 0) {
      unfiltered_data_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(name),
          std::forward_as_tuple(
              reader,
              batch_size,
              fragment_metadata,
              result_tiles,
              name,
              var_sized,
              nullable,
              dups));
    } else {
      unfiltered_data_.at(name).add_tiles(
          reader,
          batch_size,
          fragment_metadata,
          result_tiles,
          name,
          var_sized,
          nullable,
          dups);
    }
  }

  /**
   * Get the unfiltered data object for a field.
   *
   * @param name Name of the field.
   * @return UnfilteredData object.
   */
  UnfilteredData& get(std::string name) {
    return unfiltered_data_.find(name)->second;
  }

  /**
   * Clear the data for a fragment.
   *
   * @param f Fragment index.
   * @param keep_last_block Option to keep the last block. This is used for
   * global order reads with no duplicates and fragments consolidated with
   * timestamps.
   */
  void clear_fragment_data(unsigned f, bool keep_last_block) {
    for (auto& unfiltered_data : unfiltered_data_) {
      unfiltered_data.second.clear_fragment_data(f, keep_last_block);
    }
  }

  /** Clears all unfiltered data. */
  void clear() {
    unfiltered_data_.clear();
  }

  /** @return Memory usage for the unfiltered data. */
  uint64_t memory_usage() const {
    uint64_t memory_usage = 0;
    for (auto& unfiltered_data : unfiltered_data_) {
      memory_usage += unfiltered_data.second.memory_usage();
    }

    return memory_usage;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */
  std::unordered_map<std::string, UnfilteredData> unfiltered_data_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNFILTERED_DATA_H
