/**
 * @file filtered_data.h
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
 * Defines the FilteredData class.
 */

#ifndef TILEDB_FILTERED_DATA_H
#define TILEDB_FILTERED_DATA_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A filtered data block containing filtered data for multiple tiles. The block
 * will contain a number of contiguous on-disk tiles and the data is identified
 * by the fragment index and offset/size of the data in the on-disk file.
 *
 * This uses a vector for storage which will be replaced by datablocks when
 * ready.
 */
class FilteredDataBlock {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param frag_idx Fragment index that identifies which fragment the data is
   * coming from.
   * @param offset File offset of the on-disk data for this datablock.
   * @param size Size of the on-disk data for this data block.
   */
  FilteredDataBlock(unsigned frag_idx, uint64_t offset, uint64_t size)
      : frag_idx_(frag_idx)
      , offset_(offset)
      , filtered_data_(size) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** @return Fragment index for the data block. */
  inline unsigned frag_idx() const {
    return frag_idx_;
  }

  /** @return File offset of the on-disk data for this datablock. */
  inline storage_size_t offset() const {
    return offset_;
  }

  /**
   * @return Pointer to the data at a particular offset in the filtered data
   * file.
   */
  inline void* data_at(storage_size_t offset) {
    return filtered_data_.data() + offset - offset_;
  }

  /** @return Pointer to the data inside of the filtered data block. */
  inline void* data() {
    return filtered_data_.data();
  }

  /** @return Size of the data block. */
  inline storage_size_t size() const {
    return filtered_data_.size();
  }

  /**
   * @return Does the current data block contain the data in the fragment
   * index, offset, and size.
   */
  inline bool contains(
      unsigned frag_idx, storage_size_t offset, storage_size_t size) const {
    return frag_idx == frag_idx_ && offset >= offset_ &&
           offset + size <= offset_ + filtered_data_.size();
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Fragment index for the data this data block contains. */
  unsigned frag_idx_;

  /** File offset of the on-disk data for this datablock. */
  storage_size_t offset_;

  /** Data for the data block. */
  std::vector<char> filtered_data_;
};

/**
 * Filtered data object that contains multiple data blocks for an attribute.
 * Each data block represents a single read that will be done by the VFS layer.
 * Tiles will point inside of the data block objects for their filtered data.
 * The data blocks are stored separately for fixed/var/nullable data. The
 * lifetime of this object will only be between the read_tiles call and the
 * unfilter_tiles call.
 */
class FilteredData {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor using a sorted list of result tiles.
   *
   * @param reader Reader object used to know which tile to skip.
   * @param min_batch_size Minimum batch size we are trying to reach.
   * @param max_batch_size Maximum batch size to create.
   * @param min_batch_gap Min gap between tiles we can tolerate in the data
   * block.
   * @param fragment_metadata Fragment metadata for the array.
   * @param result_tiles Sorted list (per fragment/tile index) of result tiles.
   * Only the fragment index and tile index of each result tiles is used here.
   * Nothing is mutated inside of the vector.
   * @param name Name of the attribute.
   * @param var_sized Is the attribute var sized?
   * @param nullable Is the attribute nullable?
   * @param storage_manager Storage manager.
   * @param read_tasks Read tasks to queue new tasks on for new data blocks.
   */
  FilteredData(
      const ReaderBase& reader,
      const uint64_t min_batch_size,
      const uint64_t max_batch_size,
      const uint64_t min_batch_gap,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const std::string& name,
      const bool var_sized,
      const bool nullable,
      StorageManager* storage_manager,
      std::vector<ThreadPool::Task>& read_tasks)
      : name_(name)
      , fragment_metadata_(fragment_metadata)
      , var_sized_(var_sized)
      , nullable_(nullable)
      , storage_manager_(storage_manager)
      , read_tasks_(read_tasks) {
    if (result_tiles.size() == 0) {
      return;
    }

    // Store data on the datablock in progress for fixed, var and nullable data.
    std::optional<unsigned> current_frag_idx{nullopt};
    storage_size_t current_fixed_offset{0};
    storage_size_t current_fixed_size{0};
    storage_size_t current_var_offset{0};
    storage_size_t current_var_size{0};
    storage_size_t current_nullable_offset{0};
    storage_size_t current_nullable_size{0};

    // Go through all the result tiles and create data blocks as we go.
    for (auto rt : result_tiles) {
      // See if we need to skip this tile.
      if (reader.skip_field(rt->frag_idx(), name)) {
        continue;
      }

      // Make new blocks, if required as we go for fixed, var and nullable data.
      auto fragment{fragment_metadata[rt->frag_idx()].get()};
      make_new_block_if_required(
          fragment,
          min_batch_size,
          max_batch_size,
          min_batch_gap,
          current_frag_idx,
          current_fixed_offset,
          current_fixed_size,
          rt,
          TileType::FIXED);

      if (var_sized) {
        make_new_block_if_required(
            fragment,
            min_batch_size,
            max_batch_size,
            min_batch_gap,
            current_frag_idx,
            current_var_offset,
            current_var_size,
            rt,
            TileType::VAR);
      }

      if (nullable) {
        make_new_block_if_required(
            fragment,
            min_batch_size,
            max_batch_size,
            min_batch_gap,
            current_frag_idx,
            current_nullable_offset,
            current_nullable_size,
            rt,
            TileType::NULLABLE);
      }

      current_frag_idx = rt->frag_idx();
    }

    // Finish by pushing the last in progress blocks.
    if (current_fixed_size != 0) {
      fixed_data_blocks_.emplace_back(
          *current_frag_idx, current_fixed_offset, current_fixed_size);
      queue_last_block_for_read(TileType::FIXED);
    }

    if (current_var_size != 0) {
      var_data_blocks_.emplace_back(
          *current_frag_idx, current_var_offset, current_var_size);
      queue_last_block_for_read(TileType::VAR);
    }

    if (current_nullable_size != 0) {
      nullable_data_blocks_.emplace_back(
          *current_frag_idx, current_nullable_offset, current_nullable_size);
      queue_last_block_for_read(TileType::NULLABLE);
    }

    current_fixed_data_block_ = fixed_data_blocks_.begin();
    current_var_data_block_ = var_data_blocks_.begin();
    current_nullable_data_block_ = nullable_data_blocks_.begin();
  }

  /** Destructor. */
  ~FilteredData() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Get the fixed filtered data for the result tile.
   *
   * @param fragment Fragment metadata for the tile.
   * @param rt Result tile.
   * @return Fixed filtered data pointer.
   */
  inline void* fixed_filtered_data(
      const FragmentMetadata* fragment, const ResultTile* rt) {
    auto offset{fragment->file_offset(name_, rt->tile_idx())};
    ensure_data_block_current(TileType::FIXED, fragment, rt, offset);
    return current_data_block(TileType::FIXED)->data_at(offset);
  }

  /**
   * Get the var filtered data for the result tile.
   *
   * @param fragment Fragment metadata for the tile.
   * @param rt Result tile.
   * @return Var filtered data pointer.
   */
  inline void* var_filtered_data(
      const FragmentMetadata* fragment, const ResultTile* rt) {
    if (!var_sized_) {
      return nullptr;
    }

    auto offset{fragment->file_var_offset(name_, rt->tile_idx())};
    ensure_data_block_current(TileType::VAR, fragment, rt, offset);
    return current_data_block(TileType::VAR)->data_at(offset);
  }

  /**
   * Get the nullable filtered data for the result tile.
   *
   * @param fragment Fragment metadata for the tile.
   * @param rt Result tile.
   * @return Nullable filtered data pointer.
   */
  inline void* nullable_filtered_data(
      const FragmentMetadata* fragment, const ResultTile* rt) {
    if (!nullable_) {
      return nullptr;
    }

    auto offset{fragment->file_validity_offset(name_, rt->tile_idx())};
    ensure_data_block_current(TileType::NULLABLE, fragment, rt, offset);
    return current_data_block(TileType::NULLABLE)->data_at(offset);
  }

 private:
  /* ********************************* */
  /*           PRIVATE ENUMS           */
  /* ********************************* */

  /** Tile type. */
  enum TileType { FIXED = 0, VAR = 1, NULLABLE = 2 };

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Queue the last added data block for read.
   *
   * @param type Tile type.
   */
  void queue_last_block_for_read(TileType type) {
    auto& block{data_blocks(type).back()};
    auto offset{block.offset()};
    auto data{block.data()};
    auto size{block.size()};
    URI uri{file_uri(fragment_metadata_[block.frag_idx()].get(), type)};
    auto task =
        storage_manager_->io_tp()->execute([this, offset, data, size, uri]() {
          RETURN_NOT_OK(
              storage_manager_->vfs()->read(uri, offset, data, size, false));
          return Status::Ok();
        });
    read_tasks_.push_back(std::move(task));
  }

  /** @return Data blocks corresponding to the tile type. */
  inline std::vector<FilteredDataBlock>& data_blocks(const TileType type) {
    switch (type) {
      case TileType::FIXED:
        return fixed_data_blocks_;
      case TileType::VAR:
        return var_data_blocks_;
      case TileType::NULLABLE:
        return nullable_data_blocks_;
      default:
        throw std::logic_error("Unexpected");
    }
  }

  /** @return Current data block corresponding to the tile type. */
  inline std::vector<FilteredDataBlock>::iterator& current_data_block(
      const TileType type) {
    switch (type) {
      case TileType::FIXED:
        return current_fixed_data_block_;
      case TileType::VAR:
        return current_var_data_block_;
      case TileType::NULLABLE:
        return current_nullable_data_block_;
      default:
        throw std::logic_error("Unexpected");
    }
  }

  /**
   * Get the file offset for the tile type.
   *
   * @param fragment Fragment metadata for the tile.
   * @param type Tile type.
   * @param tile_idx Tile index.
   * @return File offset.
   */
  inline storage_size_t file_offset(
      const FragmentMetadata* fragment,
      const TileType type,
      const uint64_t tile_idx) {
    switch (type) {
      case TileType::FIXED:
        return fragment->file_offset(name_, tile_idx);
      case TileType::VAR:
        return fragment->file_var_offset(name_, tile_idx);
      case TileType::NULLABLE:
        return fragment->file_validity_offset(name_, tile_idx);
      default:
        throw std::logic_error("Unexpected");
    }
  }

  /**
   * Get the tile persisted size for the tile type.
   *
   * @param fragment Fragment metadata for the tile.
   * @param idx Tile type.
   * @param tile_idx Tile index.
   * @return Tile persisted size.
   */
  inline storage_size_t persisted_tile_size(
      const FragmentMetadata* fragment,
      const TileType type,
      const uint64_t tile_idx) {
    switch (type) {
      case TileType::FIXED:
        return fragment->persisted_tile_size(name_, tile_idx);
      case TileType::VAR:
        return fragment->persisted_tile_var_size(name_, tile_idx);
      case TileType::NULLABLE:
        return fragment->persisted_tile_validity_size(name_, tile_idx);
      default:
        throw std::logic_error("Unexpected");
    }
  }

  /**
   * Get the file uri for an the attribute.
   *
   * @param fragment Fragment metadata for the tile.
   * @param type Tile type.
   * @param tile_idx Tile index.
   * @return File uri.
   */
  inline URI file_uri(const FragmentMetadata* fragment, const TileType type) {
    switch (type) {
      case TileType::FIXED: {
        auto&& [status, uri]{fragment->uri(name_)};
        throw_if_not_ok(status);
        return std::move(*uri);
      }

      case TileType::VAR: {
        auto&& [status, uri]{fragment->var_uri(name_)};
        throw_if_not_ok(status);
        return std::move(*uri);
      }

      case TileType::NULLABLE: {
        auto&& [status, uri]{fragment->validity_uri(name_)};
        throw_if_not_ok(status);
        return std::move(*uri);
      }

      default:
        throw std::logic_error("Unexpected");
    }
  }

  /**
   * We create a new block if the fragment indexes between this tile and the
   * previous tile don't match, or if we have reached the minimum batch size and
   * the gap between this tile and the previous one is too large, or if we have
   * reached the maximum size.
   *
   * @param fragment Fragment metadata for the tile.
   * @param min_batch_size Minimum batch size we are trying to reach.
   * @param max_batch_size Maximum batch size to create.
   * @param min_batch_gap Min gap between tiles we can tolerate in the data
   * block.
   * @param current_frag_idx Current fragment index for the data blocks.
   * @param current_block_offset Current block offset.
   * @param current_block_size Current block size.
   * @param frag_idx Fragment index for the new tile.
   * @param offset Offset for the new tile.
   * @param size Size of the new tile.
   * @param blocks Currently created blocks.
   */
  void make_new_block_if_required(
      const FragmentMetadata* fragment,
      const uint64_t min_batch_size,
      const uint64_t max_batch_size,
      const uint64_t min_batch_gap,
      optional<unsigned> current_block_frag_idx,
      storage_size_t& current_block_offset,
      storage_size_t& current_block_size,
      const ResultTile* rt,
      const TileType type) {
    const auto tile_idx{rt->tile_idx()};
    storage_size_t offset{file_offset(fragment, type, tile_idx)};
    storage_size_t size{persisted_tile_size(fragment, type, tile_idx)};

    if (current_block_frag_idx == nullopt) {
      current_block_offset = offset;
      current_block_size = size;
      return;
    }

    uint64_t new_size{(offset + size) - current_block_offset};
    uint64_t gap{offset - (current_block_offset + current_block_size)};
    if (current_block_frag_idx == rt->frag_idx() &&
        new_size <= max_batch_size &&
        (new_size <= min_batch_size || gap <= min_batch_gap)) {
      // Extend current batch.
      current_block_size = new_size;
    } else {
      // Push the old batch and start a new one.
      data_blocks(type).emplace_back(
          *current_block_frag_idx, current_block_offset, current_block_size);
      queue_last_block_for_read(type);
      current_block_offset = offset;
      current_block_size = size;
    }
  }

  /**
   * Ensures the current data block corresponding to the tile type is the
   * correct one.
   *
   * @param type Tile type.
   * @param fragment Fragment metadata.
   * @param rt Result tile.
   * @param offset File offset of the data.
   */
  void ensure_data_block_current(
      const TileType type,
      const FragmentMetadata* fragment,
      const ResultTile* rt,
      const storage_size_t offset) {
    storage_size_t size{persisted_tile_size(fragment, type, rt->tile_idx())};

    auto& current_block{current_data_block(type)};
    if (!current_block->contains(rt->frag_idx(), offset, size)) {
      current_block++;

      if (current_block == data_blocks(type).end() ||
          !current_block->contains(rt->frag_idx(), offset, size)) {
        throw std::logic_error("Unexpected data block");
      }
    }
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Fixed data blocks. */
  std::vector<FilteredDataBlock> fixed_data_blocks_;

  /** Current fixed data block used when creating fixed tiles. */
  std::vector<FilteredDataBlock>::iterator current_fixed_data_block_;

  /** Var data blocks. */
  std::vector<FilteredDataBlock> var_data_blocks_;

  /** Current var data block used when creating var tiles. */
  std::vector<FilteredDataBlock>::iterator current_var_data_block_;

  /** Nullable data blocks. */
  std::vector<FilteredDataBlock> nullable_data_blocks_;

  /** Current nullable data block used when creating nullable tiles. */
  std::vector<FilteredDataBlock>::iterator current_nullable_data_block_;

  /** Name of the attribute. */
  const std::string& name_;

  /** Fragment metadata. */
  const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata_;

  /** Is the attribute var sized? */
  const bool var_sized_;

  /** Is the attribute nullable? */
  const bool nullable_;

  /** Storage manager. */
  StorageManager* storage_manager_;

  /** Read tasks. */
  std::vector<ThreadPool::Task>& read_tasks_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTERED_DATA_H
