/**
 * @file  offsets_fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines class OffsetsFragmentMetadata.
 */

#ifndef TILEDB_OFFSETS_FRAGMENT_METADATA_H
#define TILEDB_OFFSETS_FRAGMENT_METADATA_H

#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb {
namespace sm {

class FragmentMetadata;

/** Collection of lazily loaded fragment metadata */
class OffsetsFragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /* Destructor */
  virtual ~OffsetsFragmentMetadata() = default;

  /**
   * Constructor.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   */
  OffsetsFragmentMetadata(
      FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the tile offsets. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_offsets()
      const {
    return tile_offsets_;
  }

  /** tile_offsets accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_offsets() {
    return tile_offsets_;
  }

  /** Returns the variable tile offsets. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_offsets()
      const {
    return tile_var_offsets_;
  }

  /** tile_var_offsets accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_offsets() {
    return tile_var_offsets_;
  }

  /** tile_var_offsets_mtx accessor */
  std::deque<std::mutex>& tile_var_offsets_mtx() {
    return tile_var_offsets_mtx_;
  }

  /** Returns the sizes of the uncompressed variable tiles. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_sizes()
      const {
    return tile_var_sizes_;
  }

  /** tile_var_sizes  accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_sizes() {
    return tile_var_sizes_;
  }

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given attribute or dimension and tile index.
   * If the attribute/dimension is var-sized, this will return the persisted
   * size of the offsets tile.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Size.
   */
  uint64_t persisted_tile_size(
      const std::string& name, uint64_t tile_idx) const;

  /**
   * Loads tile offsets for the attribute/dimension names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute/dimension names.
   */
  void load_tile_offsets(
      const EncryptionKey& encryption_key, std::vector<std::string>& names);

  /** Frees the memory associated with tile_offsets. */
  void free_tile_offsets();

  /**
   * Retrieves the starting offset of the input tile of the input attribute
   * or dimension in the file. If the attribute/dimension is var-sized, it
   * returns the starting offset of the offsets tile.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return The file offset to be retrieved.
   */
  uint64_t file_offset(const std::string& name, uint64_t tile_idx) const;

  /**
   * Resize tile offsets related vectors.
   */
  void resize_tile_offsets_vectors(uint64_t size);

  /**
   * Resize tile var offsets related vectors.
   */
  void resize_tile_var_offsets_vectors(uint64_t size);

  /**
   * Resize tile var sizes related vectors.
   */
  void resize_tile_var_sizes_vectors(uint64_t size);

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given var-sized attribute or dimension
   * and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Size.
   */
  uint64_t persisted_tile_var_size(
      const std::string& name, uint64_t tile_idx) const;

  /**
   * Retrieves the starting offset of the input tile of input attribute or
   * dimension in the file. The attribute/dimension must be var-sized.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return The file offset to be retrieved.
   */
  uint64_t file_var_offset(const std::string& name, uint64_t tile_idx) const;

  /**
   * Retrieves the (uncompressed) tile size for a given var-sized attribute or
   * dimension and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Size.
   */
  uint64_t tile_var_size(const std::string& name, uint64_t tile_idx);

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   * */
  void load_tile_var_sizes(
      const EncryptionKey& encryption_key, const std::string& name);

 protected:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  FragmentMetadata& parent_fragment_;

  /**
   * The memory tracker of the array this fragment metadata corresponds to.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /**
   * The tile offsets in their corresponding attribute files. Meaningful only
   * when there is compression.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> tile_offsets_;

  /** Mutex per tile offset loading. */
  std::deque<std::mutex> tile_offsets_mtx_;

  /** Mutex per tile var offset loading. */
  std::deque<std::mutex> tile_var_offsets_mtx_;

  /**
   * The variable tile offsets in their corresponding attribute files.
   * Meaningful only for variable-sized tiles.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> tile_var_offsets_;

  /**
   * The sizes of the uncompressed variable tiles.
   * Meaningful only when there is compression for variable tiles.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> tile_var_sizes_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Loads the tile offsets for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the tile offsets for the input attribute or dimension from the
   * input buffer.
   */
  void load_tile_offsets(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the variable tile offsets for the input attribute or dimension from
   * the input buffer.
   */
  void load_tile_var_offsets(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_var_sizes(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the variable tile sizes for the input attribute or dimension from
   * the input buffer.
   */
  void load_tile_var_sizes(unsigned idx, Deserializer& deserializer);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OFFSETS_FRAGMENT_METADATA_H
