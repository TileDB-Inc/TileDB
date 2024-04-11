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

  /** Returns the validity tile offsets. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>&
  tile_validity_offsets() const {
    return tile_validity_offsets_;
  }

  /** tile_validity_offsets accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_validity_offsets() {
    return tile_validity_offsets_;
  }

  /**
   * Resize tile validity offsets related vectors.
   */
  void resize_tile_validity_offsets_vectors(uint64_t size);

  /**
   * Retrieves the starting offset of the input validity tile of the
   * input attribute in the file.
   *
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The file offset to be retrieved.
   */
  uint64_t file_validity_offset(
      const std::string& name, uint64_t tile_idx) const;

  /**
   * Retrieves the size of the validity tile when it is persisted (e.g. the size
   * of the compressed tile on disk) for a given attribute.
   *
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return Size.
   */
  uint64_t persisted_tile_validity_size(
      const std::string& name, uint64_t tile_idx) const;

  /** Returns the tile min buffers variable length data. */
  inline const tdb::pmr::vector<tdb::pmr::vector<char>>& tile_min_var_buffer()
      const {
    return tile_min_var_buffer_;
  }

  /** tile_min_var_buffer accessor */
  tdb::pmr::vector<tdb::pmr::vector<char>>& tile_min_var_buffer() {
    return tile_min_var_buffer_;
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

  /**
   * Loads min values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   */
  void load_tile_min_values(
      const EncryptionKey& encryption_key, std::vector<std::string>& names);

  /**
   * Loads max values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   */
  void load_tile_max_values(
      const EncryptionKey& encryption_key, std::vector<std::string>& names);

  /**
   * Loads sum values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   */
  void load_tile_sum_values(
      const EncryptionKey& encryption_key, std::vector<std::string>& names);

  /**
   * Loads null count values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   */
  void load_tile_null_count_values(
      const EncryptionKey& encryption_key, std::vector<std::string>& names);

  /** Loads the R-tree from storage. */
  virtual void load_rtree(const EncryptionKey& encryption_key) = 0;

  /**
   * Loads the min max sum null count values for the fragment.
   *
   * @param encryption_key The key the array got opened with.
   */
  virtual void load_fragment_min_max_sum_null_count(
      const EncryptionKey& encryption_key) = 0;

  /**
   * Loads the processed conditions for the fragment. The processed conditions
   * is the list of delete/update conditions that have already been applied for
   * this fragment and don't need to be applied again.
   *
   * @param encryption_key The key the array got opened with.
   */
  virtual void load_processed_conditions(
      const EncryptionKey& encryption_key) = 0;

  /**
   * Retrieves the processed conditions. The processed conditions is the list
   * of delete/update conditions that have already been applied for this
   * fragment and don't need to be applied again.
   *
   * @return Processed conditions.
   */
  std::vector<std::string>& get_processed_conditions();

  /**
   * Retrieves the processed conditions set. The processed conditions is the
   * list of delete/update conditions that have already been applied for this
   * fragment and don't need to be applied again.
   *
   * @return Processed conditions set.
   */
  std::unordered_set<std::string>& get_processed_conditions_set();

  /**
   * Retrieves the min value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Value.
   */
  std::vector<uint8_t>& get_min(const std::string& name);

  /**
   * Retrieves the max value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Value.
   */
  std::vector<uint8_t>& get_max(const std::string& name);

  /**
   * Retrieves the sum value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Sum.
   */
  void* get_sum(const std::string& name);

  /**
   * Retrieves the null count value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Count.
   */
  uint64_t get_null_count(const std::string& name);

  /**
   * Retrieves the tile sum value for a given attribute or dimension and tile
   * index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Sum.
   */
  const void* get_tile_sum(const std::string& name, uint64_t tile_idx) const;

  /**
   * Retrieves the tile null count value for a given attribute or dimension
   * and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Count.
   */
  uint64_t get_tile_null_count(
      const std::string& name, uint64_t tile_idx) const;

  /** Returns the tile null count values for attributes/dimensions. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_null_counts()
      const {
    return tile_null_counts_;
  }

  /** tile_null_counts accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_null_counts() {
    return tile_null_counts_;
  }

  /** Returns the tile sum values for fixed sized data. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_sums() const {
    return tile_sums_;
  }

  /** tile_sums accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_sums() {
    return tile_sums_;
  }

  /** Returns the tile max buffers. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer()
      const {
    return tile_max_buffer_;
  }

  /** tile_max_buffer accessor */
  tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer() {
    return tile_max_buffer_;
  }

  /** Returns the tile max buffers variable length data. */
  inline const tdb::pmr::vector<tdb::pmr::vector<char>>& tile_max_var_buffer()
      const {
    return tile_max_var_buffer_;
  }

  /** tile_max_var_buffer accessor */
  tdb::pmr::vector<tdb::pmr::vector<char>>& tile_max_var_buffer() {
    return tile_max_var_buffer_;
  }

  /** Returns an RTree for the MBRs. */
  inline const RTree& rtree() const {
    return rtree_;
  }

  /** rtree accessor */
  RTree& rtree() {
    return rtree_;
  }

 protected:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  FragmentMetadata& parent_fragment_;

  /**
   * The memory tracker of the array this fragment metadata corresponds to.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** An RTree for the MBRs. */
  RTree rtree_;

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

  /**
   * The validity tile offsets in their corresponding attribute files.
   * Meaningful only when there is compression.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> tile_validity_offsets_;

  /**
   * The tile min buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint8_t>> tile_min_buffer_;

  /**
   * The tile min buffers variable length data.
   */
  tdb::pmr::vector<tdb::pmr::vector<char>> tile_min_var_buffer_;

  /**
   * The tile max buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint8_t>> tile_max_buffer_;

  /**
   * The tile max buffers variable length data.
   */
  tdb::pmr::vector<tdb::pmr::vector<char>> tile_max_var_buffer_;

  /**
   * The tile sum values, ignored for var sized attributes/dimensions.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint8_t>> tile_sums_;

  /**
   * The tile null count values for attributes/dimensions.
   */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> tile_null_counts_;

  /**
   * Fragment min values.
   */
  std::vector<std::vector<uint8_t>> fragment_mins_;

  /**
   * Fragment max values.
   */
  std::vector<std::vector<uint8_t>> fragment_maxs_;

  /**
   * Fragment sum values, ignored for var sized attributes/dimensions.
   */
  std::vector<uint64_t> fragment_sums_;

  /**
   * Null count for fragment for attributes/dimensions.
   */
  std::vector<uint64_t> fragment_null_counts_;

  /**
   * Ordered list of already processed delete/update conditions for this
   * fragment.
   */
  std::vector<std::string> processed_conditions_;

  /** Set of already processed delete/update conditions for this fragment. */
  std::unordered_set<std::string> processed_conditions_set_;

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
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   */
  virtual void load_tile_var_sizes(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the validity tile offsets for the input attribute idx from storage.
   */
  virtual void load_tile_validity_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the min values for the input attribute idx from storage.
   */
  virtual void load_tile_min_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the max values for the input attribute idx from storage.
   */
  virtual void load_tile_max_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the sum values for the input attribute idx from storage.
   */
  virtual void load_tile_sum_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the null count values for the input attribute idx from storage.
   */
  virtual void load_tile_null_count_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OFFSETS_FRAGMENT_METADATA_H
