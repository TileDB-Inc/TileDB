/**
 * @file  loaded_fragment_metadata.h
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
 * This file defines class LoadedFragmentMetadata.
 */

#ifndef TILEDB_LOADED_FRAGMENT_METADATA_H
#define TILEDB_LOADED_FRAGMENT_METADATA_H

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/storage_manager/context_resources.h"

namespace tiledb {
namespace sm {

class FragmentMetadata;

/** Collection of lazily loaded fragment metadata */
class LoadedFragmentMetadata {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Keeps track of which metadata is loaded. */
  struct LoadedMetadata {
    bool rtree_ = false;
    std::vector<bool> tile_offsets_;
    std::vector<bool> tile_var_offsets_;
    std::vector<bool> tile_var_sizes_;
    std::vector<bool> tile_validity_offsets_;
    std::vector<bool> tile_min_;
    std::vector<bool> tile_max_;
    std::vector<bool> tile_sum_;
    std::vector<bool> tile_null_count_;
    bool fragment_min_max_sum_null_count_ = false;
    bool processed_conditions_ = false;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /* Destructor */
  virtual ~LoadedFragmentMetadata() = default;

  /**
   * Constructor.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   */
  LoadedFragmentMetadata(
      FragmentMetadata& parent, shared_ptr<MemoryTracker> memory_tracker);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Create loaded metadata objects given a format version
   *
   * @param parent The parent fragment
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   * @param version The format version of the fragment
   */
  static shared_ptr<LoadedFragmentMetadata> create(
      FragmentMetadata& parent,
      shared_ptr<MemoryTracker> memory_tracker,
      format_version_t version);

  /** Returns the tile offsets. */
  inline std::span<const tdb::pmr::vector<uint64_t>> tile_offsets() const {
    return tile_offsets_;
  }

  /** tile_offsets accessor */
  std::span<tdb::pmr::vector<uint64_t>> tile_offsets() {
    return tile_offsets_;
  }

  /** Returns the variable tile offsets. */
  inline std::span<const tdb::pmr::vector<uint64_t>> tile_var_offsets() const {
    return tile_var_offsets_;
  }

  /** tile_var_offsets accessor */
  std::span<tdb::pmr::vector<uint64_t>> tile_var_offsets() {
    return tile_var_offsets_;
  }

  /** tile_var_offsets_mtx accessor */
  std::deque<std::mutex>& tile_var_offsets_mtx() {
    return tile_var_offsets_mtx_;
  }

  /** Returns the sizes of the uncompressed variable tiles. */
  inline std::span<const tdb::pmr::vector<uint64_t>> tile_var_sizes() const {
    return tile_var_sizes_;
  }

  /** tile_var_sizes  accessor */
  std::span<tdb::pmr::vector<uint64_t>> tile_var_sizes() {
    return tile_var_sizes_;
  }

  /** Returns the validity tile offsets. */
  inline std::span<const tdb::pmr::vector<uint64_t>> tile_validity_offsets()
      const {
    return tile_validity_offsets_;
  }

  /** tile_validity_offsets accessor */
  std::span<tdb::pmr::vector<uint64_t>> tile_validity_offsets() {
    return tile_validity_offsets_;
  }

  /**
   * Resize tile validity offsets related vectors.
   *
   * @param size The new size
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
  inline std::span<const tdb::pmr::vector<char>> tile_min_var_buffer() const {
    return tile_min_var_buffer_;
  }

  /** tile_min_var_buffer accessor */
  std::span<tdb::pmr::vector<char>> tile_min_var_buffer() {
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
   *
   * @param size The new size
   */
  void resize_tile_offsets_vectors(uint64_t size);

  /**
   * Resize tile var offsets related vectors.
   *
   * @param size The new size
   */
  void resize_tile_var_offsets_vectors(uint64_t size);

  /**
   * Resize tile var sizes related vectors.
   *
   * @param size The new size
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
  inline std::span<const tdb::pmr::vector<uint64_t>> tile_null_counts() const {
    return tile_null_counts_;
  }

  /** tile_null_counts accessor */
  std::span<tdb::pmr::vector<uint64_t>> tile_null_counts() {
    return tile_null_counts_;
  }

  /** Returns the tile sum values for fixed sized data. */
  inline std::span<const tdb::pmr::vector<uint8_t>> tile_sums() const {
    return tile_sums_;
  }

  /** tile_sums accessor */
  std::span<tdb::pmr::vector<uint8_t>> tile_sums() {
    return tile_sums_;
  }

  /** Returns the tile max buffers. */
  inline std::span<const tdb::pmr::vector<uint8_t>> tile_max_buffer() const {
    return tile_max_buffer_;
  }

  /** tile_max_buffer accessor */
  std::span<tdb::pmr::vector<uint8_t>> tile_max_buffer() {
    return tile_max_buffer_;
  }

  /** Returns the tile max buffers variable length data. */
  inline std::span<const tdb::pmr::vector<char>> tile_max_var_buffer() const {
    return tile_max_var_buffer_;
  }

  /** tile_max_var_buffer accessor */
  std::span<tdb::pmr::vector<char>> tile_max_var_buffer() {
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

  /** loaded_metadata_.rtree_ accessor */
  void set_rtree_loaded() {
    loaded_metadata_.rtree_ = true;
  }

  /**
   * Retrieves the overlap of all MBRs with the input ND range.
   *
   * @param range The range to use
   * @param is_default If default range should be used
   * @param tile_overlap The resulted tile overlap
   */
  virtual void get_tile_overlap(
      const NDRange& range,
      std::vector<bool>& is_default,
      TileOverlap* tile_overlap) = 0;

  /**
   * Compute tile bitmap for the curent fragment/range/dimension.
   *
   * @param range The range to use
   * @param d The dimension index
   * @param tile_bitmap The resulted tile bitmap
   */
  virtual void compute_tile_bitmap(
      const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap) = 0;

  /** Frees the memory associated with the rtree. */
  void free_rtree();

  /**
   * Sets loaded metadata, used in serialization
   *
   * @param loaded_metadata The metadata to set
   */
  inline void set_loaded_metadata(const LoadedMetadata& loaded_metadata) {
    loaded_metadata_ = loaded_metadata;
  }

  /** loaded_metadata accessor . */
  inline const LoadedMetadata& loaded_metadata() const {
    return loaded_metadata_;
  }

  /** loaded_metadata accessor */
  LoadedMetadata& loaded_metadata() {
    return loaded_metadata_;
  }

  /**
   * Resizes all offsets and reset their loaded flags.
   *
   * @param The new size
   */
  void resize_offsets(uint64_t size);

  /** Returns the fragment mins. */
  inline const std::vector<std::vector<uint8_t>>& fragment_mins() const {
    return fragment_mins_;
  }

  /** Returns the fragment maxs. */
  inline const std::vector<std::vector<uint8_t>>& fragment_maxs() const {
    return fragment_maxs_;
  }

  /** Returns the fragment sums. */
  inline const std::vector<uint64_t>& fragment_sums() const {
    return fragment_sums_;
  }

  /** Returns the fragment null counts. */
  inline const std::vector<uint64_t>& fragment_null_counts() const {
    return fragment_null_counts_;
  }

  /** fragment_mins accessor */
  std::vector<std::vector<uint8_t>>& fragment_mins() {
    return fragment_mins_;
  }

  /** fragment_maxs accessor */
  std::vector<std::vector<uint8_t>>& fragment_maxs() {
    return fragment_maxs_;
  }

  /** fragment_sums accessor */
  std::vector<uint64_t>& fragment_sums() {
    return fragment_sums_;
  }

  /** fragment_null_counts accessor */
  std::vector<uint64_t>& fragment_null_counts() {
    return fragment_null_counts_;
  }

  /** Returns the tile min buffers. */
  inline std::span<const tdb::pmr::vector<uint8_t>> tile_min_buffer() const {
    return tile_min_buffer_;
  }

  /** tile_min_buffer accessor */
  std::span<tdb::pmr::vector<uint8_t>> tile_min_buffer() {
    return tile_min_buffer_;
  }

  /** Returns the processed conditions vector. */
  inline const std::vector<std::string>& processed_conditions() const {
    return processed_conditions_;
  }

  /** processed_conditions_ accessor */
  std::vector<std::string>& processed_conditions() {
    return processed_conditions_;
  }

  /** Returns the processed conditions set */
  std::unordered_set<std::string>& processed_conditions_set() {
    return processed_conditions_set_;
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
  pmr::vector_nfields<tdb::pmr::vector<uint64_t>> tile_offsets_;

  /** Mutex per tile offset loading. */
  std::deque<std::mutex> tile_offsets_mtx_;

  /** Mutex per tile var offset loading. */
  std::deque<std::mutex> tile_var_offsets_mtx_;

  /**
   * The variable tile offsets in their corresponding attribute files.
   * Meaningful only for variable-sized tiles.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint64_t>> tile_var_offsets_;

  /**
   * The sizes of the uncompressed variable tiles.
   * Meaningful only when there is compression for variable tiles.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint64_t>> tile_var_sizes_;

  /**
   * The validity tile offsets in their corresponding attribute files.
   * Meaningful only when there is compression.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint64_t>> tile_validity_offsets_;

  /**
   * The tile min buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint8_t>> tile_min_buffer_;

  /**
   * The tile min buffers variable length data.
   */
  pmr::vector_nfields<tdb::pmr::vector<char>> tile_min_var_buffer_;

  /**
   * The tile max buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint8_t>> tile_max_buffer_;

  /**
   * The tile max buffers variable length data.
   */
  pmr::vector_nfields<tdb::pmr::vector<char>> tile_max_var_buffer_;

  /**
   * The tile sum values, ignored for var sized attributes/dimensions.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint8_t>> tile_sums_;

  /**
   * The tile null count values for attributes/dimensions.
   */
  pmr::vector_nfields<tdb::pmr::vector<uint64_t>> tile_null_counts_;

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

  /** Keeps track of which metadata has been loaded. */
  LoadedMetadata loaded_metadata_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Sorts a dims vector by index
   *
   * @param names the vector to sort
   */
  void sort_names_by_index(std::vector<std::string>& names);

  /**
   * Loads the tile offsets for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_var_sizes(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the validity tile offsets for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_validity_offsets(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the min values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_min_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the max values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_max_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the sum values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_sum_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;

  /**
   * Loads the null count values for the input attribute idx from storage.
   *
   * @param encription_key The encryption key
   * @param idx Dimension index
   */
  virtual void load_tile_null_count_values(
      const EncryptionKey& encryption_key, unsigned idx) = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_LOADED_FRAGMENT_METADATA_H
