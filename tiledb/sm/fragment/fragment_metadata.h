/**
 * @file  fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class FragmentMetadata.
 */

#ifndef TILEDB_FRAGMENT_METADATA_H
#define TILEDB_FRAGMENT_METADATA_H

#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/fragment/loaded_fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class Range;
}

namespace tiledb {
namespace sm {

class ArraySchema;
class Buffer;
class EncryptionKey;
class TileMetadata;
class MemoryTracker;

class FragmentMetadataStatusException : public StatusException {
 public:
  explicit FragmentMetadataStatusException(const std::string& message)
      : StatusException("FragmentMetadata", message) {
  }
};

/** Stores the metadata structures of a fragment. */
class FragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   */
  FragmentMetadata(
      ContextResources* resources,
      shared_ptr<MemoryTracker> memory_tracker,
      format_version_t version);

  /**
   * Constructor.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   * @param array_schema The schema of the array the fragment belongs to.
   * @param fragment_uri The fragment URI.
   * @param timestamp_range The timestamp range of the fragment.
   *     In TileDB, timestamps are in ms elapsed since
   *     1970-01-01 00:00:00 +0000 (UTC).
   * @param memory_tracker Memory tracker for the fragment metadata.
   * @param dense Indicates whether the fragment is dense or sparse.
   * @param has_timestamps Does the fragment contains timestamps.
   * @param has_delete_meta Does the fragment contains delete metadata.
   */
  FragmentMetadata(
      ContextResources* resources,
      const shared_ptr<const ArraySchema>& array_schema,
      const URI& fragment_uri,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      shared_ptr<MemoryTracker> memory_tracker,
      bool dense = true,
      bool has_timestamps = false,
      bool has_delete_mata = false);

  /** Destructor. */
  ~FragmentMetadata() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(FragmentMetadata);
  DISABLE_MOVE_AND_MOVE_ASSIGN(FragmentMetadata);

  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Stores the start offsets of the generic tiles stored in the
   * metadata file, each separately storing the various metadata
   * (e.g., R-Tree, tile offsets, etc).
   */
  struct GenericTileOffsets {
    uint64_t rtree_ = 0;
    std::vector<uint64_t> tile_offsets_;
    std::vector<uint64_t> tile_var_offsets_;
    std::vector<uint64_t> tile_var_sizes_;
    std::vector<uint64_t> tile_validity_offsets_;
    std::vector<uint64_t> tile_min_offsets_;
    std::vector<uint64_t> tile_max_offsets_;
    std::vector<uint64_t> tile_sum_offsets_;
    std::vector<uint64_t> tile_null_count_offsets_;
    uint64_t fragment_min_max_sum_null_count_offset_;
    uint64_t processed_conditions_offsets_;
  };

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns the number of dimensions and attributes.
   */
  inline uint64_t num_dims_and_attrs() const {
    return array_schema_->attribute_num() + array_schema_->dim_num() + 1 +
           has_timestamps_ + (has_delete_meta_ * 2);
  }

  /** Returns the number of cells in the fragment. */
  uint64_t cell_num() const;

  /** Returns the number of cells in the tile at the input position. */
  uint64_t cell_num(uint64_t tile_pos) const;

  /**
   * Returns the dimensions types from the array schema asssociated
   * with this fragment metadata.
   */
  std::vector<Datatype> dim_types() const;

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`.
   *
   * @param encryption_key The encryption key the array was opened with.
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   */
  void add_max_buffer_sizes(
      const EncryptionKey& encryption_key,
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the dense
   * case.
   *
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   */
  void add_max_buffer_sizes_dense(
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the dense
   * case.
   *
   * @tparam T The coordinates type.
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   */
  template <class T>
  void add_max_buffer_sizes_dense(
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the sparse
   * case.
   *
   * @param encryption_key The encryption key the array was opened with.
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   */
  void add_max_buffer_sizes_sparse(
      const EncryptionKey& encryption_key,
      const NDRange& subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Returns the ids (positions) of the tiles overlapping `subarray`, along with
   * with the coverage of the overlap.
   */
  template <class T>
  std::vector<std::pair<uint64_t, double>> compute_overlapping_tile_ids_cov(
      const T* subarray) const;

  /** Retrieves the fragment size. */
  uint64_t fragment_size() const;

  /**
   * Returns true if the corresponding fragment is dense, and false if it
   * is sparse.
   */
  inline bool dense() const {
    return dense_;
  }

  /** Returns the (expanded) domain in which the fragment is constrained. */
  inline const NDRange& domain() const {
    return domain_;
  }

  /** Returns the format version of this fragment. */
  inline format_version_t format_version() const {
    return version_;
  }

  /** Returns the fragment URI. */
  inline const URI& fragment_uri() const {
    return fragment_uri_;
  }

  /** Returns true if the metadata footer is consolidated. */
  inline bool has_consolidated_footer() const {
    return has_consolidated_footer_;
  }

  /** Returns true if the fragment has timestamps. */
  inline bool has_timestamps() const {
    return has_timestamps_;
  }

  /** Returns true if the fragment has delete metadata. */
  inline bool has_delete_meta() const {
    return has_delete_meta_;
  }

  inline bool has_tile_metadata() {
    return version_ >= constants::tile_metadata_min_version;
  }

  /** Returns the sizes of each attribute file. */
  inline const std::vector<uint64_t>& file_sizes() const {
    return file_sizes_;
  }

  /** Returns the sizes of each variable attribute file. */
  inline const std::vector<uint64_t>& file_var_sizes() const {
    return file_var_sizes_;
  }

  /** Returns the sizes of each validity attribute file. */
  inline const std::vector<uint64_t>& file_validity_sizes() const {
    return file_validity_sizes_;
  }

  /** Returns the number of sparse tiles. */
  inline const uint64_t& sparse_tile_num() const {
    return sparse_tile_num_;
  }

  /** Returns the tile index base value. */
  inline const uint64_t& tile_index_base() const {
    return tile_index_base_;
  }

  /** Returns the fragment timestamp range. */
  inline const std::pair<uint64_t, uint64_t>& timestamp_range() const {
    return timestamp_range_;
  }

  /** Returns the number of cells in the last tile. */
  inline const uint64_t& last_tile_cell_num() const {
    return last_tile_cell_num_;
  }

  /** Returns the non-empty domain of the fragment. */
  inline const NDRange& non_empty_domain() const {
    return non_empty_domain_;
  }

  /** Returns the generic tile offsets. */
  inline const GenericTileOffsets& generic_tile_offsets() const {
    return gt_offsets_;
  }

  /**
   * Initializes the fragment metadata structures.
   *
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   */
  void init(const NDRange& non_empty_domain);

  /**
   * Initializes the fragment's internal domain and non-empty domain members
   */
  void init_domain(const NDRange& non_empty_domain);

  /**
   * Loads the basic metadata from storage or `f_buff` for later
   * versions if it is not `nullptr`.
   */
  void load(
      const EncryptionKey& encryption_key,
      Tile* fragment_metadata_tile,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /**
   * Loads the fragment metadata of an open array given a vector of
   * fragment URIs `fragments_to_load`.
   * The function stores the fragment metadata of each fragment
   * in `fragments_to_load` into the returned vector, such
   * that there is a one-to-one correspondence between the two vectors.
   *
   * If `meta_buf` has data, then some fragment metadata may be contained
   * in there and does not need to be loaded from storage. In that
   * case, `offsets` helps identifying each fragment metadata in the
   * buffer.
   *
   * @param resources A context resources instance.
   * @param memory_tracker The memory tracker of the array
   *     for which the metadata is loaded. This will be passed to
   *     the constructor of each of the metadata loaded.
   * @param array_schema_latest The latest array schema.
   * @param array_schemas_all All the array schemas in a map keyed by the
   *     schema filename.
   * @param encryption_key The encryption key to use.
   * @param fragments_to_load The fragments whose metadata to load.
   * @param offsets A map from a fragment name to an offset in `meta_buff`
   *     where the basic fragment metadata can be found. If the offset
   *     cannot be found, then the metadata of that fragment will be loaded from
   *     storage instead.
   * @return Vector of FragmentMetadata is the fragment metadata to be
   * retrieved.
   */
  static std::vector<shared_ptr<FragmentMetadata>> load(
      ContextResources& resources,
      shared_ptr<MemoryTracker> memory_tracker,
      const shared_ptr<const ArraySchema> array_schema,
      const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
          array_schemas_all,
      const EncryptionKey& encryption_key,
      const std::vector<TimestampedURI>& fragments_to_load,
      const std::unordered_map<std::string, std::pair<Tile*, uint64_t>>&
          offsets);

  /**
   * Writes the R-tree to a tile.
   * @param loaded_metadata The loaded fragment metadata.
   */
  shared_ptr<WriterTile> write_rtree(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata);

  /**
   * Writes the R-tree to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the R-tree.
   */
  void store_rtree(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Stores all the metadata to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   */
  void store(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 7 to 10.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   */
  void store_v7_v10(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 11.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   */
  void store_v11(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 12 or higher.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   */
  void store_v12_v14(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 15 or higher.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   */
  void store_v15_or_higher(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key);

  /**
   * Simply sets the number of cells for the last tile.
   *
   * @param cell_num The number of cells for the last tile.
   * @return void
   */
  void set_last_tile_cell_num(uint64_t cell_num);

  /**
   * Resizes the per-tile metadata vectors for the given number of tiles. This
   * is not serialized, and is only used during writes.
   *
   * @param num_tiles Number of tiles
   */
  void set_num_tiles(uint64_t num_tiles);

  /**
   * Sets the tile "index base" which is added to the tile index in the set_*()
   * functions. Only used during global order writes/appends.
   *
   * Ex: if the first global order write adds 2 tiles (indices 0 and 1) to the
   * metadata, then tile index 0 in the second global order write should be tile
   * index 2 in the metadata, since there are already 2 tiles in the metadata.
   *
   * @param tile_base New tile index base
   */
  void set_tile_index_base(uint64_t tile_base);

  /**
   * Sets a tile offset for the input attribute or dimension.
   *
   * @param name The attribute/dimension for which the offset is set.
   * @param tid The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last tile size.
   * @param tile_offsets The tile offsets to set.
   * @return void
   */
  void set_tile_offset(
      const std::string& name,
      uint64_t tid,
      uint64_t step,
      tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_offsets);

  /**
   * Sets a variable tile offset for the input attribute or dimension.
   *
   * @param name The attribute/dimension for which the offset is set.
   * @param tid The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last variable tile size.
   * @param tile_var_offsets The variable tile offsets to set.
   * @return void
   */
  void set_tile_var_offset(
      const std::string& name,
      uint64_t tid,
      uint64_t step,
      tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_offsets);

  /**
   * Sets a variable tile size for the input attribute or dimension.
   *
   * @param name The attribute/dimension for which the size is set.
   * @param tid The index of the tile for which the offset is set.
   * @param size The size to be appended.
   * @param tile_var_sizes The variable tile sizes to set.
   * @return void
   */
  void set_tile_var_size(
      const std::string& name,
      uint64_t tid,
      uint64_t size,
      tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_var_sizes);

  /**
   * Sets a validity tile offset for the input attribute.
   *
   * @param name The attribute for which the offset is set.
   * @param tid The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last tile size.
   * @param tile_validity_offsets The validity tile offsets to set.
   * @return void
   */
  void set_tile_validity_offset(
      const std::string& name,
      uint64_t tid,
      uint64_t step,
      tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_validity_offsets);

  /**
   * Sets a tile min for the fixed input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param min The minimum.
   * @param tile_min_buffer The tile min buffer.
   * @return void
   */
  void set_tile_min(
      const std::string& name,
      uint64_t tid,
      const ByteVec& min,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_min_buffer);

  /**
   * Sets a tile min size for the var input attribute.
   *
   * @param name The attribute for which the min size is set.
   * @param tid The index of the tile for which the min is set.
   * @param size The size.
   * @param tile_min_buffer The tile min buffer.
   * @return void
   */
  void set_tile_min_var_size(
      const std::string& name,
      uint64_t tid,
      uint64_t size,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_min_buffer);

  /**
   * Sets a tile min for the var input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param min The minimum.
   * @param tile_min_buffer The tile min buffer.
   * @param tile_min_var_buffer The tile min var buffer.
   * @return void
   */
  void set_tile_min_var(
      const std::string& name,
      uint64_t tid,
      const ByteVec& min,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_min_buffer,
      tdb::pmr::vector<tdb::pmr::vector<char>>& tile_min_var_buffer);

  /**
   * Sets a tile max for the input attribute.
   *
   * @param name The attribute for which the max is set.
   * @param tid The index of the tile for which the max is set.
   * @param max The maximum.
   * @param tile_max_buffer The tile max buffer.
   * @return void
   */
  void set_tile_max(
      const std::string& name,
      uint64_t tid,
      const ByteVec& max,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer);

  /**
   * Sets a tile max for the var input attribute.
   *
   * @param name The attribute for which the min size is set.
   * @param tid The index of the tile for which the min is set.
   * @param size The size.
   * @param tile_max_buffer The tile max buffer.
   * @return void
   */
  void set_tile_max_var_size(
      const std::string& name,
      uint64_t tid,
      uint64_t size,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer);

  /**
   * Sets a tile max for the var input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param max The maximum.
   * @param tile_max_buffer The tile max buffer.
   * @param tile_max_var_buffer The tile max var buffer.
   * @return void
   */
  void set_tile_max_var(
      const std::string& name,
      uint64_t tid,
      const ByteVec& max,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer,
      tdb::pmr::vector<tdb::pmr::vector<char>>& tile_max_var_buffer);

  /**
   * Converts min/max sizes to offsets.
   *
   * @param name The attribute for which the offsets are converted
   * @param tile_min_var_buffer The tile min var buffer.
   * @param tile_min_buffer The tile min buffer.
   * @param tile_max_var_buffer The tile max var buffer.
   * @param tile_max_buffer The tile max buffer.
   * @return void
   */
  void convert_tile_min_max_var_sizes_to_offsets(
      const std::string& name,
      tdb::pmr::vector<tdb::pmr::vector<char>>& tile_min_var_buffer,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_min_buffer,
      tdb::pmr::vector<tdb::pmr::vector<char>>& tile_max_var_buffer,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_max_buffer);

  /**
   * Sets a tile sum for the input attribute.
   *
   * @param name The attribute for which the sum is set.
   * @param tid The index of the tile for which the sum is set.
   * @param sum The sum.
   * @param tile_sums The tile sums to set.
   * @return void
   */
  void set_tile_sum(
      const std::string& name,
      uint64_t tid,
      const ByteVec& sum,
      tdb::pmr::vector<tdb::pmr::vector<uint8_t>>& tile_sums);

  /**
   * Sets a tile null count for the input attribute.
   *
   * @param name The attribute for which the null count is set.
   * @param tid The index of the tile for which the null count is set.
   * @param sum The null count.
   * @param tile_null_counts The tile null counts to set.
   * @return void
   */
  void set_tile_null_count(
      const std::string& name,
      uint64_t tid,
      uint64_t null_count,
      tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& tile_null_counts);

  /**
   * Sets array schema pointer.
   *
   * @param array_schema The schema pointer.
   * @return void
   */
  void set_array_schema(const shared_ptr<const ArraySchema>& array_schema);

  /** Sets the array_schema name */
  void set_schema_name(const std::string& name);

  /** Sets the internal dense_ field*/
  void set_dense(bool dense);

  /** Returns the number of tiles in the fragment. */
  uint64_t tile_num() const;

  /** Returns the URI of the input attribute/dimension. */
  URI uri(const std::string& name) const;

  /** Returns the URI of the input variable-sized attribute/dimension. */
  URI var_uri(const std::string& name) const;

  /** Returns the validity URI of the input nullable attribute. */
  URI validity_uri(const std::string& name) const;

  /** Return the array schema name. */
  const std::string& array_schema_name() const;

  uint64_t footer_size() const;

  /**
   * Returns the (uncompressed) tile size for a given attribute or dimension
   * and tile index. If the attribute/dimension is var-sized, this will return
   * the size of the offsets tile.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return The tile size.
   */
  uint64_t tile_size(const std::string& name, uint64_t tile_idx) const;

  /** Returns the first timestamp of the fragment timestamp range. */
  uint64_t first_timestamp() const;

  /**
   * Returns `true` if the timestamp of the first operand is smaller,
   * breaking ties based on the URI string.
   */
  bool operator<(const FragmentMetadata& metadata) const;

  /** Serializes the fragment metadata footer into the input buffer. */
  void write_footer(Serializer& serializer) const;

  /**
   * Checks if the fragment overlaps partially (not fully) with a given
   * array open - end time. Assumes overlapping fragment and array open - close
   * times.
   *
   * @param array_start_timestamp Array open time
   * @param array_end_timestamp Array end time
   *
   * @return True if there is partial overlap, false if there is full
   */
  inline bool partial_time_overlap(
      const uint64_t array_start_timestamp,
      const uint64_t array_end_timestamp) const {
    const auto fragment_timestamp_start = timestamp_range_.first;
    const auto fragment_timestamp_end = timestamp_range_.second;
    // This method assumes overlapping fragment and array times so checking that
    // we don't have full overlap is sufficient for detecting partial overlap.
    auto full_fragment_overlap =
        (fragment_timestamp_start >= array_start_timestamp) &&
        (fragment_timestamp_end <= array_end_timestamp);
    return !full_fragment_overlap;
  }

  /**
   * Returns ArraySchema
   *
   * @return
   */
  const shared_ptr<const ArraySchema>& array_schema() const;

  /** File sizes accessor */
  std::vector<uint64_t>& file_sizes() {
    return file_sizes_;
  }

  /** File var sizes accessor */
  std::vector<uint64_t>& file_var_sizes() {
    return file_var_sizes_;
  }

  /** File validity sizes accessor */
  std::vector<uint64_t>& file_validity_sizes() {
    return file_validity_sizes_;
  }

  /** Fragment uri accessor */
  URI& fragment_uri() {
    return fragment_uri_;
  }

  /** has_timestamps accessor */
  bool& has_timestamps() {
    return has_timestamps_;
  }

  /** has_delete_meta accessor */
  bool& has_delete_meta() {
    return has_delete_meta_;
  }

  /** has_consolidated_footer accessor */
  bool& has_consolidated_footer() {
    return has_consolidated_footer_;
  }

  /** sparse_tile_num accessor */
  uint64_t& sparse_tile_num() {
    return sparse_tile_num_;
  }

  /** tile_index_base accessor */
  uint64_t& tile_index_base() {
    return tile_index_base_;
  }

  /** version accessor */
  uint32_t& version() {
    return version_;
  }

  /** timestamp_range accessor */
  std::pair<uint64_t, uint64_t>& timestamp_range() {
    return timestamp_range_;
  }

  /** last_tile_cell_num accessor */
  uint64_t& last_tile_cell_num() {
    return last_tile_cell_num_;
  }

  /** non_empty_domain accessor */
  NDRange& non_empty_domain() {
    return non_empty_domain_;
  }

  /** gt_offsets_ accessor */
  inline GenericTileOffsets& generic_tile_offsets() {
    return gt_offsets_;
  }

  /** set the CR pointer during deserialization*/
  void set_context_resources(ContextResources* cr) {
    resources_ = cr;
  }

  inline LoadedFragmentMetadata* loaded_metadata() {
    return loaded_metadata_ptr_;
  }

  inline const LoadedFragmentMetadata* loaded_metadata() const {
    return loaded_metadata_ptr_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager. */
  ContextResources* resources_;

  /**
   * The memory tracker of the array this fragment metadata corresponds to.
   */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The array schema */
  shared_ptr<const ArraySchema> array_schema_;

  /** The array schema name */
  std::string array_schema_name_;

  /**
   * Maps an attribute or dimension to an index used in the various vector
   * class members. Attributes are first, then TILEDB_COORDS, then the
   * dimensions, then timestamp.
   */
  std::unordered_map<std::string, unsigned> idx_map_;

  /** True if the fragment is dense, and false if it is sparse. */
  bool dense_;

  /**
   * The (expanded) domain in which the fragment is constrained. "Expanded"
   * means that the domain is enlarged minimally to coincide with tile
   * boundaries (if there is a tile grid imposed by tile extents). Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  NDRange domain_;

  /** Stores the size of each attribute file. */
  std::vector<uint64_t> file_sizes_;

  /** Stores the size of each variable attribute file. */
  std::vector<uint64_t> file_var_sizes_;

  /** Stores the size of each validity attribute file. */
  std::vector<uint64_t> file_validity_sizes_;

  /** Size of the fragment metadata footer. */
  uint64_t footer_size_;

  /** Offset of the fragment metadata footer. */
  uint64_t footer_offset_;

  /** The uri of the fragment the metadata belongs to. */
  URI fragment_uri_;

  /** True if the fragment metadata footer appears in a consolidated file. */
  bool has_consolidated_footer_;

  /** Number of cells in the last tile (meaningful only in the sparse case). */
  uint64_t last_tile_cell_num_;

  /** True if the fragment has timestamps, and false otherwise. */
  bool has_timestamps_;

  /** True if the fragment has delete metadata, and false otherwise. */
  bool has_delete_meta_;

  /** Number of sparse tiles. */
  uint64_t sparse_tile_num_;

  /** The size of the fragment metadata file. */
  uint64_t meta_file_size_;

  /** Local mutex for thread-safety. */
  std::mutex mtx_;

  /** The non-empty domain of the fragment. */
  NDRange non_empty_domain_;

  /**
   * The tile index base which is added to tile indices in setter functions.
   * Only used in global order writes.
   */
  uint64_t tile_index_base_;

  /** The format version of this metadata. */
  uint32_t version_;

  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** Stores the generic tile offsets, facilitating loading. */
  GenericTileOffsets gt_offsets_;

  /** The uri of the array the metadata belongs to. */
  URI array_uri_;

  shared_ptr<LoadedFragmentMetadata> loaded_metadata_;

  LoadedFragmentMetadata* loaded_metadata_ptr_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Retrieves the offset in the fragment metadata file of the footer
   * (which contains the generic tile offsets) along with its size.
   */
  void get_footer_offset_and_size(uint64_t* offset, uint64_t* size) const;

  /**
   * Returns the size of the fragment metadata footer
   * (which contains the generic tile offsets) along with its size.
   *
   * Applicable to format versions 3 and 4.
   */
  uint64_t footer_size_v3_v4() const;

  /**
   * Returns the size of the fragment metadata footer
   * (which contains the generic tile offsets) along with its size.
   *
   * Applicable to format versions 5 and 6.
   */
  uint64_t footer_size_v5_v6() const;

  /**
   * Returns the size of the fragment metadata footer
   * (which contains the generic tile offsets) along with its size.
   *
   * Applicable to format version 7 to 9.
   */
  uint64_t footer_size_v7_v9() const;

  /**
   * Returns the ids (positions) of the tiles overlapping `subarray`.
   * Applicable only to dense arrays.
   */
  template <class T>
  std::vector<uint64_t> compute_overlapping_tile_ids(const T* subarray) const;

  /**
   * Retrieves the tile domain for the input `subarray` based on the expanded
   * `domain_`.
   *
   * @tparam T The domain type.
   * @param subarray The targeted subarray.
   * @param subarray_tile_domain The tile domain to be retrieved.
   */
  template <class T>
  void get_subarray_tile_domain(
      const T* subarray, T* subarray_tile_domain) const;

  /**
   * Expands the non-empty domain using the input MBR.
   */
  void expand_non_empty_domain(const NDRange& mbr);

  /** Loads the generic tile offsets from the buffer. */
  void load_generic_tile_offsets(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 3 and 4.
   */
  void load_generic_tile_offsets_v3_v4(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 5 and 6.
   */
  void load_generic_tile_offsets_v5_v6(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 7 to 10.
   */
  void load_generic_tile_offsets_v7_v10(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 11.
   */
  void load_generic_tile_offsets_v11(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 12 to 15.
   */
  void load_generic_tile_offsets_v12_v15(Deserializer& deserializer);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 16 or higher.
   */
  void load_generic_tile_offsets_v16_or_higher(Deserializer& deserializer);

  /**
   * Loads the array schema name.
   */
  void load_array_schema_name(Deserializer& deserializer);

  /**
   * Loads the bounding coordinates from the fragment metadata buffer.
   *
   * @param deserializer Deserializer to get data from.
   */
  void load_bounding_coords(Deserializer& deserializer);

  /** Loads the sizes of each attribute or dimension file from the buffer. */
  void load_file_sizes(Deserializer& deserializer);

  /**
   * Loads the sizes of each attribute or dimension file from the buffer.
   * Applicable to format versions 1 to 4.
   */
  void load_file_sizes_v1_v4(Deserializer& deserializer);

  /**
   * Loads the sizes of each attribute or dimension file from the buffer.
   * Applicable to format version 5 or higher.
   */
  void load_file_sizes_v5_or_higher(Deserializer& deserializer);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer.
   */
  void load_file_var_sizes(Deserializer& deserializer);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer. Applicable to version 1 to 4.
   */
  void load_file_var_sizes_v1_v4(Deserializer& deserializer);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer. Applicable to version 5 or higher.
   */
  void load_file_var_sizes_v5_or_higher(Deserializer& deserializer);

  /** Loads the sizes of each attribute validity file from the buffer. */
  void load_file_validity_sizes(Deserializer& deserializer);

  /**
   * Loads the cell number of the last tile from the fragment metadata buffer.
   *
   * @param deserializer Deserializer to get data from.
   */
  void load_last_tile_cell_num(Deserializer& deserializer);

  /**
   * Loads the `has_timestamps_` field from the buffer.
   *
   * @param deserializer Deserializer to get data from.
   */
  void load_has_timestamps(Deserializer& deserializer);

  /**
   * Loads the `has_delete_meta_` field from the buffer.
   *
   * @param deserializer Deserializer to get data from.
   */
  void load_has_delete_meta(Deserializer& deserializer);

  /**
   * Loads the MBRs from the fragment metadata buffer.
   *
   * @param deserializer Deserializer to get data from.
   */
  void load_mbrs(Deserializer& deserializer);

  /** Loads the non-empty domain from the input buffer. */
  void load_non_empty_domain(Deserializer& deserializer);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions <= 2.
   */
  void load_non_empty_domain_v1_v2(Deserializer& deserializer);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions 3 and 4.
   */
  void load_non_empty_domain_v3_v4(Deserializer& deserializer);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions >= 5.
   */
  void load_non_empty_domain_v5_or_higher(Deserializer& deserializer);

  /** Loads the format version from the buffer. */
  void load_version(Deserializer& deserializer);

  /** Loads the `dense_` field from the buffer. */
  void load_dense(Deserializer& deserializer);

  /** Loads the number of sparse tiles from the buffer. */
  void load_sparse_tile_num(Deserializer& deserializer);

  /** Loads the basic metadata from storage (version 2 or before). */
  void load_v1_v2(
      const EncryptionKey& encryption_key,
      const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
          array_schemas);

  /**
   * Loads the basic metadata from storage or the input `f_buff` if
   * it is not `nullptr` (version 3 or after).
   */
  void load_v3_or_higher(
      const EncryptionKey& encryption_key,
      Tile* fragment_metadata_tile,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /**
   * Loads the footer of the metadata file, which contains
   * only some basic info. If `f_buff` is `nullptr, then
   * the footer will be loaded from the file, otherwise it
   * will be loaded from `f_buff`.
   */
  void load_footer(
      const EncryptionKey& encryption_key,
      Tile* fragment_metadata_tile,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /** Writes the sizes of each attribute file to the buffer. */
  void write_file_sizes(Serializer& serializer) const;

  /** Writes the sizes of each variable attribute file to the buffer. */
  void write_file_var_sizes(Serializer& serializer) const;

  /** Writes the sizes of each validitiy attribute file to the buffer. */
  void write_file_validity_sizes(Serializer& serializer) const;

  /** Writes the generic tile offsets to the buffer. */
  void write_generic_tile_offsets(Serializer& serializer) const;

  /** Writes the array schema name. */
  void write_array_schema_name(Serializer& serializer) const;

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   */
  void write_last_tile_cell_num(Serializer& serializer) const;

  /**
   * Writes the `has_timestamps_` field to the fragment metadata buffer.
   */
  void write_has_timestamps(Serializer& serializer) const;

  /**
   * Writes the `has_delete_meta_` field to the fragment metadata buffer.
   */
  void write_has_delete_meta(Serializer& serializer) const;

  /** Stores a footer with the basic information. */
  void store_footer(const EncryptionKey& encryption_key);

  /** Writes the non-empty domain to the input buffer. */
  void write_non_empty_domain(Serializer& serializer) const;

  /**
   * Writes the tile offsets of the input attribute or dimension to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile offsets.
   */
  void store_tile_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the tile offsets of the input attribute or dimension idx to the
   * input buffer.
   */
  void write_tile_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the variable tile offsets of the input attribute or dimension
   * to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var offsets.
   */
  void store_tile_var_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the variable tile offsets of the input attribute or dimension idx
   * to the buffer.
   */
  void write_tile_var_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the variable tile sizes for the input attribute or dimension to
   * the buffer.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var sizes.
   */
  void store_tile_var_sizes(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the variable tile sizes for the input attribute or dimension
   * to storage.
   */
  void write_tile_var_sizes(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the validity tile offsets of the input attribute to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the validity tile
   * offsets.
   */
  void store_tile_validity_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the validity tile offsets of the input attribute idx to the
   * input buffer.
   */
  void write_tile_validity_offsets(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the mins of the input attribute to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the mins.
   */
  void store_tile_mins(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the mins of the input attribute idx to the input buffer.
   */
  void write_tile_mins(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the maxs of the input attribute to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the maxs.
   */
  void store_tile_maxs(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the maxs of the input attribute idx to the input buffer.
   */
  void write_tile_maxs(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the sums of the input attribute to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the sums.
   */
  void store_tile_sums(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the sums of the input attribute idx to the input buffer.
   */
  void write_tile_sums(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the null counts of the input attribute to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the null counts.
   */
  void store_tile_null_counts(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the null counts of the input attribute idx to the input buffer.
   */
  void write_tile_null_counts(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      unsigned idx,
      Serializer& serializer);

  /**
   * Writes the fragment min, max, sum and null count to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param num The number of attributes.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written.
   */
  void store_fragment_min_max_sum_null_count(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      uint64_t num,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /**
   * Writes the processed conditions to storage.
   *
   * @param loaded_metadata The loaded fragment metadata.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written.
   */
  void store_processed_conditions(
      shared_ptr<LoadedFragmentMetadata> loaded_metadata,
      const EncryptionKey& encryption_key,
      uint64_t* nbytes);

  /** Writes the format version to the buffer. */
  void write_version(Serializer& serializer) const;

  /** Writes the `dense_` field to the buffer. */
  void write_dense(Serializer& serializer) const;

  /** Writes the number of sparse tiles to the buffer. */
  void write_sparse_tile_num(Serializer& serializer) const;

  /**
   * Reads the contents of a generic tile starting at the input offset,
   * and returns a tile.
   */
  shared_ptr<Tile> read_generic_tile_from_file(
      const EncryptionKey& encryption_key, uint64_t offset) const;

  /**
   * Reads the fragment metadata file footer (which contains the generic tile
   * offsets) into the input buffer.
   */
  void read_file_footer(
      std::shared_ptr<Tile>& tile,
      uint64_t* footer_offset,
      uint64_t* footer_size) const;

  /**
   * Writes the contents of the input tile as a separate
   * generic tile to the metadata file.
   *
   * @param encryption_key The encryption key.
   * @param tile The tile whose contents the function will write.
   * @param nbytes The total number of bytes written to the file.
   */
  void write_generic_tile_to_file(
      const EncryptionKey& encryption_key,
      shared_ptr<WriterTile> tile,
      uint64_t* nbytes) const;

  /**
   * Writes the contents of the input buffer at the end of the fragment
   * metadata file, without applying any filters. This helps its quick
   * retrieval upon reading (as its size is predictable based on the
   * number of attributes).
   */
  void write_footer_to_file(shared_ptr<WriterTile>) const;

  /**
   * Simple clean up function called in the case of error. It removes the
   * fragment metadata file and unlocks the array.
   */
  void clean_up();

  /**
   * Encodes a dimension/attribute name to use in a file name. The
   * motiviation is to encode illegal/reserved file name characters.
   *
   * @param name The dimension/attribute name.
   * @return the encoded dimension/attribute name.
   */
  std::string encode_name(const std::string& name) const;

  /**
   * This builds the index mapping for attribute/dimension name to id.
   */
  void build_idx_map();

  friend class LoadedFragmentMetadata;
  friend class OndemandFragmentMetadata;
  friend class V1V2PreloadedFragmentMetadata;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_METADATA_H
