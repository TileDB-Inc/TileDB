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
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/rtree/rtree.h"

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
class MemoryTracker;
class StorageManager;

/** Stores the metadata structures of a fragment. */
class FragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FragmentMetadata();

  /**
   * Constructor.
   *
   * @param storage_manager A storage manager instance.
   * @param memory_tracker The memory tracker of the array this fragment
   *     metadata corresponds to.
   * @param array_schema The schema of the array the fragment belongs to.
   * @param fragment_uri The fragment URI.
   * @param timestamp_range The timestamp range of the fragment.
   *     In TileDB, timestamps are in ms elapsed since
   *     1970-01-01 00:00:00 +0000 (UTC).
   * @param dense Indicates whether the fragment is dense or sparse.
   * @param has_timestamps Does the fragment contains timestamps.
   * @param has_delete_meta Does the fragment contains delete metadata.
   */
  FragmentMetadata(
      StorageManager* storage_manager,
      MemoryTracker* memory_tracker,
      const shared_ptr<const ArraySchema>& array_schema,
      const URI& fragment_uri,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      bool dense = true,
      bool has_timestamps = false,
      bool has_delete_mata = false);

  /** Destructor. */
  ~FragmentMetadata();

  // Copy initialization
  FragmentMetadata(const FragmentMetadata& other);

  FragmentMetadata& operator=(const FragmentMetadata& other);

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
   * @return Status
   */
  Status add_max_buffer_sizes(
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
   * @return Status
   */
  Status add_max_buffer_sizes_dense(
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
   * @return Status
   */
  template <class T>
  Status add_max_buffer_sizes_dense(
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
   * @return Status
   */
  Status add_max_buffer_sizes_sparse(
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
  Status fragment_size(uint64_t* size) const;

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
  inline uint32_t format_version() const {
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

  /** Returns the tile offsets. */
  inline const std::vector<std::vector<uint64_t>>& tile_offsets() const {
    return tile_offsets_;
  }

  /** Returns the variable tile offsets. */
  inline const std::vector<std::vector<uint64_t>>& tile_var_offsets() const {
    return tile_var_offsets_;
  }

  /** Returns the sizes of the uncompressed variable tiles. */
  inline const std::vector<std::vector<uint64_t>>& tile_var_sizes() const {
    return tile_var_sizes_;
  }

  /** Returns the validity tile offsets. */
  inline const std::vector<std::vector<uint64_t>>& tile_validity_offsets()
      const {
    return tile_validity_offsets_;
  }

  /** Returns the tile min buffers. */
  inline const std::vector<std::vector<uint8_t>>& tile_min_buffer() const {
    return tile_min_buffer_;
  }

  /** Returns the tile min buffers variable length data. */
  inline const std::vector<std::vector<char>>& tile_min_var_buffer() const {
    return tile_min_var_buffer_;
  }

  /** Returns the tile max buffers. */
  inline const std::vector<std::vector<uint8_t>>& tile_max_buffer() const {
    return tile_max_buffer_;
  }

  /** Returns the tile max buffers variable length data. */
  inline const std::vector<std::vector<char>>& tile_max_var_buffer() const {
    return tile_max_var_buffer_;
  }

  /** Returns the tile sum values for fixed sized data. */
  inline const std::vector<std::vector<uint8_t>>& tile_sums() const {
    return tile_sums_;
  }

  /** Returns the tile null count values for attributes/dimensions. */
  inline const std::vector<std::vector<uint64_t>>& tile_null_counts() const {
    return tile_null_counts_;
  }

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

  /** Returns an RTree for the MBRs. */
  inline const RTree& rtree() const {
    return rtree_;
  }

  /**
   * Retrieves the overlap of all MBRs with the input ND range.
   */
  Status get_tile_overlap(
      const NDRange& range,
      std::vector<bool>& is_default,
      TileOverlap* tile_overlap);

  /**
   * Compute tile bitmap for the curent fragment/range/dimension.
   */
  void compute_tile_bitmap(
      const Range& range, unsigned d, std::vector<uint8_t>* tile_bitmap);

  /**
   * Initializes the fragment metadata structures.
   *
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   * @return Status
   */
  Status init(const NDRange& non_empty_domain);

  /**
   * Initializes the fragment's internal domain and non-empty domain members
   */
  Status init_domain(const NDRange& non_empty_domain);

  /**
   * Loads the basic metadata from storage or `f_buff` for later
   * versions if it is not `nullptr`.
   */
  Status load(
      const EncryptionKey& encryption_key,
      Buffer* f_buff,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /** Stores all the metadata to storage. */
  void store(const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 7 to 10.
   */
  Status store_v7_v10(const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 11.
   */
  Status store_v11(const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 12 or higher.
   */
  Status store_v12_v14(const EncryptionKey& encryption_key);

  /**
   * Stores all the metadata to storage.
   *
   * Applicable to format versions 15 or higher.
   */
  Status store_v15_or_higher(const EncryptionKey& encryption_key);

  /**
   * Simply sets the number of cells for the last tile.
   *
   * @param cell_num The number of cells for the last tile.
   * @return void
   */
  void set_last_tile_cell_num(uint64_t cell_num);

  /**
   * Sets the input tile's MBR in the fragment metadata. It also expands the
   * non-empty domain of the fragment.
   *
   * @param tile The tile index whose MBR will be set.
   * @param mbr The MBR to be set.
   * @return Status
   */
  Status set_mbr(uint64_t tile, const NDRange& mbr);

  /**
   * Resizes the per-tile metadata vectors for the given number of tiles. This
   * is not serialized, and is only used during writes.
   *
   * @param num_tiles Number of tiles
   * @return Status
   */
  Status set_num_tiles(uint64_t num_tiles);

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
   * @return void
   */
  void set_tile_offset(const std::string& name, uint64_t tid, uint64_t step);

  /**
   * Sets a variable tile offset for the input attribute or dimension.
   *
   * @param name The attribute/dimension for which the offset is set.
   * @param tid The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last variable tile size.
   * @return void
   */
  void set_tile_var_offset(
      const std::string& name, uint64_t tid, uint64_t step);

  /**
   * Sets a variable tile size for the input attribute or dimension.
   *
   * @param name The attribute/dimension for which the size is set.
   * @param tid The index of the tile for which the offset is set.
   * @param size The size to be appended.
   * @return void
   */
  void set_tile_var_size(const std::string& name, uint64_t tid, uint64_t size);

  /**
   * Sets a validity tile offset for the input attribute.
   *
   * @param name The attribute for which the offset is set.
   * @param tid The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last tile size.
   * @return void
   */
  void set_tile_validity_offset(
      const std::string& name, uint64_t tid, uint64_t step);

  /**
   * Sets a tile min for the fixed input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param min The minimum.
   * @return void
   */
  void set_tile_min(const std::string& name, uint64_t tid, const ByteVec& min);

  /**
   * Sets a tile min size for the var input attribute.
   *
   * @param name The attribute for which the min size is set.
   * @param tid The index of the tile for which the min is set.
   * @param size The size.
   * @return void
   */
  void set_tile_min_var_size(
      const std::string& name, uint64_t tid, uint64_t size);

  /**
   * Sets a tile min for the var input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param min The minimum.
   * @return void
   */
  void set_tile_min_var(
      const std::string& name, uint64_t tid, const ByteVec& min);

  /**
   * Sets a tile max for the input attribute.
   *
   * @param name The attribute for which the max is set.
   * @param tid The index of the tile for which the max is set.
   * @param max The maximum.
   * @return void
   */
  void set_tile_max(const std::string& name, uint64_t tid, const ByteVec& max);

  /**
   * Sets a tile max for the var input attribute.
   *
   * @param name The attribute for which the min size is set.
   * @param tid The index of the tile for which the min is set.
   * @param size The size.
   * @return void
   */
  void set_tile_max_var_size(
      const std::string& name, uint64_t tid, uint64_t size);

  /**
   * Sets a tile max for the var input attribute.
   *
   * @param name The attribute for which the min is set.
   * @param tid The index of the tile for which the min is set.
   * @param max The maximum.
   * @return void
   */
  void set_tile_max_var(
      const std::string& name, uint64_t tid, const ByteVec& max);

  /**
   * Converts min/max sizes to offsets.
   *
   * @param name The attribute for which the offsets are converted
   * @return void
   */
  void convert_tile_min_max_var_sizes_to_offsets(const std::string& name);

  /**
   * Sets a tile sum for the input attribute.
   *
   * @param name The attribute for which the sum is set.
   * @param tid The index of the tile for which the sum is set.
   * @param sum The sum.
   * @return void
   */
  void set_tile_sum(const std::string& name, uint64_t tid, const ByteVec& sum);

  /**
   * Sets a tile null count for the input attribute.
   *
   * @param name The attribute for which the null count is set.
   * @param tid The index of the tile for which the null count is set.
   * @param sum The null count.
   * @return void
   */
  void set_tile_null_count(
      const std::string& name, uint64_t tid, uint64_t null_count);

  /**
   * Compute fragment min, max, sum, null count for all dimensions/attributes.
   *
   * @return Status.
   */
  Status compute_fragment_min_max_sum_null_count();

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
  tuple<Status, optional<URI>> uri(const std::string& name) const;

  /** Returns the URI of the input variable-sized attribute/dimension. */
  tuple<Status, optional<URI>> var_uri(const std::string& name) const;

  /** Returns the validity URI of the input nullable attribute. */
  tuple<Status, optional<URI>> validity_uri(const std::string& name) const;

  /** Return the array schema name. */
  const std::string& array_schema_name();

  /**
   * Retrieves the starting offset of the input tile of the input attribute
   * or dimension in the file. If the attribute/dimension is var-sized, it
   * returns the starting offset of the offsets tile.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_offset(
      const std::string& name, uint64_t tile_idx, uint64_t* offset);

  /**
   * Retrieves the starting offset of the input tile of input attribute or
   * dimension in the file. The attribute/dimension must be var-sized.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_var_offset(
      const std::string& name, uint64_t tile_idx, uint64_t* offset);

  /**
   * Retrieves the starting offset of the input validity tile of the
   * input attribute in the file.
   *
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_validity_offset(
      const std::string& name, uint64_t tile_idx, uint64_t* offset);

  /**
   * Retrieves the size of the fragment metadata footer
   * (which contains the generic tile offsets) along with its size.
   */
  Status get_footer_size(uint32_t version, uint64_t* size) const;

  uint64_t footer_size() const;

  /** Returns the MBR of the input tile. */
  const NDRange& mbr(uint64_t tile_idx) const;

  /** Returns all the MBRs of all tiles in the fragment. */
  const std::vector<NDRange>& mbrs() const;

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given attribute or dimension and tile index.
   * If the attribute/dimension is var-sized, this will return the persisted
   * size of the offsets tile.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, size
   */
  tuple<Status, optional<uint64_t>> persisted_tile_size(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given var-sized attribute or dimension
   * and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, size
   */
  tuple<Status, optional<uint64_t>> persisted_tile_var_size(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the size of the validity tile when it is persisted (e.g. the size
   * of the compressed tile on disk) for a given attribute.
   *
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, size
   */
  tuple<Status, optional<uint64_t>> persisted_tile_validity_size(
      const std::string& name, uint64_t tile_idx);

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

  /**
   * Retrieves the (uncompressed) tile size for a given var-sized attribute or
   * dimension and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, size.
   */
  tuple<Status, optional<uint64_t>> tile_var_size(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the tile min value for a given attribute or dimension and tile
   * index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, value, size.
   */
  tuple<Status, optional<void*>, optional<uint64_t>> get_tile_min(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the tile max value for a given attribute or dimension and tile
   * index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, value, size.
   */
  tuple<Status, optional<void*>, optional<uint64_t>> get_tile_max(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the tile sum value for a given attribute or dimension and tile
   * index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, sum.
   */
  tuple<Status, optional<void*>> get_tile_sum(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the tile null count value for a given attribute or dimension
   * and tile index.
   *
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @return Status, count.
   */
  tuple<Status, optional<uint64_t>> get_tile_null_count(
      const std::string& name, uint64_t tile_idx);

  /**
   * Retrieves the min value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Status, value.
   */
  tuple<Status, optional<std::vector<uint8_t>>> get_min(
      const std::string& name);

  /**
   * Retrieves the max value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Status, value.
   */
  tuple<Status, optional<std::vector<uint8_t>>> get_max(
      const std::string& name);

  /**
   * Retrieves the sum value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Status, sum.
   */
  tuple<Status, optional<void*>> get_sum(const std::string& name);

  /**
   * Retrieves the null count value for a given attribute or dimension.
   *
   * @param name The input attribute/dimension.
   * @return Status, count.
   */
  tuple<Status, optional<uint64_t>> get_null_count(const std::string& name);

  /**
   * Set the processed conditions. The processed conditions is the list
   * of delete/update conditions that have already been applied for this
   * fragment and don't need to be applied again.
   *
   * @param processed_conditions The processed conditions.
   */
  void set_processed_conditions(std::vector<std::string>& processed_conditions);

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

  /** Returns the first timestamp of the fragment timestamp range. */
  uint64_t first_timestamp() const;

  /**
   * Returns `true` if the timestamp of the first operand is smaller,
   * breaking ties based on the URI string.
   */
  bool operator<(const FragmentMetadata& metadata) const;

  /** Serializes the fragment metadata footer into the input buffer. */
  Status write_footer(Buffer* buff) const;

  /** Loads the R-tree from storage. */
  Status load_rtree(const EncryptionKey& encryption_key);

  /** Frees the memory associated with the rtree. */
  void free_rtree();

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   * */
  Status load_tile_var_sizes(
      const EncryptionKey& encryption_key, const std::string& name);

  /**
   * Loads tile offsets for the attribute/dimension names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute/dimension names.
   * @return Status
   */
  Status load_tile_offsets(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

  /**
   * Loads min values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   * @return Status
   */
  Status load_tile_min_values(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

  /**
   * Loads max values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   * @return Status
   */
  Status load_tile_max_values(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

  /**
   * Loads sum values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   * @return Status
   */
  Status load_tile_sum_values(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

  /**
   * Loads null count values for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   * @return Status
   */
  Status load_tile_null_count_values(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

  /**
   * Loads the min max sum null count values for the fragment.
   *
   * @param encryption_key The key the array got opened with.
   * @return Status
   */
  Status load_fragment_min_max_sum_null_count(
      const EncryptionKey& encryption_key);

  /**
   * Loads the processed conditions for the fragment. The processed conditions
   * is the list of delete/update conditions that have already been applied for
   * this fragment and don't need to be applied again.
   *
   * @param encryption_key The key the array got opened with.
   * @return Status
   */
  Status load_processed_conditions(const EncryptionKey& encryption_key);

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

  /** tile_offsets accessor */
  std::vector<std::vector<uint64_t>>& tile_offsets() {
    return tile_offsets_;
  }

  /** tile_var_offsets accessor */
  std::vector<std::vector<uint64_t>>& tile_var_offsets() {
    return tile_var_offsets_;
  }

  /** tile_var_sizes  accessor */
  std::vector<std::vector<uint64_t>>& tile_var_sizes() {
    return tile_var_sizes_;
  }

  /** tile_validity_offsets accessor */
  std::vector<std::vector<uint64_t>>& tile_validity_offsets() {
    return tile_validity_offsets_;
  }

  /** tile_min_buffer accessor */
  std::vector<std::vector<uint8_t>>& tile_min_buffer() {
    return tile_min_buffer_;
  }

  /** tile_min_var_buffer accessor */
  std::vector<std::vector<char>>& tile_min_var_buffer() {
    return tile_min_var_buffer_;
  }

  /** tile_max_buffer accessor */
  std::vector<std::vector<uint8_t>>& tile_max_buffer() {
    return tile_max_buffer_;
  }

  /** tile_max_var_buffer accessor */
  std::vector<std::vector<char>>& tile_max_var_buffer() {
    return tile_max_var_buffer_;
  }

  /** tile_sums accessor */
  std::vector<std::vector<uint8_t>>& tile_sums() {
    return tile_sums_;
  }

  /** tile_null_counts accessor */
  std::vector<std::vector<uint64_t>>& tile_null_counts() {
    return tile_null_counts_;
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

  /** rtree accessor */
  RTree& rtree() {
    return rtree_;
  }

  /** set the SM pointer during deserialization*/
  void set_storage_manager(StorageManager* sm) {
    storage_manager_ = sm;
  }

  /** loaded_metadata_.rtree_ accessor */
  void set_rtree_loaded() {
    loaded_metadata_.rtree_ = true;
  }

 private:
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

  /** Keeps track of which metadata is loaded. */
  struct LoadedMetadata {
    bool footer_ = false;
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
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager. */
  StorageManager* storage_manager_;

  /**
   * The memory tracker of the array this fragment metadata corresponds to.
   */
  MemoryTracker* memory_tracker_;

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

  /** A vector storing the first and last coordinates of each tile. */
  std::vector<std::vector<uint8_t>> bounding_coords_;

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

  /** Keeps track of which metadata has been loaded. */
  LoadedMetadata loaded_metadata_;

  /** The size of the fragment metadata file. */
  uint64_t meta_file_size_;

  /** Local mutex for thread-safety. */
  std::mutex mtx_;

  /** Mutex per tile offset loading. */
  std::deque<std::mutex> tile_offsets_mtx_;

  /** Mutex per tile var offset loading. */
  std::deque<std::mutex> tile_var_offsets_mtx_;

  /** The non-empty domain of the fragment. */
  NDRange non_empty_domain_;

  /** An RTree for the MBRs. */
  RTree rtree_;

  /**
   * The tile index base which is added to tile indices in setter functions.
   * Only used in global order writes.
   */
  uint64_t tile_index_base_;

  /**
   * The tile offsets in their corresponding attribute files. Meaningful only
   * when there is compression.
   */
  std::vector<std::vector<uint64_t>> tile_offsets_;

  /**
   * The variable tile offsets in their corresponding attribute files.
   * Meaningful only for variable-sized tiles.
   */
  std::vector<std::vector<uint64_t>> tile_var_offsets_;

  /**
   * The sizes of the uncompressed variable tiles.
   * Meaningful only when there is compression for variable tiles.
   */
  std::vector<std::vector<uint64_t>> tile_var_sizes_;

  /**
   * The validity tile offsets in their corresponding attribute files.
   * Meaningful only when there is compression.
   */
  std::vector<std::vector<uint64_t>> tile_validity_offsets_;

  /**
   * The tile min buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  std::vector<std::vector<uint8_t>> tile_min_buffer_;

  /**
   * The tile min buffers variable length data.
   */
  std::vector<std::vector<char>> tile_min_var_buffer_;

  /**
   * The tile max buffers, for variable attributes/dimensions, this will store
   * offsets.
   */
  std::vector<std::vector<uint8_t>> tile_max_buffer_;

  /**
   * The tile max buffers variable length data.
   */
  std::vector<std::vector<char>> tile_max_var_buffer_;

  /**
   * The tile sum values, ignored for var sized attributes/dimensions.
   */
  std::vector<std::vector<uint8_t>> tile_sums_;

  /**
   * The tile null count values for attributes/dimensions.
   */
  std::vector<std::vector<uint64_t>> tile_null_counts_;

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

  /** The format version of this metadata. */
  uint32_t version_;

  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** Stores the generic tile offsets, facilitating loading. */
  GenericTileOffsets gt_offsets_;

  /** The uri of the array the metadata belongs to. */
  URI array_uri_;

  /** Set of already processed delete/update conditions for this fragment. */
  std::unordered_set<std::string> processed_conditions_set_;

  /**
   * Ordered list of already processed delete/update conditions for this
   * fragment.
   */
  std::vector<std::string> processed_conditions_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Retrieves the offset in the fragment metadata file of the footer
   * (which contains the generic tile offsets) along with its size.
   */
  Status get_footer_offset_and_size(uint64_t* offset, uint64_t* size) const;

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
   * Applicable to format version 7 to 10.
   */
  uint64_t footer_size_v7_v10() const;

  /**
   * Returns the size of the fragment metadata footer
   * (which contains the generic tile offsets) along with its size.
   *
   * Applicable to format version 11 or higher.
   */
  uint64_t footer_size_v11_or_higher() const;

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
  Status expand_non_empty_domain(const NDRange& mbr);

  /**
   * Loads the tile offsets for the input attribute or dimension idx
   * from storage.
   */
  Status load_tile_offsets(const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the variable tile offsets for the input attribute or dimension idx
   * from storage.
   */
  Status load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the variable tile sizes for the input attribute or dimension idx
   * from storage.
   * */
  Status load_tile_var_sizes(const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the validity tile offsets for the input attribute idx from storage.
   */
  Status load_tile_validity_offsets(
      const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the min values for the input attribute idx from storage.
   */
  Status load_tile_min_values(
      const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the max values for the input attribute idx from storage.
   */
  Status load_tile_max_values(
      const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the sum values for the input attribute idx from storage.
   */
  Status load_tile_sum_values(
      const EncryptionKey& encryption_key, unsigned idx);

  /**
   * Loads the null count values for the input attribute idx from storage.
   */
  Status load_tile_null_count_values(
      const EncryptionKey& encryption_key, unsigned idx);

  /** Loads the generic tile offsets from the buffer. */
  Status load_generic_tile_offsets(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 3 and 4.
   */
  Status load_generic_tile_offsets_v3_v4(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 5 and 6.
   */
  Status load_generic_tile_offsets_v5_v6(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 7 to 10.
   */
  Status load_generic_tile_offsets_v7_v10(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 11.
   */
  Status load_generic_tile_offsets_v11(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 12 to 15.
   */
  Status load_generic_tile_offsets_v12_v15(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 16 or higher.
   */
  Status load_generic_tile_offsets_v16_or_higher(ConstBuffer* buff);

  /**
   * Loads the array schema name.
   */
  Status load_array_schema_name(ConstBuffer* buff);

  /**
   * Loads the bounding coordinates from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_bounding_coords(ConstBuffer* buff);

  /** Loads the sizes of each attribute or dimension file from the buffer. */
  Status load_file_sizes(ConstBuffer* buff);

  /**
   * Loads the sizes of each attribute or dimension file from the buffer.
   * Applicable to format versions 1 to 4.
   */
  Status load_file_sizes_v1_v4(ConstBuffer* buff);

  /**
   * Loads the sizes of each attribute or dimension file from the buffer.
   * Applicable to format version 5 or higher.
   */
  Status load_file_sizes_v5_or_higher(ConstBuffer* buff);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer.
   */
  Status load_file_var_sizes(ConstBuffer* buff);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer. Applicable to version 1 to 4.
   */
  Status load_file_var_sizes_v1_v4(ConstBuffer* buff);

  /**
   * Loads the sizes of each variable attribute or dimension file from the
   * buffer. Applicable to version 5 or higher.
   */
  Status load_file_var_sizes_v5_or_higher(ConstBuffer* buff);

  /** Loads the sizes of each attribute validity file from the buffer. */
  Status load_file_validity_sizes(ConstBuffer* buff);

  /**
   * Loads the cell number of the last tile from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_last_tile_cell_num(ConstBuffer* buff);

  /**
   * Loads the `has_timestamps_` field from the buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_has_timestamps(ConstBuffer* buff);

  /**
   * Loads the `has_delete_meta_` field from the buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_has_delete_meta(ConstBuffer* buff);

  /**
   * Loads the MBRs from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_mbrs(ConstBuffer* buff);

  /** Loads the non-empty domain from the input buffer. */
  Status load_non_empty_domain(ConstBuffer* buff);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions <= 2.
   */
  Status load_non_empty_domain_v1_v2(ConstBuffer* buff);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions 3 and 4.
   */
  Status load_non_empty_domain_v3_v4(ConstBuffer* buff);

  /**
   * Loads the non-empty domain from the input buffer,
   * for format versions >= 5.
   */
  Status load_non_empty_domain_v5_or_higher(ConstBuffer* buff);

  /**
   * Loads the tile offsets for the input attribute from the input buffer.
   * Applicable to versions 1 and 2
   */
  Status load_tile_offsets(ConstBuffer* cbuff);

  /**
   * Loads the tile offsets for the input attribute or dimension from the
   * input buffer.
   */
  void load_tile_offsets(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the variable tile offsets from the input buffer.
   * Applicable to versions 1 and 2
   */
  Status load_tile_var_offsets(ConstBuffer* buff);

  /**
   * Loads the variable tile offsets for the input attribute or dimension from
   * the input buffer.
   */
  void load_tile_var_offsets(unsigned idx, Deserializer& deserializer);

  /** Loads the variable tile sizes from the input buffer. */
  Status load_tile_var_sizes(ConstBuffer* buff);

  /**
   * Loads the variable tile sizes for the input attribute or dimension
   * from the input buffer.
   */
  void load_tile_var_sizes(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the validity tile offsets for the input attribute from the
   * input buffer.
   */
  Status load_tile_validity_offsets(unsigned idx, ConstBuffer* buff);

  /**
   * Loads the min values for the input attribute from the input buffer.
   */
  void load_tile_min_values(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the max values for the input attribute from the input buffer.
   */
  // Status load_tile_max_values(unsigned idx, ConstBuffer* buff);
  void load_tile_max_values(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the sum values for the input attribute from the input buffer.
   */
  void load_tile_sum_values(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the null count values for the input attribute from the input
   * buffer.
   */
  void load_tile_null_count_values(unsigned idx, Deserializer& deserializer);

  /**
   * Loads the min max sum null count values for the fragment.
   */
  void load_fragment_min_max_sum_null_count(Deserializer& deserializer);

  /**
   * Loads the processed conditions for the fragment.
   */
  void load_processed_conditions(Deserializer& deserializer);

  /** Loads the format version from the buffer. */
  Status load_version(ConstBuffer* buff);

  /** Loads the `dense_` field from the buffer. */
  Status load_dense(ConstBuffer* buff);

  /** Loads the number of sparse tiles from the buffer. */
  Status load_sparse_tile_num(ConstBuffer* buff);

  /** Loads the basic metadata from storage (version 2 or before). */
  Status load_v1_v2(
      const EncryptionKey& encryption_key,
      const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
          array_schemas);

  /**
   * Loads the basic metadata from storage or the input `f_buff` if
   * it is not `nullptr` (version 3 or after).
   */
  Status load_v3_or_higher(
      const EncryptionKey& encryption_key,
      Buffer* f_buff,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /**
   * Loads the footer of the metadata file, which contains
   * only some basic info. If `f_buff` is `nullptr, then
   * the footer will be loaded from the file, otherwise it
   * will be loaded from `f_buff`.
   */
  Status load_footer(
      const EncryptionKey& encryption_key,
      Buffer* f_buff,
      uint64_t offset,
      std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas);

  /** Writes the sizes of each attribute file to the buffer. */
  Status write_file_sizes(Buffer* buff) const;

  /** Writes the sizes of each variable attribute file to the buffer. */
  Status write_file_var_sizes(Buffer* buff) const;

  /** Writes the sizes of each validitiy attribute file to the buffer. */
  Status write_file_validity_sizes(Buffer* buff) const;

  /** Writes the generic tile offsets to the buffer. */
  Status write_generic_tile_offsets(Buffer* buff) const;

  /** Writes the array schema name. */
  Status write_array_schema_name(Buffer* buff) const;

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   */
  Status write_last_tile_cell_num(Buffer* buff) const;

  /**
   * Writes the `has_timestamps_` field to the fragment metadata buffer.
   */
  Status write_has_timestamps(Buffer* buff) const;

  /**
   * Writes the `has_delete_meta_` field to the fragment metadata buffer.
   */
  Status write_has_delete_meta(Buffer* buff) const;

  /**
   * Writes the R-tree to storage.
   *
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the R-tree.
   * @return Status
   */
  Status store_rtree(const EncryptionKey& encryption_key, uint64_t* nbytes);

  /** Stores a footer with the basic information. */
  Status store_footer(const EncryptionKey& encryption_key);

  /** Writes the R-tree to a tile. */
  Tile write_rtree();

  /** Writes the non-empty domain to the input buffer. */
  Status write_non_empty_domain(Buffer* buff) const;

  /**
   * Writes the tile offsets of the input attribute or dimension to storage.
   *
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile offsets.
   * @return Status
   */
  void store_tile_offsets(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the tile offsets of the input attribute or dimension idx to the
   * input buffer.
   */
  void write_tile_offsets(unsigned idx, Serializer& serializer);

  /**
   * Writes the variable tile offsets of the input attribute or dimension
   * to storage.
   *
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var offsets.
   * @return Status
   */
  void store_tile_var_offsets(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the variable tile offsets of the input attribute or dimension idx
   * to the buffer.
   */
  void write_tile_var_offsets(unsigned idx, Serializer& serializer);

  /**
   * Writes the variable tile sizes for the input attribute or dimension to
   * the buffer.
   *
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var sizes.
   * @return Status
   */
  void store_tile_var_sizes(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the variable tile sizes for the input attribute or dimension
   * to storage.
   */
  void write_tile_var_sizes(unsigned idx, Serializer& serializer);

  /**
   * Writes the validity tile offsets of the input attribute to storage.
   *
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the validity tile
   * offsets.
   * @return Status
   */
  Status store_tile_validity_offsets(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the validity tile offsets of the input attribute idx to the
   * input buffer.
   */
  Status write_tile_validity_offsets(unsigned idx, Buffer* buff);

  /**
   * Writes the mins of the input attribute to storage.
   *
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the mins.
   * @return Status
   */
  void store_tile_mins(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the mins of the input attribute idx to the input buffer.
   */
  void write_tile_mins(unsigned idx, Serializer& serializer);

  /**
   * Writes the maxs of the input attribute to storage.
   *
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the maxs.
   * @return Status
   */
  void store_tile_maxs(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the maxs of the input attribute idx to the input buffer.
   */
  void write_tile_maxs(unsigned idx, Serializer& serializer);

  /**
   * Writes the sums of the input attribute to storage.
   *
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the sums.
   * @return Status
   */
  void store_tile_sums(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the sums of the input attribute idx to the input buffer.
   */
  void write_tile_sums(unsigned idx, Serializer& serializer);

  /**
   * Writes the null counts of the input attribute to storage.
   *
   * @param idx The index of the attribute.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the null counts.
   * @return Status
   */
  void store_tile_null_counts(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the null counts of the input attribute idx to the input buffer.
   */
  void write_tile_null_counts(unsigned idx, Serializer& serializer);

  /**
   * Writes the fragment min, max, sum and null count to storage.
   *
   * @param num The number of attributes.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written.
   */
  void store_fragment_min_max_sum_null_count(
      uint64_t num, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the processed conditions to storage.
   *
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written.
   * @return Status
   */
  void store_processed_conditions(
      const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Compute the fragment min, max and sum values.
   *
   * @param name The attribute/dimension name.
   */
  template <class T>
  void compute_fragment_min_max_sum(const std::string& name);

  /**
   * Compute the fragment sum value.
   *
   * @param idx The attribute/dimension index.
   * @param nullable Is the attribute/dimension nullable.
   */
  template <class SumT>
  void compute_fragment_sum(const uint64_t idx, const bool nullable);

  /**
   * Compute the fragment min and max values for var sized attributes.
   *
   * @param name The attribute/dimension name.
   */
  void min_max_var(const std::string& name);

  /** Writes the format version to the buffer. */
  Status write_version(Buffer* buff) const;

  /** Writes the `dense_` field to the buffer. */
  Status write_dense(Buffer* buff) const;

  /** Writes the number of sparse tiles to the buffer. */
  Status write_sparse_tile_num(Buffer* buff) const;

  /**
   * Reads the contents of a generic tile starting at the input offset,
   * and returns a tile.
   */
  tuple<Status, optional<Tile>> read_generic_tile_from_file(
      const EncryptionKey& encryption_key, uint64_t offset) const;

  /**
   * Reads the fragment metadata file footer (which contains the generic tile
   * offsets) into the input buffer.
   */
  Status read_file_footer(
      Buffer* buff, uint64_t* footer_offset, uint64_t* footer_size) const;

  /**
   * Writes the contents of the input buffer as a separate
   * generic tile to the metadata file.
   *
   * @param encryption_key The encryption key.
   * @param buff The buffer whose contents the function will write.
   * @param nbytes The total number of bytes written to the file.
   * @return Status
   */
  Status write_generic_tile_to_file(
      const EncryptionKey& encryption_key,
      Buffer& buff,
      uint64_t* nbytes) const;

  /**
   * Writes the contents of the input tile as a separate
   * generic tile to the metadata file.
   *
   * @param encryption_key The encryption key.
   * @param tile The tile whose contents the function will write.
   * @param nbytes The total number of bytes written to the file.
   * @return Status
   */
  Status write_generic_tile_to_file(
      const EncryptionKey& encryption_key, Tile& tile, uint64_t* nbytes) const;

  /**
   * Writes the contents of the input buffer at the end of the fragment
   * metadata file, without applying any filters. This helps its quick
   * retrieval upon reading (as its size is predictable based on the
   * number of attributes).
   */
  Status write_footer_to_file(Buffer* buff) const;

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
   * return Status, the encoded dimension/attribute name.
   */
  tuple<Status, optional<std::string>> encode_name(
      const std::string& name) const;

  /**
   * This builds the index mapping for attribute/dimension name to id.
   */
  void build_idx_map();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_METADATA_H
