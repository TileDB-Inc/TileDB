/**
 * @file  fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <mutex>
#include <unordered_map>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/rtree/rtree.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArraySchema;
class Buffer;
class EncryptionKey;
class StorageManager;

/** Stores the metadata structures of a fragment. */
class FragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param storage_manager A storage manager instance.
   * @param array_schema The schema of the array the fragment belongs to.
   * @param fragment_uri The fragment URI.
   * @param timestamp_range The timestamp range of the fragment.
   *     In TileDB, timestamps are in ms elapsed since
   *     1970-01-01 00:00:00 +0000 (UTC).
   * @param dense Indicates whether the fragment is dense or sparse.
   */
  FragmentMetadata(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      const URI& fragment_uri,
      const std::pair<uint64_t, uint64_t>& timestamp_range,
      bool dense = true);

  /** Destructor. */
  ~FragmentMetadata();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the number of cells in the fragment. */
  uint64_t cell_num() const;

  /** Returns the number of cells in the tile at the input position. */
  uint64_t cell_num(uint64_t tile_pos) const;

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
   * @param encryption_key The encryption key the array was opened with.
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   * @return Status
   */
  Status add_max_buffer_sizes_dense(
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
   * @tparam T The coordinates type.
   * @param encryption_key The encryption key the array was opened with.
   * @param subarray The targeted subarray.
   * @param buffer_sizes The upper bounds will be added to this map. The latter
   *     maps an attribute to a buffer size pair. For fix-sized attributes, only
   *     the first size is useful. For var-sized attributes, the first is the
   *     offsets size, whereas the second is the data size.
   * @return Status
   */
  template <class T>
  Status add_max_buffer_sizes_dense(
      const EncryptionKey& encryption_key,
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

  /**
   * Returns ture if the corresponding fragment is dense, and false if it
   * is sparse.
   */
  bool dense() const;

  /** Returns the (expanded) domain in which the fragment is constrained. */
  const NDRange& domain() const;

  /** Returns the format version of this fragment. */
  uint32_t format_version() const;

  /** Retrieves the fragment size. */
  Status fragment_size(uint64_t* size) const;

  /** Returns the fragment URI. */
  const URI& fragment_uri() const;

  /** Returns true if the metadata footer is consolidated. */
  bool has_consolidated_footer() const;

  /**
   * Returns true if the input range overlaps the non-empty
   * domain of the fragment.
   */
  bool overlaps_non_empty_domain(const NDRange& range) const;

  /**
   * Retrieves the overlap of all MBRs with the input ND range. The encryption
   * key is needed because certain metadata may have to be loaded on-the-fly.
   */
  Status get_tile_overlap(const NDRange& range, TileOverlap* tile_overlap);

  /**
   * Initializes the fragment metadata structures.
   *
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   * @return Status
   */
  Status init(const NDRange& non_empty_domain);

  /** Returns the number of cells in the last tile. */
  uint64_t last_tile_cell_num() const;

  /**
   * Loads the basic metadata from storage or `f_buff` for later
   * versions if it is not `nullptr`.
   */
  Status load(
      const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset);

  /** Stores all the metadata to storage. */
  Status store(const EncryptionKey& encryption_key);

  /** Returns the non-empty domain in which the fragment is constrained. */
  const NDRange& non_empty_domain();

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

  /** Returns the tile index base value. */
  uint64_t tile_index_base() const;

  /** Returns the number of tiles in the fragment. */
  uint64_t tile_num() const;

  /** Returns the URI of the input attribute/dimension. */
  URI uri(const std::string& name) const;

  /** Returns the URI of the input variable-sized attribute/dimension. */
  URI var_uri(const std::string& name) const;

  /** Returns the validity URI of the input nullable attribute. */
  URI validity_uri(const std::string& name) const;

  /**
   * Retrieves the starting offset of the input tile of the input attribute
   * or dimension in the file. If the attribute/dimension is var-sized, it
   * returns the starting offset of the offsets tile.
   *
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_offset(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* offset);

  /**
   * Retrieves the starting offset of the input tile of input attribute or
   * dimension in the file. The attribute/dimension must be var-sized.
   *
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_var_offset(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* offset);

  /**
   * Retrieves the starting offset of the input validity tile of the
   * input attribute in the file.
   *
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_validity_offset(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* offset);

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
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status persisted_tile_size(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given var-sized attribute or dimension
   * and tile index.
   *
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status persisted_tile_var_size(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /**
   * Retrieves the size of the validity tile when it is persisted (e.g. the size
   * of the compressed tile on disk) for a given attribute.
   *
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status persisted_tile_validity_size(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* tile_size);

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
   * @param encryption_key The key the array got opened with.
   * @param name The input attribute/dimension.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status tile_var_size(
      const EncryptionKey& encryption_key,
      const std::string& name,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /** Returns the first timestamp of the fragment timestamp range. */
  uint64_t first_timestamp() const;

  /** Returns the fragment timestamp range. */
  const std::pair<uint64_t, uint64_t>& timestamp_range() const;

  /**
   * Returns `true` if the timestamp of the first operand is smaller,
   * breaking ties based on the URI string.
   */
  bool operator<(const FragmentMetadata& metadata) const;

  /** Serializes the fragment metadata footer into the input buffer. */
  Status write_footer(Buffer* buff) const;

  /** Loads the R-tree from storage. */
  Status load_rtree(const EncryptionKey& encryption_key);

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
   * Loads validity tile offsets for the attribute names.
   *
   * @param encryption_key The key the array got opened with.
   * @param names The attribute names.
   * @return Status
   */
  Status load_tile_validity_offsets(
      const EncryptionKey& encryption_key, std::vector<std::string>&& names);

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
  };

  /** Keeps track of which metadata is loaded. */
  struct LoadedMetadata {
    bool footer_ = false;
    bool rtree_ = false;
    std::vector<bool> tile_offsets_;
    std::vector<bool> tile_var_offsets_;
    std::vector<bool> tile_var_sizes_;
    std::vector<bool> tile_validity_offsets_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The array schema */
  const ArraySchema* array_schema_;

  /**
   * Maps an attribute or dimension to an index used in the various vector
   * class members. Attributes are first, then TILEDB_COORDS, then the
   * dimensions.
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

  /** The format version of this metadata. */
  uint32_t version_;

  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** Stores the generic tile offsets, facilitating loading. */
  GenericTileOffsets gt_offsets_;

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
   * Applicable to format version 7 or higher.
   */
  uint64_t footer_size_v7_or_higher() const;

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
   * Loads the validity tile offsets for the input attribute idx
   * from storage.
   */
  Status load_tile_validity_offsets(
      const EncryptionKey& encryption_key, unsigned idx);

  /** Loads the generic tile offsets from the buffer. */
  Status load_generic_tile_offsets(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 4 and 5.
   */
  Status load_generic_tile_offsets_v3_v4(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 5 and 6.
   */
  Status load_generic_tile_offsets_v5_v6(ConstBuffer* buff);

  /**
   * Loads the generic tile offsets from the buffer. Applicable to
   * versions 7 or higher.
   */
  Status load_generic_tile_offsets_v7_or_higher(ConstBuffer* buff);

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
  Status load_tile_offsets(ConstBuffer* buff);

  /**
   * Loads the tile offsets for the input attribute or dimension from the
   * input buffer.
   */
  Status load_tile_offsets(unsigned idx, ConstBuffer* buff);

  /**
   * Loads the variable tile offsets from the input buffer.
   * Applicable to versions 1 and 2
   */
  Status load_tile_var_offsets(ConstBuffer* buff);

  /**
   * Loads the variable tile offsets for the input attribute or dimension from
   * the input buffer.
   */
  Status load_tile_var_offsets(unsigned idx, ConstBuffer* buff);

  /** Loads the variable tile sizes from the input buffer. */
  Status load_tile_var_sizes(ConstBuffer* buff);

  /**
   * Loads the variable tile sizes for the input attribute or dimension
   * from the input buffer.
   */
  Status load_tile_var_sizes(unsigned idx, ConstBuffer* buff);

  /**
   * Loads the validity tile offsets for the input attribute from the
   * input buffer.
   */
  Status load_tile_validity_offsets(unsigned idx, ConstBuffer* buff);

  /** Loads the format version from the buffer. */
  Status load_version(ConstBuffer* buff);

  /** Loads the `dense_` field from the buffer. */
  Status load_dense(ConstBuffer* buff);

  /** Loads the number of sparse tiles from the buffer. */
  Status load_sparse_tile_num(ConstBuffer* buff);

  /** Loads the basic metadata from storage (version 2 or before). */
  Status load_v1_v2(const EncryptionKey& encryption_key);

  /**
   * Loads the basic metadata from storage or the input `f_buff` if
   * it is not `nullptr` (version 3 or after).
   */
  Status load_v3_or_higher(
      const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset);

  /**
   * Loads the footer of the metadata file, which contains
   * only some basic info. If `f_buff` is `nullptr, then
   * the footer will be loaded from the file, otherwise it
   * will be loaded from `f_buff`.
   */
  Status load_footer(
      const EncryptionKey& encryption_key, Buffer* f_buff, uint64_t offset);

  /** Writes the sizes of each attribute file to the buffer. */
  Status write_file_sizes(Buffer* buff) const;

  /** Writes the sizes of each variable attribute file to the buffer. */
  Status write_file_var_sizes(Buffer* buff) const;

  /** Writes the sizes of each validitiy attribute file to the buffer. */
  Status write_file_validity_sizes(Buffer* buff) const;

  /** Writes the generic tile offsets to the buffer. */
  Status write_generic_tile_offsets(Buffer* buff) const;

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   */
  Status write_last_tile_cell_num(Buffer* buff) const;

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

  /** Writes the R-tree to the input buffer. */
  Status write_rtree(Buffer* buff);

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
  Status store_tile_offsets(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the tile offsets of the input attribute or dimension idx to the
   * input buffer.
   */
  Status write_tile_offsets(unsigned idx, Buffer* buff);

  /**
   * Writes the variable tile offsets of the input attribute or dimension
   * to storage.
   *
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var offsets.
   * @return Status
   */
  Status store_tile_var_offsets(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the variable tile offsets of the input attribute or dimension idx
   * to the buffer.
   */
  Status write_tile_var_offsets(unsigned idx, Buffer* buff);

  /**
   * Writes the variable tile sizes for the input attribute or dimension to
   * the buffer.
   *
   * @param idx The index of the attribute or dimension.
   * @param encryption_key The encryption key.
   * @param nbytes The total number of bytes written for the tile var sizes.
   * @return Status
   */
  Status store_tile_var_sizes(
      unsigned idx, const EncryptionKey& encryption_key, uint64_t* nbytes);

  /**
   * Writes the variable tile sizes for the input attribute or dimension
   * to storage.
   */
  Status write_tile_var_sizes(unsigned idx, Buffer* buff);

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

  /** Writes the format version to the buffer. */
  Status write_version(Buffer* buff) const;

  /** Writes the `dense_` field to the buffer. */
  Status write_dense(Buffer* buff) const;

  /** Writes the number of sparse tiles to the buffer. */
  Status write_sparse_tile_num(Buffer* buff) const;

  /**
   * Reads the contents of a generic tile starting at the input offset,
   * and stores them into buffer ``buff``.
   */
  Status read_generic_tile_from_file(
      const EncryptionKey& encryption_key, uint64_t offset, Buffer* buff) const;

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
      Buffer&& buff,
      uint64_t* nbytes) const;

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
   * return std::string The encoded dimension/attribute name.
   */
  std::string encode_name(const std::string& name) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_METADATA_H
