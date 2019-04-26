/**
 * @file  fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/rtree/rtree.h"

#include <mutex>
#include <vector>

namespace tiledb {
namespace sm {

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
   * @param dense Indicates whether the fragment is dense or sparse.
   * @param fragment_uri The fragment URI.
   * @param timestamp The timestamp of the fragment creation. In TileDB,
   * timestamps are in ms elapsed since 1970-01-01 00:00:00 +0000 (UTC).
   */
  FragmentMetadata(
      StorageManager* storage_manager,
      const ArraySchema* array_schema,
      bool dense,
      const URI& fragment_uri,
      uint64_t timestamp);

  /** Destructor. */
  ~FragmentMetadata();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the number of cells in the tile at the input position. */
  uint64_t cell_num(uint64_t tile_pos) const;

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`.
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
  Status add_max_buffer_sizes(
      const EncryptionKey& encryption_key,
      const T* subarray,
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
  Status add_max_buffer_sizes_sparse(
      const EncryptionKey& encryption_key,
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`.
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
  Status add_est_read_buffer_sizes(
      const EncryptionKey& encryption_key,
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes);

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
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
  Status add_est_read_buffer_sizes_dense(
      const EncryptionKey& encryption_key,
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes);

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the sparse
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
  Status add_est_read_buffer_sizes_sparse(
      const EncryptionKey& encryption_key,
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes);

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
  const void* domain() const;

  /** Returns the format version of this fragment. */
  uint32_t format_version() const;

  /** Retrieves the fragment size. */
  Status fragment_size(uint64_t* size) const;

  /** Returns the fragment URI. */
  const URI& fragment_uri() const;

  /**
   * Given as input global tile coordinates, it retrieves the tile position
   * within the fragment.
   *
   * @tparam T The domain type.
   * @param tile_coords The global tile coordinates.
   * @return The tile position in the fragment.
   */
  template <class T>
  uint64_t get_tile_pos(const T* tile_coords) const;

  /**
   * Initializes the fragment metadata structures.
   *
   * @param non_empty_domain The non-empty domain in which the array read/write
   *     will be constrained.
   * @return Status
   */
  Status init(const void* non_empty_domain);

  /** Returns the number of cells in the last tile. */
  uint64_t last_tile_cell_num() const;

  /** Loads the basic metadata from storage. */
  Status load(const EncryptionKey& encryption_key);

  /** Stores all the metadata to storage. */
  Status store(const EncryptionKey& encryption_key);

  /** Retrieves the MBRs. */
  // TODO: Remove after the new dense read algorithm is in
  Status mbrs(
      const EncryptionKey& encryption_key, const std::vector<void*>** mbrs);

  /** Returns the non-empty domain in which the fragment is constrained. */
  const void* non_empty_domain() const;

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
  Status set_mbr(uint64_t tile, const void* mbr);

  /**
   * Sets the input tile's MBR in the fragment metadata. It also expands the
   * non-empty domain of the fragment.
   *
   * @tparam T The coordinates type.
   * @param tile The tile index whose MBR will be set.
   * @param mbr The MBR to be set.
   * @return Status
   */
  template <class T>
  Status set_mbr(uint64_t tile, const void* mbr);

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
   * Sets a tile offset for the input attribute.
   *
   * @param attribute The attribute for which the offset is set.
   * @param tile The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last tile size.
   * @return void
   */
  void set_tile_offset(
      const std::string& attribute, uint64_t tile, uint64_t step);

  /**
   * Sets a variable tile offset for the input attribute.
   *
   * @param attribute The attribute for which the offset is set.
   * @param tile The index of the tile for which the offset is set.
   * @param step This is essentially the step by which the previous
   *     offset will be expanded. It is practically the last variable tile size.
   * @return void
   */
  void set_tile_var_offset(
      const std::string& attribute, uint64_t tile, uint64_t step);

  /**
   * Sets a variable tile size for the input attribute.
   *
   * @param attribute The attribute for which the size is set.
   * @param tile The index of the tile for which the offset is set.
   * @param size The size to be appended.
   * @return void
   */
  void set_tile_var_size(
      const std::string& attribute, uint64_t tile, uint64_t size);

  /** Returns the tile index base value. */
  uint64_t tile_index_base() const;

  /** Returns the number of tiles in the fragment. */
  uint64_t tile_num() const;

  /** Returns the URI of the input attribute. */
  URI attr_uri(const std::string& attribute) const;

  /** Returns the URI of the input variable-sized attribute. */
  URI attr_var_uri(const std::string& attribute) const;

  /**
   * Retrieves the starting offset of the input tile of input attribute
   * in the file. If the attribute is var-sized, it returns the starting
   * offset of the offsets tile.
   *
   * @param encryption_key The key the array got opened with.
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_offset(
      const EncryptionKey& encryption_key,
      const std::string& attribute,
      uint64_t tile_idx,
      uint64_t* offset);

  /**
   * Retrieves the starting offset of the input tile of input attribute
   * in the file. The attribute must be var-sized.
   *
   * @param encryption_key The key the array got opened with.
   * @param attribute_id The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param offset The file offset to be retrieved.
   * @return Status
   */
  Status file_var_offset(
      const EncryptionKey& encryption_key,
      const std::string& attribute,
      uint64_t tile_idx,
      uint64_t* offset);

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given attribute and tile index. If the
   * attribute is var-sized, this will return the persisted size of the offsets
   * tile.
   *
   * @param encryption_key The key the array got opened with.
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status persisted_tile_size(
      const EncryptionKey& encryption_key,
      const std::string& attribute,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /**
   * Retrieves the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given var-sized attribute and tile index.
   *
   * @param encryption_key The key the array got opened with.
   * @param attribute The inout attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status persisted_tile_var_size(
      const EncryptionKey& encryption_key,
      const std::string& attribute,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /** Retrieves the RTree. */
  Status rtree(const EncryptionKey& encryption_key, const RTree** rtree);

  /**
   * Returns the (uncompressed) tile size for a given attribute
   * and tile index. If the attribute is var-sized, this will return
   * the size of the offsets tile.
   *
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The tile size.
   */
  uint64_t tile_size(const std::string& attribute, uint64_t tile_idx) const;

  /**
   * Retrieves the (uncompressed) tile size for a given var-sized attribute
   * and tile index.
   *
   * @param encryption_key The key the array got opened with.
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @param tile_size The tile size to be retrieved.
   * @return Status
   */
  Status tile_var_size(
      const EncryptionKey& encryption_key,
      const std::string& attribute,
      uint64_t tile_idx,
      uint64_t* tile_size);

  /** The creation timestamp of the fragment. */
  uint64_t timestamp() const;

  /**
   * Returns `true` if the timestamp of the first operand is smaller,
   * breaking ties based on the URI string.
   */
  bool operator<(const FragmentMetadata& metadata) const;

 private:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Stores the start offsets of the generic tiles stored in the
   * metadata file, each separately storing the various metadata
   * (e.g., basic, mbrs, etc).
   */
  struct GenericTileOffsets {
    uint64_t basic_ = 0;
    uint64_t rtree_ = 0;
    uint64_t mbrs_ = 0;
    std::vector<uint64_t> tile_offsets_;
    std::vector<uint64_t> tile_var_offsets_;
    std::vector<uint64_t> tile_var_sizes_;
  };

  /** Keeps track of which metadata is loaded. */
  struct LoadedMetadata {
    bool basic_ = false;
    bool generic_tile_offsets_ = false;
    bool rtree_ = false;
    bool mbrs_ = false;
    std::vector<bool> tile_offsets_;
    std::vector<bool> tile_var_offsets_;
    std::vector<bool> tile_var_sizes_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The array schema */
  const ArraySchema* array_schema_;

  /** Maps an attribute to an index used in the various vector class members. */
  std::unordered_map<std::string, unsigned> attribute_idx_map_;

  /** Maps an attribute to its absolute URI within this fragment. */
  std::unordered_map<std::string, URI> attribute_uri_map_;

  /** Maps an attribute to its absolute '_var' URI within this fragment. */
  std::unordered_map<std::string, URI> attribute_var_uri_map_;

  /** A vector storing the first and last coordinates of each tile. */
  std::vector<void*> bounding_coords_;

  /** True if the fragment is dense, and false if it is sparse. */
  bool dense_;

  /**
   * The (expanded) domain in which the fragment is constrained. "Expanded"
   * means that the domain is enlarged minimally to coincide with tile
   * boundaries (if there is a tile grid imposed by tile extents). Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* domain_;

  /** Stores the size of each attribute file. */
  std::vector<uint64_t> file_sizes_;

  /** Stores the size of each variable attribute file. */
  std::vector<uint64_t> file_var_sizes_;

  /** The uri of the fragment the metadata belongs to. */
  URI fragment_uri_;

  /** Number of cells in the last tile (meaningful only in the sparse case). */
  uint64_t last_tile_cell_num_;

  /** Number of sparse tiles. */
  uint64_t sparse_tile_num_;

  /** Keeps track of which metadata has been loaded. */
  LoadedMetadata loaded_metadata_;

  // TODO(sp): remove after the new dense algorithm is implemented
  /** The MBRs (applicable only to the sparse case with irregular tiles). */
  std::vector<void*> mbrs_;

  /** Local mutex for thread-safety. */
  std::mutex mtx_;

  /** The offsets of the next tile for each attribute. */
  std::vector<uint64_t> next_tile_offsets_;

  /** The offsets of the next variable tile for each attribute. */
  std::vector<uint64_t> next_tile_var_offsets_;

  /**
   * The non-empty domain in which the fragment is constrained. Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* non_empty_domain_;

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

  /** The format version of this metadata. */
  uint32_t version_;

  /** The creation timestamp of the fragment. */
  uint64_t timestamp_;

  /** Stores the generic tile offsets, facilitating loading. */
  GenericTileOffsets gt_offsets_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Returns the ids (positions) of the tiles overlapping `subarray`. */
  template <class T>
  std::vector<uint64_t> compute_overlapping_tile_ids(const T* subarray) const;

  /** Creates an RTree (stored in `rtree_`) on top of `mbrs_`. */
  Status create_rtree();

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
   *
   * @tparam T The coordinates type.
   * @param mbr The MBR to expand the non-empty domain with.
   * @return Status
   */
  template <class T>
  Status expand_non_empty_domain(const T* mbr);

  /** Loads the basic metadata from storage. */
  Status load_basic(const EncryptionKey& encryption_key);

  /** Loads the R-tree from storage. */
  Status load_rtree(const EncryptionKey& encryption_key);

  /** Loads the MBRs from storage. */
  Status load_mbrs(const EncryptionKey& encryption_key);

  /** Loads the tile offsets for the input attribute from storage. */
  Status load_tile_offsets(
      const EncryptionKey& encryption_key, unsigned attr_id);

  /** Loads the variable tile offsets for the input attribute from storage. */
  Status load_tile_var_offsets(
      const EncryptionKey& encryption_key, unsigned attr_id);

  /** Loads the variable tile sizes for the input attribute from storage. */
  Status load_tile_var_sizes(
      const EncryptionKey& encryption_key, unsigned attr_id);

  /**
   * Loads the bounding coordinates from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_bounding_coords(ConstBuffer* buff);

  /** Loads the sizes of each attribute file from the buffer. */
  Status load_file_sizes(ConstBuffer* buff);

  /** Loads the sizes of each variable attribute file from the buffer. */
  Status load_file_var_sizes(ConstBuffer* buff);

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

  /**
   * Loads the non-empty domain from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_non_empty_domain(ConstBuffer* buff);

  /**
   * Loads the tile offsets for the input attribute from the input buffer.
   */
  Status load_tile_offsets(ConstBuffer* buff);

  /**
   * Loads the tile offsets for the input attribute from the input buffer.
   */
  Status load_tile_offsets(unsigned attr_id, ConstBuffer* buff);

  /**
   * Loads the variable tile offsets from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_tile_var_offsets(ConstBuffer* buff);

  /**
   * Loads the variable tile offsets for the input attribute from the buffer.
   */
  Status load_tile_var_offsets(unsigned attr_id, ConstBuffer* buff);

  /**
   * Loads the variable tile sizes from the fragment metadata.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_tile_var_sizes(ConstBuffer* buff);

  /**
   * Loads the variable tile sizes for the input attribute from the buffer.
   */
  Status load_tile_var_sizes(unsigned attr_id, ConstBuffer* buff);

  /** Loads the format version from the buffer. */
  Status load_version(ConstBuffer* buff);

  /** Loads the number of sparse tiles from the buffer. */
  Status load_sparse_tile_num(ConstBuffer* buff);

  /**
   * Retrieves the size of the generic tile starting at the input offset.
   */
  Status get_generic_tile_size(uint64_t offset, uint64_t* size);

  /**
   * Loads the offsets of each generic tile (to be used to load the basic
   * metadata, mbrs, tile offsets, etc. - each written in a separate generic
   * tile).
   */
  Status load_generic_tile_offsets();

  /** Loads the basic metadata from storage (version 2 or before). */
  Status load_v2(const EncryptionKey& encryption_key);

  /** Loads the basic metadata from storage (version 3). */
  Status load_v3(const EncryptionKey& encryption_key);

  /** Writes the sizes of each attribute file to the buffer. */
  Status write_file_sizes(Buffer* buff);

  /** Writes the sizes of each variable attribute file to the buffer. */
  Status write_file_var_sizes(Buffer* buff);

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   */
  Status write_last_tile_cell_num(Buffer* buff);

  /** Writes the R-tree to storage. */
  Status store_rtree(const EncryptionKey& encryption_key);

  /** Writes the MBRs to storage. */
  Status store_mbrs(const EncryptionKey& encryption_key);

  /** Writes the R-tree to the input buffer. */
  Status write_rtree(Buffer* buff);

  /** Writes the MBRs to the input buffer. */
  Status write_mbrs(Buffer* buff);

  /** Writes the non-empty domain to the input buffer. */
  Status write_non_empty_domain(Buffer* buff);

  /** Writes the tile offsets of the input attribute to storage. */
  Status store_tile_offsets(
      unsigned attr_id, const EncryptionKey& encryption_key);

  /** Writes the tile offsets of the input attribut$ to the input buffer. */
  Status write_tile_offsets(unsigned attr_id, Buffer* buff);

  /** Writes the variable tile offsets of the input attribute to storage. */
  Status store_tile_var_offsets(
      unsigned attr_id, const EncryptionKey& encryption_key);

  /** Writes the variable tile offsets of the input attribute to the buffer. */
  Status write_tile_var_offsets(unsigned attr_id, Buffer* buff);

  /** Writes the variable tile sizes for the input attribute to the buffer. */
  Status store_tile_var_sizes(
      unsigned attr_id, const EncryptionKey& encryption_key);

  /** Writes the variable tile sizes to storage. */
  Status write_tile_var_sizes(unsigned attr_id, Buffer* buff);

  /** Writes the format version to the buffer. */
  Status write_version(Buffer* buff);

  /** Writes the number of sparse tiles to the buffer. */
  Status write_sparse_tile_num(Buffer* buff);

  /** Writes the basic metadata to storage. */
  Status store_basic(const EncryptionKey& encryption_key);

  /**
   * Reads the contents of a generic tile starting at the input offset,
   * and stores them into buffer ``buff``.
   */
  Status read_generic_tile_from_file(
      const EncryptionKey& encryption_key, uint64_t offset, Buffer* buff) const;

  /**
   * Writes the contents of the input buffer as a separate
   * generic tile to the metadata file.
   */
  Status write_generic_tile_to_file(
      const EncryptionKey& encryption_key, Buffer* buff) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_METADATA_H
