/**
 * @file  fragment_metadata.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/status.h"

#include <capnp/message.h>
#include <vector>

namespace tiledb {
namespace sm {

/** Stores the metadata structures of a fragment. */
class FragmentMetadata {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array_schema The schema of the array the fragment belongs to.
   * @param dense Indicates whether the fragment is dense or sparse.
   * @param fragment_uri The fragment URI.
   * @param timestamp The timestamp of the fragment creation. In TileDB,
   * timestamps are in ms elapsed since 1970-01-01 00:00:00 +0000 (UTC).
   */
  FragmentMetadata(
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

  /**
   * Serialize a Fragment to capnp format
   * @param builder
   * @return  Status of fragment
   */
  Status capnp(::FragmentMetadata::Builder* fragmentMetadataBuilder) const;

  /** Returns the number of cells in the tile at the input position. */
  uint64_t cell_num(uint64_t tile_pos) const;

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`.
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
  Status add_max_buffer_sizes(
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes) const;

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
          buffer_sizes) const;

  /**
   * Computes an upper bound on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the sparse
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
  Status add_max_buffer_sizes_sparse(
      const T* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes) const;

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`.
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
  Status add_est_read_buffer_sizes(
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes)
      const;

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
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
  Status add_est_read_buffer_sizes_dense(
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes)
      const;

  /**
   * Computes an estimate on the buffer sizes needed when reading a subarray
   * from the fragment, for a given set of attributes. Note that these upper
   * bounds is added to those in `buffer_sizes`. Applicable only to the sparse
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
  Status add_est_read_buffer_sizes_sparse(
      const T* subarray,
      std::unordered_map<std::string, std::pair<double, double>>* buffer_sizes)
      const;

  /**
   * Returns ture if the corresponding fragment is dense, and false if it
   * is sparse.
   */
  bool dense() const;

  /**
   * Loads the fragment metadata structures from the input binary buffer.
   *
   * @param buff The binary buffer to deserialize from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /** Returns the (expanded) domain in which the fragment is constrained. */
  const void* domain() const;

  /** Returns the size of the input attribute. */
  uint64_t file_sizes(const std::string& attribute) const;

  /** Returns the size of the input variable attribute. */
  uint64_t file_var_sizes(const std::string& attribute) const;

  /** Returns the fragment URI. */
  const URI& fragment_uri() const;

  /**
   * Deserialize from a capnp message
   * @param writerBuilder
   * @return
   */
  Status from_capnp(::FragmentMetadata::Reader* fragmentMetadataReader);

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

  /** Returns the MBRs. */
  const std::vector<void*>& mbrs() const;

  /** Returns the non-empty domain in which the fragment is constrained. */
  const void* non_empty_domain() const;

  /**
   * Serializes the metadata structures into a binary buffer.
   *
   * @param buff The buffer to serialize into.
   * @return Status
   */
  Status serialize(Buffer* buff);

  /**
   * Sets the input tile's bounding coordinates in the fragment metadata.
   *
   * @param tile The tile index whose bounding coords will be set.
   * @param bounding_coords The bounding coordinates to be set.
   * @return void
   */
  void set_bounding_coords(uint64_t tile, const void* bounding_coords);

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
   * Returns the starting offset of the input tile of input attribute
   * in the file. If the attribute is var-sized, it returns the starting
   * offset of the offsets tile.
   *
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The file offset.
   */
  uint64_t file_offset(const std::string& attribute, uint64_t tile_idx) const;

  /**
   * Returns the starting offset of the input tile of input attribute
   * in the file. The attribute must be var-sized.
   *
   * @param attribute_id The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The file offset.
   */
  uint64_t file_var_offset(
      const std::string& attribute, uint64_t tile_idx) const;

  /**
   * Returns the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given attribute and tile index. If the
   * attribute is var-sized, this will return the persisted size of the offsets
   * tile.
   *
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The tile size.
   */
  uint64_t persisted_tile_size(
      const std::string& attribute, uint64_t tile_idx) const;

  /**
   * Returns the size of the tile when it is persisted (e.g. the size of the
   * compressed tile on disk) for a given var-sized attribute and tile index.
   *
   * @param attribute The inout attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The tile size.
   */
  uint64_t persisted_tile_var_size(
      const std::string& attribute, uint64_t tile_idx) const;

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
   * Returns the (uncompressed) tile size for a given var-sized attribute
   * and tile index.
   *
   * @param attribute The input attribute.
   * @param tile_idx The index of the tile in the metadata.
   * @return The tile size.
   */
  uint64_t tile_var_size(const std::string& attribute, uint64_t tile_idx) const;

  /** The creation timestamp of the fragment. */
  uint64_t timestamp() const;

  /**
   * Returns `true` if the timestamp of the first operand is smaller,
   * breaking ties based on the URI string.
   */
  bool operator<(const FragmentMetadata& metadata) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

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

  /** The MBRs (applicable only to the sparse case with irregular tiles). */
  std::vector<void*> mbrs_;

  /** The offsets of the next tile for each attribute. */
  std::vector<uint64_t> next_tile_offsets_;

  /** The offsets of the next variable tile for each attribute. */
  std::vector<uint64_t> next_tile_var_offsets_;

  /**
   * The non-empty domain in which the fragment is constrained. Note that the
   * type of the domain must be the same as the type of the array coordinates.
   */
  void* non_empty_domain_;

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

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Returns the ids (positions) of the tiles overlapping `subarray`. */
  template <class T>
  std::vector<uint64_t> compute_overlapping_tile_ids(const T* subarray) const;

  /**
   * Returns the ids (positions) of the tiles overlapping `subarray`, along with
   * with the coverage of the overlap.
   */
  template <class T>
  std::vector<std::pair<uint64_t, double>> compute_overlapping_tile_ids_cov(
      const T* subarray) const;

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
   * Loads the tile offsets from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_tile_offsets(ConstBuffer* buff);

  /**
   * Loads the variable tile offsets from the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_tile_var_offsets(ConstBuffer* buff);

  /**
   * Loads the variable tile sizes from the fragment metadata.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status load_tile_var_sizes(ConstBuffer* buff);

  /** Loads the format version from the buffer. */
  Status load_version(ConstBuffer* buff);

  /**
   * Writes the bounding coordinates to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_bounding_coords(Buffer* buff);

  /** Writes the sizes of each attribute file in the buffer. */
  Status write_file_sizes(Buffer* buff);

  /** Writes the sizes of each variable attribute file in the buffer. */
  Status write_file_var_sizes(Buffer* buff);

  /**
   * Writes the cell number of the last tile to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_last_tile_cell_num(Buffer* buff);

  /**
   * Writes the MBRs to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_mbrs(Buffer* buff);

  /**
   * Writes the non-empty domain to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_non_empty_domain(Buffer* buff);

  /**
   * Writes the tile offsets to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_tile_offsets(Buffer* buff);

  /**
   * Writes the variable tile offsets to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_tile_var_offsets(Buffer* buff);

  /**
   * Writes the variable tile sizes to the fragment metadata buffer.
   *
   * @param buff Metadata buffer.
   * @return Status
   */
  Status write_tile_var_sizes(Buffer* buff);

  /** Writes the format version to the buffer. */
  Status write_version(Buffer* buff);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FRAGMENT_METADATA_H
