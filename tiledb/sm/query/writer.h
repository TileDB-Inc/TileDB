/**
 * @file   writer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class Writer.
 */

#ifndef TILEDB_WRITER_H
#define TILEDB_WRITER_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/dense_cell_range_iter.h"
#include "tiledb/sm/tile/tile.h"

#include <memory>

namespace tiledb {
namespace sm {

class StorageManager;

/** Processes write queries. */
class Writer {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Contains the buffer(s) and buffer size(s) for some attribute. */
  struct AttributeBuffer {
    /**
     * The attribute buffer. In case the attribute is var-sized, this is
     * the offsets buffer.
     */
    void* buffer_;
    /**
     * For a var-sized attribute, this is the data buffer. It is `nullptr`
     * for fixed-sized attributes.
     */
    void* buffer_var_;
    /** The size (in bytes) of `buffer_`. */
    uint64_t* buffer_size_;
    /** The size (in bytes) of `buffer_var_`. */
    uint64_t* buffer_var_size_;

    /** Constructor. */
    AttributeBuffer(
        void* buffer,
        void* buffer_var,
        uint64_t* buffer_size,
        uint64_t* buffer_var_size)
        : buffer_(buffer)
        , buffer_var_(buffer_var)
        , buffer_size_(buffer_size)
        , buffer_var_size_(buffer_var_size) {
    }
  };

  /**
   * State used only in global writes, where the user can "append"
   * by successive query submissions until the query is finalized.
   */
  struct GlobalWriteState {
    /**
     * Stores the last tile of each attribute for each write operation.
     * For fixed-sized attributes, the second tile is ignored. For
     * var-sized attributes, the first tile is the offsets tile, whereas
     * the second tile is the values tile.
     */
    std::unordered_map<std::string, std::pair<Tile, Tile>> last_tiles_;

    /**
     * Stores the number of cells written for each attribute across the
     * write operations.
     */
    std::unordered_map<std::string, uint64_t> cells_written_;

    /** The fragment metadata. */
    std::shared_ptr<FragmentMetadata> frag_meta_;
  };

  /** Cell range to be written. */
  struct WriteCellRange {
    /** The position in the tile where the range will be copied. */
    uint64_t pos_;
    /** The starting cell in the user buffers. */
    uint64_t start_;
    /** The ending cell in the user buffers. */
    uint64_t end_;

    /** Constructor. */
    WriteCellRange(uint64_t pos, uint64_t start, uint64_t end)
        : pos_(pos)
        , start_(start)
        , end_(end) {
    }
  };

  /** A vector of write cell ranges. */
  typedef std::vector<WriteCellRange> WriteCellRangeVec;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Writer();

  /** Destructor. */
  ~Writer();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Finalizes the reader. */
  Status finalize();

  /** Initializes the writer. */
  Status init();

  /** Returns the cell layout. */
  Layout layout() const;

  /** Sets the array schema. */
  void set_array_schema(const ArraySchema* array_schema);

  /**
   * Sets the buffers to the query for a set of attributes.
   *
   * @param attributes The attributes the query will focus on.
   * @param attribute_num The number of attributes.
   * @param buffers The buffers that either have the input data to be written.
   *     Note that there is one buffer per fixed-sized attribute, and two
   *     buffers for each variable-sized attribute (the first holds the offsets,
   *     and the second the actual values).
   * @param buffer_sizes There must be an one-to-one correspondence with
   *     *buffers*. They contain the sizes of *buffers*.
   * @return Status
   */
  Status set_buffers(
      const char** attributes,
      unsigned int attribute_num,
      void** buffers,
      uint64_t* buffer_sizes);

  /** Sets the query buffers. */
  void set_buffers(void** buffers, uint64_t* buffer_sizes);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   * @return Status
   */
  Status set_subarray(const void* subarray);

  /** Performs a write query using its set members. */
  Status write();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array schema. */
  const ArraySchema* array_schema_;

  /** The names of the attributes involved in the query. */
  std::vector<std::string> attributes_;

  /** Maps attribute names to their buffers. */
  std::unordered_map<std::string, AttributeBuffer> attr_buffers_;

  /** The state associated with global writes. */
  std::unique_ptr<GlobalWriteState> global_write_state_;

  /** The layout of the cells in the user buffers. */
  Layout layout_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The subarray the query is constrained on. */
  void* subarray_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Checks if attributes has been appropriately set for the query. */
  Status check_attributes();

  /** Correctness checks for buffer sizes. */
  Status check_buffer_sizes() const;

  /** Correctness checks for `subarray`. */
  Status check_subarray(const void* subarray) const;

  /** Correctness checks for `subarray`. */
  template <class T>
  Status check_subarray(const T* subarray) const;

  /** Closes all attribute files, flushing their state to storage. */
  Status close_files(FragmentMetadata* meta) const;

  /**
   * Computes the coordinates metadata (e.g., MBRs).
   *
   * @tparam T The domain type.
   * @param tiles The tiles to calculate the coords metadata from.
   * @param meta The fragment metadata that will store the coords metadata.
   * @return Status
   */
  template <class T>
  Status compute_coords_metadata(
      const std::vector<Tile>& tiles, FragmentMetadata* meta) const;

  /**
   * Computes the cell ranges to be written, derived from a
   * dense cell range iterator for a specific tile.
   *
   * @tparam T The domain type.
   * @param iter The dense cell range iterator for one
   *     tile overlapping with the write subarray.
   * @param write_cell_ranges The write cell ranges to be created.
   * @return Status
   */
  template <class T>
  Status compute_write_cell_ranges(
      DenseCellRangeIter<T>* iters, WriteCellRangeVec* write_cell_ranges) const;

  /**
   * Creates a new fragment.
   *
   * @param dense Whether the fragment is dense or not.
   * @param frag_meta The fragment metadata to be generated.
   * @return Status
   */
  Status create_fragment(
      bool dense, std::shared_ptr<FragmentMetadata>* frag_meta) const;

  /** Finalizes the global write state. */
  Status finalize_global_write_state();

  /**
   * Finalizes the global write state.
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status finalize_global_write_state();

  /**
   * Writes in the global layout. Applicable to both dense and sparse
   * arrays.
   */
  Status global_write();

  /**
   * Writes in the global layout. Applicable to both dense and sparse
   * arrays.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status global_write();

  /**
   * Applicable only to global writes. Writes the last tiles for each
   * attribute remaining in the state, and records the metadata for
   * the coordinates attribute (if present).
   *
   * @tparam T The domain type.
   * @return Status
   */
  template <class T>
  Status global_write_handle_last_tile();

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /** Initializes the global write state. */
  Status init_global_write_state();

  /**
   * Initializes a fixed-sized tile.
   *
   * @param attribute The attribute the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(const std::string& attribute, Tile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param attribute The attribute the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      const std::string& attribute, Tile* tile, Tile* tile_var) const;

  /**
   * Initializes dense cell range iterators for the subarray to be writte,
   * one per overlapping tile.
   *
   * @tparam T The domain type.
   * @param iters The dense cell range iterators to be created.
   * @return Status
   */
  template <class T>
  Status init_tile_dense_cell_range_iters(
      std::vector<DenseCellRangeIter<T>>* iters) const;

  /**
   * Initializes the tiles for writing for the input attribute.
   *
   * @param attribute The attribute the tiles belong to.
   * @param tile_num The number of tiles.
   * @param tiles The tiles to be initialized. Note that the vector
   *     has been already preallocated.
   * @return Status
   */
  Status init_tiles(
      const std::string& attribute,
      uint64_t tile_num,
      std::vector<Tile>* tiles) const;

  /**
   * Returns a new fragment name, which is in the form: <br>
   * .__thread-id_timestamp. For instance,
   *  __6426153_1458759561320
   *
   * Note that this is a temporary name, initiated by a new write process.
   * After the new fragmemt is finalized, the array will change its name
   * by removing the leading '.' character.
   *
   * @return A new special fragment name on success, or "" (empty string) on
   *     error.
   */
  std::string new_fragment_name() const;

  /**
   * Writes in an ordered layout (col- or row-major order). Applicable only
   * to dense arrays.
   */
  Status ordered_write();

  /**
   * Writes in an ordered layout (col- or row-major order). Applicable only
   * to dense arrays.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status ordered_write();

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state_->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to fixed-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_fixed(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state_->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to var-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_var(
      const std::string& attribute, std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, copying from the user buffers into the tiles
   * the values based on the input write cell ranges, focusing on the
   * input attribute.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param write_cell_ranges The write cell ranges.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& attribute,
      const std::vector<WriteCellRangeVec>& write_cell_ranges,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions. Applicable only
   * to fixed-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_fixed(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /**
   * It prepares the tiles, re-organizing the cells from the user
   * buffers based on the input sorted positions. Applicable only
   * to var-sized attributes.
   *
   * @param attribute The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_var(
      const std::string& attribute,
      const std::vector<uint64_t>& cell_pos,
      std::vector<Tile>* tiles) const;

  /** Sets the query attributes. */
  Status set_attributes(const char** attributes, unsigned int attribute_num);

  /**
   * Sorts the coordinates of the user buffers, creating a vector with
   * the sorted positions.
   *
   * @tparam T The domain type.
   * @param cell_pos The sorted cell positions to be created.
   * @return Status
   */
  template <class T>
  Status sort_coords(std::vector<uint64_t>* cell_pos) const;

  /**
   * Writes in unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   */
  Status unordered_write();

  /**
   * Writes in unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   *
   * @tparam T The domain type.
   */
  template <class T>
  Status unordered_write();

  /**
   * Writes an empty cell range to the input tile.
   * Applicable to **fixed-sized** attributes.
   *
   * @param num Number of empty values to write.
   * @param tile The tile to write to.
   * @return Status
   */
  Status write_empty_cell_range_to_tile(uint64_t num, Tile* tile) const;

  /**
   * Writes an empty cell range to the input tile.
   * Applicable to **variable-sized** attributes.
   *
   * @param num Number of empty values to write.
   * @param tile The tile offsets to write to.
   * @param tile_var The tile with the var-sized cells to write to.
   * @return Status
   */
  Status write_empty_cell_range_to_tile_var(
      uint64_t num, Tile* tile, Tile* tile_var) const;

  /**
   * Writes the input cell range to the input tile, for a particular
   * buffer. Applicable to **fixed-sized** attributes.
   *
   * @param buff The write buffer where the cells will be copied from.
   * @param start The start element in the write buffer.
   * @param end The end element in the write buffer.
   * @param tile The tile to write to.
   * @return Status
   */
  Status write_cell_range_to_tile(
      ConstBuffer* buff, uint64_t start, uint64_t end, Tile* tile) const;

  /**
   * Writes the input cell range to the input tile, for a particular
   * buffer. Applicable to **variable-sized** attributes.
   *
   * @param buff The write buffer where the cell offsets will be copied from.
   * @param buff_var The write buffer where the cell values will be copied from.
   * @param start The start element in the write buffer.
   * @param end The end element in the write buffer.
   * @param tile The tile offsets to write to.
   * @param tile_var The tile with the var-sized cells to write to.
   * @return Status
   */
  Status write_cell_range_to_tile_var(
      ConstBuffer* buff,
      ConstBuffer* buff_var,
      uint64_t start,
      uint64_t end,
      Tile* tile,
      Tile* tile_var) const;

  /**
   * Writes the input tiles for the input attribute to storage.
   *
   * @param attribute The attribute the tiles belong to.
   * @param frag_meta The fragment metadata.
   * @param tiles The tiles to be written.
   * @return Status
   */
  Status write_tiles(
      const std::string& attribute,
      FragmentMetadata* frag_meta,
      std::vector<Tile>& tiles) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_WRITER_H
