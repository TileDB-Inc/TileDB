/**
 * @file   global_order_writer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines class GlobalOrderWriter.
 */

#ifndef TILEDB_GLOBAL_ORDER_WRITER_H
#define TILEDB_GLOBAL_ORDER_WRITER_H

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/query/writers/domain_buffer.h"
#include "tiledb/sm/query/writers/writer_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Processes write queries. */
class GlobalOrderWriter : public WriterBase {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * State used only in global writes, where the user can "append"
   * by successive query submissions until the query is finalized.
   */
  struct GlobalWriteState {
    /**
     * Stores the last tile of each attribute/dimension for each write
     * operation. The key is the attribute/dimension name. For fixed-sized
     * attributes/dimensions, the second tile is ignored. For var-sized
     * attributes/dimensions, the first tile is the offsets tile, whereas the
     * second tile is the values tile. In both cases, the third tile stores a
     * validity tile for nullable attributes.
     */
    std::unordered_map<std::string, WriterTileVector> last_tiles_;

    /**
     * Stores the last offset into the var size tile buffer for var size
     * dimensions/attributes. The key is the attribute/dimension name.
     *
     * Note: Once tiles are created with the correct size from the beginning,
     * this variable can go awaty.
     */
    std::unordered_map<std::string, uint64_t> last_var_offsets_;

    /**
     * Stores the number of cells written for each attribute/dimension across
     * the write operations.
     */
    std::unordered_map<std::string, uint64_t> cells_written_;

    /** The fragment metadata that the writer will focus on. */
    shared_ptr<FragmentMetadata> frag_meta_;

    /** The last cell written. */
    std::optional<SingleCoord> last_cell_coords_;

    /** The last hilbert value written. */
    uint64_t last_hilbert_value_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  GlobalOrderWriter(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      std::vector<WrittenFragmentInfo>& written_fragment_info,
      bool disable_checks_consolidation,
      Query::CoordsInfo& coords_info_,
      URI fragment_uri = URI(""),
      bool skip_checks_serialization = false);

  /** Destructor. */
  ~GlobalOrderWriter();

  DISABLE_COPY_AND_COPY_ASSIGN(GlobalOrderWriter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(GlobalOrderWriter);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Performs a write query using its set members. */
  Status dowork();

  /** Finalizes the writer. */
  Status finalize();

  /** Resets the writer object, rendering it incomplete. */
  void reset();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The state associated with global writes. */
  tdb_unique_ptr<GlobalWriteState> global_write_state_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Throws an error if there are coordinate duplicates. This function
   * assumes that the coordinates are written in the global layout,
   * which means that they are already sorted in the attribute buffers.
   *
   * @return Status
   */
  Status check_coord_dups() const;

  /**
   * Throws an error if there are coordinates that do not obey the
   * global order.
   *
   * @return Status
   */
  Status check_global_order() const;

  /**
   * Throws an error if there are coordinates that do not obey the
   * global order. Applicable only to Hilbert order.
   *
   * @return Status
   */
  Status check_global_order_hilbert() const;

  /**
   * Invoked on error. It removes the directory of the input URI and
   * resets the global write state.
   */
  void clean_up(const URI& uri);

  /**
   * Computes the positions of the coordinate duplicates (if any). Note
   * that only the duplicate occurrences are determined, i.e., if the same
   * coordinates appear 3 times, only 2 will be marked as duplicates,
   * whereas the first occurrence will not be marked as duplicate.
   *
   * This functions assumes that the coordinates are laid out in the
   * global order and, hence, they are sorted in the attribute buffers.
   *
   * @param A set indicating the positions of the duplicates.
   *     If there are not duplicates, this vector will be **empty** after
   *     the termination of the function.
   * @return Status
   */
  Status compute_coord_dups(std::set<uint64_t>* coord_dups) const;

  /**
   * Applicable only to global writes. Filters the last attribute and
   * coordinate tiles.
   */
  Status filter_last_tiles(uint64_t cell_num);

  /** Finalizes the global write state. */
  Status finalize_global_write_state();

  /**
   * Writes in the global layout. Applicable to both dense and sparse
   * arrays.
   */
  Status global_write();

  /**
   * Applicable only to global writes. Writes the last tiles for each
   * attribute remaining in the state, and records the metadata for
   * the coordinates (if present).
   *
   * @return Status
   */
  Status global_write_handle_last_tile();

  /** Initializes the global write state. */
  Status init_global_write_state();

  /**
   * This deletes the global write state and deletes the potentially
   * partially written fragment.
   */
  void nuke_global_write_state();

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation.
   *
   * @param coord_dups The positions of the duplicate coordinates.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles(
      const std::set<uint64_t>& coord_dups,
      std::unordered_map<std::string, WriterTileVector>* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation.
   *
   * @param name The attribute/dimension to prepare the tiles for.
   * @param coord_dups The positions of the duplicate coordinates.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles(
      const std::string& name,
      const std::set<uint64_t>& coord_dups,
      WriterTileVector* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state_->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to fixed-sized attributes.
   *
   * @param name The attribute/dimension to prepare the tiles for.
   * @param coord_dups The positions of the duplicate coordinates.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_fixed(
      const std::string& name,
      const std::set<uint64_t>& coord_dups,
      WriterTileVector* tiles) const;

  /**
   * Applicable only to write in global order. It prepares only full
   * tiles, storing the last potentially non-full tile in
   * `global_write_state_->last_tiles_` as part of the state to be used in
   * the next write invocation. The last tiles are written to storage
   * upon `finalize`. Upon each invocation, the function first
   * populates the partially full last tile from the previous
   * invocation. Applicable only to var-sized attributes.
   *
   * @param name The attribute/dimension to prepare the tiles for.
   * @param coord_dups The positions of the duplicate coordinates.
   * @param tiles The **full** tiles to be created.
   * @return Status
   */
  Status prepare_full_tiles_var(
      const std::string& name,
      const std::set<uint64_t>& coord_dups,
      WriterTileVector* tiles) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_GLOBAL_ORDER_WRITER_H
