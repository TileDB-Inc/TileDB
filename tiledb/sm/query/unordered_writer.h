/**
 * @file   unordered_writer.h
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
 * This file defines class UnorderedWriter.
 */

#ifndef TILEDB_UNORDERED_WRITER_H
#define TILEDB_UNORDERED_WRITER_H

#include <atomic>

#include "tiledb/common/status.h"
#include "tiledb/sm/query/writer_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Processes write queries. */
class UnorderedWriter : public WriterBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  UnorderedWriter(
      stats::Stats* stats,
      tdb_shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      std::vector<WrittenFragmentInfo>& written_fragment_info,
      bool disable_check_global_order,
      Query::CoordsInfo& coords_info_,
      URI fragment_uri = URI(""));

  /** Destructor. */
  ~UnorderedWriter();

  DISABLE_COPY_AND_COPY_ASSIGN(UnorderedWriter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(UnorderedWriter);

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

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Throws an error if there are coordinate duplicates.
   *
   * @param cell_pos The sorted positions of the coordinates in the
   *     `attr_buffers_`.
   * @return Status
   */
  Status check_coord_dups(const std::vector<uint64_t>& cell_pos) const;

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
   * @param cell_pos The sorted positions of the coordinates in the
   *     `attr_buffers_`.
   * @param A set indicating the positions of the duplicates.
   *     If there are not duplicates, this vector will be **empty** after
   *     the termination of the function.
   * @return Status
   */
  Status compute_coord_dups(
      const std::vector<uint64_t>& cell_pos,
      std::set<uint64_t>* coord_dups) const;

  /**
   * It prepares the attribute and coordinate tiles, re-organizing the cells
   * from the user buffers based on the input sorted positions and coordinate
   * duplicates.
   *
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param coord_dups The set with the positions
   *     of duplicate coordinates/cells.
   * @param tiles The tiles to be created, one vector per attribute or
   *     coordinate.
   * @return Status
   */
  Status prepare_tiles(
      const std::vector<uint64_t>& cell_pos,
      const std::set<uint64_t>& coord_dups,
      std::unordered_map<std::string, std::vector<WriterTile>>* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   *
   * @param name The attribute or dimension to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param coord_dups The set with the positions
   *     of duplicate coordinates/cells.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& name,
      const std::vector<uint64_t>& cell_pos,
      const std::set<uint64_t>& coord_dups,
      std::vector<WriterTile>* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   * Applicable only to fixed-sized attributes or dimensions.
   *
   * @param name The attribute or dimension to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param coord_dups The set with the positions
   *     of duplicate coordinates/cells.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_fixed(
      const std::string& name,
      const std::vector<uint64_t>& cell_pos,
      const std::set<uint64_t>& coord_dups,
      std::vector<WriterTile>* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   * Applicable only to var-sized attributes or dimensions.
   *
   * @param name The attribute to prepare the tiles for.
   * @param cell_pos The positions that resulted from sorting and
   *     according to which the cells must be re-arranged.
   * @param coord_dups The set with the positions
   *     of duplicate coordinates/cells.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_var(
      const std::string& name,
      const std::vector<uint64_t>& cell_pos,
      const std::set<uint64_t>& coord_dups,
      std::vector<WriterTile>* tiles) const;

  /**
   * Sorts the coordinates of the user buffers, creating a vector with
   * the sorted positions.
   *
   * @param cell_pos The sorted cell positions to be created.
   * @return Status
   */
  Status sort_coords(std::vector<uint64_t>* cell_pos) const;

  /**
   * Writes in unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   */
  Status unordered_write();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNORDERED_WRITER_H
