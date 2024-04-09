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

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/query/writers/writer_base.h"

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
      shared_ptr<Logger> logger,
      StrategyParams& params,
      std::vector<WrittenFragmentInfo>& written_fragment_info,
      Query::CoordsInfo& coords_info,
      std::unordered_set<std::string>& written_buffers,
      bool remote_query,
      optional<std::string> fragment_name = nullopt);

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

  /** Returns the name of the strategy. */
  std::string name();

  /** Alloc a new fragment metadata. */
  Status alloc_frag_meta();

  /** Returns the cell position vector. */
  tdb::pmr::vector<uint64_t>& cell_pos() {
    return cell_pos_;
  }

  /** Returns the coord duplicates set. */
  std::set<uint64_t>& coord_dups() {
    return coord_dups_;
  }

  /** Returns the fragment metadata. */
  shared_ptr<FragmentMetadata> frag_meta() {
    return frag_meta_;
  }

  /** Return the is coords pass boolean. */
  bool& is_coords_pass() {
    return is_coords_pass_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Fragment URI. */
  std::optional<URI> frag_uri_;

  /**
   * The positions that resulted from sorting and according to which the cells
   * must be re-arranged.
   */
  tdb::pmr::vector<uint64_t> cell_pos_;

  /** The set with the positions of duplicate coordinates/cells. */
  std::set<uint64_t> coord_dups_;

  /** Pointer to the fragment metadata. */
  shared_ptr<FragmentMetadata> frag_meta_;

  /** Already written buffers. */
  std::unordered_set<std::string>& written_buffers_;

  /**
   * Does this pass of the write include coordinates. This is used when we are
   * doing a partial attribute write with multiple passes.
   */
  bool is_coords_pass_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Invoked on error. It removes the directory of the input URI. */
  void clean_up();

  /**
   * Throws an error if there are coordinate duplicates.
   *
   * @return Status
   */
  Status check_coord_dups() const;

  /**
   * Computes the positions of the coordinate duplicates (if any). Note
   * that only the duplicate occurrences are determined, i.e., if the same
   * coordinates appear 3 times, only 2 will be marked as duplicates,
   * whereas the first occurrence will not be marked as duplicate.
   *
   * @param A set indicating the positions of the duplicates.
   *     If there are not duplicates, this vector will be **empty** after
   *     the termination of the function.
   * @return Status
   */
  Status compute_coord_dups();

  /**
   * It prepares the attribute and coordinate tiles, re-organizing the cells
   * from the user buffers based on the input sorted positions and coordinate
   * duplicates.
   *
   * @param tiles The tiles to be created, one vector per attribute or
   *     coordinate.
   * @return Status
   */
  Status prepare_tiles(
      tdb::pmr::unordered_map<std::string, WriterTileTupleVector>* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   *
   * @param name The attribute or dimension to prepare the tiles for.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles(
      const std::string& name, WriterTileTupleVector* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   * Applicable only to fixed-sized attributes or dimensions.
   *
   * @param name The attribute or dimension to prepare the tiles for.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_fixed(
      const std::string& name, WriterTileTupleVector* tiles) const;

  /**
   * It prepares the tiles for the input attribute or dimension, re-organizing
   * the cells from the user buffers based on the input sorted positions.
   * Applicable only to var-sized attributes or dimensions.
   *
   * @param name The attribute to prepare the tiles for.
   * @param tiles The tiles to be created.
   * @return Status
   */
  Status prepare_tiles_var(
      const std::string& name, WriterTileTupleVector* tiles) const;

  /**
   * Sorts the coordinates of the user buffers, creating a vector with
   * the sorted positions.
   *
   * @return Status
   */
  Status sort_coords();

  /**
   * Writes in unordered layout. Applicable to both dense and sparse arrays.
   * Explicit coordinates must be provided for this write.
   */
  Status unordered_write();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UNORDERED_WRITER_H
