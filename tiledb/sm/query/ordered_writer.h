/**
 * @file   ordered_writer.h
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
 * This file defines class OrderedWriter.
 */

#ifndef TILEDB_ORDERED_WRITER_H
#define TILEDB_ORDERED_WRITER_H

#include <atomic>

#include "tiledb/common/status.h"
#include "tiledb/sm/query/writer_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Processes write queries. */
class OrderedWriter : public WriterBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  OrderedWriter(
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
  ~OrderedWriter();

  DISABLE_COPY_AND_COPY_ASSIGN(OrderedWriter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(OrderedWriter);

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
   * Prepares, filters and writes dense tiles for the given attribute.
   *
   * @tparam T The array domain datatype.
   * @param name The attribute name.
   * @param tile_batches The attribute tile batches.
   * @param frag_meta The metadata of the new fragment.
   * @param dense_tiler The dense tiler that will prepare the tiles.
   * @param thread_num The number of threads to be used for the function.
   * @param stats Statistics to gather in the function.
   */
  template <class T>
  Status prepare_filter_and_write_tiles(
      const std::string& name,
      std::vector<std::vector<WriterTile>>& tile_batches,
      tdb_shared_ptr<FragmentMetadata> frag_meta,
      DenseTiler<T>* dense_tiler,
      uint64_t thread_num);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ORDERED_WRITER_H
