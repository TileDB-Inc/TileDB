/**
 * @file   relevant_fragments_generator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines class RelevantFragmentsGenerator.
 */

#ifndef TILEDB_RELEVANT_FRAGMENTS_GENERATOR_H
#define TILEDB_RELEVANT_FRAGMENTS_GENERATOR_H

#include "tiledb/common/common.h"
#include "tiledb/common/thread_pool/thread_pool.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

class OpenedArray;
class RelevantFragments;
class Subarray;
class SubarrayTileOverlap;

namespace stats {
class Stats;
}

/**
 * This class contains the code to generate the list of relevant fragments.
 */
class RelevantFragmentGenerator {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor a generator. */
  RelevantFragmentGenerator(
      const shared_ptr<OpenedArray> opened_array,
      const Subarray& subarray,
      stats::Stats* stats);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Updates the range coords and returns if the coords were updated or not.
   *
   * @param tile_overlap Current tile overlap.
   * @return `true` if the range coords were updated.
   */
  bool update_range_coords(const SubarrayTileOverlap* tile_overlap);

  /**
   * Computes the indexes of the fragments that are relevant to the query,
   * that is those whose non-empty domain intersects with at least one
   * range.
   *
   * @param compute_tp Thread pool for compute-bound tasks.
   */
  RelevantFragments compute_relevant_fragments(ThreadPool* compute_tp);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /**
   * The last calibrated start coordinates.
   */
  std::vector<uint64_t> start_coords_;

  /**
   * The last calibrated end coordinates.
   */
  std::vector<uint64_t> end_coords_;

  /**
   * The fragment bytemaps for each dimension. The inner
   * vector is the fragment bytemap that has a byte element
   * for each fragment. Non-zero bytes represent relevant
   * fragments for a specific dimension. Each dimension
   * has its own fragment bytemap (the outer vector).
   */
  std::vector<std::vector<uint8_t>> fragment_bytemaps_;

  /** The class stats. */
  stats::Stats* stats_;

  /** Reference to the opened opened array. */
  const shared_ptr<OpenedArray> array_;

  /** Reference to the subarray. */
  const Subarray& subarray_;
};
}  // namespace tiledb::sm

#endif  // TILEDB_RELEVANT_FRAGMENTS_GENERATOR_H
