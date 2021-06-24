/**
 * @file   reader_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines class ReaderBase.
 */

#ifndef TILEDB_READER_BASE_H
#define TILEDB_READER_BASE_H

#include "strategy_base.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class StorageManager;
class Subarray;
class QueryCondition;

/** Processes read queries. */
class ReaderBase : public StrategyBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ReaderBase(
      stats::Stats* stats,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition);

  /** Destructor. */
  ~ReaderBase() = default;

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The query condition. */
  QueryCondition& condition_;

  /** The fragment metadata that the reader will focus on. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /** Correctness checks for `subarray_`. */
  Status check_subarray() const;

  /** Correctness checks validity buffer sizes in `buffers_`. */
  Status check_validity_buffer_sizes() const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_BASE_H