/**
 * @file   deletes_and_updates.h
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
 * This file defines class Deletes.
 */

#ifndef TILEDB_DELETES_H
#define TILEDB_DELETES_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/strategy_base.h"
using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Processes delete queries. */
class DeletesAndUpdates : public StrategyBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  DeletesAndUpdates(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition,
      std::vector<UpdateValue>& update_values,
      bool skip_checks_serialization = false);

  /** Destructor. */
  ~DeletesAndUpdates();

  DISABLE_COPY_AND_COPY_ASSIGN(DeletesAndUpdates);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DeletesAndUpdates);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Finalizes the delete. */
  Status finalize();

  /** Delete is never in an imcomplete state. */
  bool incomplete() const {
    return false;
  }

  /** Delete is never in an imcomplete state. */
  QueryStatusDetailsReason status_incomplete_reason() const {
    return QueryStatusDetailsReason::REASON_NONE;
  }

  /** Initialize the memory budget variables. */
  Status initialize_memory_budget();

  /** Performs a delete query using its set members. */
  Status dowork();

  /** Resets the delete object. */
  void reset();

 private:
  /* ********************************* */
  /*         PRIVATE  ATTRIBUTES       */
  /* ********************************* */

  /** The query condition. */
  QueryCondition& condition_;

  /** The update values, owned by the query. */
  std::vector<UpdateValue>& update_values_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DELETES_H
